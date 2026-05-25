import json
import requests
import os
from pymongo import MongoClient
from bson.objectid import ObjectId
from datetime import datetime

# Lấy cấu hình từ biến môi trường (Pulumi đã giải mã và truyền vào)
MONGO_URI = os.environ.get('MONGO_URI')
HF_TOKEN = os.environ.get('HF_TOKEN')
HF_API_URL = "https://api-inference.huggingface.co/pipeline/feature-extraction/sentence-transformers/all-MiniLM-L6-v2"

# Khởi tạo kết nối DB bên ngoài handler để tái sử dụng connection (tối ưu tốc độ)
client = MongoClient(MONGO_URI)
db = client['horashi-api']
collection = db['movie']

def get_embedding(text):
    headers = {"Authorization": f"Bearer {HF_TOKEN}"}
    payload = {"inputs": [text]}
    response = requests.post(HF_API_URL, headers=headers, json=payload)
    if response.status_code == 200:
        return response.json()[0]
    return None

def lambda_handler(event, context):
    # Duyệt qua các tin nhắn SQS bốc được (BatchSize = 5)
    for record in event['Records']:
        body = json.loads(record['body'])
        title = body['title']
        overview = body['overview']
        
        # 1. Trích xuất tên thể loại (Genre)
        genres_list = json.loads(body['genres'])
        genre_name = genres_list[0]['name'] if len(genres_list) > 0 else "Unknown"
        
        text_to_embed = f"Tên phim: {title}. Nội dung: {overview}"
        
        # 2. Gọi AI tạo Vector
        embedding = get_embedding(text_to_embed)
        
        if not embedding:
            print(f"❌ Bỏ qua phim '{title}' do lỗi gọi API Hugging Face.")
            continue # Bỏ qua để xử lý phim tiếp theo
            
        # 3. Chuẩn bị Document theo đúng cấu trúc Golang Model của bạn
        movie_doc = {
            "_id": ObjectId(),
            "name": title,
            "topic": overview,
            "genre_id": genre_name,
            "movie_url": f"https://via.placeholder.com/500?text={title}",
            "embedding": embedding,
            "created_at": datetime.utcnow(),
            "updated_at": datetime.utcnow()
        }
        
        # 4. Lưu vào MongoDB
        collection.insert_one(movie_doc)
        print(f"✅ Đã lưu thành công phim: {title}")
        
    return {'statusCode': 200}