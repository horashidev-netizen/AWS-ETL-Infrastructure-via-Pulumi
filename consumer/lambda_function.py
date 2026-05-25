import json
import urllib.request
import os
import time
from pymongo import MongoClient
from bson.objectid import ObjectId
from datetime import datetime

# Lấy cấu hình từ biến môi trường
MONGO_URI = os.environ.get('MONGO_URI')
HF_TOKEN = os.environ.get('HF_TOKEN')
HF_API_URL = "https://api-inference.huggingface.co/pipeline/feature-extraction/sentence-transformers/all-MiniLM-L6-v2"

# Khởi tạo kết nối DB 
client = MongoClient(MONGO_URI)
db = client['horashi-api']
collection = db['movie']

def get_embedding(text):
    # Đóng gói dữ liệu chuẩn bị gửi đi
    payload = json.dumps({"inputs": [text]}).encode('utf-8')
    
    # Sử dụng thư viện chuẩn của Python thay vì 'requests'
    req = urllib.request.Request(HF_API_URL, data=payload, method='POST')
    req.add_header('Authorization', f'Bearer {HF_TOKEN}')
    req.add_header('Content-Type', 'application/json')
    
    try:
        # Gọi API với timeout 10 giây
        with urllib.request.urlopen(req, timeout=10) as response:
            result = json.loads(response.read().decode('utf-8'))
            return result[0]
    except Exception as e:
        print(f"Lỗi phân giải/gọi AI: {e}")
        return None

def lambda_handler(event, context):
    for record in event['Records']:
        body = json.loads(record['body'])
        title = body['title']
        overview = body['overview']
        
        genres_list = json.loads(body.get('genres', '[]'))
        genre_name = genres_list[0]['name'] if len(genres_list) > 0 else "Unknown"
        
        text_to_embed = f"Tên phim: {title}. Nội dung: {overview}"
        
        # Gọi AI tạo Vector
        embedding = get_embedding(text_to_embed)
        
        # 2. NGỦ 1 GIÂY ĐỂ TRÁNH BỊ HUGGING FACE KHÓA IP
        time.sleep(1) 
        
        if not embedding:
            print(f"❌ Bỏ qua phim '{title}' do lỗi gọi API.")
            continue  
        # Lưu vào MongoDB
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
        
        collection.insert_one(movie_doc)
        print(f"✅ Đã lưu thành công phim: {title}")
        
    return {'statusCode': 200}