import json
import urllib.request
import os
import time
from pymongo import MongoClient
from bson.objectid import ObjectId
from datetime import datetime

MONGO_URI = os.environ.get('MONGO_URI')
HF_TOKEN = os.environ.get('HF_TOKEN')
HF_API_URL = "https://api-inference.huggingface.co/pipeline/feature-extraction/sentence-transformers/all-MiniLM-L6-v2"

# Khởi tạo kết nối DB
client = MongoClient(MONGO_URI)
db = client['horashi-api']
collection = db['movie']

def get_embedding(text):
    payload = json.dumps({"inputs": [text]}).encode('utf-8')
    req = urllib.request.Request(HF_API_URL, data=payload, method='POST')
    req.add_header('Authorization', f'Bearer {HF_TOKEN}')
    req.add_header('Content-Type', 'application/json')
    
    # Kỹ thuật Exponential Backoff: Thử tối đa 5 lần
    max_retries = 5
    for attempt in range(max_retries):
        try:
            with urllib.request.urlopen(req, timeout=15) as response:
                result = json.loads(response.read().decode('utf-8'))
                return result[0] # Thành công thì trả về Vector ngay lập tức
                
        except Exception as e:
            print(f"Lỗi mạng cục bộ AWS (Lần {attempt+1}/{max_retries}): {e}")
            if attempt < max_retries - 1:
                # Ngủ với thời gian tăng dần: 1s, 2s, 4s, 8s...
                sleep_time = 2 ** attempt 
                print(f"⏳ Nghỉ {sleep_time} giây để DNS AWS phục hồi...")
                time.sleep(sleep_time)
            else:
                print(" Đã thử 5 lần vẫn thất bại. Chấp nhận bó tay để SQS gửi lại sau!")
                
    return None

def lambda_handler(event, context):
    for record in event['Records']:
        body = json.loads(record['body'])
        title = body['title']
        overview = body['overview']
        
        # 1. KIỂM TRA CHỐNG TRÙNG LẶP (IDEMPOTENT)
        # Nếu DB đã có phim này (do các lần chạy trước) thì bỏ qua luôn, không gọi AI nữa
        if collection.find_one({"name": title}):
            print(f"⏭️ Phim '{title}' đã có trong DB, tự động bỏ qua.")
            continue
        
        genres_list = json.loads(body.get('genres', '[]'))
        genre_name = genres_list[0]['name'] if len(genres_list) > 0 else "Unknown"
        text_to_embed = f"Tên phim: {title}. Nội dung: {overview}"
        
        # 2. GỌI AI VÀ NGHỈ NGƠI
        embedding = get_embedding(text_to_embed)
        time.sleep(1.5) # Cố tình ngủ 1.5 giây để đánh lừa bộ chống spam của Hugging Face
        
        if not embedding:
            # 3. QUAN TRỌNG NHẤT: BÁO LỖI ĐỂ SQS THỬ LẠI
            # Lệnh raise này sẽ làm sập Lambda hiện tại, thông báo cho SQS biết 
            # "Tôi chưa làm xong, đừng xóa phim này, lát gửi lại nhé!"
            raise Exception(f"Mạng nghẽn/HF chặn IP khi xử lý '{title}'. Yêu cầu SQS retry!")
            
        # 4. LƯU DATABASE
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