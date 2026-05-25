import json
import urllib.request
import os
import time
from pymongo import MongoClient
from bson.objectid import ObjectId
from datetime import datetime

MONGO_URI = os.environ.get('MONGO_URI')
HF_TOKEN = os.environ.get('HF_TOKEN')

# DÙNG IP TRỰC TIẾP ĐỂ BỎ QUA DNS CỦA AWS LAMBDA
HF_IP_URL = "https://104.18.23.194/pipeline/feature-extraction/sentence-transformers/all-MiniLM-L6-v2"

client = MongoClient(MONGO_URI)
db = client['horashi-api']
collection = db['movie']

def get_embedding(text):
    payload = json.dumps({"inputs": [text]}).encode('utf-8')
    
    # Bắn request tới IP trực tiếp
    req = urllib.request.Request(HF_IP_URL, data=payload, method='POST')
    req.add_header('Authorization', f'Bearer {HF_TOKEN}')
    req.add_header('Content-Type', 'application/json')
    
    # ⚠️ BẮT BUỘC: Định danh Host header để Cloudflare nhận diện tên miền
    req.add_header('Host', 'api-inference.huggingface.co')
    
    max_retries = 3
    for attempt in range(max_retries):
        try:
            # Thiết lập cấu hình bỏ qua kiểm tra SSL nghiêm ngặt nếu IP đổi chứng chỉ
            # (Thư viện urllib mặc định sẽ kiểm tra trùng khớp tên miền trong chứng chỉ)
            import ssl
            ctx = ssl.create_default_context()
            ctx.check_hostname = False
            ctx.verify_mode = ssl.CERT_NONE
            
            with urllib.request.urlopen(req, timeout=15, context=ctx) as response:
                result = json.loads(response.read().decode('utf-8'))
                return result[0]
                
        except Exception as e:
            print(f"⚠️ Thử lại bằng IP trực tiếp (Lần {attempt+1}/{max_retries}): {e}")
            if attempt < max_retries - 1:
                time.sleep(2)
            else:
                print("❌ Bó tay hoàn toàn kể cả khi gọi bằng IP!")
                
    return None

def lambda_handler(event, context):
    for record in event['Records']:
        body = json.loads(record['body'])
        title = body['title']
        overview = body['overview']
        
        if collection.find_one({"name": title}):
            print(f"⏭️ Phim '{title}' đã có trong DB, tự động bỏ qua.")
            continue
        
        genres_list = json.loads(body.get('genres', '[]'))
        genre_name = genres_list[0]['name'] if len(genres_list) > 0 else "Unknown"
        text_to_embed = f"Tên phim: {title}. Nội dung: {overview}"
        
        embedding = get_embedding(text_to_embed)
        time.sleep(1) # Giữ nhịp độ từ tốn
        
        if not embedding:
            raise Exception(f"❌ Lỗi kết nối máy chủ khi xử lý '{title}'. Yêu cầu SQS retry!")
            
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