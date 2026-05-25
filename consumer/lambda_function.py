import json
import os
import time
import socket
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from pymongo import MongoClient
from bson.objectid import ObjectId
from datetime import datetime

# ==========================================
# VŨ KHÍ TỐI THƯỢNG 1: ÉP PYTHON CHỈ DÙNG IPv4
# Sửa lỗi [Errno -5] do AWS Lambda cố gọi IPv6 nhưng thất bại
old_getaddrinfo = socket.getaddrinfo

def ipv4_getaddrinfo(*args, **kwargs):
    responses = old_getaddrinfo(*args, **kwargs)
    # Lọc bỏ AF_INET6 (IPv6), chỉ giữ lại AF_INET (IPv4)
    return [res for res in responses if res[0] == socket.AF_INET]

socket.getaddrinfo = ipv4_getaddrinfo
# ==========================================

MONGO_URI = os.environ.get('MONGO_URI')
HF_TOKEN = os.environ.get('HF_TOKEN')
HF_API_URL = "https://api-inference.huggingface.co/pipeline/feature-extraction/sentence-transformers/all-MiniLM-L6-v2"

client = MongoClient(MONGO_URI)
db = client['horashi-api']
collection = db['movie']

session = requests.Session()
retry_strategy = Retry(
    total=3,
    backoff_factor=2,
    status_forcelist=[429, 500, 502, 503, 504],
    allowed_methods=["POST"]
)
adapter = HTTPAdapter(max_retries=retry_strategy, pool_connections=10, pool_maxsize=10)
session.mount("https://", adapter)

# VŨ KHÍ TỐI THƯỢNG 2: CẢI TRANG THÀNH TRÌNH DUYỆT BÌNH THƯỜNG
# Bypass hệ thống Anti-Bot chặn SSL của Cloudflare
session.headers.update({
    'Authorization': f'Bearer {HF_TOKEN}',
    'Content-Type': 'application/json',
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36'
})

def get_embedding(text):
    payload = {"inputs": [text]}
    try:
        # Gọi tên miền chuẩn, ép IPv4, giả lập trình duyệt Chrome
        response = session.post(HF_API_URL, json=payload, timeout=20)
        response.raise_for_status()
        result = response.json()
        return result[0]
    except Exception as e:
        print(f"⚠️ Lỗi kết nối API: {e}")
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
        time.sleep(1.5) # Giãn nhịp tránh Hugging Face Rate Limit
        
        if not embedding:
            raise Exception(f"❌ Không thể lấy Vector cho '{title}'. SQS vui lòng Retry!")
            
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
        print(f"✅ Đã lưu thành công: {title}")
        
    return {'statusCode': 200}