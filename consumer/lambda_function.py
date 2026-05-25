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
# VŨ KHÍ TỐI THƯỢNG: MONKEY PATCHING DNS
# Ép Python phân giải tên miền HF ra IP Cloudflare cố định, bỏ qua DNS của AWS
prv_getaddrinfo = socket.getaddrinfo

def new_getaddrinfo(*args, **kwargs):
    if args[0] == 'api-inference.huggingface.co':
        # Trả về cấu trúc fake socket trỏ thẳng tới IP 104.18.23.194
        return [(socket.AF_INET, socket.SOCK_STREAM, 6, '', ('104.18.23.194', args[1]))]
    return prv_getaddrinfo(*args, **kwargs)

socket.getaddrinfo = new_getaddrinfo
# ==========================================

MONGO_URI = os.environ.get('MONGO_URI')
HF_TOKEN = os.environ.get('HF_TOKEN')
HF_API_URL = "https://api-inference.huggingface.co/pipeline/feature-extraction/sentence-transformers/all-MiniLM-L6-v2"

client = MongoClient(MONGO_URI)
db = client['horashi-api']
collection = db['movie']

# Cấu hình Session giữ kết nối liên tục
session = requests.Session()
retry_strategy = Retry(
    total=3,
    backoff_factor=2,
    status_forcelist=[429, 500, 502, 503, 504],
    allowed_methods=["POST"]
)
adapter = HTTPAdapter(max_retries=retry_strategy, pool_connections=10, pool_maxsize=10)
session.mount("https://", adapter)
session.headers.update({
    'Authorization': f'Bearer {HF_TOKEN}',
    'Content-Type': 'application/json'
})

def get_embedding(text):
    payload = {"inputs": [text]}
    try:
        # Nhờ có Monkey Patch ở trên, dòng này sẽ chạy mượt mà không bị lỗi Errno -5
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
        time.sleep(1) # Nghỉ 1 nhịp để tránh bị Hugging Face Rate Limit
        
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