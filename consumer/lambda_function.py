import json
import os
import time
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from pymongo import MongoClient
from bson.objectid import ObjectId
from datetime import datetime

MONGO_URI = os.environ.get('MONGO_URI')
HF_TOKEN = os.environ.get('HF_TOKEN')
HF_API_URL = "https://api-inference.huggingface.co/pipeline/feature-extraction/sentence-transformers/all-MiniLM-L6-v2"

client = MongoClient(MONGO_URI)
db = client['horashi-api']
collection = db['movie']

# --- VŨ KHÍ TỐI THƯỢNG: SESSION & CONNECTION POOLING ---
# Tạo một session duy nhất, giữ kết nối mạng liên tục (Keep-Alive)
session = requests.Session()

# Cấu hình tự động Retry ở tầng thấp (TCP/HTTP)
# Nếu AWS ngắt mạng hoặc Hugging Face báo lỗi 429 (Too Many Requests), 500, 502, 503, 504... nó sẽ tự động thử lại.
retry_strategy = Retry(
    total=5,  # Thử tối đa 5 lần
    backoff_factor=2,  # Nghỉ 2s, 4s, 8s...
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
        # Sử dụng session tái chế, không tạo kết nối mới, miễn nhiễm lỗi DNS
        response = session.post(HF_API_URL, json=payload, timeout=20)
        response.raise_for_status() # Quăng lỗi nếu status code không phải 200 OK
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
        time.sleep(1) # Nghỉ 1 nhịp để tránh bị Rate Limit
        
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