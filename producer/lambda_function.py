import boto3
import csv
import json
import urllib.parse
import os

s3_client = boto3.client('s3')
sqs_client = boto3.client('sqs')

# Lấy URL của Queue từ biến môi trường (Pulumi đã tự động nhét vào đây)
QUEUE_URL = os.environ.get('QUEUE_URL')

def lambda_handler(event, context):
    # Lấy thông tin file vừa upload từ event của S3
    bucket = event['Records'][0]['s3']['bucket']['name']
    key = urllib.parse.unquote_plus(event['Records'][0]['s3']['object']['key'])
    
    # Đọc file CSV từ S3
    response = s3_client.get_object(Bucket=bucket, Key=key)
    lines = response['Body'].read().decode('utf-8').split('\n')
    
    reader = csv.DictReader(lines)
    count = 0
    
    for row in reader:
        title = row.get('original_title', '')
        overview = row.get('overview', '')
        
        # Bỏ qua dòng trống hoặc phim không có nội dung
        if not title or not overview:
            continue
            
        # Đóng gói dữ liệu gửi vào SQS
        message_body = {
            "title": title,
            "overview": overview,
            "genres": row.get('genres', '[]')
        }
        
        sqs_client.send_message(
            QueueUrl=QUEUE_URL,
            MessageBody=json.dumps(message_body)
        )
        count += 1
        
    return {
        'statusCode': 200,
        'body': f'Đã băm nhỏ và ném {count} bộ phim vào SQS thành công!'
    }