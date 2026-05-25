use aws_lambda_events::event::sqs::SqsEvent;
use lambda_runtime::{run, service_fn, Error, LambdaEvent};
use mongodb::{bson::{doc, oid::ObjectId}, Client};
use reqwest::header::{AUTHORIZATION, CONTENT_TYPE};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::{env, time::Duration};

#[derive(Serialize)]
struct MovieDoc {
    #[serde(rename = "_id")]
    id: ObjectId,
    name: String,
    topic: String,
    genre_id: String,
    movie_url: String,
    embedding: Vec<f32>,
    created_at: chrono::DateTime<chrono::Utc>,
    updated_at: chrono::DateTime<chrono::Utc>,
}

async fn function_handler(
    event: LambdaEvent<SqsEvent>,
    mongo_client: &Client,
    http_client: &reqwest::Client,
    hf_token: &str,
) -> Result<(), Error> {
    let db = mongo_client.database("horashi-api");
    let collection = db.collection::<MovieDoc>("movie");

    for record in event.payload.records {
        let body_str = record.body.unwrap_or_default();
        let body: Value = serde_json::from_str(&body_str)?;

        let title = body["title"].as_str().unwrap_or_default().to_string();
        let overview = body["overview"].as_str().unwrap_or_default().to_string();

        // 1. Chống trùng lặp (Idempotent)
        let filter = doc! { "name": &title };
        if collection.find_one(filter, None).await?.is_some() {
            println!("⏭️ Phim '{}' đã có trong DB, bỏ qua.", title);
            continue;
        }

        let genres_str = body["genres"].as_str().unwrap_or("[]");
        let genres: Vec<Value> = serde_json::from_str(genres_str).unwrap_or_default();
        let genre_name = if let Some(g) = genres.get(0) {
            g["name"].as_str().unwrap_or("Unknown").to_string()
        } else {
            "Unknown".to_string()
        };

        let text_to_embed = format!("Tên phim: {}. Nội dung: {}", title, overview);

        // 2. Gọi Hugging Face API bằng reqwest (Rust-native TLS)
        let mut embedding: Option<Vec<f32>> = None;
        let mut retries = 3;

        while retries > 0 {
            let payload = serde_json::json!({ "inputs": [text_to_embed] });
            let res = http_client
                .post("https://api-inference.huggingface.co/pipeline/feature-extraction/sentence-transformers/all-MiniLM-L6-v2")
                .header(AUTHORIZATION, format!("Bearer {}", hf_token))
                .header(CONTENT_TYPE, "application/json")
                .json(&payload)
                .send()
                .await;

            match res {
                Ok(response) if response.status().is_success() => {
                    let vectors: Vec<Vec<f32>> = response.json().await?;
                    embedding = vectors.into_iter().next();
                    break;
                }
                _ => {
                    retries -= 1;
                    tokio::time::sleep(Duration::from_secs(2)).await;
                }
            }
        }

        tokio::time::sleep(Duration::from_secs(1)).await; // Giãn nhịp rate limit

        if let Some(vec) = embedding {
            // 3. Lưu vào MongoDB
            let doc = MovieDoc {
                id: ObjectId::new(),
                name: title.clone(),
                topic: overview,
                genre_id: genre_name,
                movie_url: format!("https://via.placeholder.com/500?text={}", title),
                embedding: vec,
                created_at: chrono::Utc::now(),
                updated_at: chrono::Utc::now(),
            };
            collection.insert_one(doc, None).await?;
            println!("✅ Đã lưu thành công: {}", title);
        } else {
            return Err(format!("❌ Thất bại khi lấy Vector cho '{}'. SQS Retry!", title).into());
        }
    }
    Ok(())
}

#[tokio::main]
async fn main() -> Result<(), Error> {
    let mongo_uri = env::var("MONGO_URI").expect("Thiếu MONGO_URI");
    let hf_token = env::var("HF_TOKEN").expect("Thiếu HF_TOKEN");

    // Khởi tạo Connection Pool (Chỉ chạy 1 lần lúc Lambda Cold Start)
    let mongo_client = Client::with_uri_str(&mongo_uri).await?;
    let http_client = reqwest::Client::builder()
        .timeout(Duration::from_secs(20))
        .build()?;

    // Ép kiểu Shared References để luân chuyển qua các Event an toàn
    let mongo_ref = &mongo_client;
    let http_ref = &http_client;
    let token_ref = &hf_token;

    run(service_fn(move |event| async move {
        function_handler(event, mongo_ref, http_ref, token_ref).await
    }))
    .await
}