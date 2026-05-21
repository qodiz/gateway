import json
import logging
import uuid
import hashlib
from typing import List, Optional, Dict, Any
from datetime import datetime
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
from kafka import KafkaProducer
from kafka.errors import KafkaError
import redis
import uvicorn
import os

# Настройка логирования
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

app = FastAPI(title="User Data Kafka Producer API")

# Модель данных
class UserData(BaseModel):
    created_at: Optional[str] = None
    first_name: Optional[str] = None
    last_name: Optional[str] = None
    email: Optional[str] = None
    phone: Optional[str] = None
    birthday: Optional[str] = None
    sex: Optional[str] = None
    non_processing_features: Optional[Dict[str, Any]] = None
    realtime_features: Optional[Dict[str, Any]] = None
    fs_features: Optional[Dict[str, Any]] = None
    profile_id: Optional[str] = None
    
    class Config:
        extra = "allow"

# Настройка Redis
def get_redis_client():
    """Подключение к Redis"""
    try:
        redis_host = os.getenv('REDIS_HOST', 'localhost')
        redis_port = int(os.getenv('REDIS_PORT', 6379))
        client = redis.Redis(
            host=redis_host,
            port=redis_port,
            db=0,
            decode_responses=True
        )
        client.ping()
        logger.info("Successfully connected to Redis")
        return client
    except redis.ConnectionError as e:
        logger.error(f"Failed to connect to Redis: {e}")
        return None

# Настройка Kafka producer
def get_kafka_producer():
    max_retries = 5
    retry_delay = 3
    
    for attempt in range(max_retries):
        try:
            logger.info(f"Attempting to connect to Kafka (attempt {attempt + 1}/{max_retries})")
            kafka_servers = os.getenv('KAFKA_BOOTSTRAP_SERVERS', '127.0.0.1:9092')
            producer = KafkaProducer(
                bootstrap_servers=[kafka_servers],
                value_serializer=lambda v: json.dumps(v, ensure_ascii=False, default=str).encode('utf-8'),
                key_serializer=lambda k: k.encode('utf-8') if k else None,
                acks='all',
                retries=3,
                api_version_auto_timeout_ms=10000,
                request_timeout_ms=10000
            )
            logger.info("Successfully connected to Kafka")
            return producer
        except Exception as e:
            logger.warning(f"Failed to connect to Kafka (attempt {attempt + 1}): {e}")
            if attempt < max_retries - 1:
                import time
                time.sleep(retry_delay)
            else:
                logger.error("Max retries reached. Could not connect to Kafka")
                raise

# Инициализация клиентов
try:
    producer = get_kafka_producer()
except Exception as e:
    logger.error(f"Failed to initialize Kafka producer: {e}")
    producer = None

try:
    redis_client = get_redis_client()
except Exception as e:
    logger.error(f"Failed to initialize Redis client: {e}")
    redis_client = None

@app.on_event("shutdown")
async def shutdown_event():
    if producer:
        producer.flush()
        producer.close()
        logger.info("Kafka producer closed")
    if redis_client:
        redis_client.close()
        logger.info("Redis client closed")

def get_request_hash(users_data: List[UserData]) -> str:
    """
    Создает хэш запроса для дедупликации
    
    Сортируем пользователей по profile_id для одинакового хэша
    независимо от порядка в запросе
    """
    # Преобразуем в словари и сортируем по profile_id
    users_list = []
    for user_data in users_data:
        user_dict = user_data.model_dump(exclude_none=False)
        users_list.append(user_dict)
    
    # Сортируем по profile_id для консистентности
    sorted_users = sorted(users_list, key=lambda x: (x.get('profile_id') or ''))
    
    # Создаем строку для хэширования
    request_str = json.dumps(sorted_users, sort_keys=True, ensure_ascii=False)
    
    # Создаем MD5 хэш
    return hashlib.md5(request_str.encode('utf-8')).hexdigest()

@app.post("/send-batch")
async def send_user_data_batch(users_data: List[UserData]):
    """
    Отправляет батч пользователей в Kafka
    Поддерживает дедупликацию: одинаковые запросы получают одинаковый batch_id
    """
    if not producer:
        raise HTTPException(status_code=503, detail="Kafka producer not available")
    
    # Вычисляем хэш запроса
    request_hash = get_request_hash(users_data)
    dedup_key = f"dedup:{request_hash}"
    
    # Проверяем, не обрабатывали ли мы уже этот запрос
    if redis_client:
        existing_batch_id = redis_client.get(dedup_key)
        if existing_batch_id:
            logger.info(f"🔄 Duplicate request detected! Returning existing batch_id: {existing_batch_id}")
            
            # Проверяем, существует ли еще батч в Redis
            batch_exists = redis_client.exists(f"batch:{existing_batch_id}")
            if batch_exists:
                return {"batch_id": existing_batch_id, "cached": True}
            else:
                # Батч был удален (истек TTL), создаем новый
                logger.info(f"Previous batch {existing_batch_id} expired, creating new one")
                redis_client.delete(dedup_key)
    
    # Создаем новый батч
    batch_id = str(uuid.uuid4())
    batch_timestamp = datetime.now().isoformat()
    
    # Преобразуем данные в словари
    users_list = []
    for user_data in users_data:
        user_dict = user_data.model_dump(exclude_none=False)
        users_list.append(user_dict)
    
    # Создаем сообщение с batch_id и данными
    kafka_message = {
        "batch_id": batch_id,
        "batch_timestamp": batch_timestamp,
        "data": users_list
    }
    
    try:
        # Сохраняем связку хэш -> batch_id в Redis (TTL 24 часа)
        if redis_client:
            redis_client.setex(dedup_key, 86400, batch_id)  # 24 часа
            logger.info(f"Saved dedup key: {dedup_key} -> {batch_id}")
        
        # Отправка в Kafka
        future = producer.send(
            'user-data-topic',
            value=kafka_message,
            key=batch_id
        )
        
        record_metadata = future.get(timeout=10)
        
        logger.info(f"✅ Batch {batch_id} sent to Kafka: "
                   f"topic={record_metadata.topic}, "
                   f"partition={record_metadata.partition}, "
                   f"offset={record_metadata.offset}, "
                   f"users_count={len(users_list)}, "
                   f"request_hash={request_hash}")
        
        return {"batch_id": batch_id, "cached": False}
    
    except KafkaError as e:
        logger.error(f"Kafka error for batch {batch_id}: {e}")
        # Удаляем dedup ключ в случае ошибки
        if redis_client:
            redis_client.delete(dedup_key)
        raise HTTPException(status_code=500, detail=f"Kafka error: {str(e)}")
    except Exception as e:
        logger.error(f"Unexpected error for batch {batch_id}: {e}")
        if redis_client:
            redis_client.delete(dedup_key)
        raise HTTPException(status_code=500, detail=f"Unexpected error: {str(e)}")

@app.get("/batch/{batch_id}")
async def get_batch(batch_id: str):
    """
    Получает полный батч по batch_id из Redis
    """
    if not redis_client:
        raise HTTPException(status_code=503, detail="Redis not available")
    
    try:
        # Получаем данные из Redis
        batch_data = redis_client.get(f"batch:{batch_id}")
        
        if not batch_data:
            raise HTTPException(status_code=404, detail=f"Batch {batch_id} not found")
        
        # Парсим JSON
        batch_json = json.loads(batch_data)
        
        return batch_json
    
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error getting batch {batch_id}: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/health")
async def health_check():
    """Проверка здоровья сервиса"""
    kafka_status = "connected" if producer else "disconnected"
    redis_status = "connected" if redis_client else "disconnected"
    
    return {
        "status": "healthy",
        "service": "user-data-producer-api",
        "kafka": kafka_status,
        "redis": redis_status
    }

if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=8000)