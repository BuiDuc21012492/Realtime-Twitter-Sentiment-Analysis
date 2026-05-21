import json
import time
import logging
from kafka import KafkaConsumer, KafkaProducer
from vaderSentiment.vaderSentiment import SentimentIntensityAnalyzer

# --- Cấu hình log để dễ dàng theo dõi ---
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# --- Cấu hình Kafka ---
KAFKA_SERVER = 'kafka:29092'
SOURCE_TOPIC = 'amazon_reviews'
DESTINATION_TOPIC = 'sentiment_topic'

def create_kafka_client(client_type, topic=None):
    """Hàm chung để tạo Kafka client với logic retry, giúp mã nguồn gọn hơn."""
    for attempt in range(5):
        try:
            if client_type == 'consumer':
                client = KafkaConsumer(
                    topic,
                    bootstrap_servers=KAFKA_SERVER,
                    value_deserializer=lambda m: json.loads(m.decode('utf-8')),
                    auto_offset_reset='earliest',
                    api_version=(2, 7)
                )
            elif client_type == 'producer':
                client = KafkaProducer(
                    bootstrap_servers=KAFKA_SERVER,
                    value_serializer=lambda m: json.dumps(m).encode('utf-8'),
                    api_version=(2, 7)
                )
            logging.info(f"Kafka {client_type} connected successfully.")
            return client
        except Exception as e:
            logging.error(f"Attempt {attempt + 1} to connect {client_type} failed: {e}")
            time.sleep(5)
    raise Exception(f"Could not connect to Kafka {client_type} after multiple retries.")

# --- Khởi tạo các client và công cụ phân tích ---
consumer = create_kafka_client('consumer', SOURCE_TOPIC)
producer = create_kafka_client('producer')
analyzer = SentimentIntensityAnalyzer()

logging.info("Sentiment processor is running and waiting for messages...")

try:
    for message in consumer:
        review_data = message.value
        
        # Lấy các trường dữ liệu từ message nhận được
        text = review_data.get('text', '')
        ground_truth = review_data.get('ground_truth_sentiment', 'unknown')

        # Phân tích cảm xúc bằng Vader
        score = analyzer.polarity_scores(text)
        predicted_sentiment = 'neutral'
        if score['compound'] >= 0.05:
            predicted_sentiment = 'positive'
        elif score['compound'] <= -0.05:
            predicted_sentiment = 'negative'

        # Tạo message mới đã được làm giàu thông tin
        processed_data = {
            'text': text,
            'predicted_sentiment': predicted_sentiment,
            'ground_truth_sentiment': ground_truth,
            'timestamp': time.time()
        }

        # Gửi kết quả vào topic mới
        producer.send(DESTINATION_TOPIC, value=processed_data)

except Exception as e:
    logging.error(f"An error occurred in the consumer loop: {e}")
finally:
    if consumer:
        consumer.close()
    if producer:
        producer.close()
    logging.info("Consumer and producer have been closed.")