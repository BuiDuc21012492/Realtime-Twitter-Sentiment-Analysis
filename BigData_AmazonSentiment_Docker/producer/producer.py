# producer/producer.py (đã sửa)
from kafka import KafkaProducer
import pandas as pd
import json
import time

# Retry logic khi kết nối producer
max_retries = 5
retry_delay = 5
producer = None
for attempt in range(max_retries):
    try:
        producer = KafkaProducer(
            bootstrap_servers='kafka:29092',
            value_serializer=lambda m: json.dumps(m).encode('utf-8'),
            api_version=(2, 7) # Thêm phiên bản API để ổn định
        )
        print("Producer connected successfully.")
        break
    except Exception as e:
        print(f"Attempt {attempt + 1} to connect producer failed: {e}")
        if attempt < max_retries - 1:
            time.sleep(retry_delay)
        else:
            raise

try:
    # Đảm bảo file CSV nằm trong thư mục producer
    df = pd.read_csv('Tweets.csv') 
    print(f"Reading {len(df)} rows from Tweets.csv")
    for _, row in df.iterrows():
        data = {
            'text': str(row['text']),
            'ground_truth_sentiment': str(row['airline_sentiment']),
            'timestamp': int(time.time()) # Dùng timestamp hiện tại cho đơn giản
        }
        producer.send('amazon_reviews', value=data)
        print(f"Sent: {data['text'][:50]}...") # In ra để theo dõi
    
    print("All data sent to Kafka.")

except FileNotFoundError:
    print("Error: Tweets.csv not found. Make sure it's in the 'producer' directory.")
except Exception as e:
    print(f"An error occurred in the producer: {e}")
finally:
    if producer:
        producer.flush() # Gửi nốt những message còn lại trong buffer
        producer.close()
        print("Producer closed.")