from confluent_kafka import Producer
import json

def do_sent_json_to_kafka(data):
     # Hàm callback xử lý lỗi (nếu có)
    def _delivery_report(err, msg):
        if err is not None:
            print(f"Lỗi gửi tin nhắn: {err}")
        else:
            print(f"Tin nhắn đã được gửi tới topic {msg.topic()} tại phân vùng {msg.partition()}")
            
    # Cấu hình Kafka
    config = {
        'bootstrap.servers': 'localhost:9092',  # Địa chỉ Kafka Broker
        'client.id': 'pyspark-producer'         # ID của client
    }

    # Tạo Kafka Producer
    producer = Producer(config)

    # Gửi JSON tới Kafka
    topic = 'test_input'

    # Serialize JSON thành chuỗi
    json_data = json.dumps(data)

    # Gửi dữ liệu
    producer.produce(topic, key="test_key", value=json_data, callback=_delivery_report)

    # Đảm bảo tất cả dữ liệu được gửi trước khi thoát
    producer.flush()
