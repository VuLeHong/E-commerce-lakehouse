#!/usr/bin/env python3
"""
Kafka consumer test để xem có nhận được dữ liệu từ producer không.
"""

from kafka import KafkaConsumer
import json

# ==== CONFIG ====
BOOTSTRAP_SERVERS = ["localhost:29092"]
TOPIC_PATTERN = "events.*"   # bắt tất cả topic dạng events.page_view, events.purchase...
GROUP_ID = "test-consumer"
AUTO_OFFSET_RESET = "earliest"   # đọc từ đầu nếu chưa có offset
# ================

def main():
    consumer = KafkaConsumer(
        bootstrap_servers=BOOTSTRAP_SERVERS,
        group_id=GROUP_ID,
        auto_offset_reset=AUTO_OFFSET_RESET,
        value_deserializer=lambda v: json.loads(v.decode("utf-8")),
        key_deserializer=lambda k: k.decode("utf-8") if k else None,
    )

    consumer.subscribe(pattern=TOPIC_PATTERN)
    print(f"📡 Consumer đã subscribe pattern `{TOPIC_PATTERN}`")

    try:
        for message in consumer:
            print(
                f"[{message.topic}] key={message.key}, "
                f"value={message.value}, "
                f"partition={message.partition}, offset={message.offset}"
            )
    except KeyboardInterrupt:
        print("🛑 Dừng consumer...")
    finally:
        consumer.close()

if __name__ == "__main__":
    main()
