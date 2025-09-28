#!/usr/bin/env python3
"""
Kafka consumer test để đọc kết quả từ topic recommend.reranked
"""

import json
import os
from kafka import KafkaConsumer

# ==== CONFIG ====
BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:29092")
TOPIC = os.getenv("RERANK_TOPIC", "recommend.reranked")
GROUP_ID = os.getenv("CONSUMER_GROUP", "rerank-test-group")
AUTO_OFFSET = os.getenv("AUTO_OFFSET_RESET", "earliest")  # "earliest" hoặc "latest"
# ================

def main():
    consumer = KafkaConsumer(
        TOPIC,
        bootstrap_servers=BOOTSTRAP_SERVERS.split(","),
        group_id=GROUP_ID,
        auto_offset_reset=AUTO_OFFSET,
        enable_auto_commit=True,
        value_deserializer=lambda v: json.loads(v.decode("utf-8")),
        key_deserializer=lambda k: k.decode("utf-8") if k else None,
    )

    print(f"🚀 Bắt đầu consume từ topic: {TOPIC}")
    try:
        for msg in consumer:
            key = msg.key
            value = msg.value
            print("="*60)
            print(f"User Key     : {key}")
            print(f"Partition    : {msg.partition}, Offset: {msg.offset}")
            print(f"Payload      : {json.dumps(value, indent=2, ensure_ascii=False)}")
    except KeyboardInterrupt:
        print("⏹ Consumer stop (Ctrl+C)")
    finally:
        consumer.close()

if __name__ == "__main__":
    main()
