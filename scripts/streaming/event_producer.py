#!/usr/bin/env python3
"""
Kafka producers gửi sự kiện random cho 4 event types.
"""

import json, time, random, threading
from pathlib import Path
from kafka import KafkaProducer

EVENT_TYPES = ["page_view", "add_to_cart", "purchase", "review"]

# ==== CONFIG ====
BOOTSTRAP_SERVERS = ["localhost:29092"]
TOPIC_PREFIX = "events"
DATA_DIR = Path(__file__).resolve().parents[2] / "data" / "stream"
MIN_DELAY = 0.2
MAX_DELAY = 10
MAX_EVENTS = 0    # 0 = vô hạn, >0 = số event tối đa mỗi producer
KEY_BY_USER = True
VERBOSE = True
# ================

def load_jsonl(path):
    data = []
    with open(path, "r", encoding="utf-8") as f:
        for line in f:
            if line.strip():
                data.append(json.loads(line))
    return data

def make_producer():
    return KafkaProducer(
        bootstrap_servers=BOOTSTRAP_SERVERS,
        value_serializer=lambda v: json.dumps(v, ensure_ascii=False).encode("utf-8"),
        key_serializer=lambda k: k.encode("utf-8") if k else None,
    )

def producer_loop(event_type, records, producer, topic):
    sent = 0
    if not records:
        print(f"[{event_type}] ❌ Không có dữ liệu.")
        return

    print(f"[{event_type}] ✅ Bắt đầu producer -> {topic}")

    try:
        while True:
            if MAX_EVENTS and sent >= MAX_EVENTS:
                break

            record = random.choice(records).copy()
            now_str = time.strftime("%Y-%m-%d %H:%M:%S")
            record["timestamp"] = now_str
            record["_ingest_time"] = now_str

            key = str(record.get("user_id")) if KEY_BY_USER else None
            producer.send(topic, key=key, value=record)
            sent += 1

            if VERBOSE and sent % 100 == 0:
                print(f"[{event_type}] Đã gửi {sent} sự kiện")

            time.sleep(random.uniform(MIN_DELAY, MAX_DELAY))

    except KeyboardInterrupt:
        print(f"[{event_type}] stop (Ctrl+C)")
    finally:
        producer.flush()
        producer.close()
        print(f"[{event_type}] ⏹ Dừng producer sau {sent} sự kiện")

def main():
    data_dir = Path(DATA_DIR)
    threads = []

    for etype in EVENT_TYPES:
        fpath = data_dir / f"{etype}.jsonl"
        records = load_jsonl(fpath) if fpath.exists() else []

        topic = f"{TOPIC_PREFIX}.{etype}"
        producer = make_producer()

        t = threading.Thread(
            target=producer_loop,
            args=(etype, records, producer, topic),
            daemon=True
        )
        t.start()
        threads.append(t)
        time.sleep(0.2)

    print("🚀 Producers đã chạy. Ctrl+C để dừng.")

    try:
        while any(t.is_alive() for t in threads):
            time.sleep(1)
    except KeyboardInterrupt:
        print("\n🛑 Nhận Ctrl+C – đang dừng producers...")

if __name__ == "__main__":
    main()
