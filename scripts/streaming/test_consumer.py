from kafka import KafkaConsumer
import json

consumer = KafkaConsumer(
    "recommend.reranked",
    bootstrap_servers="localhost:29092",
    value_deserializer=lambda v: json.loads(v.decode("utf-8")),
    key_deserializer=lambda k: k.decode("utf-8") if k else None,
    auto_offset_reset="latest",
    group_id="debug-recommend"
)

print("🚀 Listening recommend.reranked ...")

for msg in consumer:
    print("=" * 40)
    print(f"user_id: {msg.key}")
    print(json.dumps(msg.value, indent=2))
