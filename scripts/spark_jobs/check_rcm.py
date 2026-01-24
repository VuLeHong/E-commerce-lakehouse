import redis
import sys
import json

REDIS_HOST = "localhost"
REDIS_PORT = 6379

def main(user_id: str):
    r = redis.Redis(
        host=REDIS_HOST,
        port=REDIS_PORT,
        decode_responses=True
    )

    key = f"recommend:realtime:{user_id}"
    recs = r.lrange(key, 0, -1)

    if not recs:
        print(f"❌ No recommendations found for user {user_id}")
        return

    print(f"✅ Realtime recommendations for user {user_id}:")
    for i, pid in enumerate(recs, 1):
        print(f"{i}. product_id = {pid}")

if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Usage: python check_recommendations.py <user_id>")
        sys.exit(1)

    main(sys.argv[1])
