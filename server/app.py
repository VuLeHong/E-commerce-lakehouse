from fastapi import FastAPI
import redis

app = FastAPI()
r = redis.Redis(host="redis", port=6379, db=0)

def rerank(offline_recs, views, cart):
    # Simple rule-based rerank
    boosted = list(cart) + list(views) + [
        x for x in offline_recs if x not in cart and x not in views
    ]
    return boosted[:10]  # top 10 recommend

@app.get("/recommend/{user_id}")
def get_recommend(user_id: str):
    # 1. lấy baseline từ offline model đã lưu Redis
    offline_recs = [x.decode() for x in r.lrange(f"recommend:offline:{user_id}", 0, -1)]

    # 2. lấy hành vi realtime Spark đã cache vào Redis
    recent_views = [x.decode() for x in r.lrange(f"user:{user_id}:views", 0, 4)]
    recent_cart  = [x.decode() for x in r.lrange(f"user:{user_id}:cart", 0, 2)]

    # 3. rerank = offline recs + realtime events
    final_recs = rerank(offline_recs, recent_views, recent_cart)

    return {
        "user_id": user_id,
        "recommendations": final_recs,
        "baseline": offline_recs,
        "recent_views": recent_views,
        "recent_cart": recent_cart,
    }
