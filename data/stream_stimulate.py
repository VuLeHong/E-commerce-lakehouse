import pandas as pd
import random
import json
from pathlib import Path
from datetime import datetime, timedelta

random.seed(42)

# ======================================================
# PATH CONFIG
# ======================================================
BASE_DIR = Path(__file__).resolve().parent
BATCH_DIR = BASE_DIR / "batch"
STREAM_DIR = BASE_DIR / "stream"
STREAM_DIR.mkdir(exist_ok=True)

# ======================================================
# LOAD BATCH DATA
# ======================================================
users_df = pd.read_csv(BATCH_DIR / "users.csv")
products_df = pd.read_csv(BATCH_DIR / "products.csv")
orders_df = pd.read_csv(BATCH_DIR / "orders.csv")
order_items_df = pd.read_csv(BATCH_DIR / "order_items.csv")
reviews_df = pd.read_csv(BATCH_DIR / "reviews.csv")

USER_IDS = users_df["user_id"].tolist()

product_price = dict(zip(products_df["product_id"], products_df["price"]))
product_category = dict(zip(products_df["product_id"], products_df["category_id"]))

# ======================================================
# USER HISTORY FROM BATCH
# ======================================================
user_purchased_products = (
    order_items_df
    .merge(orders_df[["order_id", "user_id"]], on="order_id")
    .groupby("user_id")["product_id"]
    .apply(set)
    .to_dict()
)

user_reviews = (
    reviews_df
    .groupby("user_id", group_keys=False)[["product_id", "rating"]]
    .apply(lambda x: dict(zip(x["product_id"], x["rating"])))
    .to_dict()
)

# Favourite category per user
user_fav_category = {}
for uid, products in user_purchased_products.items():
    cats = [product_category[p] for p in products if p in product_category]
    if cats:
        user_fav_category[uid] = max(set(cats), key=cats.count)

# ======================================================
# PERSONA DERIVED FROM BATCH
# ======================================================
def derive_persona(uid):
    n = len(user_purchased_products.get(uid, []))
    if n > 20:
        return "power"
    elif n > 3:
        return "normal"
    return "casual"

PERSONA_CONF = {
    "power":  {
        "views": (3, 6),
        "p_cart": 0.6,
        "p_purchase_from_view": 0.25,
        "p_purchase_from_cart": 0.7,
        "p_review": 0.4
    },
    "normal": {
        "views": (2, 4),
        "p_cart": 0.4,
        "p_purchase_from_view": 0.15,
        "p_purchase_from_cart": 0.5,
        "p_review": 0.25
    },
    "casual": {
        "views": (1, 3),
        "p_cart": 0.2,
        "p_purchase_from_view": 0.08,
        "p_purchase_from_cart": 0.3,
        "p_review": 0.1
    },
}

# ======================================================
# EVENT BUFFERS
# ======================================================
events = {
    "page_view": [],
    "add_to_cart": [],
    "purchase": [],
    "review": []
}

event_id = 1
start_time = datetime.now()

def next_time(step_days=0, step_seconds=0):
    return (start_time + timedelta(days=step_days, seconds=step_seconds)) \
        .strftime("%Y-%m-%d %H:%M:%S")

# ======================================================
# STREAM GENERATION
# ======================================================
time_cursor = 0
future_reviews = []   # hold delayed reviews

for uid in USER_IDS:
    persona = derive_persona(uid)
    conf = PERSONA_CONF[persona]

    purchased_before = user_purchased_products.get(uid, set())
    reviews_before = user_reviews.get(uid, {})

    # ---------------------------
    # Candidate products
    # ---------------------------
    candidates = []

    # Avoid negatively reviewed products
    for pid, rating in reviews_before.items():
        if rating >= 4:
            candidates.append(pid)

    fav_cat = user_fav_category.get(uid)
    if fav_cat:
        candidates.extend(
            products_df[products_df["category_id"] == fav_cat]
            .sample(min(5, len(products_df)))
            ["product_id"].tolist()
        )

    if not candidates:
        candidates = products_df.sample(5)["product_id"].tolist()

    viewed = random.sample(
        candidates,
        k=min(len(candidates), random.randint(*conf["views"]))
    )

    purchased_now = []

    # ---------------------------
    # Page view & purchase/cart
    # ---------------------------
    for pid in viewed:
        events["page_view"].append({
            "event_id": event_id,
            "timestamp": next_time(step_seconds=time_cursor),
            "user_id": uid,
            "product_id": pid
        })
        event_id += 1
        time_cursor += random.randint(10, 60)

        # Purchase directly from view (impulse buy)
        if random.random() < conf["p_purchase_from_view"]:
            events["purchase"].append({
                "event_id": event_id,
                "timestamp": next_time(step_seconds=time_cursor),
                "user_id": uid,
                "product_id": pid,
                "quantity": 1,
                "price": float(product_price[pid])
            })
            purchased_now.append(pid)
            event_id += 1
            time_cursor += random.randint(30, 120)
            continue

        # Add to cart
        if random.random() < conf["p_cart"]:
            events["add_to_cart"].append({
                "event_id": event_id,
                "timestamp": next_time(step_seconds=time_cursor),
                "user_id": uid,
                "product_id": pid,
                "quantity": 1
            })
            event_id += 1
            time_cursor += random.randint(30, 120)

            # Purchase from cart
            if random.random() < conf["p_purchase_from_cart"]:
                events["purchase"].append({
                    "event_id": event_id,
                    "timestamp": next_time(step_seconds=time_cursor),
                    "user_id": uid,
                    "product_id": pid,
                    "quantity": 1,
                    "price": float(product_price[pid])
                })
                purchased_now.append(pid)
                event_id += 1
                time_cursor += random.randint(30, 120)

    # ---------------------------
    # Schedule delayed reviews
    # ---------------------------
    for pid in purchased_now:
        if random.random() < conf["p_review"]:
            review_delay_days = random.randint(2, 14)   # delivery + usage time
            future_reviews.append({
                "event_id": event_id,
                "timestamp": next_time(step_days=review_delay_days),
                "user_id": uid,
                "product_id": pid,
                "rating": random.choices(
                    [1, 2, 3, 4, 5],
                    weights=[0.05, 0.1, 0.25, 0.35, 0.25]
                )[0],
                "review_text": "Good quality and worth the price."
            })
            event_id += 1

# Add delayed reviews
events["review"].extend(future_reviews)

# ======================================================
# WRITE FILES
# ======================================================
for etype, rows in events.items():
    out = STREAM_DIR / f"{etype}.jsonl"
    with open(out, "w", encoding="utf-8") as f:
        for r in rows:
            f.write(json.dumps(r) + "\n")
    print(f"✅ Generated {len(rows)} {etype} events → {out.name}")

print("🎉 Streaming files generated successfully (realistic behaviour)")
