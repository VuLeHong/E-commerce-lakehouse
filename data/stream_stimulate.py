import pandas as pd
import random
from faker import Faker
from datetime import datetime, timedelta

fake = Faker()

# Config
NUM_EVENTS = 50000
EVENT_TYPES = ["page_view", "add_to_cart", "purchase", "review"]

# Load users & products
users_df = pd.read_csv(r"D:\code\work\BIGDATA\testing-thesis\data\batch\users.csv")
products_df = pd.read_csv(r"D:\code\work\BIGDATA\testing-thesis\data\batch\products.csv")

user_ids = users_df["user_id"].tolist()
product_ids = products_df["product_id"].tolist()

start_time = datetime.now() - timedelta(days=30)

# Dict chứa events theo type
events_dict = {etype: [] for etype in EVENT_TYPES}

for _ in range(NUM_EVENTS):
    event_time = start_time + timedelta(seconds=random.randint(0, 30*24*60*60))
    product_id = random.choice(product_ids)
    event_type = random.choice(EVENT_TYPES)

    # user_id: cho phép anonymous ở page_view
    if event_type == "page_view":
        if random.random() < 0.25:
            user_id = f"anonymous_{random.randint(1000, 9999)}"
        else:
            user_id = random.choice(user_ids)
    else:
        user_id = random.choice(user_ids)

    # build record theo type
    if event_type == "page_view":
        events_dict[event_type].append([
            event_time.strftime("%Y-%m-%d %H:%M:%S"), user_id, product_id
        ])

    elif event_type == "add_to_cart":
        events_dict[event_type].append([
            event_time.strftime("%Y-%m-%d %H:%M:%S"), user_id, product_id,
            random.randint(1, 3)
        ])

    elif event_type == "purchase":
        events_dict[event_type].append([
            event_time.strftime("%Y-%m-%d %H:%M:%S"), user_id, product_id,
            random.randint(1, 3),
            float(products_df.loc[product_id - 1, "price"])
        ])

    elif event_type == "review":
        events_dict[event_type].append([
            event_time.strftime("%Y-%m-%d %H:%M:%S"), user_id, product_id,
            random.randint(1, 5),
            fake.sentence(nb_words=12)
        ])

# Schema riêng cho từng file (event_id sẽ gán sau)
schemas = {
    "page_view": ["event_id", "timestamp", "user_id", "product_id"],
    "add_to_cart": ["event_id", "timestamp", "user_id", "product_id", "quantity"],
    "purchase": ["event_id", "timestamp", "user_id", "product_id", "quantity", "price"],
    "review": ["event_id", "timestamp", "user_id", "product_id", "rating", "review_text"],
}

# Xuất file với event_id local cho từng type
for etype, events in events_dict.items():
    df = pd.DataFrame(events, columns=schemas[etype][1:])  # bỏ event_id
    df.insert(0, "event_id", range(1, len(df) + 1))  # thêm event_id local
    df.to_csv(f"{etype}.csv", index=False)
    df.to_json(f"{etype}.jsonl", orient="records", lines=True)
    print(f"Generated {len(df)} {etype} events -> {etype}.csv / {etype}.jsonl")

print("✅ Done! Each event_type now has its own event_id sequence.")
