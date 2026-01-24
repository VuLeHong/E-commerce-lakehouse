import pandas as pd
import random
from faker import Faker
from datetime import timedelta

fake = Faker()
random.seed(42)

def gen_phone_number():
    prefix = random.choice(["03", "05", "07", "08", "09"])
    return prefix + "".join(str(random.randint(0, 9)) for _ in range(8))

# ======================================================
# 1. CATEGORIES, BRANDS, KEYWORDS
# ======================================================
categories_list = [
    "Electronics", "Books", "Clothing", "Home & Kitchen", "Sports", "Beauty",
    "Toys & Games", "Automotive", "Groceries", "Pet Supplies",
    "Music & Instruments", "Office Supplies", "Garden & Outdoor"
]

brands = {
    "Electronics": ["Sony", "Samsung", "Apple", "Logitech", "Dell", "HP", "Lenovo", "Asus"],
    "Books": ["Penguin", "HarperCollins", "O'Reilly", "Random House", "Simon & Schuster", "Macmillan"],
    "Clothing": ["Nike", "Adidas", "Puma", "Uniqlo", "Zara", "H&M", "Under Armour"],
    "Home & Kitchen": ["Ikea", "Philips", "Tefal", "Dyson", "KitchenAid", "Panasonic"],
    "Sports": ["Wilson", "Spalding", "Yonex", "Decathlon", "Nike", "Adidas"],
    "Beauty": ["Loreal", "Maybelline", "Nivea", "Sephora", "Estee Lauder", "Clinique"],
    "Toys & Games": ["Lego", "Mattel", "Hasbro", "Nerf", "Fisher-Price"],
    "Automotive": ["Bosch", "Michelin", "Goodyear", "Castrol", "Mobil"],
    "Groceries": ["Nestle", "Kellogg's", "Heinz", "PepsiCo", "Unilever"],
    "Pet Supplies": ["Pedigree", "Whiskas", "Purina", "Hill's", "Royal Canin"],
    "Music & Instruments": ["Yamaha", "Fender", "Gibson", "Roland", "Casio"],
    "Office Supplies": ["Staples", "Pilot", "3M", "Bic", "Faber-Castell"],
    "Garden & Outdoor": ["Husqvarna", "John Deere", "Black+Decker", "Gardena"]
}

category_keywords = {
    "Electronics": ["Wireless", "Smart", "Bluetooth", "Portable", "Digital"],
    "Books": ["Guide", "Handbook", "Advanced", "Essentials"],
    "Clothing": ["Classic", "Casual", "Sport", "Premium"],
    "Home & Kitchen": ["Stainless Steel", "Wooden", "Electric"],
    "Sports": ["Professional", "Training", "Outdoor"],
    "Beauty": ["Hydrating", "Natural", "Repair"],
    "Toys & Games": ["Educational", "Interactive", "Creative"],
    "Automotive": ["Heavy Duty", "All-weather", "Performance"],
    "Groceries": ["Organic", "Fresh", "Premium"],
    "Pet Supplies": ["Dry", "Healthy", "Nutritious"],
    "Music & Instruments": ["Acoustic", "Electric", "Professional"],
    "Office Supplies": ["Ergonomic", "Durable", "Compact"],
    "Garden & Outdoor": ["Outdoor", "Weather-resistant", "Heavy-duty"]
}

# ======================================================
# 2. CATEGORIES & PRODUCTS
# ======================================================
categories_df = pd.DataFrame({
    "category_id": range(1, len(categories_list) + 1),
    "category_name": categories_list,
    "updated_at": fake.date_time_between(start_date="-1y", end_date="now")
})
category_id_map = dict(zip(categories_list, categories_df["category_id"]))

NUM_PRODUCTS = 20000
products = []

for pid in range(1, NUM_PRODUCTS + 1):
    category = random.choice(categories_list)
    brand = random.choice(brands[category])
    keyword = random.choice(category_keywords[category])
    suffix = fake.word().capitalize()

    product_name = f"{brand} {keyword} {category.split('&')[0]} {suffix}"
    price = round(random.uniform(5, 2000), 2)

    products.append([
        pid,
        product_name,
        category_id_map[category],
        brand,
        price,
        fake.date_time_between(start_date="-1y", end_date="now")
    ])

products_df = pd.DataFrame(products, columns=[
    "product_id", "product_name", "category_id", "brand", "price", "updated_at"
])

# 🔥 OPTIMIZATION: price lookup map
price_map = dict(zip(products_df["product_id"], products_df["price"]))

# ======================================================
# 3. USERS + PERSONAS
# ======================================================
NUM_USERS = 50000

PERSONA_DIST = {
    "power": 0.15,
    "normal": 0.55,
    "casual": 0.30
}

ORDER_RANGE = {
    "power": (10, 40),
    "normal": (2, 8),
    "casual": (1, 2)
}

REVIEW_PROB = {
    "power": 0.5,
    "normal": 0.25,
    "casual": 0.08
}

users = []
user_persona = {}

for uid in range(1, NUM_USERS + 1):
    persona = random.choices(
        list(PERSONA_DIST.keys()),
        weights=PERSONA_DIST.values()
    )[0]
    user_persona[uid] = persona

    users.append([
        uid,
        fake.first_name(),
        fake.last_name(),
        f"user{uid}@example.com",
        gen_phone_number(),
        fake.street_address(),
        fake.city(),
        fake.country(),
        fake.date_time_between(start_date="-5y", end_date="now")
    ])

users_df = pd.DataFrame(users, columns=[
    "user_id", "first_name", "last_name", "email",
    "phone_number", "address", "city", "country", "created_at"
])

# ======================================================
# 4. ORDERS & ORDER_ITEMS (BASKET LOGIC)
# ======================================================
orders, order_items = [], []
order_id, order_item_id = 1, 1

product_by_category = products_df.groupby("category_id")["product_id"].apply(list).to_dict()
all_categories = list(product_by_category.keys())

def sample_order_date():
    r = random.random()
    if r < 0.5:
        return fake.date_time_between(start_date="-1y", end_date="now")
    elif r < 0.8:
        return fake.date_time_between(start_date="-3y", end_date="-1y")
    else:
        return fake.date_time_between(start_date="-5y", end_date="-3y")

for user_id, persona in user_persona.items():
    num_orders = random.randint(*ORDER_RANGE[persona])
    fav_categories = random.sample(all_categories, k=2)

    for _ in range(num_orders):
        order_date = sample_order_date()
        basket_size = random.randint(1, 4)
        total_price = 0
        used_products = set()

        # Anchor item
        anchor_cat = random.choice(fav_categories)
        anchor_pid = random.choice(product_by_category[anchor_cat])
        used_products.add(anchor_pid)

        price = float(price_map[anchor_pid])
        order_items.append([order_item_id, order_id, anchor_pid, 1, price, price])
        order_item_id += 1
        total_price += price

        # Add-on items
        for _ in range(basket_size - 1):
            cat = random.choice(fav_categories if random.random() < 0.7 else all_categories)
            pid = random.choice(product_by_category[cat])
            if pid in used_products:
                continue

            used_products.add(pid)
            qty = random.choices([1, 2, 3], weights=[0.85, 0.1, 0.05])[0]
            price = float(price_map[pid])
            order_items.append([order_item_id, order_id, pid, qty, price, qty * price])
            order_item_id += 1
            total_price += qty * price

        orders.append([order_id, user_id, round(total_price, 2), order_date])
        order_id += 1

orders_df = pd.DataFrame(orders, columns=["order_id", "user_id", "total_price", "order_date"])
order_items_df = pd.DataFrame(order_items, columns=[
    "order_item_id", "order_id", "product_id", "quantity", "price", "item_total"
])

# ======================================================
# 5. REVIEWS (ONLY AFTER PURCHASE)
# ======================================================
reviews, review_id = [], 1
order_products = order_items_df.groupby("order_id")["product_id"].apply(list)

for _, order in orders_df.iterrows():
    persona = user_persona[order["user_id"]]
    if random.random() > REVIEW_PROB[persona]:
        continue

    for pid in random.sample(
        order_products[order["order_id"]],
        k=random.randint(1, min(2, len(order_products[order["order_id"]])))
    ):
        reviews.append([
            review_id,
            order["user_id"],
            pid,
            random.choices([1, 2, 3, 4, 5], weights=[0.05, 0.1, 0.25, 0.35, 0.25])[0],
            fake.sentence(nb_words=15),
            order["order_date"] + timedelta(days=random.randint(1, 14))
        ])
        review_id += 1

reviews_df = pd.DataFrame(reviews, columns=[
    "review_id", "user_id", "product_id", "rating", "review_text", "review_date"
])

# ======================================================
# 6. SAVE ALL
# ======================================================
categories_df.to_csv("categories.csv", index=False)
products_df.to_csv("products.csv", index=False)
users_df.to_csv("users.csv", index=False)
orders_df.to_csv("orders.csv", index=False)
order_items_df.to_csv("order_items.csv", index=False)
reviews_df.to_csv("reviews.csv", index=False)

print("✅ Batch data generated successfully (FIXED & OPTIMIZED version)")
