import pandas as pd
import random
from faker import Faker

fake = Faker()

# -------------------------
# 1. Categories & Products
# -------------------------
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

categories_table = pd.DataFrame({
    "category_id": range(1, len(categories_list) + 1),
    "category_name": categories_list,
    "updated_at": fake.date_time_between(start_date="-1m", end_date="now")
})

category_to_id = {name: idx + 1 for idx, name in enumerate(categories_list)}

products = []
for i in range(20000):
    category = random.choice(categories_list)
    brand = random.choice(brands[category])
    product_name = f"{brand} {fake.word().capitalize()}"
    price = round(random.uniform(5, 2000), 2)
    updated_at = fake.date_time_between(start_date="-1m", end_date="now")
    products.append([i + 1, product_name, category_to_id[category], brand, price, updated_at])

products_table = pd.DataFrame(products, columns=["product_id", "product_name", "category_id", "brand", "price", "updated_at"])

# -------------------------
# 2. Users
# -------------------------
users = []
for i in range(10000):
    first_name = fake.first_name()
    last_name = fake.last_name()
    email = f"{first_name.lower()}.{last_name.lower()}{i}@example.com"
    phone_number = fake.phone_number()
    address = fake.street_address()
    city = fake.city()
    country = fake.country()
    created_at = fake.date_time_between(start_date="-5y", end_date="now")
    users.append([i + 1, first_name, last_name, email, phone_number, address, city, country, created_at])

users_table = pd.DataFrame(users, columns=[
    "user_id", "first_name", "last_name", "email", "phone_number", "address", "city", "country", "created_at"
])

# -------------------------
# 3. Orders & Order Items
# -------------------------
orders = []
order_items = []
order_id_counter = 1
order_item_id_counter = 1  # NEW counter for order items

for _ in range(10000):  # 10k orders
    user_id = random.randint(1, 10000)
    order_date = fake.date_time_between(start_date="-5y", end_date="now")
    num_products = random.randint(2, 5)
    
    total_price = 0
    for _ in range(num_products):
        product_id = random.randint(1, 20000)
        quantity = random.randint(1, 3)
        price = products_table.loc[product_id - 1, "price"]
        item_total = price * quantity
        total_price += item_total
        
        order_items.append([
            order_item_id_counter,  
            order_id_counter,
            product_id,
            quantity,
            price,
            item_total
        ])
        order_item_id_counter += 1  
    
    orders.append([order_id_counter, user_id, round(total_price, 2), order_date])
    order_id_counter += 1

orders_table = pd.DataFrame(
    orders,
    columns=["order_id", "user_id", "total_price", "order_date"]
)

order_items_table = pd.DataFrame(
    order_items,
    columns=["order_item_id", "order_id", "product_id", "quantity", "price", "item_total"]
)


# -------------------------
# 4. Reviews
# -------------------------
reviews = []
review_id_counter = 1
for _ in range(15000):
    user_id = random.randint(1, 10000)
    product_id = random.randint(1, 20000)
    rating = random.randint(1, 5)
    review_text = fake.sentence(nb_words=15)
    review_date = fake.date_time_between(start_date="-5y", end_date="now")
    reviews.append([review_id_counter, user_id, product_id, rating, review_text, review_date])
    review_id_counter += 1

reviews_table = pd.DataFrame(reviews, columns=["review_id", "user_id", "product_id", "rating", "review_text", "review_date"])

# -------------------------
# Save all
# -------------------------
categories_table.to_csv("categories.csv", index=False)
products_table.to_csv("products.csv", index=False)
users_table.to_csv("users.csv", index=False)
orders_table.to_csv("orders.csv", index=False)
order_items_table.to_csv("order_items.csv", index=False)
reviews_table.to_csv("reviews.csv", index=False)

print("Generated:")
print(f"- categories.csv ({len(categories_table)})")
print(f"- products.csv ({len(products_table)})")
print(f"- users.csv ({len(users_table)})")
print(f"- orders.csv ({len(orders_table)})")
print(f"- order_items.csv ({len(order_items_table)})")
print(f"- reviews.csv ({len(reviews_table)})")
print("Data generation complete.")