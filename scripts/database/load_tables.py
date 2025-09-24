import os
import sys
import psycopg2
from dotenv import load_dotenv

# -------------------
# Load env variables
# -------------------
load_dotenv()

conn = psycopg2.connect(
    host=os.getenv("POSTGRES_HOST"),
    port=os.getenv("POSTGRES_PORT"),
    dbname=os.getenv("POSTGRES_DATABASE"),
    user=os.getenv("POSTGRES_USER"),
    password=os.getenv("POSTGRES_PASSWORD")
)
cur = conn.cursor()

# -------------------
# Create tables
# -------------------
create_tables_sql = """
CREATE TABLE IF NOT EXISTS categories (
    category_id INT PRIMARY KEY,
    category_name VARCHAR(255),
    updated_at TIMESTAMP
);

CREATE TABLE IF NOT EXISTS products (
    product_id INT PRIMARY KEY,
    product_name VARCHAR(255),
    category_id INT REFERENCES categories(category_id),
    brand VARCHAR(255),
    price NUMERIC(10,2),
    updated_at TIMESTAMP
);

CREATE TABLE IF NOT EXISTS users (
    user_id INT PRIMARY KEY,
    first_name VARCHAR(100),
    last_name VARCHAR(100),
    email VARCHAR(255),
    phone_number VARCHAR(50),
    address VARCHAR(255),
    city VARCHAR(100),
    country VARCHAR(100),
    created_at TIMESTAMP
);

CREATE TABLE IF NOT EXISTS orders (
    order_id INT PRIMARY KEY,
    user_id INT REFERENCES users(user_id),
    total_price NUMERIC(10,2),
    order_date TIMESTAMP
);

CREATE TABLE IF NOT EXISTS order_items (
    order_item_id INT PRIMARY KEY,
    order_id INT REFERENCES orders(order_id),
    product_id INT REFERENCES products(product_id),
    quantity INT,
    price NUMERIC(10,2),
    item_total NUMERIC(10,2)
);

CREATE TABLE IF NOT EXISTS reviews (
    review_id INT PRIMARY KEY,
    user_id INT REFERENCES users(user_id),
    product_id INT REFERENCES products(product_id),
    rating INT,
    review_text TEXT,
    review_date TIMESTAMP
);
"""
cur.execute(create_tables_sql)
conn.commit()

print("✅ Tables created.")

# -------------------
# Table → columns mapping
# -------------------
table_columns = {
    "categories": ["category_id", "category_name", "updated_at"],
    "products": ["product_id", "product_name", "category_id", "brand", "price", "updated_at"],
    "users": ["user_id", "first_name", "last_name", "email", "phone_number", "address", "city", "country", "created_at"],
    "orders": ["order_id", "user_id", "total_price", "order_date"],
    "order_items": ["order_item_id", "order_id", "product_id", "quantity", "price", "item_total"],
    "reviews": ["review_id", "user_id", "product_id", "rating", "review_text", "review_date"],
}

# -------------------
# Load CSV into tables
# -------------------

# Base directory: scripts/database
base_dir = os.path.dirname(os.path.abspath(__file__))

# Data directory: ../data/batch
data_dir = os.path.join(base_dir, "..", "..", "data", "batch")

tables_and_files = [
    ("categories", "categories.csv"),
    ("products", "products.csv"),
    ("users", "users.csv"),
    ("orders", "orders.csv"),
    ("order_items", "order_items.csv"),
    ("reviews", "reviews.csv"),
]

for table, filename in tables_and_files:
    file_path = os.path.join(data_dir, filename)
    if not os.path.exists(file_path):
        print(f"❌ File not found: {file_path}")
        sys.exit(1)

    with open(file_path, "r", encoding="utf-8") as f:
        next(f)  # skip header
        cols = ",".join(table_columns[table])
        sql = f"COPY {table}({cols}) FROM STDIN WITH CSV"
        cur.copy_expert(sql, f)
    conn.commit()
    print(f"📥 Loaded {filename} → {table}")

# -------------------
# Done
# -------------------
cur.close()
conn.close()
print("🎉 All CSV files loaded into PostgreSQL.")
