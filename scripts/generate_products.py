import psycopg2
import psycopg2.extras
from faker import Faker
import random

fake = Faker()

# Product categories and realistic products
PRODUCT_CATALOG = {
    "Electronics": [
        ("Wireless Headphones", 79.99),
        ("Bluetooth Speaker", 49.99),
        ("USB-C Cable", 12.99),
        ("Laptop Stand", 34.99),
        ("Wireless Mouse", 29.99),
        ("Mechanical Keyboard", 89.99),
        ("Webcam HD", 69.99),
        ("Phone Case", 19.99),
        ("Power Bank", 39.99),
        ("Screen Protector", 9.99)
    ],
    "Home & Garden": [
        ("Coffee Maker", 59.99),
        ("Desk Lamp", 24.99),
        ("Storage Bins", 15.99),
        ("Picture Frame", 12.99),
        ("Throw Pillow", 18.99),
        ("Plant Pot", 14.99),
        ("Wall Clock", 22.99),
        ("Candle Set", 16.99),
        ("Door Mat", 19.99),
        ("Shower Curtain", 24.99)
    ],
    "Sports & Outdoors": [
        ("Yoga Mat", 29.99),
        ("Water Bottle", 19.99),
        ("Resistance Bands", 24.99),
        ("Gym Bag", 34.99),
        ("Running Socks", 12.99),
        ("Camping Chair", 44.99),
        ("Backpack", 49.99),
        ("Sunglasses", 39.99),
        ("Baseball Cap", 16.99),
        ("Jump Rope", 14.99)
    ],
    "Books": [
        ("Mystery Novel", 14.99),
        ("Cookbook", 24.99),
        ("Self-Help Book", 18.99),
        ("Biography", 19.99),
        ("Science Fiction", 16.99),
        ("Travel Guide", 22.99),
        ("Children's Book", 12.99),
        ("Art Book", 29.99),
        ("History Book", 21.99),
        ("Poetry Collection", 15.99)
    ],
    "Apparel": [
        ("T-Shirt", 19.99),
        ("Hoodie", 39.99),
        ("Jeans", 49.99),
        ("Sneakers", 69.99),
        ("Belt", 24.99),
        ("Scarf", 18.99),
        ("Gloves", 14.99),
        ("Socks Pack", 12.99),
        ("Hat", 16.99),
        ("Jacket", 79.99)
    ]
}

# Connect to Postgres
conn = psycopg2.connect(
    host="localhost",
    port=5432,
    database="analytics_db",
    user="analytics_user",
    password="analytics_pass"
)
cur = conn.cursor()

# Create products table
cur.execute("""
CREATE TABLE IF NOT EXISTS raw.products (
    product_id VARCHAR(20) PRIMARY KEY,
    product_name VARCHAR(200),
    product_category VARCHAR(100),
    product_price DECIMAL(10,2),
    product_brand VARCHAR(100),
    created_at TIMESTAMP DEFAULT NOW()
);
""")

print("Generating products...")

product_count = 0
for category, products in PRODUCT_CATALOG.items():
    for product_name, base_price in products:
        product_count += 1
        product_id = f"PROD_{product_count:04d}"
        
        # Add some price variation
        price = round(base_price * random.uniform(0.9, 1.1), 2)
        
        # Generate a fake brand
        brand = fake.company()
        
        cur.execute("""
            INSERT INTO raw.products (product_id, product_name, product_category, product_price, product_brand)
            VALUES (%s, %s, %s, %s, %s)
        """, (product_id, product_name, category, price, brand))

conn.commit()

print(f"✅ Generated {product_count} products")

# Show sample
cur.execute("SELECT * FROM raw.products LIMIT 5")
print("\nSample products:")
for row in cur.fetchall():
    print(row)

cur.close()
conn.close()