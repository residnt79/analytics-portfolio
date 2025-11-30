from faker import Faker
import psycopg2
import psycopg2.extras
from datetime import datetime, timedelta
import random
import os
import uuid

fake = Faker()

# Configuration
PAGE_TYPES = ['home', 'category', 'product', 'cart', 'checkout', 'account']
CONVERSION_RATE = 0.15  # 15% of sessions result in purchase
REVIEW_RATE = 0.30  # 30% of purchases get reviews

# Connect to Postgres
conn = psycopg2.connect(
    host=os.getenv("POSTGRES_HOST", "localhost"),  # Fallback to localhost
    port=os.getenv("POSTGRES_PORT", "5432"),
    database=os.getenv("POSTGRES_DB", "analytics_db"),
    user=os.getenv("POSTGRES_USER", "analytics_user"),
    password=os.getenv("POSTGRES_PASSWORD", "analytics_pass")
)
cur = conn.cursor()

# Create tables
cur.execute("CREATE SCHEMA IF NOT EXISTS raw;")

cur.execute("""
CREATE TABLE IF NOT EXISTS raw.session_events (
    event_id SERIAL PRIMARY KEY,
    timestamp TIMESTAMP,
    user_id VARCHAR(12),
    session_id VARCHAR(50),
    event_type VARCHAR(50),
    parameters JSONB
);
""")

cur.execute("""
CREATE TABLE IF NOT EXISTS raw.orders (
    order_id VARCHAR(50) PRIMARY KEY,
    order_date TIMESTAMP,
    user_id VARCHAR(12),
    session_id VARCHAR(50),
    subtotal DECIMAL(10,2),
    discount_amount DECIMAL(10,2),
    tax DECIMAL(10,2),
    shipping DECIMAL(10,2),
    total DECIMAL(10,2)
);
""")

cur.execute("""
CREATE TABLE IF NOT EXISTS raw.order_items (
    order_item_id SERIAL PRIMARY KEY,
    order_id VARCHAR(50),
    product_id VARCHAR(20),
    product_name VARCHAR(200),
    product_category VARCHAR(100),
    quantity INTEGER,
    unit_price DECIMAL(10,2),
    line_total DECIMAL(10,2)
);
""")

# Get all products for random selection
cur.execute("SELECT product_id, product_name, product_category, product_price FROM raw.products")
PRODUCTS = cur.fetchall()

print(f"Loaded {len(PRODUCTS)} products from catalog")

# Check mode: initial vs incremental
cur.execute("SELECT COUNT(*) FROM raw.session_events")
existing_events = cur.fetchone()[0]

if existing_events == 0:
    mode = "initial"
    print("=" * 50)
    print("INITIAL LOAD MODE")
    print("=" * 50)
    
    # Get all successful logins
    cur.execute("""
        SELECT session_id, user_id, timestamp 
        FROM raw.login_events 
        WHERE status = 'success'
        ORDER BY timestamp
    """)
    
else:
    mode = "incremental"
    print("=" * 50)
    print("INCREMENTAL LOAD MODE")
    print("=" * 50)
    
    # Get today's successful logins only
    cur.execute("""
        SELECT session_id, user_id, timestamp 
        FROM raw.login_events 
        WHERE status = 'success' 
        AND DATE(timestamp) = CURRENT_DATE
        ORDER BY timestamp
    """)

successful_logins = cur.fetchall()
print(f"Generating events for {len(successful_logins)} sessions...")
print("-" * 50)

def insert_event(timestamp, user_id, session_id, event_type, parameters):
    """Helper to insert session event"""
    cur.execute("""
        INSERT INTO raw.session_events (timestamp, user_id, session_id, event_type, parameters)
        VALUES (%s, %s, %s, %s, %s)
    """, (timestamp, user_id, session_id, event_type, psycopg2.extras.Json(parameters)))

def generate_session_events(session_id, user_id, login_time):
    """Generate realistic event sequence for a session"""
    
    current_time = login_time
    cart = []
    viewed_products = []
    
    # Determine session length (number of events)
    session_length = random.choices(
        [random.randint(2, 5), random.randint(5, 15), random.randint(15, 30)],
        weights=[0.5, 0.4, 0.1]  # Most sessions are short
    )[0]
    
    # Will this session convert?
    will_purchase = random.random() < CONVERSION_RATE
    
    for event_num in range(session_length):
        # Add realistic time between events (5 seconds to 3 minutes)
        current_time += timedelta(seconds=random.randint(5, 180))
        
        # Event type logic based on session flow
        if event_num == 0:
            # First event is usually page_view (home)
            insert_event(current_time, user_id, session_id, 'page_view', {
                'page_name': 'home',
                'page_url': '/',
                'referrer_url': ''
            })
        
        elif event_num == 1 and random.random() < 0.4:
            # Sometimes search early
            search_query = random.choice(['headphones', 'laptop', 'shoes', 'book', 'chair', 'coffee'])
            results = [p for p in PRODUCTS if search_query.lower() in p[1].lower()]
            
            insert_event(current_time, user_id, session_id, 'search', {
                'search_query': search_query,
                'results_count': len(results) if results else random.randint(5, 30)
            })
        
        elif len(viewed_products) < 3 or random.random() < 0.3:
            # View a product
            product = random.choice(PRODUCTS)
            viewed_products.append(product)
            
            insert_event(current_time, user_id, session_id, 'product_view', {
                'product_id': product[0],
                'product_name': product[1],
                'product_category': product[2],
                'product_price': float(product[3])
            })
        
        elif len(viewed_products) > 0 and len(cart) < 5 and random.random() < 0.4:
            # Add to cart (from viewed products)
            product = random.choice(viewed_products)
            quantity = random.choices([1, 2, 3], weights=[0.7, 0.2, 0.1])[0]
            
            cart.append({
                'product_id': product[0],
                'product_name': product[1],
                'product_category': product[2],
                'quantity': quantity,
                'price': float(product[3])
            })
            
            insert_event(current_time, user_id, session_id, 'add_to_cart', {
                'product_id': product[0],
                'product_name': product[1],
                'product_price': float(product[3]),
                'quantity': quantity
            })
        
        elif len(cart) > 0 and random.random() < 0.1:
            # Sometimes remove from cart
            item = random.choice(cart)
            cart.remove(item)
            
            insert_event(current_time, user_id, session_id, 'remove_from_cart', {
                'product_id': item['product_id'],
                'product_name': item['product_name'],
                'quantity': item['quantity']
            })
        
        else:
            # Page view (category or other)
            page = random.choice(['category', 'account', 'cart'])
            insert_event(current_time, user_id, session_id, 'page_view', {
                'page_name': page,
                'page_url': f'/{page}',
                'referrer_url': '/home'
            })
    
    # End of session - attempt purchase if intended and cart has items
    if will_purchase and len(cart) > 0:
        current_time += timedelta(seconds=random.randint(10, 60))
        
        # Checkout start
        subtotal = sum(item['price'] * item['quantity'] for item in cart)
        insert_event(current_time, user_id, session_id, 'checkout_start', {
            'cart_total': float(subtotal),
            'items_count': len(cart)
        })
        
        current_time += timedelta(seconds=random.randint(30, 120))
        
        # Create order
        order_id = f"ORDER_{uuid.uuid4().hex[:12].upper()}"
        discount = round(subtotal * random.choice([0, 0, 0, 0.05, 0.10]), 2)  # Occasional discount
        tax = round((subtotal - discount) * 0.08, 2)
        shipping = random.choice([0, 0, 5.00, 7.99])  # Free or paid shipping
        total = round(subtotal - discount + tax + shipping, 2)
        
        # Insert purchase event
        insert_event(current_time, user_id, session_id, 'purchase', {
            'order_id': order_id,
            'order_contents': cart
        })
        
        # Insert into orders table
        cur.execute("""
            INSERT INTO raw.orders 
            (order_id, order_date, user_id, session_id, subtotal, discount_amount, tax, shipping, total)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
        """, (order_id, current_time, user_id, session_id, subtotal, discount, tax, shipping, total))
        
        # Insert order items
        for item in cart:
            line_total = round(item['price'] * item['quantity'], 2)
            cur.execute("""
                INSERT INTO raw.order_items 
                (order_id, product_id, product_name, product_category, quantity, unit_price, line_total)
                VALUES (%s, %s, %s, %s, %s, %s, %s)
            """, (order_id, item['product_id'], item['product_name'], item['product_category'],
                  item['quantity'], item['price'], line_total))
        
        # Maybe submit a review later
        if random.random() < REVIEW_RATE:
            current_time += timedelta(hours=random.randint(1, 72))
            reviewed_product = random.choice(cart)
            
            insert_event(current_time, user_id, session_id, 'review_submit', {
                'product_id': reviewed_product['product_id'],
                'order_id': order_id,
                'rating': random.randint(3, 5),  # Mostly positive reviews
                'review_length': random.randint(50, 300)
            })

# Generate events for all sessions
for i, (session_id, user_id, login_time) in enumerate(successful_logins):
    generate_session_events(session_id, user_id, login_time)
    
    if (i + 1) % 100 == 0:
        print(f"  Processed {i + 1} sessions...")
        conn.commit()  # Commit periodically

conn.commit()

# Summary
print("-" * 50)
print("✅ Session events generated successfully")
print("\n" + "=" * 50)
print("DATABASE SUMMARY")
print("=" * 50)

cur.execute("SELECT COUNT(*) FROM raw.session_events")
print(f"Total session events: {cur.fetchone()[0]}")

cur.execute("SELECT COUNT(*) FROM raw.orders")
print(f"Total orders: {cur.fetchone()[0]}")

cur.execute("SELECT COUNT(*) FROM raw.order_items")
print(f"Total order items: {cur.fetchone()[0]}")

cur.execute("SELECT event_type, COUNT(*) FROM raw.session_events GROUP BY event_type ORDER BY COUNT(*) DESC")
print("\nEvent type distribution:")
for event_type, count in cur.fetchall():
    print(f"  {event_type}: {count}")

cur.close()
conn.close()