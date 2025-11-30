from faker import Faker
import psycopg2
from datetime import datetime, timedelta
import random
import json
import argparse
import os

fake = Faker()

# Parse arguments
parser = argparse.ArgumentParser()
parser.add_argument('--simulate-days', type=int, help='Run X days of simulation for backfill')
args = parser.parse_args()

# Connect to Postgres
conn = psycopg2.connect(
    host=os.getenv("POSTGRES_HOST", "localhost"),  # Fallback to localhost
    port=os.getenv("POSTGRES_PORT", "5432"),
    database=os.getenv("POSTGRES_DB", "analytics_db"),
    user=os.getenv("POSTGRES_USER", "analytics_user"),
    password=os.getenv("POSTGRES_PASSWORD", "analytics_pass")
)
cur = conn.cursor()

# Create tables if not exist
cur.execute("CREATE SCHEMA IF NOT EXISTS raw;")

cur.execute("""
CREATE TABLE IF NOT EXISTS raw.order_status_events (
    status_event_id SERIAL PRIMARY KEY,
    order_id VARCHAR(50),
    status VARCHAR(50),
    timestamp TIMESTAMP,
    tracking_number VARCHAR(100),
    carrier VARCHAR(50),
    notes TEXT
);
""")

cur.execute("""
CREATE TABLE IF NOT EXISTS raw.refund_return_events (
    event_id SERIAL PRIMARY KEY,
    order_id VARCHAR(50),
    event_type VARCHAR(20),
    event_date TIMESTAMP,
    refund_amount DECIMAL(10,2),
    returned_items JSONB,
    reason VARCHAR(100),
    status VARCHAR(20)
);
""")

# Determine simulation mode
if args.simulate_days:
    simulation_mode = True
    simulation_days = args.simulate_days
    print("=" * 50)
    print(f"SIMULATION MODE: {simulation_days} DAYS")
    print("=" * 50)
else:
    simulation_mode = False
    print("=" * 50)
    print("INCREMENTAL MODE")
    print("=" * 50)

def get_latest_status(order_id):
    """Get the most recent status for an order"""
    cur.execute("""
        SELECT status, timestamp 
        FROM raw.order_status_events 
        WHERE order_id = %s 
        ORDER BY timestamp DESC 
        LIMIT 1
    """, (order_id,))
    result = cur.fetchone()
    return result if result else (None, None)

def insert_status(order_id, status, timestamp, tracking=None, carrier=None, notes=None):
    """Insert a new status event"""
    cur.execute("""
        INSERT INTO raw.order_status_events (order_id, status, timestamp, tracking_number, carrier, notes)
        VALUES (%s, %s, %s, %s, %s, %s)
    """, (order_id, status, timestamp, tracking, carrier, notes))

def insert_refund_return(order_id, event_type, event_date, refund_amount, returned_items=None, reason=None):
    """Insert refund/return event"""
    cur.execute("""
        INSERT INTO raw.refund_return_events (order_id, event_type, event_date, refund_amount, returned_items, reason, status)
        VALUES (%s, %s, %s, %s, %s, %s, %s)
    """, (order_id, event_type, event_date, refund_amount, returned_items, reason, 'completed'))

def get_order_total(order_id):
    """Get order total for refunds"""
    cur.execute("SELECT total FROM raw.orders WHERE order_id = %s", (order_id,))
    return float(cur.fetchone()[0])

def get_order_items(order_id):
    """Get order items for returns"""
    cur.execute("""
        SELECT product_id, product_name, product_category, quantity, unit_price 
        FROM raw.order_items 
        WHERE order_id = %s
    """, (order_id,))
    return cur.fetchall()

def process_orders(current_date):
    """Process all orders and advance statuses if ready"""
    
    # Get all orders placed on or before current_date
    cur.execute("""
        with latest_status as (
            select distinct on (order_id)
            order_id
            , status
            , timestamp
            from raw.order_status_events
            where timestamp <= %s
            order by order_id, timestamp desc
        )

        select o.order_id
            , o.order_date
            , coalesce(ls.status, 'new') as current_status
            , ls.timestamp as status_timestamp
        from raw.orders o
        left join latest_status ls
            on o.order_id = ls.order_id
        where o.order_date <= %s
        and not exists (
            select 1
            from raw.order_status_events ose
            where ose.order_id = o.order_id
            and ose.status in ('final', 'refunded')
                )
    """, (current_date, current_date))
    
    orders = cur.fetchall()
    
    placed_count = 0
    processing_count = 0
    shipped_count = 0
    delivered_count = 0
    cancelled_count = 0
    refunded_count = 0
    returned_count = 0
    final_count = 0
    
    for order_id, order_date, current_status, status_timestamp in orders:
        # current_status, status_timestamp = get_latest_status(order_id)
        
        if current_status == 'new':
            insert_status(order_id, 'placed', order_date)
            placed_count += 1
            continue
        
        # Calculate days since last status
        days_elapsed = (current_date - status_timestamp).days
        
        # Status progression logic
        if current_status == 'placed':
            # Advance to processing after 0-1 days
            if days_elapsed >= random.randint(0, 1):
                new_time = status_timestamp + timedelta(days=random.randint(0, 1), hours=random.randint(1, 12))
                insert_status(order_id, 'processing', new_time)
                processing_count += 1
        
        elif current_status == 'processing':
            # 5% chance to cancel
            if random.random() < 0.05:
                # Cancel after 1-4 days
                if days_elapsed >= random.randint(1, 4):
                    new_time = status_timestamp + timedelta(days=random.randint(1, 4), hours=random.randint(1, 8))
                    insert_status(order_id, 'cancelled', new_time, notes=random.choice([
                        'Customer requested cancellation',
                        'Payment issue',
                        'Inventory unavailable'
                    ]))
                    cancelled_count += 1
            else:
                # Advance to shipped after 1-4 days
                if days_elapsed >= random.randint(1, 4):
                    new_time = status_timestamp + timedelta(days=random.randint(1, 4), hours=random.randint(0, 12))
                    tracking = f"1Z{fake.random_number(digits=16)}"
                    carrier = random.choice(['UPS', 'FedEx', 'USPS', 'DHL'])
                    insert_status(order_id, 'shipped', new_time, tracking, carrier)
                    shipped_count += 1
        
        elif current_status == 'cancelled':
            # Refund after 1-3 days
            if days_elapsed >= random.randint(1, 3):
                new_time = status_timestamp + timedelta(days=random.randint(1, 3), hours=random.randint(1, 8))
                insert_status(order_id, 'refunded', new_time, notes='Cancellation refund processed')
                
                # Create refund event
                order_total = get_order_total(order_id)
                insert_refund_return(
                    order_id, 
                    'refund', 
                    new_time, 
                    order_total,
                    None,
                    'customer_cancelled'
                )
                refunded_count += 1
        
        elif current_status == 'shipped':
            # Advance to delivered after 2-5 days
            if days_elapsed >= random.randint(2, 5):
                new_time = status_timestamp + timedelta(days=random.randint(2, 5), hours=random.randint(2, 10))
                
                # Get tracking/carrier from shipped event
                cur.execute("""
                    SELECT tracking_number, carrier 
                    FROM raw.order_status_events 
                    WHERE order_id = %s AND status = 'shipped'
                """, (order_id,))
                tracking, carrier = cur.fetchone()
                
                insert_status(order_id, 'delivered', new_time, tracking, carrier, 
                             random.choice(['Left at front door', 'Handed to resident', 'Signed by recipient']))
                delivered_count += 1
        
        elif current_status == 'delivered':
            # 10% chance to return
            if random.random() < 0.10 and days_elapsed >= 2:
                # Return after 2-30 days
                if days_elapsed >= random.randint(2, 30):
                    new_time = status_timestamp + timedelta(days=random.randint(2, 30), hours=random.randint(1, 12))
                    insert_status(order_id, 'returned', new_time, notes='Customer initiated return')
                    returned_count += 1
            else:
                # Mark as final after 14 days (if no return)
                if days_elapsed >= 14:
                    new_time = status_timestamp + timedelta(days=14)
                    insert_status(order_id, 'final', new_time)
                    final_count += 1
        
        elif current_status == 'returned':
            # Refund after 1-3 days
            if days_elapsed >= random.randint(1, 3):
                new_time = status_timestamp + timedelta(days=random.randint(1, 3), hours=random.randint(1, 8))
                insert_status(order_id, 'refunded', new_time, notes='Return refund processed')
                
                # Create return event with items
                order_items = get_order_items(order_id)
                items_to_return = random.sample(order_items, k=random.randint(1, min(3, len(order_items))))
                
                returned_items = []
                refund_total = 0
                
                for product_id, product_name, product_category, quantity, unit_price in items_to_return:
                    item_refund = float(unit_price) * quantity
                    refund_total += item_refund
                    
                    returned_items.append({
                        'product_id': product_id,
                        'product_name': product_name,
                        'product_category': product_category,
                        'quantity': quantity,
                        'unit_price': float(unit_price),
                        'refund_amount': item_refund
                    })
                
                insert_refund_return(
                    order_id,
                    'return',
                    new_time,
                    refund_total,
                    json.dumps(returned_items),
                    random.choice(['defective', 'wrong_item', 'changed_mind', 'size_issue'])
                )
                refunded_count += 1
    
    return {
        'placed': placed_count,
        'processing': processing_count,
        'shipped': shipped_count,
        'delivered': delivered_count,
        'cancelled': cancelled_count,
        'returned': returned_count,
        'refunded': refunded_count,
        'final': final_count
    }

# Main execution
if simulation_mode:
    # Simulation mode: Run multiple days
    print(f"Running {simulation_days} day simulation...\n")
    
    # Check if we have existing status events
    cur.execute("SELECT MAX(timestamp) FROM raw.order_status_events")
    latest_event = cur.fetchone()[0]
    
    if latest_event:
        # Resume from day after latest event
        start_date = latest_event.date() + timedelta(days=1)
        print(f"Resuming from: {start_date}")
    else:
        # No events yet - start from earliest order
        cur.execute("SELECT MIN(order_date) FROM raw.orders")
        start_date = cur.fetchone()[0]
        print(f"Starting fresh from: {start_date}")
    
    for day in range(simulation_days):
        current_date = datetime.combine(start_date + timedelta(days=day), datetime.min.time())
        print(f"Day {day + 1}/{simulation_days} ({current_date.date()})...")
        
        counts = process_orders(current_date)
        
        if (day + 1) % 10 == 0:
            conn.commit()
            print(f"  Status changes: {sum(counts.values())}")
    
    conn.commit()
    print("\n✅ Simulation complete!")

else:
    # Incremental mode: Process once with current date
    current_date = datetime.now()
    print(f"Processing orders as of {current_date.date()}...\n")
    
    counts = process_orders(current_date)
    conn.commit()
    
    print("\n✅ Incremental update complete!")
    print("\nStatus changes:")
    for status, count in counts.items():
        if count > 0:
            print(f"  {status}: {count}")

# Final summary
print("\n" + "=" * 50)
print("DATABASE SUMMARY")
print("=" * 50)

cur.execute("SELECT COUNT(*) FROM raw.order_status_events")
print(f"Total status events: {cur.fetchone()[0]}")

cur.execute("""
    SELECT status, COUNT(*) 
    FROM raw.order_status_events 
    GROUP BY status 
    ORDER BY COUNT(*) DESC
""")
print("\nStatus distribution:")
for status, count in cur.fetchall():
    print(f"  {status}: {count}")

cur.execute("SELECT COUNT(*) FROM raw.refund_return_events")
refund_count = cur.fetchone()[0]
if refund_count > 0:
    print(f"\nTotal refund/return events: {refund_count}")
    
    cur.execute("SELECT event_type, COUNT(*) FROM raw.refund_return_events GROUP BY event_type")
    for event_type, count in cur.fetchall():
        print(f"  {event_type}: {count}")

cur.close()
conn.close()