from faker import Faker
import psycopg2
import psycopg2.extras
from datetime import datetime, timedelta
import random
import os
import uuid

fake = Faker()

# Configurations

# PRODUCTS = ['Product A', 'Product B', 'Product C']
NEW_USER_PERCENTAGE = 0.2 # Weighted for 20% new users in incremental mode
POWER_USERS_PERCENTAGE = 0.2 # Weighted for 20% of events from top 20% of users


# Connect to Postgres

conn = psycopg2.connect(
    host=os.getenv("POSTGRES_HOST", "localhost"),  # Fallback to localhost
    port=os.getenv("POSTGRES_PORT", "5432"),
    database=os.getenv("POSTGRES_DB", "analytics_db"),
    user=os.getenv("POSTGRES_USER", "analytics_user"),
    password=os.getenv("POSTGRES_PASSWORD", "analytics_pass")
)

cur = conn.cursor()

# Create raw schema and table

cur.execute('CREATE SCHEMA IF NOT EXISTS raw;')

cur.execute("""
    CREATE TABLE IF NOT EXISTS raw.login_events (
        event_id SERIAL PRIMARY KEY,
        timestamp TIMESTAMP,
        user_id VARCHAR(12),
        session_id VARCHAR(50),
        status VARCHAR(20),
        ip_address VARCHAR(45),
        parameters JSONB
    );
""")

# Check if this is initialization or incremental load
cur.execute("SELECT DISTINCT user_id FROM raw.login_events")
existing_users = [row[0] for row in cur.fetchall()]

if len(existing_users) == 0:
    # Initial load -- First run
    print("=" * 50)
    print("INITIL LOAD MODE")
    print("=" * 50)
    mode = 'initial'
    NUM_EVENTS = 400000
    DAYS_BACK = 60
    USER_POOL_SIZE = random.randint(25000, 45000)

    # Generating fresh user pool
    USER_POOL = [str(random.randint(100000000000, 999999999999)) for _ in range(USER_POOL_SIZE)]

    # Date range
    start_date = f'-{DAYS_BACK}d'
    end_date = 'now'


    print(f"Generating {NUM_EVENTS} login events across {DAYS_BACK} days")
    print(f"User pool: {USER_POOL_SIZE} users")
else:
    print("=" * 50)
    print("INCREMENTAL LOAD MODE")
    print("=" * 50)
    mode = 'incremental'
    NUM_EVENTS = random.randint(5000, 15000)
    num_new_users = int(len(existing_users) * NEW_USER_PERCENTAGE)
    new_users = [str(random.randint(100000000000, 999999999999)) for _ in range(num_new_users)]
    USER_POOL = existing_users + new_users

    # Date range
    start_date = 'now'
    end_date = 'now'


    print(f"Generating {NUM_EVENTS} login events for today")
    print(f"Existing Users: {len(existing_users)}")
    print(f"New Users: {len(new_users)}")
    print(f"Total User pool: {len(USER_POOL)} users")

print("-" * 50)

power_user_cutoff = int(len(USER_POOL) * 0.2)
power_users = USER_POOL[:power_user_cutoff]

for i in range(NUM_EVENTS):
    # Select users (weighted towards power users)
    if random.random() < POWER_USERS_PERCENTAGE:
        user_id = random.choice(power_users)
    else:
        user_id = random.choice(USER_POOL)

    # Generate random timestamp within data range
    timestamp = fake.date_time_between(
        start_date = start_date,
        end_date = end_date
    )

    # product_name = random.choice(PRODUCTS)

    status = random.choices(['success', 'failed'], weights=[0.8, 0.2])[0]

    session_id = str(uuid.uuid4()) if status == 'success' else None

    device_type = random.choice(['mobile', 'broswer'])

    # OS based on device tyoe
    if device_type == 'mobile':
        os = random.choice(['ios', 'android'])
    else:
        os = random.choice(['pc', 'mac', 'linux'])

    # Build parameters JSON
    parameters = {
        'device_type': device_type,
        'os': os,
        'mac_address': fake.mac_address(),
        'login_method': random.choice(['password', 'sso', 'oauth', 'biometric']),
        'country': fake.country_code(),
        'city': fake.city()
    }

    # Add browser or app_version based on device
    if device_type == 'browser':
        parameters['browser'] = random.choice(['chrome', 'firefox', 'safari', 'edge'])
    else:
        parameters['app_version'] = f"{random.randint(1,3)}.{random.randint(0,9)}.{random.randint(0,20)}"

    # Add failure reason if failed
    if status == 'failed':
        parameters['failure_reason'] = random.choice([
            'invalid_password',
            'account_locked',
            'invalid_username',
            'expired_credentials',
            'network_error'
        ])

    # Insert into database
    cur.execute("""
        INSERT INTO raw.login_events
        (timestamp, user_id, session_id, status, ip_address, parameters)
        VALUES (%s, %s, %s, %s, %s, %s)
    """, (
        timestamp,
        user_id,
        session_id,
        status,
        fake.ipv4(),
        psycopg2.extras.Json(parameters)
    ))

    if (i + 1) % 100 == 0:
        print(f"   Generated {i + 1} events...")

conn.commit()
print("-" * 50)
print(f"✅ Successfully generated {NUM_EVENTS} login events")

# Show summary stats
print("\n" + "=" * 50)
print("DATABASE SUMMARY")
print("=" * 50)

if mode == 'initial':

    cur.execute("SELECT COUNT(*) FROM raw.login_events")
    total_events = cur.fetchone()[0]
    print(f"Total events in database: {total_events}")

    cur.execute("SELECT COUNT(DISTINCT user_id) FROM raw.login_events")
    total_users = cur.fetchone()[0]
    print(f"Total unique users: {total_users}")

    cur.execute("SELECT MIN(date(timestamp)), MAX(date(timestamp)) FROM raw.login_events")
    date_range_result = cur.fetchone()
    print(f"Date range: {date_range_result[0]} to {date_range_result[1]}")

else:
    cur.execute("SELECT COUNT(*) FROM raw.login_events where date(timestamp) = date(now())")
    daily_events = cur.fetchone()[0]
    print(f"Todays total events: {daily_events}")

    cur.execute("SELECT COUNT(DISTINCT user_id) from raw.login_events where date(timestamp) = date(now())")
    daily_unique_users = cur.fetchone()[0]
    print(f"Unique users today: {daily_unique_users}")

cur.close()
conn.close()
