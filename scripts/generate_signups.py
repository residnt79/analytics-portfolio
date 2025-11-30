from faker import Faker
import psycopg2
from datetime import timedelta
import os

fake = Faker()

conn = psycopg2.connect(
    host=os.getenv("POSTGRES_HOST", "localhost"),  # Fallback to localhost
    port=os.getenv("POSTGRES_PORT", "5432"),
    database=os.getenv("POSTGRES_DB", "analytics_db"),
    user=os.getenv("POSTGRES_USER", "analytics_user"),
    password=os.getenv("POSTGRES_PASSWORD", "analytics_pass")
)
cur = conn.cursor()

# Create signup_events table
cur.execute("CREATE SCHEMA IF NOT EXISTS raw;")

cur.execute("""
CREATE TABLE IF NOT EXISTS raw.signup_events (
    signup_id SERIAL PRIMARY KEY,
    user_id VARCHAR(12),
    timestamp TIMESTAMP,
    email VARCHAR(100),
    first_name VARCHAR(50),
    last_name VARCHAR(50),
    address VARCHAR(200),
    city VARCHAR(100),
    state VARCHAR(50),
    postal_code VARCHAR(20),
    country VARCHAR(10),
    signup_method VARCHAR(50)
);
""")

# Find users without signup records (incremental detection)
cur.execute("""
    SELECT l.user_id, MIN(l.timestamp) as first_login
    FROM raw.login_events l
    LEFT JOIN raw.signup_events s ON l.user_id = s.user_id
    WHERE s.user_id IS NULL
    GROUP BY l.user_id
    ORDER BY first_login
""")

new_users = cur.fetchall()

if len(new_users) == 0:
    print("✅ No new users to process. All users have signup records.")
    cur.close()
    conn.close()
    exit()

print("=" * 50)
print(f"GENERATING SIGNUP EVENTS FOR {len(new_users)} NEW USERS")
print("=" * 50)

for i, (user_id, first_login) in enumerate(new_users):
    # Signup happens 1-60 minutes before first login
    signup_time = first_login - timedelta(minutes=fake.random_int(min=1, max=60))
    
    # Generate user details
    first_name = fake.first_name()
    last_name = fake.last_name()
    email = f"{first_name.lower()}.{last_name.lower()}{fake.random_int(min=1, max=999)}@{fake.free_email_domain()}"
    
    signup_method = fake.random_element(['email', 'google', 'facebook', 'apple'])
    
    cur.execute("""
        INSERT INTO raw.signup_events 
        (user_id, timestamp, email, first_name, last_name, address, city, state, postal_code, country, signup_method)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
    """, (
        user_id,
        signup_time,
        email,
        first_name,
        last_name,
        fake.street_address(),
        fake.city(),
        fake.state(),
        fake.postcode(),
        fake.country_code(),
        signup_method
    ))
    
    if (i + 1) % 1000 == 0:
        print(f"  Processed {i + 1} users...")
        conn.commit()

conn.commit()

# Summary
cur.execute("SELECT COUNT(*) FROM raw.signup_events")
total_signups = cur.fetchone()[0]

print("-" * 50)
print(f"✅ Generated {len(new_users)} new signup events")
print(f"Total signups in database: {total_signups}")

cur.close()
conn.close()