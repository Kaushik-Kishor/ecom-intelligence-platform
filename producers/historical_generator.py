import pandas as pd
import random
import uuid
from faker import Faker
from datetime import datetime, timedelta
from sqlalchemy import create_engine

fake = Faker()

DB_URL = "postgresql://ecom_user:ecom_pass@localhost:5433/ecom_warehouse"
engine = create_engine(DB_URL)

CATEGORIES = ['Electronics', 'Clothing', 'Home & Kitchen', 'Sports', 'Books', 'Beauty', 'Toys']
PRODUCTS = {
    'Electronics': ['Laptop', 'Phone', 'Headphones', 'Tablet', 'Smartwatch'],
    'Clothing': ['T-Shirt', 'Jeans', 'Jacket', 'Sneakers', 'Dress'],
    'Home & Kitchen': ['Blender', 'Coffee Maker', 'Air Fryer', 'Vacuum', 'Toaster'],
    'Sports': ['Yoga Mat', 'Dumbbells', 'Running Shoes', 'Cycling Helmet', 'Protein Powder'],
    'Books': ['Fiction Novel', 'Data Engineering Book', 'Self Help', 'Biography', 'Cookbook'],
    'Beauty': ['Moisturizer', 'Lipstick', 'Perfume', 'Sunscreen', 'Face Wash'],
    'Toys': ['LEGO Set', 'Board Game', 'Action Figure', 'Puzzle', 'RC Car']
}
PRICE_RANGE = {
    'Electronics': (199, 2999), 'Clothing': (19, 299),
    'Home & Kitchen': (29, 499), 'Sports': (15, 399),
    'Books': (9, 79), 'Beauty': (10, 199), 'Toys': (14, 149)
}

print("Generating 30 days of historical orders...")

orders = []
# Generate data for last 30 days
for day_offset in range(30, 0, -1):
    date = datetime.utcnow() - timedelta(days=day_offset)
    
    # Vary order volume by day — weekends busier, simulate a spike week 2
    base_orders = random.randint(40, 80)
    if date.weekday() >= 5:  # weekend
        base_orders = int(base_orders * 1.4)
    if 10 <= day_offset <= 17:  # simulated promo week
        base_orders = int(base_orders * 1.8)

    for _ in range(base_orders):
        category = random.choice(CATEGORIES)
        product = random.choice(PRODUCTS[category])
        price_min, price_max = PRICE_RANGE[category]
        quantity = random.choices([1,2,3,4,5], weights=[40,25,15,10,10])[0]
        unit_price = round(random.uniform(price_min, price_max), 2)

        # Spread orders throughout the day
        hour   = random.randint(8, 23)
        minute = random.randint(0, 59)
        event_time = date.replace(hour=hour, minute=minute, second=random.randint(0,59))

        orders.append({
            "order_id":       f"ORD-{str(uuid.uuid4())[:8].upper()}",
            "user_id":        f"USR-{random.randint(1, 10000):05d}",
            "product_id":     f"PRD-{random.randint(1, 500):04d}",
            "product_name":   product,
            "category":       category,
            "quantity":       quantity,
            "unit_price":     unit_price,
            "total_price":    round(quantity * unit_price, 2),
            "status":         "placed",
            "payment_method": random.choice(['credit_card','debit_card','upi','wallet','cod']),
            "city":           fake.city(),
            "country":        random.choice(['IN','IN','IN','US','UK','SG']),
            "event_time":     event_time,
            "order_date":     event_time.date(),
            "order_hour":     hour,
            "day_of_week":    event_time.weekday(),
            "is_bulk_order":  False,
            "revenue_tier":   'low' if unit_price < 50 else ('medium' if unit_price < 500 else 'high')
        })

df = pd.DataFrame(orders)
print(f"Generated {len(df)} historical orders across 30 days")


cols_to_drop = ['product_name']
df_fact = df.drop(columns=cols_to_drop)
df_fact.to_sql("fact_orders", engine, schema="analytics", if_exists="append", index=False)
print(f"Loaded into analytics.fact_orders — total rows now:")

from sqlalchemy import text
with engine.connect() as conn:
    result = conn.execute(text("SELECT COUNT(*) FROM analytics.fact_orders"))
    print(f"  fact_orders: {result.fetchone()[0]} rows")

print("\nDone! Refresh Power BI to see 30 days of data.")