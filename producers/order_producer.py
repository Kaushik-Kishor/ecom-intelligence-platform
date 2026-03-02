from confluent_kafka import Producer
from faker import Faker
import json, time, random, uuid
from datetime import datetime

fake = Faker()
producer = Producer({'bootstrap.servers': 'localhost:9092'})

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
    'Electronics': (199, 2999),
    'Clothing': (19, 299),
    'Home & Kitchen': (29, 499),
    'Sports': (15, 399),
    'Books': (9, 79),
    'Beauty': (10, 199),
    'Toys': (14, 149)
}

def generate_order():
    category = random.choice(CATEGORIES)
    product = random.choice(PRODUCTS[category])
    price_min, price_max = PRICE_RANGE[category]
    
    # Simulate occasional demand spikes (anomaly we'll detect later)
    quantity = random.choices([1, 2, 3, 4, 5, random.randint(20, 50)], 
                               weights=[40, 25, 15, 10, 7, 3])[0]
    
    return {
        "order_id": f"ORD-{str(uuid.uuid4())[:8].upper()}",
        "user_id": f"USR-{random.randint(1, 10000):05d}",
        "product_id": f"PRD-{random.randint(1, 500):04d}",
        "product_name": product,
        "category": category,
        "quantity": quantity,
        "unit_price": round(random.uniform(price_min, price_max), 2),
        "total_price": 0,  # calculated below
        "status": random.choices(
            ['placed', 'placed', 'placed', 'failed', 'pending'],
            weights=[70, 10, 10, 5, 5])[0],
        "payment_method": random.choice(['credit_card', 'debit_card', 'upi', 'wallet', 'cod']),
        "city": fake.city(),
        "country": random.choice(['IN', 'IN', 'IN', 'US', 'UK', 'SG']),
        "timestamp": datetime.utcnow().isoformat()
    }

def delivery_report(err, msg):
    if err:
        print(f'Order delivery failed: {err}')

count = 0   
print("Order producer started...")

while True:
    order = generate_order()
    order['total_price'] = round(order['quantity'] * order['unit_price'], 2)
    
    producer.produce(
        'orders',
        key=order['order_id'].encode('utf-8'),
        value=json.dumps(order).encode('utf-8'),
        callback=delivery_report
    )
    producer.poll(0)
    count += 1
    
    if count % 10 == 0:
        print(f"[Orders] Sent {count} events | Latest: {order['order_id']} | {order['category']} | ${order['total_price']}")
    
    time.sleep(random.uniform(0.5, 1.5))  # ~1 order/sec