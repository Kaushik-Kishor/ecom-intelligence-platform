from confluent_kafka import Producer
import json, time, random
from datetime import datetime

producer = Producer({'bootstrap.servers': 'localhost:9092'})

PRODUCT_IDS = [f"PRD-{i:04d}" for i in range(1, 501)]
WAREHOUSE_LOCATIONS = ['Mumbai-WH1', 'Delhi-WH2', 'Bangalore-WH3', 'Chennai-WH4', 'Hyderabad-WH5']

def generate_inventory_update():
    current_stock = random.randint(0, 500)
    
    return {
        "product_id": random.choice(PRODUCT_IDS),
        "warehouse": random.choice(WAREHOUSE_LOCATIONS),
        "current_stock": current_stock,
        "stock_change": random.randint(-50, 100),
        "reorder_triggered": current_stock < 20,  # flag we'll detect in Spark
        "update_type": random.choice(['sale_deduction', 'restock', 'adjustment', 'return_addition']),
        "timestamp": datetime.utcnow().isoformat()
    }

def delivery_report(err, msg):
    if err:
        print(f'Inventory delivery failed: {err}')

count = 0
print("Inventory producer started...")

while True:
    update = generate_inventory_update()
    
    producer.produce(
        'inventory',
        key=update['product_id'].encode('utf-8'),
        value=json.dumps(update).encode('utf-8'),
        callback=delivery_report
    )
    producer.poll(0)
    count += 1
    
    if count % 10 == 0:
        print(f"[Inventory] Sent {count} events | {update['product_id']} | Stock: {update['current_stock']}")
    
    time.sleep(random.uniform(0.8, 2.0))