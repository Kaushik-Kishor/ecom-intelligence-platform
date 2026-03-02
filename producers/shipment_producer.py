from confluent_kafka import Producer
import json, time, random, uuid
from datetime import datetime, timedelta

producer = Producer({'bootstrap.servers': 'localhost:9092'})

CARRIERS = ['BlueDart', 'Delhivery', 'FedEx', 'Amazon Logistics', 'DTDC']
STATUSES = ['dispatched', 'in_transit', 'out_for_delivery', 'delivered', 'failed_delivery', 'returned']
STATUS_WEIGHTS = [20, 35, 20, 15, 7, 3]

def generate_shipment():
    expected_delivery = datetime.utcnow() + timedelta(days=random.randint(1, 7))
    
    # Simulate SLA breaches — actual delivery sometimes later than expected
    actual_days_offset = random.choices(
        [-1, 0, 1, 2, 3, 7],
        weights=[10, 40, 20, 15, 10, 5])[0]
    
    actual_delivery = expected_delivery + timedelta(days=actual_days_offset)
    sla_breached = actual_days_offset > 0
    
    return {
        "shipment_id": f"SHP-{str(uuid.uuid4())[:8].upper()}",
        "order_id": f"ORD-{str(uuid.uuid4())[:8].upper()}",
        "carrier": random.choice(CARRIERS),
        "status": random.choices(STATUSES, weights=STATUS_WEIGHTS)[0],
        "origin_city": random.choice(['Mumbai', 'Delhi', 'Bangalore', 'Chennai', 'Hyderabad']),
        "destination_city": random.choice(['Pune', 'Kolkata', 'Ahmedabad', 'Jaipur', 'Lucknow']),
        "expected_delivery": expected_delivery.isoformat(),
        "actual_delivery": actual_delivery.isoformat(),
        "sla_breached": sla_breached,
        "delay_days": max(0, actual_days_offset),
        "timestamp": datetime.utcnow().isoformat()
    }

def delivery_report(err, msg):
    if err:
        print(f'Shipment delivery failed: {err}')

count = 0
print("Shipment producer started...")

while True:
    shipment = generate_shipment()
    
    producer.produce(
        'shipments',
        key=shipment['shipment_id'].encode('utf-8'),
        value=json.dumps(shipment).encode('utf-8'),
        callback=delivery_report
    )
    producer.poll(0)
    count += 1
    
    if count % 10 == 0:
        print(f"[Shipments] Sent {count} events | {shipment['shipment_id']} | {shipment['status']} | SLA breached: {shipment['sla_breached']}")
    
    time.sleep(random.uniform(1.0, 2.5))