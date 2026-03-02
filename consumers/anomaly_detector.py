import os
import time
import pandas as pd
import numpy as np
from sqlalchemy import create_engine, text
from sklearn.ensemble import IsolationForest
from datetime import datetime, timedelta

DB_URL = "postgresql://ecom_user:ecom_pass@localhost:5433/ecom_warehouse"
engine = create_engine(DB_URL)

# ── Create alerts table if not exists ─────────────────────────────────────
with engine.connect() as conn:
    conn.execute(text("""
        CREATE TABLE IF NOT EXISTS anomaly_alerts (
            id              SERIAL PRIMARY KEY,
            alert_type      VARCHAR(50),
            severity        VARCHAR(20),
            entity_id       VARCHAR(100),
            metric_name     VARCHAR(100),
            metric_value    NUMERIC(12,2),
            threshold       NUMERIC(12,2),
            description     TEXT,
            detected_at     TIMESTAMP DEFAULT NOW()
        );
    """))
    conn.commit()

print("Anomaly detector started. Running every 60 seconds...")
print("Monitoring: payment fraud, demand spikes, inventory drops\n")

def save_alert(alert_type, severity, entity_id, metric_name, metric_value, threshold, description):
    with engine.connect() as conn:
        conn.execute(text("""
            INSERT INTO anomaly_alerts 
            (alert_type, severity, entity_id, metric_name, metric_value, threshold, description)
            VALUES (:alert_type, :severity, :entity_id, :metric_name, 
                    :metric_value, :threshold, :description)
        """), {
            "alert_type":   alert_type,
            "severity":     severity,
            "entity_id":    entity_id,
            "metric_name":  metric_name,
            "metric_value": float(metric_value),
            "threshold":    float(threshold),
            "description":  description
        })
        conn.commit()

# ══════════════════════════════════════════════════════════════════════════
# DETECTOR 1: Payment fraud — users with abnormal order frequency
# ══════════════════════════════════════════════════════════════════════════
def detect_payment_fraud():
    query = """
        SELECT 
            user_id,
            COUNT(order_id)         as order_count,
            SUM(total_price)        as total_spent,
            AVG(total_price)        as avg_order_value,
            MAX(total_price)        as max_order_value,
            COUNT(DISTINCT payment_method) as payment_methods_used
        FROM analytics.fact_orders
        WHERE event_time >= NOW() - INTERVAL '1 hour'
        GROUP BY user_id
        HAVING COUNT(order_id) >= 2
    """
    
    df = pd.read_sql(query, engine)
    if len(df) < 10:
        print(f"  [Fraud] Not enough data yet ({len(df)} users in last hour), skipping...")
        return 0

    features = df[['order_count', 'total_spent', 'avg_order_value', 'payment_methods_used']].copy()
    
    model = IsolationForest(contamination=0.05, random_state=42)
    df['anomaly_score'] = model.fit_predict(features)
    df['raw_score'] = model.score_samples(features)
    
    anomalies = df[df['anomaly_score'] == -1]
    alerts_saved = 0
    
    for _, row in anomalies.iterrows():
        severity = 'HIGH' if row['order_count'] > 10 else 'MEDIUM'
        description = (
            f"User {row['user_id']} placed {int(row['order_count'])} orders "
            f"totaling ${row['total_spent']:.2f} in the last hour. "
            f"Isolation Forest flagged as anomalous (score: {row['raw_score']:.3f})"
        )
        save_alert(
            alert_type   = 'PAYMENT_FRAUD',
            severity     = severity,
            entity_id    = row['user_id'],
            metric_name  = 'orders_per_hour',
            metric_value = row['order_count'],
            threshold    = df['order_count'].mean(),
            description  = description
        )
        alerts_saved += 1
        print(f"  [FRAUD ALERT] {severity}: {row['user_id']} — {int(row['order_count'])} orders, ${row['total_spent']:.2f}")
    
    return alerts_saved

# ══════════════════════════════════════════════════════════════════════════
# DETECTOR 2: Demand spikes — Z-score on category order volume
# ══════════════════════════════════════════════════════════════════════════
def detect_demand_spikes():
    query = """
        SELECT 
            category,
            DATE_TRUNC('hour', event_time)  as hour_bucket,
            COUNT(order_id)                 as order_count,
            SUM(total_price)                as revenue
        FROM analytics.fact_orders
        WHERE event_time >= NOW() - INTERVAL '7 days'
          AND category IS NOT NULL
        GROUP BY category, DATE_TRUNC('hour', event_time)
        ORDER BY category, hour_bucket
    """
    
    df = pd.read_sql(query, engine)
    if df.empty:
        return 0

    alerts_saved = 0

    for category in df['category'].unique():
        cat_df = df[df['category'] == category].copy()
        if len(cat_df) < 5:
            continue

        mean   = cat_df['order_count'].mean()
        std    = cat_df['order_count'].std()
        
        if std == 0:
            continue

        cat_df['z_score'] = (cat_df['order_count'] - mean) / std
        spikes = cat_df[cat_df['z_score'] > 2.5]

        for _, row in spikes.iterrows():
            description = (
                f"Category '{category}' had {int(row['order_count'])} orders "
                f"in hour {row['hour_bucket']} — "
                f"{row['z_score']:.1f} standard deviations above normal ({mean:.1f} avg). "
                f"Revenue in this hour: ${row['revenue']:.2f}"
            )
            save_alert(
                alert_type   = 'DEMAND_SPIKE',
                severity     = 'HIGH' if row['z_score'] > 3.5 else 'MEDIUM',
                entity_id    = category,
                metric_name  = 'orders_per_hour',
                metric_value = row['order_count'],
                threshold    = mean + 2.5 * std,
                description  = description
            )
            alerts_saved += 1
            print(f"  [DEMAND SPIKE] {category}: {int(row['order_count'])} orders (z={row['z_score']:.1f}σ)")

    return alerts_saved

# ══════════════════════════════════════════════════════════════════════════
# DETECTOR 3: Revenue anomalies using Isolation Forest on gold layer
# ══════════════════════════════════════════════════════════════════════════
def detect_revenue_anomalies():
    query = """
        SELECT 
            category,
            window_start,
            total_revenue,
            order_count,
            avg_order_value,
            total_units_sold
        FROM public.gold_revenue_by_category
        ORDER BY window_start
    """
    
    df = pd.read_sql(query, engine)
    if len(df) < 10:
        return 0

    alerts_saved = 0

    for category in df['category'].unique():
        cat_df = df[df['category'] == category].copy()
        if len(cat_df) < 5:
            continue

        features = cat_df[['total_revenue', 'order_count', 'avg_order_value']].copy()
        
        model = IsolationForest(contamination=0.1, random_state=42)
        cat_df['anomaly'] = model.fit_predict(features)
        cat_df['score']   = model.score_samples(features)

        anomalies = cat_df[cat_df['anomaly'] == -1]

        for _, row in anomalies.iterrows():
            description = (
                f"Revenue anomaly in '{category}' at {row['window_start']}: "
                f"${row['total_revenue']:.2f} revenue from {int(row['order_count'])} orders. "
                f"Avg order value ${row['avg_order_value']:.2f} flagged as unusual."
            )
            save_alert(
                alert_type   = 'REVENUE_ANOMALY',
                severity     = 'MEDIUM',
                entity_id    = category,
                metric_name  = 'revenue_per_window',
                metric_value = row['total_revenue'],
                threshold    = cat_df['total_revenue'].mean(),
                description  = description
            )
            alerts_saved += 1
            print(f"  [REVENUE ANOMALY] {category}: ${row['total_revenue']:.2f} (score: {row['score']:.3f})")

    return alerts_saved

# ══════════════════════════════════════════════════════════════════════════
# MAIN LOOP — runs every 60 seconds
# ══════════════════════════════════════════════════════════════════════════
run_count = 0
while True:
    run_count += 1
    print(f"\n{'='*60}")
    print(f"Anomaly Detection Run #{run_count} — {datetime.utcnow().strftime('%H:%M:%S')}")
    print(f"{'='*60}")

    total_alerts = 0
    total_alerts += detect_payment_fraud()
    total_alerts += detect_demand_spikes()
    total_alerts += detect_revenue_anomalies()

    print(f"\nRun #{run_count} complete — {total_alerts} alerts generated")

    # Check total alerts in DB
    with engine.connect() as conn:
        result = conn.execute(text("SELECT COUNT(*), alert_type FROM anomaly_alerts GROUP BY alert_type"))
        rows = result.fetchall()
        if rows:
            print("Current alert counts:")
            for row in rows:
                print(f"  {row[1]}: {row[0]} alerts")

    print(f"Next run in 60 seconds...")
    time.sleep(60)