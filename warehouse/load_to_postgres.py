import os
import pandas as pd
from sqlalchemy import create_engine, text
from datetime import datetime

# Connection 
DB_URL = "postgresql://ecom_user:ecom_pass@localhost:5433/ecom_warehouse"
engine = create_engine(DB_URL)

BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
STORAGE_DIR = os.path.join(BASE_DIR, "storage")

def load_parquet_folder(folder_path):
    """Read all parquet files from a folder into a single DataFrame."""
    files = []
    for root, dirs, filenames in os.walk(folder_path):
        for f in filenames:
            if f.endswith(".parquet"):
                files.append(os.path.join(root, f))
    
    if not files:
        print(f"  No parquet files found in {folder_path}")
        return None
    
    dfs = [pd.read_parquet(f) for f in files]
    return pd.concat(dfs, ignore_index=True)


# CREATE SCHEMA

print("Creating schema...")

with engine.connect() as conn:
    conn.execute(text("""
        CREATE TABLE IF NOT EXISTS gold_revenue_by_category (
            id                SERIAL PRIMARY KEY,
            window_start      TIMESTAMP,
            window_end        TIMESTAMP,
            category          VARCHAR(100),
            total_revenue     NUMERIC(12,2),
            order_count       INTEGER,
            avg_order_value   NUMERIC(12,2),
            total_units_sold  INTEGER,
            loaded_at         TIMESTAMP DEFAULT NOW()
        );
    """))
    
    conn.execute(text("""
        CREATE TABLE IF NOT EXISTS gold_sla_by_carrier (
            id              SERIAL PRIMARY KEY,
            window_start    TIMESTAMP,
            window_end      TIMESTAMP,
            carrier         VARCHAR(100),
            total_shipments INTEGER,
            breached_count  INTEGER,
            avg_delay_days  NUMERIC(6,2),
            loaded_at       TIMESTAMP DEFAULT NOW()
        );
    """))

    conn.execute(text("""
        CREATE TABLE IF NOT EXISTS gold_low_stock_alerts (
            id            SERIAL PRIMARY KEY,
            product_id    VARCHAR(20),
            warehouse     VARCHAR(100),
            current_stock INTEGER,
            update_type   VARCHAR(50),
            alert_time    TIMESTAMP,
            loaded_at     TIMESTAMP DEFAULT NOW()
        );
    """))

    conn.execute(text("""
        CREATE TABLE IF NOT EXISTS silver_orders (
            order_id        VARCHAR(20) PRIMARY KEY,
            user_id         VARCHAR(20),
            product_id      VARCHAR(20),
            product_name    VARCHAR(100),
            category        VARCHAR(50),
            quantity        INTEGER,
            unit_price      NUMERIC(10,2),
            total_price     NUMERIC(10,2),
            status          VARCHAR(20),
            payment_method  VARCHAR(30),
            city            VARCHAR(100),
            country         VARCHAR(10),
            event_time      TIMESTAMP,
            loaded_at       TIMESTAMP DEFAULT NOW()
        );
    """))

    conn.commit()

print("Schema created successfully")

# LOAD DATA


def load_table(parquet_path, table_name, if_exists='replace'):
    print(f"\nLoading {table_name}...")
    df = load_parquet_folder(parquet_path)
    if df is None:
        return
    
    # Drop duplicates
    df = df.drop_duplicates()
    
    # Drop columns that don't exist in the table schema
    cols_to_drop = ['year', 'month', 'day', 'hour', 'timestamp']
    df = df.drop(columns=[c for c in cols_to_drop if c in df.columns])
    
    df.to_sql(table_name, engine, if_exists=if_exists, index=False)
    print(f"  Loaded {len(df)} rows into {table_name}")

load_table(
    os.path.join(STORAGE_DIR, "gold", "revenue_by_category"),
    "gold_revenue_by_category"
)

load_table(
    os.path.join(STORAGE_DIR, "gold", "sla_by_carrier"),
    "gold_sla_by_carrier"
)

load_table(
    os.path.join(STORAGE_DIR, "gold", "low_stock_alerts"),
    "gold_low_stock_alerts"
)

load_table(
    os.path.join(STORAGE_DIR, "silver", "orders"),
    "silver_orders",
    if_exists='replace'
)

print("\n All tables loaded successfully!")


# VERIFY

print("\n=== ROW COUNTS ===")
with engine.connect() as conn:
    for table in ['gold_revenue_by_category', 'gold_sla_by_carrier', 
                  'gold_low_stock_alerts', 'silver_orders']:
        result = conn.execute(text(f"SELECT COUNT(*) FROM {table}"))
        count = result.fetchone()[0]
        print(f"  {table}: {count} rows")