import os
import ctypes

os.environ["PATH"] = "C:\\hadoop\\bin;" + os.environ.get("PATH", "")
os.environ["HADOOP_HOME"] = "C:\\hadoop"

try:
    ctypes.CDLL("C:\\hadoop\\bin\\hadoop.dll")
    print("hadoop.dll loaded successfully")
except Exception as e:
    print(f"hadoop.dll load warning: {e}")

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    from_json, col, window, sum as spark_sum,
    count, avg, when, to_timestamp, current_timestamp,
    year, month, dayofmonth, hour
)
from pyspark.sql.types import (
    StructType, StructField, StringType,
    DoubleType, IntegerType, BooleanType
)

BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

JARS = ",".join([
    os.path.join(BASE_DIR, "jars", "spark-sql-kafka-0-10_2.12-3.5.0.jar"),
    os.path.join(BASE_DIR, "jars", "commons-pool2-2.11.1.jar"),
    os.path.join(BASE_DIR, "jars", "kafka-clients-3.4.0.jar"),
    os.path.join(BASE_DIR, "jars", "spark-token-provider-kafka-0-10_2.12-3.5.0.jar"),
])

CHECKPOINT_DIR = os.path.join(BASE_DIR, "checkpoints")
STORAGE_DIR    = os.path.join(BASE_DIR, "storage")

spark = SparkSession.builder \
    .appName("EcomIntelligence") \
    .master("local[*]") \
    .config("spark.jars", JARS) \
    .config("spark.sql.shuffle.partitions", "2") \
    .config("spark.sql.streaming.checkpointLocation", CHECKPOINT_DIR) \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")
print("Spark session started")

# Schemas 
order_schema = StructType([
    StructField("order_id",       StringType()),
    StructField("user_id",        StringType()),
    StructField("product_id",     StringType()),
    StructField("product_name",   StringType()),
    StructField("category",       StringType()),
    StructField("quantity",       IntegerType()),
    StructField("unit_price",     DoubleType()),
    StructField("total_price",    DoubleType()),
    StructField("status",         StringType()),
    StructField("payment_method", StringType()),
    StructField("city",           StringType()),
    StructField("country",        StringType()),
    StructField("timestamp",      StringType()),
])

inventory_schema = StructType([
    StructField("product_id",        StringType()),
    StructField("warehouse",         StringType()),
    StructField("current_stock",     IntegerType()),
    StructField("stock_change",      IntegerType()),
    StructField("reorder_triggered", BooleanType()),
    StructField("update_type",       StringType()),
    StructField("timestamp",         StringType()),
])

shipment_schema = StructType([
    StructField("shipment_id",       StringType()),
    StructField("order_id",          StringType()),
    StructField("carrier",           StringType()),
    StructField("status",            StringType()),
    StructField("origin_city",       StringType()),
    StructField("destination_city",  StringType()),
    StructField("expected_delivery", StringType()),
    StructField("actual_delivery",   StringType()),
    StructField("sla_breached",      BooleanType()),
    StructField("delay_days",        IntegerType()),
    StructField("timestamp",         StringType()),
])

#  Read from Kafka 
def read_kafka_topic(topic, schema):
    return (
        spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", "localhost:9092")
        .option("subscribe", topic)
        .option("startingOffsets", "latest")
        .load()
        .select(from_json(col("value").cast("string"), schema).alias("data"))
        .select("data.*")
    )

orders_raw    = read_kafka_topic("orders",    order_schema)
inventory_raw = read_kafka_topic("inventory", inventory_schema)
shipments_raw = read_kafka_topic("shipments", shipment_schema)

# Parse timestamps 
orders    = orders_raw.withColumn("event_time", to_timestamp(col("timestamp")))
inventory = inventory_raw.withColumn("event_time", to_timestamp(col("timestamp")))
shipments = shipments_raw.withColumn("event_time", to_timestamp(col("timestamp")))

# ===========================================================================
# BRONZE LAYER — raw data written as parquet, partitioned by date/hour
# "Store everything, transform nothing"
# ===========================================================================

# Add partition columns (year/month/day/hour) so files are organized
orders_bronze = orders.withColumn("year",  year("event_time")) \
                      .withColumn("month", month("event_time")) \
                      .withColumn("day",   dayofmonth("event_time")) \
                      .withColumn("hour",  hour("event_time"))

inventory_bronze = inventory.withColumn("year",  year("event_time")) \
                            .withColumn("month", month("event_time")) \
                            .withColumn("day",   dayofmonth("event_time")) \
                            .withColumn("hour",  hour("event_time"))

shipments_bronze = shipments.withColumn("year",  year("event_time")) \
                            .withColumn("month", month("event_time")) \
                            .withColumn("day",   dayofmonth("event_time")) \
                            .withColumn("hour",  hour("event_time"))

b1 = (orders_bronze.writeStream
    .outputMode("append")
    .format("parquet")
    .option("path", os.path.join(STORAGE_DIR, "bronze", "orders"))
    .option("checkpointLocation", os.path.join(CHECKPOINT_DIR, "bronze_orders"))
    .partitionBy("year", "month", "day", "hour")
    .trigger(processingTime="30 seconds")
    .start())

b2 = (inventory_bronze.writeStream
    .outputMode("append")
    .format("parquet")
    .option("path", os.path.join(STORAGE_DIR, "bronze", "inventory"))
    .option("checkpointLocation", os.path.join(CHECKPOINT_DIR, "bronze_inventory"))
    .partitionBy("year", "month", "day", "hour")
    .trigger(processingTime="30 seconds")
    .start())

b3 = (shipments_bronze.writeStream
    .outputMode("append")
    .format("parquet")
    .option("path", os.path.join(STORAGE_DIR, "bronze", "shipments"))
    .option("checkpointLocation", os.path.join(CHECKPOINT_DIR, "bronze_shipments"))
    .partitionBy("year", "month", "day", "hour")
    .trigger(processingTime="30 seconds")
    .start())

print("Bronze layer writers started")

# ===========================================================================
# SILVER LAYER — cleaned and filtered data
# "Remove garbage, enforce quality"
# ===========================================================================
# Orders silver: only valid placed orders, no nulls, no zero prices
orders_silver = orders \
    .filter(col("order_id").isNotNull()) \
    .filter(col("total_price") > 0) \
    .filter(col("status") == "placed") \
    .filter(col("quantity") > 0) \
    .filter(col("quantity") <= 100)   # filter out anomalous bulk orders for silver

# Inventory silver: only meaningful stock changes
inventory_silver = inventory \
    .filter(col("product_id").isNotNull()) \
    .filter(col("current_stock") >= 0)

# Shipments silver: only valid shipments
shipments_silver = shipments \
    .filter(col("shipment_id").isNotNull()) \
    .filter(col("carrier").isNotNull())

s1 = (orders_silver.writeStream
    .outputMode("append")
    .format("parquet")
    .option("path", os.path.join(STORAGE_DIR, "silver", "orders"))
    .option("checkpointLocation", os.path.join(CHECKPOINT_DIR, "silver_orders"))
    .trigger(processingTime="30 seconds")
    .start())

s2 = (inventory_silver.writeStream
    .outputMode("append")
    .format("parquet")
    .option("path", os.path.join(STORAGE_DIR, "silver", "inventory"))
    .option("checkpointLocation", os.path.join(CHECKPOINT_DIR, "silver_inventory"))
    .trigger(processingTime="30 seconds")
    .start())

s3 = (shipments_silver.writeStream
    .outputMode("append")
    .format("parquet")
    .option("path", os.path.join(STORAGE_DIR, "silver", "shipments"))
    .option("checkpointLocation", os.path.join(CHECKPOINT_DIR, "silver_shipments"))
    .trigger(processingTime="30 seconds")
    .start())

print("Silver layer writers started")

# ══════════════════════════════════════════════════════════════════════════
# GOLD LAYER — business aggregations
# "Answer business questions directly"
# ══════════════════════════════════════════════════════════════════════════

# Gold 1: Revenue by category per minute
revenue_by_category = (
    orders_silver
    .withWatermark("event_time", "2 minutes")
    .groupBy(window("event_time", "1 minute"), col("category"))
    .agg(
        spark_sum("total_price").alias("total_revenue"),
        count("order_id").alias("order_count"),
        avg("total_price").alias("avg_order_value"),
        spark_sum("quantity").alias("total_units_sold")
    )
    .select(
        col("window.start").alias("window_start"),
        col("window.end").alias("window_end"),
        col("category"),
        col("total_revenue"),
        col("order_count"),
        col("avg_order_value"),
        col("total_units_sold")
    )
)

# Gold 2: SLA breach summary by carrier per minute
sla_by_carrier = (
    shipments_silver
    .withWatermark("event_time", "2 minutes")
    .groupBy(window("event_time", "1 minute"), col("carrier"))
    .agg(
        count("shipment_id").alias("total_shipments"),
        spark_sum(when(col("sla_breached") == True, 1).otherwise(0)).alias("breached_count"),
        avg("delay_days").alias("avg_delay_days")
    )
    .select(
        col("window.start").alias("window_start"),
        col("window.end").alias("window_end"),
        col("carrier"),
        col("total_shipments"),
        col("breached_count"),
        col("avg_delay_days")
    )
)

# Gold 3: Low stock alerts
low_stock_alerts = (
    inventory_silver
    .filter(col("reorder_triggered") == True)
    .select(
        col("product_id"),
        col("warehouse"),
        col("current_stock"),
        col("update_type"),
        col("event_time").alias("alert_time")
    )
)

g1 = (revenue_by_category.writeStream
    .outputMode("append")
    .format("parquet")
    .option("path", os.path.join(STORAGE_DIR, "gold", "revenue_by_category"))
    .option("checkpointLocation", os.path.join(CHECKPOINT_DIR, "gold_revenue"))
    .trigger(processingTime="30 seconds")
    .start())

g2 = (sla_by_carrier.writeStream
    .outputMode("append")
    .format("parquet")
    .option("path", os.path.join(STORAGE_DIR, "gold", "sla_by_carrier"))
    .option("checkpointLocation", os.path.join(CHECKPOINT_DIR, "gold_sla"))
    .trigger(processingTime="30 seconds")
    .start())

g3 = (low_stock_alerts.writeStream
    .outputMode("append")
    .format("parquet")
    .option("path", os.path.join(STORAGE_DIR, "gold", "low_stock_alerts"))
    .option("checkpointLocation", os.path.join(CHECKPOINT_DIR, "gold_alerts"))
    .trigger(processingTime="30 seconds")
    .start())

print("Gold layer writers started")
print("\n All 9 streaming queries running (Bronze + Silver + Gold)...\n")

spark.streams.awaitAnyTermination()