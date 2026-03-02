from pyspark.sql import SparkSession
import os

os.environ["PATH"] = "C:\\hadoop\\bin;" + os.environ.get("PATH", "")
os.environ["HADOOP_HOME"] = "C:\\hadoop"

import ctypes
ctypes.CDLL("C:\\hadoop\\bin\\hadoop.dll")

spark = SparkSession.builder \
    .appName("StorageCheck") \
    .master("local[*]") \
    .getOrCreate()

spark.sparkContext.setLogLevel("ERROR")

print("\n=== BRONZE ORDERS ===")
df = spark.read.parquet("storage/bronze/orders")
print(f"Row count: {df.count()}")
df.show(3, truncate=False)

print("\n=== SILVER ORDERS ===")
df2 = spark.read.parquet("storage/silver/orders")
print(f"Row count: {df2.count()}")
df2.show(3, truncate=False)

print("\n=== GOLD REVENUE ===")
df3 = spark.read.parquet("storage/gold/revenue_by_category")
print(f"Row count: {df3.count()}")
df3.show(3, truncate=False)