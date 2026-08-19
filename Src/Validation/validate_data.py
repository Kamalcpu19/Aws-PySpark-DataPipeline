import os
import sys

os.environ["PYSPARK_PYTHON"] = sys.executable
os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable

from pyspark.sql import SparkSession
from pyspark.sql.functions import col


spark = SparkSession.builder \
    .appName("ECommerceOrderValidation") \
    .getOrCreate()


# Read orders
orders_df = spark.read.csv(
    "Data/Input/orders_20_lakh.csv",
    header=True,
    inferSchema=True
)


print("=" * 50)
print("DATA QUALITY REPORT")
print("=" * 50)


# 1. Total records
total_orders = orders_df.count()
print("Total Orders:", total_orders)


# 2. Null Order IDs
null_order_ids = orders_df.filter(
    col("order_id").isNull()
).count()

print("Null Order IDs:", null_order_ids)


# 3. Null Customer IDs
null_customer_ids = orders_df.filter(
    col("customer_id").isNull()
).count()

print("Null Customer IDs:", null_customer_ids)


# 4. Invalid Quantity
invalid_quantity = orders_df.filter(
    col("quantity") <= 0
).count()

print("Invalid Quantities:", invalid_quantity)


# 5. Invalid Unit Price
invalid_price = orders_df.filter(
    col("unit_price") <= 0
).count()

print("Invalid Prices:", invalid_price)


# 6. Invalid Discount
invalid_discount = orders_df.filter(
    (col("discount_pct") < 0) |
    (col("discount_pct") > 100)
).count()

print("Invalid Discounts:", invalid_discount)


# 7. Duplicate Order IDs
duplicate_order_ids = (
    orders_df
    .groupBy("order_id")
    .count()
    .filter(col("count") > 1)
    .count()
)

print("Duplicate Order IDs:", duplicate_order_ids)


print("=" * 50)

spark.stop()