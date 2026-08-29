import os
import sys

os.environ["HADOOP_HOME"] = r"C:\hadoop"
os.environ["PATH"] = r"C:\hadoop\bin;" + os.environ["PATH"]

os.environ["PYSPARK_PYTHON"] = sys.executable
os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable

from pyspark.sql import SparkSession
from pyspark.sql.functions import col

spark = SparkSession.builder \
    .appName("ECommerceOrderTransformation") \
    .config("spark.hadoop.io.native.lib.available", "false") \
    .config("spark.hadoop.mapreduce.fileoutputcommitter.algorithm.version", "2") \
    .getOrCreate()

# Read data
order_data=spark.read.csv("Data/Input/orders_20_lakh.csv",
    header=True,
    inferSchema=True
)
# Calculate gross amount
order_data=order_data.withColumn("Gross_amount",
                                             col("quantity")*col("unit_price"))


# Discount amount
order_data=order_data.withColumn("Discount_amount",
                        col("Gross_amount")*col("discount_pct")/100)

# Net amount
order_data=order_data.withColumn("Net_Total",
                        col("Gross_amount")-col("Discount_amount"))

#display result
order_data.select(
    "order_id",
    "quantity",
    "unit_price",
    "discount_pct",
    "Gross_amount",
    "Discount_amount",
    "Net_Total"
).show(10)




# write to Parquet file 
output_path = r"C:\Users\kamal\OneDrive\Desktop\Aws-PySpark-DataPipeline\Data\output\orders"

order_data.write \
    .mode("overwrite") \
    .option("header", True) \
    .csv(output_path)

output_df = spark.read.csv(
    output_path,
    header=True,
    inferSchema=True
)

print("Transformation completed successfully")
print("Output written to:", output_path)
print("Output record count:", output_df.count())
output_df = spark.read.csv(
    output_path,
    header=True,
    inferSchema=True
)

output_df.select(
    "order_id",
    "quantity",
    "unit_price",
    "discount_pct",
    "Gross_amount",
    "Discount_amount",
    "Net_Total"
).show(10, truncate=False)

