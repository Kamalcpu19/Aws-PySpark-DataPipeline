import os
import sys

os.environ["PYSPARK_PYTHON"] = sys.executable
os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable

from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("ECommerceOrderPipeline") \
    .master("local[*]") \
    .getOrCreate()

# reading the order csv filr
order_data_frame=spark.read.csv("Data/Input/orders_20_lakh.csv",header=True,
    inferSchema=True)
order_data_frame.show()
order_data_frame.printSchema
print("Total Orders:", order_data_frame.count())
print("================================")
print("Spark started successfully!")
print("Spark version:", spark.version)
print("================================")

spark.stop()
