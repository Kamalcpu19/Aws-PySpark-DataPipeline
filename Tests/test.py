import os
import sys

os.environ["HADOOP_HOME"] = r"C:\hadoop"
os.environ["PATH"] += os.pathsep + r"C:\hadoop\bin"

os.environ["PYSPARK_PYTHON"] = sys.executable
os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, sum

spark = SparkSession.builder \
    .appName("ECommerceOrderTransformationTest") \
    .getOrCreate()

# ============================================================
# TEST COUNTERS
# ============================================================

total_tests = 0
passed_tests = 0
failed_tests = 0


print("=" * 50)
print("OUTPUT VALIDATION")
print("=" * 50)


# ============================================================
# Test 01 - Output File Exists
# ============================================================

try:
    order_Output_data = spark.read.csv(
    "Data/output/orders",
    header=True,
    inferSchema=True
)
    
    print(f"Test 01 - Output exists                     PASS")
    total_tests += 1
    passed_tests += 1

except Exception as e:
    print(f"Test 01 - Output exists                     FAIL")
    print("Error:", e)
    total_tests += 1
    failed_tests += 1


# ============================================================
# Continue tests only if file is successfully read
# ============================================================

if 'order_Output_data' in locals():

    # ========================================================
    # Test 02 - Record Count
    # ========================================================

    print("Test 02 - Record count", end="")

    Total_output_count = order_Output_data.count()

    total_tests += 1

    if Total_output_count == 2000000:
        print("                     PASS")
        passed_tests += 1
    else:
        print("                     FAIL")
        print("Actual record count:", Total_output_count)
        failed_tests += 1


    # ========================================================
    # Test 03 - Required Transformation Columns
    # ========================================================

    print("Test 03 - Transformation columns", end="")

    required_columns = [
        "Gross_amount",
        "Discount_amount",
        "Net_Total"
    ]

    missing_columns = [
        column for column in required_columns
        if column not in order_Output_data.columns
    ]

    total_tests += 1

    if len(missing_columns) == 0:
        print("              PASS")
        passed_tests += 1
    else:
        print("              FAIL")
        print("Missing Columns:", missing_columns)
        failed_tests += 1


    # ========================================================
    # Test 04 - No NULL Order ID
    # ========================================================

    print("Test 04 - No NULL order_id", end="")

    noNull = order_Output_data.filter(
        col("order_id").isNull()
    ).count()

    total_tests += 1

    if noNull == 0:
        print("                   PASS")
        passed_tests += 1
    else:
        print("                   FAIL")
        print("NULL order_id count:", noNull)
        failed_tests += 1


    # ========================================================
    # Test 05 - No Duplicate Orders
    # ========================================================

    print("Test 05 - No duplicate orders", end="")

    NoDuplicateOrders = order_Output_data \
        .groupBy("order_id") \
        .count() \
        .filter(col("count") > 1) \
        .count()

    total_tests += 1

    if NoDuplicateOrders == 0:
        print("              PASS")
        passed_tests += 1
    else:
        print("              FAIL")
        print("Duplicate order IDs:", NoDuplicateOrders)
        failed_tests += 1


    # ========================================================
    # Test 06 - Quantity Validation
    # ========================================================

    print("Test 06 - Quantity greater than zero", end="")

    Quantity_validation = order_Output_data.filter(
        col("quantity") <= 0
    ).count()

    total_tests += 1

    if Quantity_validation == 0:
        print("       PASS")
        passed_tests += 1
    else:
        print("       FAIL")
        print("Invalid quantity records:", Quantity_validation)
        failed_tests += 1


    # ========================================================
    # Test 07 - Unit Price Validation
    # ========================================================

    print("Test 07 - Unit price greater than zero", end="")

    Unit_price_validation = order_Output_data.filter(
        col("unit_price") <= 0
    ).count()

    total_tests += 1

    if Unit_price_validation == 0:
        print("     PASS")
        passed_tests += 1
    else:
        print("     FAIL")
        print("Invalid unit price records:", Unit_price_validation)
        failed_tests += 1


    # ========================================================
    # Test 08 - Discount Validation
    # ========================================================

    print("Test 08 - Discount percentage 0-100", end="")

    Discount_validation = order_Output_data.filter(
        (col("discount_pct") < 0) |
        (col("discount_pct") > 100)
    ).count()

    total_tests += 1

    if Discount_validation == 0:
        print("          PASS")
        passed_tests += 1
    else:
        print("          FAIL")
        print("Invalid discount records:", Discount_validation)
        failed_tests += 1


    # ========================================================
    # Test 09 - Gross Amount Calculation
    # ========================================================

    print("Test 09 - Gross amount calculation", end="")

    Gross_amount_calculation = order_Output_data.filter(
        col("quantity") * col("unit_price") != col("Gross_amount")
    ).count()

    total_tests += 1

    if Gross_amount_calculation == 0:
        print("             PASS")
        passed_tests += 1
    else:
        print("             FAIL")
        print("Invalid gross amount records:",
              Gross_amount_calculation)
        failed_tests += 1


    # ========================================================
    # Test 10 - Discount Amount Calculation
    # ========================================================

    print("Test 10 - Discount amount calculation", end="")

    Discount_amount_calculation = order_Output_data.filter(
        col("Discount_amount") !=
        (col("gross_amount") * col("discount_pct") / 100)
    ).count()

    total_tests += 1

    if Discount_amount_calculation == 0:
        print("          PASS")
        passed_tests += 1
    else:
        print("          FAIL")
        print("Invalid discount amount records:",
              Discount_amount_calculation)
        failed_tests += 1


    # ========================================================
    # Test 11 - Net Amount Calculation
    # ========================================================

    print("Test 11 - Net amount calculation", end="")

    Net_amount_calculation = order_Output_data.filter(
        col("Net_Total") !=
        (col("Gross_amount") - col("Discount_amount"))
    ).count()

    total_tests += 1

    if Net_amount_calculation == 0:
        print("              PASS")
        passed_tests += 1
    else:
        print("              FAIL")
        print("Invalid net amount records:",
              Net_amount_calculation)
        failed_tests += 1


    # ========================================================
    # Test 12 - Status Values
    # ========================================================

    print("Test 12 - Valid status values", end="")

    allowed_statuses = [
        "Completed",
        "Pending",
        "Cancelled"
    ]

    Status_values = order_Output_data.filter(
        ~col("status").isin(allowed_statuses)
    ).count()

    total_tests += 1

    if Status_values == 0:
        print("                 PASS")
        passed_tests += 1
    else:
        print("                 FAIL")
        print("Unexpected status records:", Status_values)
        failed_tests += 1


    # ========================================================
    # Test 13 - Payment Methods
    # ========================================================

    print("Test 13 - Valid payment methods", end="")

    Payment_methods_allowed = [
        "UPI",
        "Credit Card",
        "Debit Card",
        "Cash on Delivery",
        "Net Banking"
    ]

    Payment_methods = order_Output_data.filter(
        ~col("payment_method").isin(Payment_methods_allowed)
    ).count()

    total_tests += 1

    if Payment_methods == 0:
        print("                PASS")
        passed_tests += 1
    else:
        print("                FAIL")
        print("Unexpected payment method records:",
              Payment_methods)
        failed_tests += 1


    # ========================================================
    # Test 14 - NULL Amount Values
    # ========================================================

    print("Test 14 - No NULL amount values", end="")

    null_records = order_Output_data.filter(
        col("Gross_amount").isNull() |
        col("Discount_amount").isNull() |
        col("Net_Total").isNull()
    ).count()

    total_tests += 1

    if null_records == 0:
        print("                PASS")
        passed_tests += 1
    else:
        print("                FAIL")
        print("NULL amount records:", null_records)
        failed_tests += 1


    # ========================================================
    # Test 15 - Total Orders Amount
    # ========================================================

    print("Test 15 - Total orders amount", end="")

    total_orders_Amount = order_Output_data.agg(
        sum(col("Net_Total")).alias("total_orders_amount")
    ).collect()[0]["total_orders_amount"]

    total_tests += 1

    if total_orders_Amount is not None:
        print("                 PASS")
        passed_tests += 1
        print("Total Orders Amount:", total_orders_Amount)
    else:
        print("                 FAIL")
        failed_tests += 1


# ============================================================
# FINAL TEST SUMMARY
# ============================================================

print()
print("=" * 40)
print(f"TOTAL TESTS: {total_tests}")
print(f"PASSED:      {passed_tests}")
print(f"FAILED:      {failed_tests}")
print("=" * 40)