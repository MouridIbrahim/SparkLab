
from pathlib import Path
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, sum, count

# Locate the project and CSV file
PROJECT_ROOT = Path(__file__).resolve().parent.parent
csv_path = PROJECT_ROOT / "data" / "orders.csv"

# Create the Spark session using the teacher's configuration
spark = (
    SparkSession.builder
    .appName("OrderAnalysis")
    .master("local[*]")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("ERROR")

# Load the dataset
df = spark.read.csv(
    str(csv_path),
    header=True,
    inferSchema=True
)

# Inspect the dataset
print("First 10 orders:")
df.show(10)

print("Schema:")
df.printSchema()

print("Total orders:", df.count())

# Check missing values in every column
print("Missing values:")
df.select([
    sum(col(column).isNull().cast("int")).alias(column)
    for column in df.columns
]).show()

# Task 1: Calculate order amount
df = df.withColumn(
    "amount",
    col("price") * col("quantity")
)

print("Analysis 1: Order amounts")
df.select(
    "order_id", "category", "price", "quantity", "amount"
).show(10)

# Task 2: Count orders by category
print("Analysis 2: Orders by category")
df.groupBy("category").count().show()

# Task 3: Revenue by category, descending
print("Analysis 3: Revenue by category")
df.groupBy("category") \
    .agg(sum("amount").alias("total_revenue")) \
    .orderBy(col("total_revenue").desc()) \
    .show()

# Task 4: Order count and revenue by city
print("Analysis 4: Orders and revenue by city")
df.groupBy("city").agg(
    count("*").alias("order_count"),
    sum("amount").alias("total_revenue")
).show()

# Task 5: Orders above 2,000 yuan
print("Analysis 5: High-value orders")
df.filter(col("amount") > 2000).show()

# Task 6: Top 10 users by total spending
print("Analysis 6: Top 10 users")
df.groupBy("user_id") \
    .agg(sum("amount").alias("total_spending")) \
    .orderBy(col("total_spending").desc()) \
    .show(10)

spark.stop()
