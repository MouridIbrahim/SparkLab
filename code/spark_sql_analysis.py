
from pathlib import Path
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, sum

# Get the project directory and CSV file path
PROJECT_ROOT = Path(__file__).resolve().parent.parent
csv_path = PROJECT_ROOT / "data" / "orders.csv"

# Create Spark session
spark = (
    SparkSession.builder
    .appName("SparkSQLAnalysis")
    .master("local[*]")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("ERROR")

# Load the orders dataset
df = spark.read.csv(
    str(csv_path),
    header=True,
    inferSchema=True
)

# Calculate the total amount for each order
df = df.withColumn(
    "amount",
    col("price") * col("quantity")
)

# Register the DataFrame as a temporary SQL view
df.createOrReplaceTempView("orders")


# Query 1: Find orders with unit price above 500 yuan
print("\nQuery 1: Orders with unit price above 500")

result1 = spark.sql("""
    SELECT *
    FROM orders
    WHERE price > 500
""")

result1.show()


# Query 2: Count orders by product category
print("\nQuery 2: Order count by category")

result2 = spark.sql("""
    SELECT category, COUNT(*) AS order_count
    FROM orders
    GROUP BY category
    ORDER BY order_count DESC
""")

result2.show()


# Query 3: Calculate total revenue by city
print("\nQuery 3: Revenue by city")

result3 = spark.sql("""
    SELECT city, SUM(amount) AS total_revenue
    FROM orders
    GROUP BY city
    ORDER BY total_revenue DESC
""")

result3.show()


# Query 4: Calculate revenue by city and category
print("\nQuery 4: Revenue by city and category")

result4 = spark.sql("""
    SELECT
        city,
        category,
        SUM(amount) AS total_revenue
    FROM orders
    GROUP BY city, category
    ORDER BY total_revenue DESC
""")

result4.show()


# Query 5: Find the top five users by total spending
print("\nQuery 5: Top five users by spending")

result5 = spark.sql("""
    SELECT user_id, SUM(amount) AS total_spending
    FROM orders
    GROUP BY user_id
    ORDER BY total_spending DESC
    LIMIT 5
""")

result5.show()


# Compare the DataFrame API with Spark SQL
print("\nDataFrame API: Revenue by city")

dataframe_result = (
    df.groupBy("city")
    .agg(sum("amount").alias("total_revenue"))
    .orderBy(col("total_revenue").desc())
)

dataframe_result.show()

print("\nSpark SQL: Revenue by city")
result3.show()


# Display the SQL execution plan
print("\nExecution plan for revenue by city")

result3.explain()


spark.stop()
