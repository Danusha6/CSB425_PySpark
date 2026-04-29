from pyspark.sql import SparkSession
from pyspark.sql.functions import when, round as spark_round, current_timestamp, col
import time

# ─────────────────────────────────────────────────────────────
# EXERCISE — coffee_orders.csv
# Complete the sections marked TODO
# ─────────────────────────────────────────────────────────────

# TODO 1: Create a SparkSession
# - appName should be "CoffeeExercise"
# - Connect to the master at spark://spark-master:7077
# - Set log level to WARN
spark = (
    SparkSession.builder
    .appName("CoffeeExercise")
    .master("spark://spark-master:7077")
    .getOrCreate()
)
spark.sparkContext.setLogLevel("WARN")

# TODO 2: Read the CSV
# Path: /opt/spark/data/raw/coffee_orders.csv
# Use header=True and inferSchema=True
# Print the schema and row count
df = (
    spark.read
    .option("header", True)
    .option("inferSchema", True)
    .csv("/opt/spark/data/raw/coffee_orders.csv")
)
 
df.printSchema()
print(f"Row count: {df.count()}")

# TODO 3: Add three columns using withColumn()
# a) "order_size": "small" if Total < 10, "medium" if Total < 30, "large" otherwise
# b) "revenue_per_item": Total / Quantity, rounded to 2 decimal places
# c) "processed_at": current timestamp
df_transformed = (
    df
    .withColumn(
        "order_size",
        when(col("Total") < 10, "small")
        .when(col("Total") < 30, "medium")
        .otherwise("large")
    )
    .withColumn(
        "revenue_per_item",
        spark_round(col("Total") / col("Quantity"), 2)
    )
    .withColumn(
        "processed_at",
        current_timestamp()
    )
)
 
print("Transformations applied (sample of 10 rows):")
df_transformed.select("Order_ID", "Item", "Quantity", "Total",
          "order_size", "revenue_per_item", "Channel").show(10)


# TODO 4: Write the transformed DataFrame to Parquet and ORC
# Paths:
#   Parquet -> /opt/spark/data/output/parquet
#   ORC     -> /opt/spark/data/output/orc
# Use mode="overwrite" for both

df_transformed.write.mode("overwrite").parquet("/opt/spark/data/output/parquet")
df_transformed.write.mode("overwrite").orc("/opt/spark/data/output/orc")
 
print("Output files written to data/output/")


# TODO 5: Register a temp view and run a Spark SQL query
# View name: "orders"
# Query: count of orders, total revenue, and average order total
# grouped by Channel and order_size, ordered by Channel
# Time the query using time.time() and print the elapsed time

df_transformed.createOrReplaceTempView("orders")
 
query = """
    SELECT
        Channel,
        COUNT(*)                        AS order_count,
        ROUND(SUM(Total), 2)            AS total_revenue,
        ROUND(AVG(Total), 2)            AS avg_order_total
    FROM orders
    GROUP BY Channel
    ORDER BY Channel
"""
 
t0 = time.time()
results = spark.sql(query).collect()
elapsed = time.time() - t0
 
# Pretty-print using a DataFrame show() call
spark.sql(query).show()
print(f"Query time: {elapsed:.4f} s")

# TODO 6: Stop the SparkSession
spark.stop()
