from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType
from pyspark.sql.functions import when, col, length, trim, lower, isnull

# Create SparkSession
spark = SparkSession.builder \
    .appName("example_app") \
    .getOrCreate()

# Sample data
data = [("John", "Hello"), ("Jane", ""), ("Alice", None), ("Bob", "None"), ("Eve", "  ")]

# Define schema
schema = StructType([
    StructField("Name", StringType(), nullable=False),
    StructField("Text", StringType(), nullable=True)
])

# Create DataFrame
df = spark.createDataFrame(data, schema)

# Validate Text column
df = df.withColumn(
    "ValidatedText", 
    when(
        ~((trim(lower(col("Text"))) == "none") | (trim(lower(col("Text"))) == "null") | (length(trim(col("Text"))) == 0) | isnull(col("Text"))), 1
    ).otherwise(0)
)

# Display modified DataFrame
df.show()
