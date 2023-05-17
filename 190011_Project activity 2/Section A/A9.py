
#Add Labels for Delayed Flights and count.
from pyspark.sql import SparkSession
from pyspark.sql.functions import when

# Create a SparkSession
spark = SparkSession.builder.getOrCreate()

# Read the Flights.csv file into a DataFrame
flights_df = spark.read.csv("flights.csv", header=True, inferSchema=True)

# Add a new column for labels based on departure delay
flights_df = flights_df.withColumn("label", when(flights_df["departure delay"] > 0, 1).otherwise(0))

# Count the number of delayed and not delayed flights
count_delayed_flights = flights_df.groupBy("label").count()

# Show the results
count_delayed_flights.show()