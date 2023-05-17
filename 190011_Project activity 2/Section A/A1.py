
#Find out the Top 5 Longest departure delays.
from pyspark.sql import SparkSession

# Create a SparkSession
spark = SparkSession.builder.getOrCreate()

# Read the Flights.csv file into a DataFrame
flights_df = spark.read.csv("flights.csv", header=True, inferSchema=True)

# Find the top 5 longest departure delays
top_delays = flights_df.orderBy(flights_df["departure delay"].desc()).limit(5)

# Show the results
top_delays.show()
