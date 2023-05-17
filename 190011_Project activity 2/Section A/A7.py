
#Find out the Count of Departure Delays by Destination.
from pyspark.sql import SparkSession

# Create a SparkSession
spark = SparkSession.builder.getOrCreate()

# Read the Flights.csv file into a DataFrame
flights_df = spark.read.csv("flights.csv", header=True, inferSchema=True)

# Group by destination airport and count the departure delays
count_delay_by_destination = flights_df.filter(flights_df["departure delay"] > 0).groupBy("destination").count()

# Show the results
count_delay_by_destination.show()