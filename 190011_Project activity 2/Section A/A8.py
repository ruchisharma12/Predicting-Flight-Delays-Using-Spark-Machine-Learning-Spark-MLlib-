
#Find out the Count of Departure Delays by Origin, Destination.
from pyspark.sql import SparkSession

# Create a SparkSession
spark = SparkSession.builder.getOrCreate()

# Read the Flights.csv file into a DataFrame
flights_df = spark.read.csv("flights.csv", header=True, inferSchema=True)

# Group by origin and destination airports and count the departure delays
count_delay_by_origin_dest = flights_df.filter(flights_df["departure delay"] > 0).groupBy("origin", "destination").count()

# Show the results
count_delay_by_origin_dest.show()
