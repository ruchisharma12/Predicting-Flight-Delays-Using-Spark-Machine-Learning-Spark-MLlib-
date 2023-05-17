
#Find out the Average Departure Delay by Carrier.
from pyspark.sql import SparkSession

# Create a SparkSession
spark = SparkSession.builder.getOrCreate()

# Read the Flights.csv file into a DataFrame
flights_df = spark.read.csv("flights.csv", header=True, inferSchema=True)

# Group by carrier and calculate the average departure delay
avg_delay_by_carrier = flights_df.groupBy("carrier").avg("departure delay")

# Show the results
avg_delay_by_carrier.show()

