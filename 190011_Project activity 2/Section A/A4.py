
#Find out the Count of Departure Delays by Day of the Week.
from pyspark.sql import SparkSession
from pyspark.sql.functions import dayofweek

# Create a SparkSession
spark = SparkSession.builder.getOrCreate()

# Read the Flights.csv file into a DataFrame
flights_df = spark.read.csv("flights.csv", header=True, inferSchema=True)

# Extract the day of the week from the date column
flights_df = flights_df.withColumn("day_of_week", dayofweek(flights_df["date"]))

# Group by day of the week and count the departure delays
count_delay_by_day = flights_df.filter(flights_df["departure delay"] > 0).groupBy("day_of_week").count()

# Show the results
count_delay_by_day.show()
