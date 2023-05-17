
#Find out the Count of Departure Delays by Hour of Day.
from pyspark.sql import SparkSession
from pyspark.sql.functions import hour

# Create a SparkSession
spark = SparkSession.builder.getOrCreate()

# Read the Flights.csv file into a DataFrame
flights_df = spark.read.csv("flights.csv", header=True, inferSchema=True)

# Extract the hour from the departure time column
flights_df = flights_df.withColumn("hour_of_day", hour(flights_df["departure time"]))

# Group by hour of the day and count the departure delays
count_delay_by_hour = flights_df.filter(flights_df["departure delay"] > 0).groupBy("hour_of_day").count()

# Show the results
count_delay_by_hour.show()
