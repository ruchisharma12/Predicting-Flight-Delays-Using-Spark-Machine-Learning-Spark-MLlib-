from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.classification import LogisticRegression
from pyspark.ml import Pipeline
from pyspark.ml.evaluation import BinaryClassificationEvaluator

# Create a SparkSession
spark = SparkSession.builder.getOrCreate()

# Step 1: Basic Operations

# Define the schema for the Flights.csv file
schema = StructType([
    StructField("day", IntegerType(), True),
    StructField("month", IntegerType(), True),
    StructField("year", IntegerType(), True),
    StructField("flight_number", StringType(), True),
    StructField("airline", StringType(), True),
    StructField("scheduled_arrival", StringType(), True),
    StructField("origin_airport", StringType(), True),
    StructField("destination_airport", StringType(), True),
    StructField("scheduled_departure", StringType(), True),
    StructField("departure_time", StringType(), True),
    StructField("taxi_out", IntegerType(), True),
    StructField("taxi_in", IntegerType(), True),
    StructField("scheduled_time", IntegerType(), True),
    StructField("departure_delay", IntegerType(), True),
    StructField("elapsed_time", IntegerType(), True),
    StructField("weather_delay", IntegerType(), True),
    StructField("distance", IntegerType(), True),
    StructField("wheels_off", StringType(), True),
    StructField("tail_number", StringType(), True),
    StructField("arrival_time", StringType(), True),
    StructField("arrival_delay", IntegerType(), True),
    StructField("diverted", IntegerType(), True),
    StructField("cancelled", IntegerType(), True),
    StructField("cancellation_reason", StringType(), True),
    StructField("air_system_delay", IntegerType(), True),
    StructField("security_delay", IntegerType(), True),
    StructField("airline_delay", IntegerType(), True),
    StructField("late_aircraft_delay", IntegerType(), True),
    StructField("air_time", IntegerType(), True)
])

# Read the Flights.csv file with the defined schema
flights_df = spark.read.csv("flights.csv", header=True, schema=schema)

# Step 2: Data Exploration

# Check the number of rows and columns in the DataFrame
num_rows = flights_df.count()
num_cols = len(flights_df.columns)

print("Number of rows:", num_rows)
print("Number of columns:", num_cols)

# Generate descriptive statistics of the DataFrame
flights_df.describe().show()

# Count the missing values in each column
missing_values = flights_df.select([sum(col(column).isNull().cast("int")).alias(column) for column in flights_df.columns])
missing_values.show()

# Count the unique values in a column
unique_values_count = flights_df.select(countDistinct("column_name"))
unique_values_count.show()

# Step 3: Build a data processing pipeline

# Define the features column
features_col = ["Transformers", "Estimators", ...]

# Create a VectorAssembler to assemble the features into a single vector column
assembler = VectorAssembler(inputCols=features_col, outputCol="features")

# Step 4: Build the classifier

# Create a LogisticRegression model
lr = LogisticRegression(labelCol="label", featuresCol="features")

# Create a pipeline with the VectorAssembler and LogisticRegression
pipeline = Pipeline(stages=[assembler, lr])

# Split the data into training and testing sets
train_data, test_data = flights_df.randomSplit([0.7, 0.3], seed=42)

# Fit the pipeline on the training data
model = pipeline.fit(train_data)

# Step 5: Train and evaluate the model

# Make predictions on the testing data
predictions = model.transform(test_data)

# Evaluate the model using a BinaryClassificationEvaluator
evaluator = BinaryClassificationEvaluator(labelCol="label")
accuracy = evaluator.evaluate(predictions)

print("Accuracy:", accuracy)
