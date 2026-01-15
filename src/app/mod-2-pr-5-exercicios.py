"""
docker exec -it spark-master \
  //opt/bitnami/spark/bin/spark-submit \
  --master spark://spark-master:7077 \
  --deploy-mode client \
  //opt/bitnami/spark/jobs/app/mod-2-pr-5-exercicios.py
"""

from pyspark.sql import SparkSession
import pyspark.sql.functions as F

# Create a Spark session
spark = SparkSession.builder \
    .appName("Basic Transformations") \
    .master("local[*]") \
    .getOrCreate()

# Load datasets
restaurants_df = spark.read.json("./storage/mysql/restaurants/01JS4W5A7YWTYRQKDA7F7N95VY.jsonl")
drivers_df = spark.read.json("./storage/postgres/drivers/01JS4W5A74BK7P4BPTJV1D3MHA.jsonl")
orders_df = spark.read.json("./storage/kafka/orders/01JS4W5A7XY65S9Z69BY51BEJ4.jsonl")

# Display the schemas for reference
print("Restaurants Schema:")
restaurants_df.printSchema()

print("\nDrivers Schema:")
drivers_df.printSchema()

print("\nOrders Schema:")
orders_df.printSchema()

## 1. Column Selection and Projection

# Select specific columns
restaurants_details = restaurants_df.select("name", "cuisine_type", "city", "average_rating")

print("restaurants details:")
restaurants_details.show(5)

### Using Column Objects
restaurant_location = restaurants_df.select(
    F.col("name"),
    F.col("city"),
    F.col("address"),
    F.col("country")
)

print("Restaurant Locations:")
restaurant_location.show(5, truncate=False)

# Rename columns using alias
rename_df = restaurants_df.select(
    F.col("name").alias("restaurant_name"),
    F.col("cuisine_type").alias("cuisine"),
    F.col("average_rating").alias("rating"),
    F.col("num_reviews").alias("reviews_count")
)

print("Renamed_Columns:")
rename_df.show(5, truncate=False)

## 2. Filtering Data

### Basic Filtering
high_rated = restaurants_df.filter(F.col('average_rating') > 4.0)

print(f"Number of high-rated restaurants: {high_rated.count()}")
high_rated.select("name", "cuisine_type", "average_rating").show(5)

# Filter with multiple conditions
popular_italian = restaurants_df.filter(
    (F.col("cuisine_type") == "Italian") & 
    (F.col("num_reviews") > 3000)
)

print(f"Number of popular Italian restaurants: {popular_italian.count()}")
popular_italian.select("name", "city", "average_rating", "num_reviews").show(5)

# Transform restaurant names to uppercase
from pyspark.sql.functions import upper, lower, concat, lit

uppercase_names = restaurants_df.select(
    upper(F.col("name")).alias("restaurant_upper_name"),
    F.col("name"),
    F.col("cuisine_type")  
)
print(f"Restaurant_new_name:")
uppercase_names.show(5, truncate=False)

# Concatenate columns
full_info = restaurants_df.select(
    F.col("name"),
    concat(F.col("city"), lit(", "), F.col("country")).alias("location")
)
print("Restauran with location:")
full_info.show(5, truncate=False)

from pyspark.sql.functions import round, sqrt, abs

# Round ratings to whole numbers
rounded_ratings = restaurants_df.select(
    F.col("name"),
    F.col("average_rating"),
    round(F.col("average_rating"), 0).alias("rounded_rating")
)
print("Rounded rating:")
rounded_ratings.show(5)

# Calculate a simple score from ratings and reviews
restaurant_scores = restaurants_df.select(
    F.col("name"),
    F.col("average_rating"),
    F.col("num_reviews"),
    round(F.col("average_rating") * sqrt(F.col("num_reviews") / 1000), 2).alias("score")
)

print("Restaurants Scores:")
restaurant_scores.show(5)

### Date and Time Operations
from pyspark.sql.functions import to_timestamp, date_format, current_timestamp, datediff

# Convert string timestamp to date
whith_data = restaurants_df.select(
    F.col("name"),
    F.col("dt_current_timestamp"),
    to_timestamp(F.col("dt_current_timestamp")).alias("timestamp")
)
print("Whith timestamp")
whith_data.show(5, truncate=False)
whith_data.printSchema()

# Format dates
formatted_dates = whith_data.select(
    F.col("name"),
    F.col("timestamp"),
    date_format(F.col("timestamp"), "yyyy-MM-dd").alias("Only_date"),
    date_format(F.col("timestamp"), "HH:mm:ss").alias("Only_time")
)
print("Formatted dates:")
formatted_dates.show(5)

## 5. Adding and Dropping Columns

# Add a new column
with_category = restaurants_df.withColumn(
    "rating_category",
    F.when(F.col("average_rating") >= 4.5, "Excellent")
    .when(F.col("average_rating") >= 4.0, "Very Good")
    .when(F.col("average_rating") >= 3.5, "Good")
    .when(F.col("average_rating") >= 3.0, "Average")
    .otherwise("Poor")
)

print("With Rating Category:")
with_category.select("name", "average_rating", "rating_category").show(10)

### Dropping Columns
simplified_df = restaurants_df.drop("uuid", "dt_current_timestamp")

# Check the columns after dropping
print("Columns after dropping:")
print(simplified_df.columns)

## 6. Combining Operations

restaurant_analysis = restaurants_df.select(
    F.col("name"),
    F.col("cuisine_type"),
    F.col("city"),
    F.col("average_rating"),
    F.col("num_reviews")
).filter(
    F.col("num_reviews") > 1000
).withColumn(
    "rating_category",
    F.when(F.col("average_rating") >= 4.5, "Excellent")
    .when(F.col("average_rating") >= 4.0, "Very Good")
    .when(F.col("average_rating") >= 3.5, "Good")
    .when(F.col("average_rating") >= 3.0, "Average")
    .otherwise("Poor")
).withColumn(
    "popularity_score",
    round(F.col("average_rating") * sqrt(F.col("num_reviews") / 1000), 2)
).orderBy(
    F.col("popularity_score").desc()
)

print("Restaurant Analysis:")
restaurant_analysis.show(10, truncate=False)

## Practical Exercise: UberEats Restaurant Report

def create_restaurant_df(restaurants_df):

    import pyspark.sql.functions as F

# Process the data
    report_df = restaurants_df.select(
        F.col("name"),
        F.col("cuisine_type"),
        F.col("city"),
        F.col("average_rating"),
        F.col("num_reviews"),
        F.col("opening_time"),
        F.col("closing_time")
    ).withColumn(
        "rating_category",
        F.when(F.col("average_rating") >= 4.5, "Excellent")
        .when(F.col("average_rating") >= 4.0, "Very Good")
        .when(F.col("average_rating") >= 3.5, "Good")
        .when(F.col("average_rating") >= 3.0, "Average")
        .otherwise("Poor")
    ).withColumn(
        "popularity_score",
        round(F.col("average_rating") * sqrt(F.col("num_reviews") / 1000),2)
    ).withColumn(
        "hours_of_operation",
        concat(F.col("opening_time"), lit(" - "), F.col("closing_time"))
    ).drop("opening_time", "closing_time")

    # Generate summary by cuisine type

    cuisine_summary = restaurants_df.groupBy("cuisine_type").agg(
        F.count("*").alias("restaurant_count"),
        round(F.avg("average_rating"), 2).alias("avg_rating"),
        round(F.avg("num_reviews"), 0).alias("avg_reviews")
    ).orderBy(F.desc("restaurant_count"))

    return {
    "restaurant_details": report_df.orderBy(F.desc("popularity_score")),
    "cuisine_summary": cuisine_summary
}

# Generate the report
report = create_restaurant_df(restaurants_df)

# Display the results
print("Top Restaurants by Popularity:")
report["restaurant_details"].show(10, truncate=False)

print("\nCuisine Type Summary:")
report["cuisine_summary"].show(10, truncate=False)

spark.stop()