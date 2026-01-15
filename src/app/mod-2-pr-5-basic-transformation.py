"""
docker exec -it spark-master `
  /opt/bitnami/spark/bin/spark-submit `
  --master spark://spark-master:7077 `
  --deploy-mode client `
  /opt/bitnami/spark/jobs/app/mod-2-pr-5-basic-transformation.py
"""
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, upper, lower, concat, lit, round, sqrt, abs, to_timestamp, date_format, current_timestamp, datediff, when

# Create a Spark session
spark = SparkSession.builder \
    .appName("Data Ingestion") \
    .master("local[*]") \
    .getOrCreate()

# Load datasets
df_restaurants = spark.read.json("./storage/mysql/restaurants/01JS4W5A7YWTYRQKDA7F7N95VY.jsonl")
df_drivers = spark.read.json("./storage/postgres/drivers/01JS4W5A74BK7P4BPTJV1D3MHA.jsonl")
df_orders = spark.read.json("./storage/kafka/orders/01JS4W5A7XY65S9Z69BY51BEJ4.jsonl")

'''
# Display the schemas for reference
print("Restaurants Schema:")
df_restaurants.printSchema()

print("Drivers Schema:")
df_drivers.printSchema()

print("Orders Schema:")
df_orders.printSchema()

# Select columns and using col() function and using alias
restaurants_details = df_restaurants.select(
    col("name").alias("restaurant_name"), 
    col("cuisine_type").alias("cuisine"),
    col("num_reviews").alias("review_count"), 
    col("average_rating").alias("rating")
)

# show result
print("Restaurants Details:")
restaurants_details.show(5, truncate=False)
'''
'''
# Filter restaurants with high ratings
high_rated = df_restaurants.filter(col("average_rating") > 4)
print(f"Number of high-rated restaurants: {high_rated.count()}")
high_rated.select("name", "cuisine_type", "average_rating").show()
'''
'''
# Filter with multiple conditions
popular_italian = df_restaurants.filter(
    (col("cuisine_type") == "Italian") &
    (col("num_reviews") > 3000)
)

print(f"Number of popular Italian restaurants: {popular_italian.count()}")
popular_italian.select("name", "city", "cuisine_type", "average_rating", "num_reviews").show(5)
'''

## 3. Logical and Comparison Operators
'''
# Restaurants with exactly 4.2 rating
exact_rating = df_restaurants.filter(col("average_rating") == 4.2)
print(f"Restaurants with exactly 4.2 rating: {exact_rating.count()}")
exact_rating.select("name", "city", "cuisine_type", "average_rating", "num_reviews").show()
'''
'''
# Restaurants with rating not equal to 3.0
not_average = df_restaurants.filter(col("average_rating") != 3.0)
print(f"Restaurants with rating not equal to 3.0: {not_average.count()}")
'''
'''
# Transform restaurant names to uppercase
upeper_restaurant_name = df_restaurants.select(
    upper(col("name")),
    col("name"),
    col("cuisine_type")
)
print("Uppercase Restaurant name:")
upeper_restaurant_name.show(5)
'''
'''
# Concatenate columns
full_info = df_restaurants.select(
    col("name"),
    concat(col("city"), lit(", "), col("country")).alias("location")
)

print("Restaurant with Location:")
full_info.show(5, truncate=False)
'''
'''
### Numeric Operations
rounded_ratings = df_restaurants.select(
    col("name"),
    col("average_rating"),
    round(col("average_rating"), 0).alias("rounded_rating")
)
print("Rounded Ratings:")
rounded_ratings.show(5)
'''
'''
# Calculate a simple score from ratings and reviews
restaurant_score = df_restaurants.select(
    col("name"),
    col("average_rating"),
    col("num_reviews"),
    round(col("average_rating") * sqrt(col("num_reviews") / 1000), 2).alias("score")
)
print("Restaurant Scores:")
restaurant_score.orderBy(col("score").desc()).show(5)
'''
'''
### Date and Time Operations

# Convert string timestamp to date
with_dates = df_restaurants.select(
    col("name"),
    col("dt_current_timestamp"),
    to_timestamp(col("dt_current_timestamp")).alias("timestamp")
)

print("With Timestamp:")
with_dates.show(5)

# Format dates
formatted_dates = with_dates.select(
    col("name"),
    col("timestamp"),
    date_format(col("timestamp"), "yyyy-MM-dd").alias("date_only"),
    date_format(col("timestamp"), "HH:mm:ss").alias("time_only")
)

print("Formatted Dates:")
formatted_dates.show(5)
'''

## 5. Adding and Dropping Columns
with_category = df_restaurants.withColumn(
    "rating_category",
    when(col("average_rating") >= 4.5, "Excellent")
    .when(col("average_rating") >= 4.0, "Very Good")
    .when(col("average_rating") >= 3.5, "Good")
    .when(col("average_rating") >= 3, "Average")
    .otherwise("Poor")
)
print("With Rating Category:")
with_category.select("name", "average_rating", "rating_category").show(5)

spark.stop()
