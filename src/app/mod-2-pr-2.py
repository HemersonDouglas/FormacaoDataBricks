"""
docker exec -it spark-master \
  //opt/bitnami/spark/bin/spark-submit \
  --master spark://spark-master:7077 \
  --deploy-mode client \
  //opt/bitnami/spark/jobs/app/mod-2-pr-2.py
"""

from pyspark.sql import SparkSession

# 1 = create spark session
spark = SparkSession.builder \
    .config("spark.executor.memory", "512m") \
    .config("spark.driver.memory", "1g") \
    .getOrCreate() 

# 2 = create spark context
sc = spark.sparkContext

# 3 = the spark code here

# 4 = stop the spark context
spark.stop()