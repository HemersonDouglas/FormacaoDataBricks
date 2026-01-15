"""
docker exec -it spark-master`
  /opt/bitnami/spark/bin/spark-submit`
  --master spark://spark-master:7077`
  --deploy-mode client`
  /opt/bitnami/spark/jobs/app/get-users-json.py
"""
"""
# Use a barra invertida "\" para pular linha
# Use duas barras "//" no início dos caminhos para evitar que o Git Bash os converta

docker exec -it spark-master \
  //opt/bitnami/spark/bin/spark-submit \
  --master spark://spark-master:7077 \
  --deploy-mode client \
  //opt/bitnami/spark/jobs/app/get-users-json.py
"""

from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .getOrCreate()

df_users = spark.read.json("/opt/bitnami/spark/storage/users.json")
count = df_users.count()
df_users.show(3)

spark.stop()


