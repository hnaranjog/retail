from pyspark.sql import SparkSession

spark = (
    SparkSession.builder
    .appName("BatchProcessingRetail")
    # IMPORTANTE: usar el mismo fs.defaultFS que devuelve hdfs getconf
    .config("spark.hadoop.fs.defaultFS", "hdfs://localhost:9000")
    .getOrCreate()
)

df = spark.read.csv(
    "hdfs:///data/online_retail_II.csv",  # ruta que ya vimos en hdfs dfs -ls /data
    header=True,
    inferSchema=True
)

print("Número de registros:", df.count())
df.printSchema()

spark.stop()
