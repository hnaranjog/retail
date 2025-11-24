from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType, StructField,
    StringType, IntegerType, DoubleType, TimestampType
)


def get_spark_session():
    """
    Crea una SparkSession para Structured Streaming con soporte Kafka.
    También establece la integración con HDFS si se requiere.
    """
    spark = (
        SparkSession.builder
        .appName("RetailKafkaStreaming")
        # Ajusta al mismo fs.defaultFS de tu clúster
        .config("spark.hadoop.fs.defaultFS", "hdfs://localhost:9000")
        .getOrCreate()
    )

    # Para que el log no sea tan ruidoso
    spark.sparkContext.setLogLevel("WARN")
    return spark


def main():
    spark = get_spark_session()

    # Esquema del JSON que envía kafka_producer.py
    schema = StructType([
        StructField("timestamp", StringType(), True),
        StructField("product", StringType(), True),
        StructField("country", StringType(), True),
        StructField("quantity", IntegerType(), True),
        StructField("amount", DoubleType(), True),
    ])

    # Lectura en streaming desde Kafka
    kafka_df = (
        spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", "localhost:9092")
        .option("subscribe", "retail-stream")
        .option("startingOffsets", "latest")
        .load()
    )

    # value viene como bytes -> string -> JSON
    json_df = (
        kafka_df
        .select(F.col("value").cast("string").alias("json_str"))
        .select(F.from_json("json_str", schema).alias("data"))
        .select("data.*")
    )

    # Convertir timestamp string a TimestampType
    json_df = json_df.withColumn(
        "event_time",
        F.to_timestamp("timestamp")
    )

    # Ventana de 1 minuto por país, con conteo e importe total
    agg_df = (
        json_df
        .withWatermark("event_time", "2 minutes")
        .groupBy(
            F.window("event_time", "1 minute"),
            F.col("country")
        )
        .agg(
            F.count("*").alias("num_eventos"),
            F.sum("amount").alias("monto_total"),
            F.sum("quantity").alias("unidades_totales")
        )
        .orderBy(F.col("window").asc(), F.col("country").asc())
    )

    # Salida en consola
    query = (
        agg_df.writeStream
        .outputMode("complete")          # update: actualiza agregados
        .format("console")             # salida estándar
        .option("truncate", "false")   # no truncar columnas
        .option("numRows", 50)
        .start()
    )

    print("Aplicación Spark Streaming en ejecución. Ctrl+C para detener.")
    query.awaitTermination()


if __name__ == "__main__":
    main()
