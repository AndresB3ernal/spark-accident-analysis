from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StructType, StringType, IntegerType

spark = SparkSession.builder \
    .appName("Accident Analysis - Streaming") \
    .getOrCreate()

schema = StructType().add("BOROUGH", StringType()).add("ACCIDENT_COUNT", IntegerType())
stream_df = spark.readStream.format("kafka").option("kafka.bootstrap.servers", "localhost:9092") \
.option("subscribe", "traffic-accidents").load()

parsed_df = stream_df.selectExpr("CAST(value AS STRING)") \
.select(from_json(col("value"), schema).alias("data")).select("data.*")

query = parsed_df.writeStream.outputMode("append").format("console").start()
query.awaitTermination()
