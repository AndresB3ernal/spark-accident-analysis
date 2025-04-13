from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("Accident Analysis - Batch") \
    .getOrCreate()

accidents = spark.read.csv("../data/sample_accidents.csv", header=True, inferSchema=True)
df_clean = accidents.dropna()
df_result = df_clean.groupBy("BOROUGH").count()
df_result.show()
df_result.write.parquet("../output/accidents_by_zone.parquet")
spark.stop()
