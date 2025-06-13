from pyspark.sql import SparkSession
from pyspark.sql.functions import explode, split

spark = SparkSession.builder.appName("SparkSS_WordCount_ejemplo2").getOrCreate()
spark.sparkContext.setLogLevel("ERROR")

# Fuente: socket en localhost:9999
lines = spark.readStream.format("socket") \
    .option("host", "localhost") \
    .option("port", 9999).load()

words = lines.select(explode(split(lines.value, " ")).alias("word"))

wordCounts = words.groupBy("word").count()

# Salida a consola
query = wordCounts.writeStream \
    .outputMode("complete") \
    .format("console") \
    .start()

query.awaitTermination()
