from pyspark.sql import SparkSession
import time

# 1. Crear sesión de Spark
spark = SparkSession.builder \
    .appName("StructuredStreamingApp") \
    .getOrCreate()


spark.sparkContext.setLogLevel("ERROR")  # O "WARN", "INFO", "DEBUG"

# 2. Leer desde una fuente de streaming simulada (rate)
df = spark.readStream \
    .format("rate") \
    .option("rowsPerSecond", 1) \
    .load()

# 3. Escribir a memoria
query = df.writeStream \
    .format("memory") \
    .queryName("rate_table") \
    .outputMode("append") \
    .start()

# 4. Esperar para acumular datos
time.sleep(10)

# 5. Consultar desde la tabla en memoria
spark.sql("SELECT * FROM rate_table").show()

# 6. Detener streaming
query.stop()
spark.stop()
