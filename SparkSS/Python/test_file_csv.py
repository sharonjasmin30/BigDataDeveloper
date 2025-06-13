from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StringType, DoubleType, TimestampType
from pyspark.sql.functions import col, window

# Crear sesión Spark
spark = SparkSession.builder \
    .appName("SparkSS_Temperatura_Por_Sensor_Ejemplo") \
    .getOrCreate()

# Reducir ruido de logs
spark.sparkContext.setLogLevel("ERROR")

# Definir esquema del CSV
schema = StructType() \
    .add("fecha", TimestampType()) \
    .add("sensor", StringType()) \
    .add("temperatura", DoubleType())

# Leer archivos CSV desde una carpeta (modo streaming)
df = spark.readStream \
    .option("sep", ",") \
    .schema(schema) \
    .csv("/workspaces/BigDataDeveloper/SparkSS/Python/file_input")  # <-- reemplaza con tu ruta

# Agregar promedio de temperatura por sensor cada 10 segundos
resumen = df.groupBy(
    window(col("fecha"), "10 seconds"),
    col("sensor")
).avg("temperatura")

# Mostrar resultados en consola
query = resumen.writeStream \
    .outputMode("complete") \
    .format("console") \
    .option("truncate", False) \
    .start()

query.awaitTermination()
