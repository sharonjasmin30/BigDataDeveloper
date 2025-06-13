from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StringType, DoubleType, TimestampType
from pyspark.sql.functions import col, window, round, avg

# Crear sesión Spark
spark = SparkSession.builder \
    .appName("SparkSS_Temperatura_Por_Sensor_Ejemplo4") \
    .getOrCreate()

# Reducir ruido de logs
spark.sparkContext.setLogLevel("ERROR")

# Definir esquema del CSV (sin encabezado)
schema = StructType() \
    .add("fecha", TimestampType()) \
    .add("sensor", StringType()) \
    .add("temperatura", DoubleType())

# Leer archivos CSV desde carpeta (modo streaming)
df = spark.readStream \
    .option("sep", ",") \
    .schema(schema) \
    .csv("/workspaces/BigDataDeveloper/SparkSS/Python/file_input/file_input_streaming")  # Ajusta tu ruta aquí

# Calcular promedio por sensor cada 10 segundos
resumen = df.groupBy(
    window(col("fecha"), "10 seconds"),
    col("sensor")
).agg(
    #round(col("temperatura").avg(), 2).alias("prom_temp")
    round(avg("temperatura"), 2).alias("prom_temp")

)

# Seleccionar columnas con nombres más claros
resumen_limpio = resumen.select(
    col("window.start").alias("inicio_ventana"),
    col("window.end").alias("fin_ventana"),
    col("sensor"),
    col("prom_temp")
)

# Mostrar resultados en consola
query = resumen_limpio.writeStream \
    .outputMode("complete") \
    .format("console") \
    .option("truncate", False) \
    .option("numRows", 50) \
    .start()

query.awaitTermination()