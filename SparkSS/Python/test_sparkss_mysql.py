from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StringType, DoubleType, TimestampType
from pyspark.sql.functions import col, window

# Crear sesión Spark
spark = SparkSession.builder \
    .appName("SparkSS_Temperatura_To_MySQL_Ejemplo5") \
    .getOrCreate()

# Reducir verbosidad de logs
spark.sparkContext.setLogLevel("ERROR")

# Esquema del CSV
schema = StructType() \
    .add("fecha", TimestampType()) \
    .add("sensor", StringType()) \
    .add("temperatura", DoubleType())

# Leer el CSV en modo streaming
df = spark.readStream \
    .option("sep", ",") \
    .schema(schema) \
    .csv("/workspaces/BigDataDeveloper/SparkSS/Python/file_input/file_input_streaming")  # reemplaza si es necesario

# Calcular promedio cada 10 segundos por sensor
resumen = df.groupBy(
    window(col("fecha"), "10 seconds"),
    col("sensor")
).avg("temperatura") \
.withColumnRenamed("avg(temperatura)", "temperatura_promedio") \
.select(
    col("window.start").alias("ventana_inicio"),
    col("window.end").alias("ventana_fin"),
    col("sensor"),
    col("temperatura_promedio")
)

# Configuración JDBC
jdbc_url = "jdbc:mysql://localhost:3310/sensores"
jdbc_props = {
    "user": "root",
    "password": "root",  # cambia si tu contraseña es distinta
    "driver": "com.mysql.cj.jdbc.Driver"
}

# Función para guardar en MySQL y mostrar mensaje
def guardar_en_mysql(df_batch, epoch_id):
    if df_batch.isEmpty():
        print(f"[Epoch {epoch_id}] : No hay datos nuevos para guardar.")
    else:
        df_batch.write \
            .mode("append") \
            .jdbc(url=jdbc_url, table="resumen_temperatura", properties=jdbc_props)
        print(f"[Epoch {epoch_id}] : Datos guardados exitosamente en MySQL.")
        df_batch.show(truncate=False)

# Escribir en MySQL
query = resumen.writeStream \
    .foreachBatch(guardar_en_mysql) \
    .outputMode("update") \
    .start()

query.awaitTermination()