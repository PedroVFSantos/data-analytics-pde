from pyspark.sql import SparkSession
from pyspark.sql.functions import hour, count, col

spark = SparkSession.builder \
    .appName("BCC-UFSCar-UberScaleAnalysis") \
    .getOrCreate()

input_path = "/opt/spark/work-dir/data/uber-raw-data-*.csv"
output_path = "/opt/spark/work-dir/data/resultado_pico_uber"

print("\n>>> Carregando dados (Lidando com a Variedade de schemas)...")
df = spark.read.csv(input_path, header=True, inferSchema=True)

df.printSchema()

print(">>> Iniciando processamento distribuído (MapReduce)...")
analise_horaria = df.withColumn("hour", hour(col("Pickup_date"))) \
                    .groupBy("hour") \
                    .agg(count("*").alias("num_viagens")) \
                    .orderBy("num_viagens", ascending=False)

analise_horaria.show(24)
print(f">>> Gravando resultados consolidados em: {output_path}")
analise_horaria.write.mode("overwrite").csv(output_path)

print(">>> Job finalizado com sucesso!")
spark.stop()