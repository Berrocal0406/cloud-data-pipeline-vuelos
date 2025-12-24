from pyspark.sql import SparkSession, functions as F
from src.utils.paths import SILVER_DIR, GOLD_DIR

def main():
    spark = (
        SparkSession.builder
        .appName("routes-transform-gold")
        .master("local[*]")
        .config("spark.driver.host", "127.0.0.1")
        .config("spark.driver.bindAddress", "127.0.0.1")
        .getOrCreate()
    )

    silver_path = str (SILVER_DIR/"routes_silver.csv")

    df = (
        spark.read
        .option("header", True)
        .option("inferSchema", True)
        .csv(silver_path)
    )

    print("SILVER rows:", df.count())
    df.printSchema()

    # Transformaciones tipo "Gold"

    df_gold = (
        df
        #Normalizar Strings
        .withColumn("source_airport", F.upper(F.trim(F.col("source_airport"))))
        .withColumn("dest_airport", F.upper(F.trim(F.col("dest_airport"))))
        # Asegurar stops como int y rango válido
        .withColumn("stops", F.coalesce(F.col("stops").cast("int"), F.lit(0)))
        .withColumn("stops", F.when(F.col("stops") < 0, 0).otherwise(F.col("stops")))
        # Flag útil para análisis
        .withColumn("is_direct", F.when(F.col("stops") == 0, F.lit(1)).otherwise(F.lit(0)))
        # Quitar filas inválidas esenciales
        .filter(F.col("source_airport").isNotNull() & (F.col("source_airport") != ""))
        .filter(F.col("dest_airport").isNotNull() & (F.col("dest_airport") != ""))
    )

    # Dataset Gold: métricas por par (origen-destino)
    agg = (
        df_gold
        .groupBy("source_airport", "dest_airport")
        .agg(
            F.count("*").alias("routes_count"),
            F.sum("is_direct").alias("direct_routes_count"),
            F.avg("stops").alias("avg_stops")
        )
        .orderBy(F.desc("routes_count"))
    )

    out_path = str(GOLD_DIR / "routes_gold.parquet")
    agg.write.mode("overwrite").parquet(out_path)

    print("GOLD saved to:", out_path)
    print("Top 10 routes:")
    agg.show(10, truncate=False)

    spark.stop()

if __name__ == "__main__":
    main()