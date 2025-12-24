from pyspark.sql import SparkSession
from src.utils.paths import GOLD_DIR

def main():
    spark = SparkSession.builder.master("local[*]").appName("check-gold").getOrCreate()
    path = str(GOLD_DIR / "routes_gold.parquet")
    df = spark.read.parquet(path)
    print("rows:", df.count())
    df.show(5, truncate=False)
    spark.stop()

if __name__ == "__main__":
    main()
