import os
import sys
from pyspark.sql import SparkSession

def main():
    python_exe = sys.executable

    # Fuerza por ENV (más fuerte que sólo SparkConf)
    os.environ["PYSPARK_PYTHON"] = python_exe
    os.environ["PYSPARK_DRIVER_PYTHON"] = python_exe
    os.environ["SPARK_LOCAL_IP"] = "127.0.0.1"

    print("sys.executable:", python_exe)
    print("PYSPARK_PYTHON:", os.environ.get("PYSPARK_PYTHON"))
    print("PYSPARK_DRIVER_PYTHON:", os.environ.get("PYSPARK_DRIVER_PYTHON"))

    spark = (
        SparkSession.builder
        .appName("spark-smoke-test")
        .master("local[*]")
        .config("spark.driver.host", "127.0.0.1")
        .config("spark.driver.bindAddress", "127.0.0.1")
        .config("spark.pyspark.python", python_exe)
        .config("spark.pyspark.driver.python", python_exe)
        .config("spark.local.dir", "C:\\spark-temp")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("ERROR")
    print("spark.pyspark.python:", spark.sparkContext.getConf().get("spark.pyspark.python", "MISSING"))
    print("spark.pyspark.driver.python:", spark.sparkContext.getConf().get("spark.pyspark.driver.python", "MISSING"))

    df = spark.createDataFrame([(1, "ok"), (2, "spark"), (3, "works")], ["id", "status"])
    df.show(truncate=False)

    spark.stop()

if __name__ == "__main__":
    main()
