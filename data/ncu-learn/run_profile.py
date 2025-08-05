# 文件名: run_profile.py
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, sum

os.environ["CUDA_VISIBLE_DEVICES"] = "2"

def main():
    spark = SparkSession.builder \
        .appName("RapidsProfileDemo") \
        .master("local[*]") \
        .config("spark.plugins", "com.nvidia.spark.SQLPlugin") \
        .config("spark.rapids.sql.enabled", "true") \
        .config("spark.rapids.sql.explain", "NONE") \
        .config("spark.executorEnv.CUDA_VISIBLE_DEVICES", "2") \
        .config("spark.jars", "/root/spark_rapids_dev/source/rapids-4-spark_2.12-25.06.0.jar") \
        .getOrCreate()

    input_path = "ncu_test_data.parquet"
    df = spark.read.parquet(input_path)

    result = df.groupBy("group") \
        .agg(sum("value").alias("sum_value")) \
        .orderBy("group")

    # ==========================================================
    # 关键步骤：添加或取消注释下面的 Action 操作来触发计算
    # ==========================================================
    print("Triggering Spark Action to start GPU computation...")
    result.collect() 
    print("Computation finished.")

    spark.stop()

if __name__ == "__main__":
    main()