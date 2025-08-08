# 文件名: local_profile_zero_copy.py
import os
import nvtx
from pyspark.sql import SparkSession
from pyspark.sql.functions import sum

os.environ["CUDA_VISIBLE_DEVICES"] = "2"

def profile_local_memory_transfer():

    with nvtx.annotate("1. Spark Session Initialization", color="blue"):
        spark = SparkSession.builder \
            .appName("RapidsProfileDemo") \
            .master("local[*]") \
            .config("spark.plugins", "com.nvidia.spark.SQLPlugin") \
            .config("spark.rapids.sql.enabled", "true") \
            .config("spark.rapids.sql.explain", "NONE") \
            .config("spark.rapids.memory.gpu.allocFraction", "0.4") \
            .config("spark.jars", "/root/spark_rapids_dev/source/rapids-4-spark_2.12-25.06.0.jar") \
            .getOrCreate()

    with nvtx.annotate("2. Data Loading", color="green"):
        input_path = "local_profile_data.parquet"
        df = spark.read.parquet(input_path)
        print(f"数据从 '{input_path}' 加载完成地。")

    with nvtx.annotate("3. Stage 1: Aggregation & Caching", color="orange"):
        aggregated_df = df.groupBy("group_key").agg(sum("value1").alias("total_value"))
        aggregated_df.cache()
        count = aggregated_df.count()

    with nvtx.annotate("4. Stage 2: Reusing Cached Data for Sort", color="red"):

        sorted_df = aggregated_df.orderBy("total_value")


        with nvtx.annotate("4.1. Final Computation (Collect)", color="purple"):
            sorted_df.collect()

    spark.stop()

if __name__ == "__main__":
    profile_local_memory_transfer()