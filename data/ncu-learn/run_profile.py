# 文件名: run_profile.py
import os
import nvtx
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, sum

os.environ["CUDA_VISIBLE_DEVICES"] = "2"

def main():
    with nvtx.annotate("Spark Session Initialization"):
        spark = SparkSession.builder \
            .appName("RapidsProfileDemo") \
            .master("local[*]") \
            .config("spark.plugins", "com.nvidia.spark.SQLPlugin") \
            .config("spark.rapids.sql.enabled", "true") \
            .config("spark.rapids.sql.explain", "NONE") \
            .config("spark.executorEnv.CUDA_VISIBLE_DEVICES", "2") \
            .config("spark.rapids.memory.gpu.pooling.enabled", "true") \
            .config("spark.rapids.memory.gpu.allocFraction", "0.25") \
            .config("spark.rapids.memory.gpu.maxAllocFraction", "0.25") \
            .config("spark.rapids.memory.gpu.minAllocFraction", "0.1") \
            .config("spark.rapids.memory.gpu.reserve", "5G") \
            .config("spark.jars", "/root/spark_rapids_dev/source/rapids-4-spark_2.12-25.06.0.jar") \
            .getOrCreate()

    with nvtx.annotate("Data Loading"):
        input_path = "ncu_test_data.parquet"
        df = spark.read.parquet(input_path)
        # 触发数据加载
        df.cache()
        df.count()

    with nvtx.annotate("Data Transformation"):
        # 分步骤进行转换操作
        with nvtx.annotate("GroupBy Operation"):
            grouped_df = df.groupBy("group")
        
        with nvtx.annotate("Aggregation"):
            agg_df = grouped_df.agg(sum("value").alias("sum_value"))
        
        with nvtx.annotate("Sorting"):
            result = agg_df.orderBy("group")

    print("Triggering Spark Action to start GPU computation...")
    with nvtx.annotate("Final Computation"):
        result.collect()
    print("Computation finished.")

    with nvtx.annotate("Spark Shutdown"):
        spark.stop()

if __name__ == "__main__":
    main()