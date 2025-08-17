import os
import nvtx
from pyspark.sql import SparkSession

# 将环境变量设置放在主函数保护之外，确保在SparkSession启动前生效
os.environ["CUDA_VISIBLE_DEVICES"] = "1"

def analyze_plan_with_cache_demo():

    with nvtx.annotate("1. Spark Session Initialization", color="blue"):
        spark = SparkSession.builder \
            .appName("RapidsCacheAnalysisDemo") \
            .master("local[*]") \
            .config("spark.plugins", "com.nvidia.spark.SQLPlugin") \
            .config("spark.rapids.sql.enabled", "true") \
            .config("spark.rapids.sql.explain", "ALL") \
            .config("spark.rapids.memory.gpu.allocFraction", "0.5") \
            .config("spark.jars", "/root/spark_rapids_dev/source/rapids-4-spark_2.12-25.06.0.jar") \
            .config("spark.rapids.sql.exec.InMemoryTableScanExec", "true") \
            .config("spark.sql.cache.serializer", "com.nvidia.spark.ParquetCachedBatchSerializer") \
            .getOrCreate()

    with nvtx.annotate("2. Data Loading and Caching", color="green"):
        input_path = "local_profile_data.parquet"

        df = spark.read.parquet(input_path).cache()
        
        print(f"数据将从 '{input_path}' 加载。")
        
        df.count()
        print("count完成。")


    with nvtx.annotate("3. First Usage: Aggregation", color="red"):
        print("\n===== 第一个任务：执行聚合操作 =====")
        agg_df = df.groupBy("group_key").agg({"value1": "avg"})
        print("聚合操作的物理执行计划：")
        agg_df.explain(extended = True)

        agg_df.collect()
        print("聚合操作完成。")


    # with nvtx.annotate("4. Second Usage: Filtering", color="purple"):
    #     print("\n===== 第二个任务：执行过滤操作 =====")
    #     # 假设你的 Parquet 文件中有 'value2' 列
    #     # 这个操作将复用已缓存的 df，而不是重新从磁盘读取
    #     filtered_df = df.filter("value2 > 0.95")
        
    #     # 关键步骤：再次打印执行计划进行分析
    #     print("过滤操作的物理执行计划：")
    #     filtered_df.explain()

    #     # 触发过滤操作
    #     filtered_df.show()
    #     print("过滤操作完成。")


    with nvtx.annotate("5. Uncaching and Stopping", color="grey"):
        # 释放缓存
        df.unpersist()
        spark.stop()

if __name__ == "__main__":
    analyze_plan_with_cache_demo()