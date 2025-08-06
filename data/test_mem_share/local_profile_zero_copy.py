# 文件名: local_profile_zero_copy.py
import os
import nvtx
from pyspark.sql import SparkSession
from pyspark.sql.functions import sum

os.environ["CUDA_VISIBLE_DEVICES"] = "2"

def profile_local_memory_transfer():
    """
    在单机单 GPU 环境下，剖析 GPU 任务间的内存流转（零拷贝）场景。
    """
    with nvtx.annotate("1. Spark Session Initialization", color="blue"):
        spark = SparkSession.builder\
            .appName("LocalZeroCopyProfile") \
            .master("local[4]") \
            .config("spark.plugins", "com.nvidia.spark.SQLPlugin")\
            .config("spark.rapids.sql.enabled", "true")\
            .config("spark.rapids.sql.concurrentGpuTasks", "1")\
            .config("spark.rapids.memory.gpu.maxAllocFraction", "0.4")\
            .config("spark.rapids.memory.gpu.pooling.enabled", "true")\
            .getOrCreate()

    with nvtx.annotate("2. Data Loading", color="green"):
        input_path = "local_profile_data.parquet"
        df = spark.read.parquet(input_path)
        print(f"数据从 '{input_path}' 加载完成。")

    # ------------ 阶段一：聚合与缓存物化 ------------
    with nvtx.annotate("3. Stage 1: Aggregation & Caching", color="orange"):
        # 这个宽依赖操作的结果，我们将尝试缓存到 GPU
        aggregated_df = df.groupBy("group_key").agg(sum("value1").alias("total_value"))
        
        aggregated_df.cache()

        print("触发 Stage 1 计算，物化缓存...")
        # .count() action 会触发计算并填充缓存
        count = aggregated_df.count()
        print(f"Stage 1 完成。聚合后的数据有 {count} 行。")
        # 此时，aggregated_df 的数据应该在 GPU 显存中（如果空间足够）

    # ------------ 阶段二：复用缓存数据进行排序 ------------
    with nvtx.annotate("4. Stage 2: Reusing Cached Data for Sort", color="red"):
        # orderBy 是另一个宽依赖，它将读取上一个 Stage 的全部输出
        # 这是我们观察零拷贝是否发生的关键点
        sorted_df = aggregated_df.orderBy("total_value")

        print("触发 Stage 2 计算，复用缓存数据...")
        # .collect() action 触发计算
        with nvtx.annotate("4.1. Final Computation (Collect)", color="purple"):
            sorted_df.collect()
        print("Stage 2 完成。")

    print("\n按任意键退出 Spark Session...")
    input()
    spark.stop()

if __name__ == "__main__":
    profile_local_memory_transfer()