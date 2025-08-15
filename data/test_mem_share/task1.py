import os
import nvtx
from pyspark.sql import SparkSession

# 将环境变量设置放在主函数保护之外，确保在SparkSession启动前生效
os.environ["CUDA_VISIBLE_DEVICES"] = "1"

def profile_local_memory_transfer_no_groupby():
    """
    一个修改版的分析脚本，去除了groupBy操作，
    以更好地观察join操作的内存使用情况。
    """
    with nvtx.annotate("1. Spark Session Initialization", color="blue"):
        spark = SparkSession.builder \
            .appName("RapidsProfileDemoNoGroupBy") \
            .master("local[*]") \
            .config("spark.plugins", "com.nvidia.spark.SQLPlugin") \
            .config("spark.rapids.sql.enabled", "true") \
            .config("spark.rapids.sql.explain", "NONE") \
            .config("spark.rapids.memory.gpu.allocFraction", "0.5") \
            .config("spark.jars", "/root/spark_rapids_dev/source/rapids-4-spark_2.12-25.06.0.jar") \
            .getOrCreate()

    """
    目标：观察 cache() 如何将数据保留在 GPU 内存中，以加速后续操作。
    场景：一个基础 DataFrame 被多次用于不同的计算分支。
    """
    
    with nvtx.annotate("1. Data Loading", color="green"):
        input_path = "local_profile_data_3m.parquet"
        df = spark.read.parquet(input_path)
        print(f"数据从 '{input_path}' 加载完成。")

    with nvtx.annotate("2. Caching Data to GPU", color="orange"):
        # cache() 会提示 RAPIDS 将这个 DataFrame 的分区优先保留在 GPU 内存中
        df.cache()
        # 第一次执行 Action，触发数据加载和缓存。这是“冷”读取。
        initial_count = df.count()
        print(f"数据已缓存，总行数: {initial_count}")

    with nvtx.annotate("3. Reuse Cached Data for Sort", color="red"):
        # 第二次使用 df。这次应该会从 GpuCache 中读取，速度更快。
        sorted_df = df.orderBy("value1")
        sorted_df.collect() # 执行 Action
        print("第一次复用：排序操作完成。")

    with nvtx.annotate("4. Reuse Cached Data for Join", color="blue"):
        # 第三次使用 df。同样应该从 GpuCache 读取。
        # 注意：这里的别名是防止自连接时列名冲突的关键
        df_aliased = df.alias("df2")
        joined_df = df.join(df_aliased, df["group_key"] == df_aliased["group_key"], "inner")
        join_count = joined_df.count() # 执行 Action
        print(f"第二次复用：Join 操作完成，结果行数: {join_count}")
        
    spark.stop()

if __name__ == "__main__":
    profile_local_memory_transfer_no_groupby()