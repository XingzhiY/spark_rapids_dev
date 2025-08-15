import os
import nvtx
from pyspark.sql import SparkSession
from pyspark.sql.functions import sum
from pyspark.sql.window import Window

os.environ["CUDA_VISIBLE_DEVICES"] = "1"

def profile_local_memory_transfer_modified():
    """
    修改后的 Spark 作业：
    1. 使用窗口函数代替 groupBy 来避免数据量减少。
    2. 在每个主要步骤后使用 .cache().count() 来触发立即执行。
    """

    with nvtx.annotate("1. Spark Session Initialization", color="blue"):
        spark = SparkSession.builder \
            .appName("RapidsProfileDemoModified") \
            .master("local[*]") \
            .config("spark.plugins", "com.nvidia.spark.SQLPlugin") \
            .config("spark.rapids.sql.enabled", "true") \
            .config("spark.rapids.sql.explain", "NONE") \
            .config("spark.rapids.memory.gpu.allocFraction", "0.5") \
            .config("spark.jars", "/root/spark_rapids_dev/source/rapids-4-spark_2.12-25.06.0.jar") \
            .getOrCreate()

    with nvtx.annotate("2. Data Loading & Immediate Execution", color="green"):
        input_path = "local_profile_data.parquet"
        df = spark.read.parquet(input_path)
        # 通过 cache() 和 count() 强制加载和物化数据
        df.cache()
        initial_count = df.count()
        print(f"数据从 '{input_path}' 加载完成并已物化。初始行数: {initial_count}")

    # with nvtx.annotate("3. Window Operation & Immediate Execution", color="orange"):
    #     # 定义窗口，按 'group_key' 分区
    #     windowSpec = Window.partitionBy("group_key")
        
    #     # 使用窗口函数计算总和，数据量不会减少
    #     window_df = df.withColumn("total_value", sum("value1").over(windowSpec))
        
    #     # 通过 cache() 和 count() 强制执行窗口操作
    #     window_df.cache()
    #     window_count = window_df.count()
    #     print(f"窗口操作执行完毕。当前行数: {window_count}")
    #     # 解除对前一个DataFrame的缓存
    #     df.unpersist()

    # with nvtx.annotate("4. Sort Operation & Immediate Execution", color="red"):
    #     # 对窗口操作的结果进行排序
    #     sorted_df = window_df.orderBy("total_value")

    #     # 通过 cache() 和 count() 强制执行排序操作
    #     sorted_df.cache()
    #     sorted_count = sorted_df.count()
    #     print(f"排序操作执行完毕。当前行数: {sorted_count}")
    #     # 解除对前一个DataFrame的缓存
    #     window_df.unpersist()

    # with nvtx.annotate("5. Final Computation (Collect)", color="purple"):
    #     # 最终的 collect 操作会触发最后的计算（如果前面没有缓存）并返回结果
    #     result = sorted_df.collect()
    #     print(f"最终 Collect 操作完成，获取了 {len(result)} 条记录。")

    spark.stop()

if __name__ == "__main__":
    profile_local_memory_transfer_modified()