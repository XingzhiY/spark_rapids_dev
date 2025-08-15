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

    with nvtx.annotate("2. Data Loading", color="green"):
        input_path = "local_profile_data_3m.parquet"
        # 直接加载数据并缓存，以便后续的多次join操作可以复用
        df = spark.read.parquet(input_path).cache()
        print(f"数据从 '{input_path}' 加载并缓存完成。")
        # 触发一次action来实际执行加载和缓存
        df.count()

    # with nvtx.annotate("3. Stage 1: Sort", color="red"):
    #     # 注意：使用原始数据中的列进行排序，这里假设为'value1'
    #     # 您需要根据parquet文件中的实际列名进行修改
    #     sorted_df = df.orderBy("value1")

    with nvtx.annotate("4. Stage 2: Reusing Cached Data for Sort", color="red"):

        sorted_df = df.orderBy("value1")
        
        # sorted_df2 = sorted_df.orderBy("total_value")
        # sorted_df3 = sorted_df2.orderBy("total_value")


        with nvtx.annotate("4.1. Final Computation (Collect)", color="purple"):
            sorted_df.collect()
    
        with nvtx.annotate("4.2. Stage 3: Join1", color="yellow"):
            joined_df = sorted_df.join(sorted_df, on="group_key", how="inner")
            joined_count = joined_df.count()
            # print(f"Join 后行数: {joined_count}")

        with nvtx.annotate("4.3. Stage 4: Join2", color="blue"):
            joined_df = sorted_df.join(sorted_df, on="group_key", how="inner")
            joined_count = joined_df.count()
            # print(f"Join 后行数: {joined_count}")

    with nvtx.annotate("5. Stage 6: Reusing Cached Data for Sort", color="red"):

        sorted_df = df.orderBy("value1")
        # sorted_df2 = sorted_df.orderBy("total_value")
        # sorted_df3 = sorted_df2.orderBy("total_value")


        with nvtx.annotate("4.1. Final Computation (Collect)", color="purple"):
            sorted_df.collect()
    
        with nvtx.annotate("5.2. Stage 7: Join1", color="yellow"):
            joined_df = sorted_df.join(sorted_df, on="group_key", how="inner")
            joined_count = joined_df.count()
            # print(f"Join 后行数: {joined_count}")

        with nvtx.annotate("5.3. Stage 8: Join2", color="blue"):
            joined_df = sorted_df.join(sorted_df, on="group_key", how="inner")
            joined_count = joined_df.count()
            # print(f"Join 后行数: {joined_count}")


    with nvtx.annotate("6. Stopping Spark Session", color="grey"):
        spark.stop()

if __name__ == "__main__":
    profile_local_memory_transfer_no_groupby()