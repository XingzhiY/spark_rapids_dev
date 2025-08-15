import os
import nvtx
from pyspark.sql import SparkSession

# 将环境变量设置放在主函数保护之外，确保在SparkSession启动前生效
os.environ["CUDA_VISIBLE_DEVICES"] = "1"

def profile_local_memory_transfer_no_groupby():

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
        input_path = "local_profile_data_10m.parquet"
        # 直接加载数据并缓存，以便后续的多次join操作可以复用
        df = spark.read.parquet(input_path).cache()
        print(f"数据从 '{input_path}' 加载并缓存完成。")
        # 触发一次action来实际执行加载和缓存
        df.count()

    # with nvtx.annotate("3. Stage 1: Sort", color="red"):
    #     # 注意：使用原始数据中的列进行排序，这里假设为'value1'
    #     # 您需要根据parquet文件中的实际列名进行修改
    #     sorted_df = df.orderBy("value1")

    with nvtx.annotate("4. Stage 2: Joins", color="purple"):
        with nvtx.annotate("4.1. Join1", color="yellow"):
            # 使用原始数据中的键进行join，这里假设为'group_key'
            # 您需要根据parquet文件中的实际列名进行修改
            joined_df1 = df.join(df, on="group_key", how="inner")
            joined_count1 = joined_df1.count()
            print(f"第一次Join完成，计数: {joined_count1}")

        with nvtx.annotate("4.2. Join2", color="blue"):
            joined_df2 = df.join(df, on="group_key", how="inner")
            joined_count2 = joined_df2.count()
            print(f"第二次Join完成，计数: {joined_count2}")

    with nvtx.annotate("5. Stage 3: More Joins on Original DF", color="cyan"):
        with nvtx.annotate("5.1. Join3", color="yellow"):
            joined_df3 = df.join(df, on="group_key", how="inner")
            joined_count3 = joined_df3.count()
            print(f"第三次Join（在原始DF上）完成，计数: {joined_count3}")

        with nvtx.annotate("5.2. Join4", color="blue"):
            joined_df4 = df.join(df, on="group_key", how="inner")
            joined_count4 = joined_df4.count()
            print(f"第四次Join（在原始DF上）完成，计数: {joined_count4}")


    with nvtx.annotate("6. Stopping Spark Session", color="grey"):
        spark.stop()

if __name__ == "__main__":
    profile_local_memory_transfer_no_groupby()