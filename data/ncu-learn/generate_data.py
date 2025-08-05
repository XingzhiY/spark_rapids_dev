# 文件名: generate_data.py
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import rand
import numpy as np

# 确保 Spark 能找到正确的 GPU
os.environ["CUDA_VISIBLE_DEVICES"] = "2"

def generate_and_save_data():
    """
    生成测试数据并将其保存为 Parquet 文件。
    这个脚本只需要运行一次，用于准备 ncu 分析所需的数据。
    """
    spark = SparkSession.builder \
        .appName("DataGenerator") \
        .master("local[*]") \
        .config("spark.plugins", "com.nvidia.spark.SQLPlugin") \
        .config("spark.rapids.sql.enabled", "true") \
        .config("spark.executorEnv.CUDA_VISIBLE_DEVICES", "2") \
        .config("spark.jars", "/root/spark_rapids_dev/source/rapids-4-spark_2.12-25.06.0.jar") \
        .getOrCreate()

    # 数据保存路径
    output_path = "ncu_test_data.parquet"
    print(f"正在生成数据并保存到: {output_path}")

    # 生成确定性的随机数据
    num_rows = 300000
    # 使用固定的种子以确保可复现
    np.random.seed(42) 
    data = [(int(i), float(np.random.random())) for i in range(num_rows)]
    df = spark.createDataFrame(data, ["id", "value"])

    # 添加一个确定性的分组列。使用带种子的 rand() 函数。
    # 将分组列也一并保存，让分析脚本完全不需要进行任何随机操作。
    df = df.withColumn("group", (rand(seed=123) * 100).cast("int"))
    
    # 将数据写入 Parquet 文件，如果存在则覆盖
    df.write.mode("overwrite").parquet(output_path)

    print("数据生成完毕。")
    spark.stop()

if __name__ == "__main__":
    generate_and_save_data()