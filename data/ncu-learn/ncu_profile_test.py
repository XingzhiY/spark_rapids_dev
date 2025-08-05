import os
os.environ["CUDA_VISIBLE_DEVICES"] = "2"

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, rand, sum
import numpy as np

def main():
    # 创建启用 RAPIDS 加速的 SparkSession
    spark = SparkSession.builder \
        .appName("RapidsProfileDemo") \
        .master("local[*]") \
        .config("spark.plugins", "com.nvidia.spark.SQLPlugin") \
        .config("spark.rapids.sql.enabled", "true") \
        .config("spark.rapids.sql.explain", "NONE") \
        .config("spark.executorEnv.CUDA_VISIBLE_DEVICES", "2") \
        .config("spark.jars", "/root/spark_rapids_dev/source/rapids-4-spark_2.12-25.06.0.jar") \
        .config("spark.sql.adaptive.enabled", "false") \
        .getOrCreate()

    num_rows = 300000
    # 使用固定的种子
    df = spark.range(num_rows).withColumn("value", rand(seed=42)) 
    df = df.withColumn("group", (rand(seed=101) * 100).cast("int"))

    # # 生成随机数据
    # num_rows = 300000  # 100万行
    # data = [(i, float(np.random.random())) for i in range(num_rows)]
    # df = spark.createDataFrame(data, ["id", "value"])

    # # 添加一个随机分组列
    # df = df.withColumn("group", (rand() * 100).cast("int"))

    # 执行聚合操作
    result = df.groupBy("group") \
        .agg(sum("value").alias("sum_value")) \
        .orderBy("group")

    # 显示结果（只显示前20行）
    # print("结果预览：")
    result.show()

    # 显示执行计划以确认 RAPIDS 加速
    # print("\nRAPIDS 执行计划：")
    # result.explain()

    # 等待用户输入，方便在 NSight Compute 中进行分析
    # input("按 Enter 键结束程序...")
    # spark.stop()

if __name__ == "__main__":
    main()