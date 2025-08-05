import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, rand, sum

def main():
    # 确保 Spark 能看到正确的 GPU 设备
    # (这行代码在你的原始脚本中，我们保留它)
    os.environ["CUDA_VISIBLE_DEVICES"] = "2"

    # --- 创建启用 RAPIDS 加速的、确定性的 SparkSession ---
    spark = SparkSession.builder \
        .appName("RapidsProfileDeterministicDemo") \
        .master("local[*]") \
        .config("spark.plugins", "com.nvidia.spark.SQLPlugin") \
        .config("spark.rapids.sql.enabled", "true") \
        .config("spark.rapids.sql.explain", "NONE") \
        .config("spark.executorEnv.CUDA_VISIBLE_DEVICES", "2") \
        .config("spark.jars", "/root/spark_rapids_dev/source/rapids-4-spark_2.12-25.06.0.jar") \
        .config("spark.sql.adaptive.enabled", "false") \
        .getOrCreate()


    # 使用 Spark 内置的 range 和带种子的 rand 函数来创建 DataFrame
    # 这比从 Python 端生成数据再传输给 Spark 更高效、更易于控制
    df = spark.range(300000) 

    # [方案二] 使用带固定种子的 rand() 函数
    # 只要种子 (seed) 不变，生成的随机数列就完全相同
    df = df.withColumn("value", rand(seed=42))
    df = df.withColumn("group", (rand(seed=101) * 100).cast("int"))

    # --- 执行与之前完全相同的聚合操作 ---
    result = df.groupBy("group") \
        .agg(sum("value").alias("sum_value")) \
        .orderBy("group")

    # 执行 action 以触发计算
    print("结果预览 (确定性执行):")
    result.show()

    # 良好的习惯：结束 SparkSession
    spark.stop()
    print("程序执行完毕。")

if __name__ == "__main__":
    main()