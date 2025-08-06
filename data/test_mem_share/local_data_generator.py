# 文件名: local_data_generator.py
import os
import argparse
from pyspark.sql import SparkSession
from pyspark.sql.functions import rand

def generate_local_data(num_rows, output_path):
    """
    为单机性能剖析生成规模可控的本地测试数据。

    :param num_rows: 要生成的总行数。
    :param output_path: Parquet 文件的输出目录。
    """
    spark = SparkSession.builder \
        .appName("LocalDataGenerator") \
        .master("local[*]") \
        .getOrCreate()

    print(f"开始生成 {num_rows:,} 行数据到 '{output_path}'...")

    # 生成包含倾斜 key 的基础 DataFrame
    df = spark.range(num_rows) \
              .withColumn("value1", rand(seed=42) * 100) \
              .withColumn("value2", rand(seed=101) * 1000)

    # 制造倾斜的 group_key，让一小部分 key 拥有大量数据
    # 这会给后续的聚合操作带来内存压力
    df = df.withColumn("group_key", (rand(seed=123) * rand(seed=456) * 500).cast("int"))

    print("数据 Schema:")
    df.printSchema()

    # 将数据写入本地 Parquet 文件，覆盖已存在的文件
    df.write.mode("overwrite").parquet(output_path)

    print(f"数据生成完毕。")
    spark.stop()

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--num-rows", type=int, default=50_000_000,
                        help="要生成的总行数。调大此值以增加内存压力。")
    parser.add_argument("--output", type=str, default="local_profile_data.parquet",
                        help="输出 Parquet 文件的路径。")
    args = parser.parse_args()

    generate_local_data(args.num_rows, args.output)