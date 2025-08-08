# 文件名: local_data_generator_batched.py
import os
import argparse
from pyspark.sql import SparkSession
from pyspark.sql.functions import rand

def generate_local_data_batched(total_rows, batch_size, output_path):
    """
    为单机性能剖析生成规模可控的本地测试数据，通过分批处理避免内存溢出。

    :param total_rows: 要生成的总行数。
    :param batch_size: 每批次生成的行数。
    :param output_path: Parquet 文件的输出目录。
    """
    spark = SparkSession.builder \
        .appName("LocalDataGeneratorBatched") \
        .master("local[*]") \
        .config("spark.sql.execution.arrow.pyspark.enabled", "true") \
        .getOrCreate()

    num_batches = (total_rows + batch_size - 1) // batch_size
    print(f"开始生成 {total_rows:,} 行数据到 '{output_path}'，共 {num_batches} 个批次...")

    for i in range(num_batches):
        start_row = i * batch_size
        end_row = min((i + 1) * batch_size, total_rows)
        current_batch_rows = end_row - start_row

        if current_batch_rows <= 0:
            continue
            
        print(f"正在生成批次 {i+1}/{num_batches}，行数: {current_batch_rows:,}...")

        # 生成当前批次的数据
        df = spark.range(current_batch_rows) \
                  .withColumn("value1", rand(seed=42 + i) * 100) \
                  .withColumn("value2", rand(seed=101 + i) * 1000) \
                  .withColumn("group_key", (rand(seed=123 + i) * rand(seed=456 + i) * 500).cast("int"))

        # 第一批使用 "overwrite"，后续批次使用 "append"
        write_mode = "overwrite" if i == 0 else "append"
        
        df.write.mode(write_mode).parquet(output_path)
    
    print("数据 Schema:")
    final_df = spark.read.parquet(output_path)
    final_df.printSchema()
    print(f"总行数: {final_df.count():,}")

    print(f"数据生成完毕。")
    spark.stop()

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--num-rows", type=int, default=500_000_000,
                        help="要生成的总行数。")
    parser.add_argument("--batch-size", type=int, default=50_000_000,
                        help="每批次生成的行数。")
    parser.add_argument("--output", type=str, default="local_profile_data.parquet",
                        help="输出 Parquet 文件的路径。")
    args = parser.parse_args()

    generate_local_data_batched(args.num_rows, args.batch_size, args.output)