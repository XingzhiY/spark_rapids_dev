import os
import nvtx
from pyspark.sql import SparkSession

# 将环境变量设置放在主函数保护之外，确保在SparkSession启动前生效
os.environ["CUDA_VISIBLE_DEVICES"] = "1"

# 将环境变量设置放在主函数保护之外，确保在SparkSession启动前生效
# 请根据您的环境修改CUDA_VISIBLE_DEVICES
os.environ["CUDA_VISIBLE_DEVICES"] = "1" 

# 请根据您的环境修改JAR包的路径
RAPIDS_JAR_PATH = "/path/to/your/rapids-4-spark_2.12-25.06.0.jar"


def profile_cache_zerocopy_comparison():
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
        # 直接加载数据，此时不缓存
        df = spark.read.parquet(input_path)
        # 触发一次action来确保数据已准备好
        initial_count = df.count()
        print(f"数据从 '{input_path}' 加载完成，共 {initial_count} 行。")

    # --- 场景一: 无缓存 ---
    with nvtx.annotate("3. UNCACHED SCENARIO", color="red"):
        # 定义一个中间的、计算成本较高的DataFrame
        intermediate_df_uncached = df.join(df, on="group_key", how="inner")

        with nvtx.annotate("3.1 Uncached - Task 1 (First Action)", color="orange"):
            print("\n--- 开始无缓存场景 ---")
            print("执行第一次Action (count)... Spark需要执行Join。")
            count1 = intermediate_df_uncached.count()
            print(f"无缓存 - 第一次Join和Count完成，计数: {count1}")

        with nvtx.annotate("3.2 Uncached - Task 2 (Second Action)", color="orange"):
            print("执行第二次Action (show)... Spark需要重新执行Join。")
            # 这里的 .show() 会触发对 intermediate_df_uncached 的重新计算
            intermediate_df_uncached.show(5)
            print("无缓存 - 第二次Join和Show完成。")
            
    # --- 场景二: 有缓存 ---
    with nvtx.annotate("4. CACHED SCENARIO", color="purple"):
        # 定义并缓存中间DataFrame
        # .cache() 是一个转换(Transformation)，需要一个动作(Action)来触发
        intermediate_df_cached = df.join(df, on="group_key", how="inner").cache()

        with nvtx.annotate("4.1 Cached - Task 1 (Generate & Cache)", color="cyan"):
            print("\n--- 开始有缓存场景 ---")
            print("执行第一次Action (count)... Spark执行Join并将结果缓存到GPU内存。")
            count2 = intermediate_df_cached.count()
            print(f"有缓存 - 第一次Join和Count完成，计数: {count2}")

        with nvtx.annotate("4.2 Cached - Task 2 (Consume from Cache)", color="cyan"):
            print("执行第二次Action (show)... Spark将从GPU缓存中直接读取数据 (Zero-Copy)。")
            # 这里的 .show() 将直接使用缓存的数据，不会重新计算Join
            intermediate_df_cached.show(5)
            print("有缓存 - 第二次Show完成。")
            
    # 清理缓存
    intermediate_df_cached.unpersist()

    with nvtx.annotate("5. Stopping Spark Session", color="grey"):
        spark.stop()

if __name__ == "__main__":
    profile_cache_zerocopy_comparison()