import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._ // 引入 Spark SQL 的函数，例如 avg

object RapidsCacheAnalysisDemo {

  def main(args: Array[String]): Unit = {

    // 关于环境变量的说明:
    // 在 Python 中使用了 os.environ。在 Scala/JVM 应用中，
    // 像 CUDA_VISIBLE_DEVICES 这样的环境变量最好在启动应用的脚本中设置
    // (例如，通过 spark-submit 的 --conf spark.executorEnv.CUDA_VISIBLE_DEVICES=1)。

    // 1. Spark Session 初始化
    val spark = SparkSession.builder
      .appName("RapidsCacheAnalysisDemoScala")
      .master("local[*]")
      .config("spark.plugins", "com.nvidia.spark.SQLPlugin")
      .config("spark.rapids.sql.enabled", "true")
      .config("spark.rapids.sql.explain", "ALL")
      .config("spark.rapids.memory.gpu.allocFraction", "0.5")
      .config("spark.rapids.sql.exec.InMemoryTableScanExec", "true")
      .config("spark.sql.cache.serializer", "com.nvidia.spark.ParquetCachedBatchSerializer")
      .getOrCreate()

    // 2. 数据加载与缓存
    val inputPath = "local_profile_data.parquet"
    println(s"数据将从 '$inputPath' 加载。")

    // 读取数据并声明要缓存它
    val df = spark.read.parquet(inputPath).cache()
    
    // 执行一个 action (例如 count) 来触发实际的加载和缓存过程
    df.count()
    println("count完成，数据已缓存。")


    // 3. 第一次使用缓存: 执行聚合操作
    println("\n===== 第一个任务：执行聚合操作 =====")
    // 在 Scala 中，聚合操作更地道的写法是使用 sql.functions 里的函数
    val aggDf = df.groupBy("group_key").agg(avg("value1"))
    
    println("聚合操作的物理执行计划 (explain 格式):")
    aggDf.explain(extended = true)

    // ==================== 关键实现部分 ====================
    // 以编程方式访问计划树并打印其 JSON 格式
    println("\n===== 物理执行计划 (JSON 格式) =====")
    val physicalPlan = aggDf.queryExecution.sparkPlan
    println(physicalPlan.prettyJson) // 直接打印出 JSON 格式的计划树

    // 同样地，我们也可以访问并打印逻辑计划
    println("\n===== 逻辑执行计划 (JSON 格式) =====")
    val logicalPlan = aggDf.queryExecution.logical
    println(logicalPlan.prettyJson)
    // =====================================================

    // 触发聚合操作
    aggDf.collect()
    println("聚合操作完成。")


    // 5. 释放缓存并停止 Spark Session
    println("\n缓存已释放。")
    df.unpersist()
    spark.stop()
  }
}
