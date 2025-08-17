#!/bin/bash

# 当任何命令执行失败时，脚本将立即退出
set -e

# --- 要清理的文件列表 ---
FILES_TO_CLEAR=(
  "spark_info.log"
  "spark_logical_plan.json"
  "spark_physical_plan.json"
  "spark_stdout.txt"
  "spark_warn.log"
)

echo "================================================="
echo "STEP 0: Clearing previous log and output files..."
echo "================================================="
for file in "${FILES_TO_CLEAR[@]}"; do
  # 检查文件是否存在，如果存在则清空它
  if [ -f "$file" ]; then
    > "$file"
    chmod 777 "$file"
    echo "Cleared: $file"
  else
    echo "Skipped (not found): $file"
  fi
done
echo ""

echo "================================================="
echo "STEP 1: Compiling Scala source code..."
echo "================================================="
scalac -classpath "/root/spark_rapids_dev/source/spark-3.5.6-bin-hadoop3/jars/*" RapidsCacheAnalysisDemo.scala

echo ""
echo "================================================="
echo "STEP 2: Packaging compiled classes into a JAR..."
echo "================================================="
jar cvf RapidsCacheAnalysisDemo.jar RapidsCacheAnalysisDemo*.class

echo ""
echo "================================================="
echo "STEP 3: Submitting the job to Spark..."
echo "================================================="
# 注意：这里我将错误输出也重定向到了 spark_stdout.txt 文件
spark-submit \
  --class RapidsCacheAnalysisDemo \
  --files log4j.properties \
  --driver-java-options "-Dfile.encoding=UTF-8 -Dlog4j.configuration=file:log4j.properties" \
  --jars /root/spark_rapids_dev/source/rapids-4-spark_2.12-25.06.0.jar \
  RapidsCacheAnalysisDemo.jar > spark_stdout.txt 2>&1

echo ""
echo "================================================="
echo "Job finished. Check spark_stdout.txt for logs."
echo "================================================="