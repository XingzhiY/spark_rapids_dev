#!/bin/bash




PYTHON_SCRIPT="run_profile.py"


KERNEL_TO_PROFILE="single_pass_shmem_aggs_kernel"

OUTPUT_REPORT_NAME="ncu-reports/ncu_profile_$(date +%Y%m%d_%H%M%S).ncu-rep"


set -e


# 打印出当前配置，方便确认
echo "分析目标脚本:   ${PYTHON_SCRIPT}"
echo "目标 CUDA 核函数: ${KERNEL_TO_PROFILE}"
echo "输出报告文件:   ${OUTPUT_REPORT_NAME}"
echo

# 检查目标 Python 脚本是否存在
if [ ! -f "$PYTHON_SCRIPT" ]; then
    echo "❌ 错误: 目标分析脚本 '${PYTHON_SCRIPT}' 未找到。"
    echo "请确保脚本与本脚本在同一目录下，或提供正确路径。"
    exit 1
fi

# 执行 ncu 命令
#   --kernel-name: 精准定位目标核函数
#   --set full:    收集所有可用的硬件性能指标，进行最详尽的分析
#   -o:            指定输出报告的文件名
#   -f:            如果报告文件已存在，则强制覆盖 (如果不用时间戳命名，这个很有用)
echo "⏳ 正在运行 ncu，请稍候..."
ncu \
  --kernel-name "${KERNEL_TO_PROFILE}" \
  --set full \
  -o "${OUTPUT_REPORT_NAME}" -f \
  python3 "${PYTHON_SCRIPT}"

echo
echo "=================================================="
echo "报告文件已生成: ${OUTPUT_REPORT_NAME}"
echo "=================================================="