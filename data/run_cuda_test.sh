#!/bin/bash

# 这是一个用于编译和运行简单 CUDA 测试并使用 nsys 进行性能分析的脚本。

# --- 配置 ---
PROJECT_ROOT="/root/spark_rapids_dev"
NSYS_REPORTS_DIR="${PROJECT_ROOT}/data/nsys-reports"
CUDA_SOURCE_FILE="${PROJECT_ROOT}/data/simple_cuda_test.cu"
EXECUTABLE_NAME="simple_cuda_test"
EXECUTABLE_PATH="${PROJECT_ROOT}/data/${EXECUTABLE_NAME}"

# --- 检查 nvcc 是否可用 ---
if ! command -v nvcc &> /dev/null
then
    echo "错误：'nvcc' 命令未找到。"
    echo "请确保 NVIDIA CUDA Toolkit 已安装并且 'nvcc' 在您的系统 PATH 中。"
    exit 1
fi

# --- 检查 nsys 是否可用 ---
if ! command -v nsys &> /dev/null
then
    echo "错误：'nsys' 命令未找到。"
    echo "请确保 NVIDIA Nsight Systems 已安装并且 'nsys' 在您的系统 PATH 中。"
    exit 1
fi

# --- 准备工作 ---
echo ">>> 准备目录..."
mkdir -p "${NSYS_REPORTS_DIR}"
rm -f "${NSYS_REPORTS_DIR}/cuda_test_*.nsys-rep"
rm -f "${EXECUTABLE_PATH}"

# --- 编译 CUDA 代码 ---
echo ">>> 正在使用 nvcc 编译 CUDA 源代码 (包含调试信息)..."
nvcc -g -G "${CUDA_SOURCE_FILE}" -o "${EXECUTABLE_PATH}"
if [ $? -ne 0 ]; then
    echo "错误：CUDA 代码编译失败。"
    exit 1
fi
echo ">>> 编译成功！可执行文件位于: ${EXECUTABLE_PATH}"


# --- 使用 nsys 运行可执行文件 ---
echo ">>> 使用 nsys 启动可执行文件进行性能分析 (包含调用栈采样)..."
nsys profile \
    --trace=cuda,nvtx --stats=true \
    --sample=process-tree --backtrace=dwarf \
    -o "${NSYS_REPORTS_DIR}/cuda_test_profile_$(date +%Y%m%d_%H%M%S)" \
    -f true \
    "${EXECUTABLE_PATH}"

echo ">>> 脚本执行完毕。"
echo ">>> 请检查输出目录 ${NSYS_REPORTS_DIR} 中的分析报告。"
echo ">>> 您可以使用 NVIDIA Nsight Systems UI 打开 .nsys-rep 文件来查看结果。" 