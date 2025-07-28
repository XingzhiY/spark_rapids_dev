#!/bin/bash
set -e

IMAGE_NAME="spark-rapids-dev:1.0.0"
CONTAINER_NAME="xy_rapids_dev_env"
# 定义项目目录，并挂载到容器内的相同路径
PROJECT_DIR=$(realpath ~/spark_rapids_dev)
CONTAINER_PROJECT_DIR="/root/spark_rapids_dev"

# 确保主机上的项目目录和子目录存在
mkdir -p "${PROJECT_DIR}/source"
mkdir -p "${PROJECT_DIR}/data"
mkdir -p "${PROJECT_DIR}/cache/m2_cache"
mkdir -p "${PROJECT_DIR}/cache/ccache"
mkdir -p "${PROJECT_DIR}/cache/conda_cache"

# --- 核心逻辑变更 ---

# 1. 检查容器是否正在运行
if [ "$(docker ps -q -f name=^/${CONTAINER_NAME}$)" ]; then
    echo "✅ Attaching to running container: ${CONTAINER_NAME}"
    docker exec -it "${CONTAINER_NAME}" /bin/bash
    exit 0
# 2. 如果没有在运行，检查容器是否存在（即处于停止状态）
elif [ "$(docker ps -aq -f name=^/${CONTAINER_NAME}$)" ]; then
    echo "🚀 Starting existing stopped container: ${CONTAINER_NAME}"
    # 使用 docker start 启动已停止的容器，并将输出重定向到 /dev/null 以保持界面清洁
    docker start "${CONTAINER_NAME}" > /dev/null
    echo "✅ Attaching to container..."
    docker exec -it "${CONTAINER_NAME}" /bin/bash
    exit 0
fi

# 3. 如果容器完全不存在，则创建新容器
echo "✨ Creating and starting new container: ${CONTAINER_NAME}"

docker run -it  \
    --name "${CONTAINER_NAME}" \
    --gpus all \
    --cap-add=SYS_ADMIN \
    -p 4040:4040 \
    -p 18081:18081 \
    -p 18082:18082 \
    --shm-size=4g \
    -v "${PROJECT_DIR}:${CONTAINER_PROJECT_DIR}" \
    -v "${PROJECT_DIR}/cache/m2_cache:/root/.m2" \
    -v "${PROJECT_DIR}/cache/ccache:/root/.ccache" \
    -v "${PROJECT_DIR}/cache/conda_cache:/root/.conda/pkgs" \
    -w "${CONTAINER_PROJECT_DIR}" \
    "${IMAGE_NAME}" \
    /bin/bash -c "echo 'export SPARK_HOME=${CONTAINER_PROJECT_DIR}/source/spark-3.5.6-bin-hadoop3' >> /root/.bashrc && echo 'export PATH=\$PATH:\$SPARK_HOME/bin:\$SPARK_HOME/sbin' >> /root/.bashrc && exec /bin/bash"