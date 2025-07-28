#include <iostream>
#include <cmath>

// 一个简单的 CUDA 核函数，用于产生一些 GPU 活动
__global__ void simple_kernel() {
    int idx = threadIdx.x + blockIdx.x * blockDim.x;
    float val = 0.0f;
    // 执行一些任意的计算来让 GPU 保持繁忙
    for (int i = 0; i < 2000; ++i) {
        val += sinf(static_cast<float>(i)) * cosf(static_cast<float>(idx));
    }
    // 结果没有被存储，因为这只是为了分析活动
    printf("Hello World from GPU %d\n", idx);
}

int main() {
    // 定义网格和块维度
    // 启动足够多的线程以确保 GPU 被利用
    int blockSize = 8;
    int gridSize = 1;

    std::cout << ">>> 正在为 nsys 分析启动简单的 CUDA 核函数..." << std::endl;

    // 启动核函数
    simple_kernel<<<gridSize, blockSize>>>();

    // cudaDeviceSynchronize() 等待核函数执行完成。
    // 这对于确保应用程序在核函数结束前不退出非常重要。
    cudaDeviceSynchronize();
    printf("Hello World from CPU\n");

    // 检查核函数执行期间是否有任何错误
    cudaError_t err = cudaGetLastError();
    if (err != cudaSuccess) {
        std::cerr << "CUDA 错误: " << cudaGetErrorString(err) << std::endl;
        return -1;
    }

    std::cout << ">>> CUDA 核函数成功执行完毕。" << std::endl;

    return 0;
} 
