import cupy
import nvtx

# 使用 nvtx.annotate 上下文管理器来标记代码范围
with nvtx.annotate("init", color="green"):
    # 使用 CuPy 创建 CUDA 流
    stream1 = cupy.cuda.Stream()
    stream2 = cupy.cuda.Stream()
    # 使用 CuPy 在 GPU 上创建一个数组
    a = cupy.zeros(233)

# 将任务提交到 stream1
with stream1:
    with nvtx.annotate("first add", color="blue"):
        a += 1

# 将任务提交到 stream2
with stream2:
    with nvtx.annotate("second add", color="red"):
        a += 1

# 同步并打印结果
with nvtx.annotate("print", color="purple"):
    # 等待所有流中的所有 CUDA 任务完成
    cupy.cuda.runtime.deviceSynchronize()
    # 计算平均值并从 GPU 获取结果到 CPU
    average = a.mean().get()
    print(f'Average of the elements in array a: {average}')