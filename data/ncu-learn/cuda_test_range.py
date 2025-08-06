import torch
from torch.cuda import nvtx
# 导入 torch.cuda.profiler 模块来调用 start() 和 stop()
import torch.cuda.profiler

# --- 初始化部分 (不变) ---
with nvtx.range("init"):
    stream1 = torch.cuda.Stream()
    stream2 = torch.cuda.Stream()
    a = torch.zeros(233, device='cuda')

# --- 第一个加法操作 (不进行性能分析) ---
with torch.cuda.stream(stream1):
    with nvtx.range("first add"):
        torch.cuda.profiler.start()
        a += 1
        torch.cuda.profiler.stop()

# --- 第二个加法操作 (只对此部分进行性能分析) ---
with torch.cuda.stream(stream2):
    with nvtx.range("second add"):
        

        # 这是我们唯一想要分析的内核启动
        a += 1

        

# --- 同步和打印结果 (不变) ---
with nvtx.range("print"):
    torch.cuda.synchronize()
    average = a.mean().item()
    print(f'Average of the elements in tensor a: {average}')