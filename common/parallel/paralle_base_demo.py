"""
在这里进行并行计算，其他模块参考这个函数编写多进程代码
"""
from multiprocessing import Pool    # 进程池
from threading import Thread        # 线程

# -----------------------------------------------------------------------------------
# 进程池
# 第一步：定义需要并行的函数
def parallel_func(func,*args,**kwargs):
    """需要并行的函数，在实际应用中需要重写这个函数"""
    return func(*args,**kwargs)

# 第二步：组装参数，对应parallel_func的参数
all_data=None
all_params=None
tasks=[[data, params] for data,params in zip(all_data, all_params)]

# 第三步：并行计算
def parallel(func, tasks, n_cpu):
    """并行计算"""
    pool=Pool(n_cpu)
    result=pool.map(func, iterable=tasks)
    pool.close()
    pool.join()
    return result


# -----------------------------------------------------------------------------------
# 线程池
# 第一步：定义需要并行的函数
def parallel_func(func,*args,**kwargs):
    return func(*args,**kwargs)

# 第二步，组装参数，对应parallel_func的参数
all_data=None
all_params=None
tasks=[[data, params] for data,params in zip(all_data, all_params)]

# 第三步：并行计算
def parallel(tasks):
    """并行计算"""
    all_tasks=[]
    # 创建线程
    for t_p in tasks:
        thread = Thread(target=parallel_func, args=t_p)
        all_tasks.append(thread)
    # 启动线程
    for t in all_tasks:
        t.start()
    # 等待结束
    for t in all_tasks:
        t.join()





