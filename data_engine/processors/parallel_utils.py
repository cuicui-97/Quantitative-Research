"""
并行处理工具函数

提供股票列表并行处理的通用框架
"""
import numpy as np
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Callable, List, Tuple
from tqdm import tqdm


def parallel_process_stocks(
    stocks: List[str],
    process_func: Callable[[int, str], Tuple[int, np.ndarray]],
    desc: str = "处理股票",
    n_jobs: int = 4
) -> np.ndarray:
    """
    并行处理股票列表的通用框架

    Args:
        stocks: 股票代码列表
        process_func: 处理函数，签名为 (index, ts_code) -> (index, result)
                     其中 result 是 numpy 数组
        desc: 进度条描述
        n_jobs: 并行线程数（默认 4）

    Returns:
        numpy 数组，每个股票一列（按 stocks 顺序）
    """
    n_stocks = len(stocks)

    # 使用 ThreadPoolExecutor 并行处理
    with ThreadPoolExecutor(max_workers=n_jobs) as executor:
        # 提交所有任务
        futures = {
            executor.submit(process_func, i, ts_code): (i, ts_code)
            for i, ts_code in enumerate(stocks)
        }

        # 收集结果（按索引存储）
        results = [None] * n_stocks

        with tqdm(total=n_stocks, desc=desc) as pbar:
            for future in as_completed(futures):
                i, result = future.result()
                results[i] = result
                pbar.update(1)

    # 堆叠为矩阵（假设所有 result 形状相同）
    # 如果 result 是 1D 数组，则转为列向量
    if results[0].ndim == 1:
        matrix = np.column_stack(results)
    else:
        # 如果 result 是 2D 数组，按列堆叠
        matrix = np.hstack([r.reshape(-1, 1) if r.ndim == 1 else r for r in results])

    return matrix


def parallel_process_with_args(
    args_list: List[Tuple],
    process_func: Callable,
    desc: str = "处理任务",
    n_jobs: int = 4
) -> List:
    """
    并行处理任意参数列表的通用框架

    Args:
        args_list: 参数列表，每个元素是一个 tuple，作为 process_func 的参数
        process_func: 处理函数，接受 args_list 中的一个元素作为参数
        desc: 进度条描述
        n_jobs: 并行线程数（默认 4）

    Returns:
        结果列表（按 args_list 顺序）
    """
    n_tasks = len(args_list)

    with ThreadPoolExecutor(max_workers=n_jobs) as executor:
        # 提交所有任务
        futures = {
            executor.submit(process_func, i, *args): i
            for i, args in enumerate(args_list)
        }

        # 收集结果
        results = [None] * n_tasks

        with tqdm(total=n_tasks, desc=desc) as pbar:
            for future in as_completed(futures):
                i = futures[future]
                results[i] = future.result()
                pbar.update(1)

    return results
