"""
矩阵构建器

提供可复用的矩阵构建模式：
1. 从长格式数据构建矩阵（ST、停牌）
2. 从日线文件构建矩阵（缺失、涨跌停、收益率）
3. 从基础信息构建矩阵（上市天数）
4. 组合多个矩阵（交易可用性）
"""
import logging
import numpy as np
import pandas as pd
from pathlib import Path
from typing import Optional, List, Callable
from tqdm import tqdm
from concurrent.futures import ThreadPoolExecutor, as_completed

from config.config import Config

logger = logging.getLogger(__name__)


class MatrixBuilder:
    """矩阵构建器 - 提供可复用的构建模式"""

    @staticmethod
    def from_long_format(
        df: pd.DataFrame,
        value_col: Optional[str] = None,
        all_stocks: Optional[List[str]] = None,
        all_dates: Optional[List[str]] = None,
        default_value: int = 0
    ) -> pd.DataFrame:
        """
        从长格式数据构建矩阵（适用于 ST、停牌等）

        Args:
            df: 长格式数据，必须包含 ts_code 和 trade_date 列
            value_col: 值列名（如果为 None，则将所有记录标记为 1）
            all_stocks: 所有股票代码列表（如果为 None，从数据中提取）
            all_dates: 所有交易日期列表（如果为 None，从数据中提取）
            default_value: 默认值（通常为 0）

        Returns:
            DataFrame: 矩阵
                - 索引（行）: trade_date（交易日期）
                - 列: ts_code（股票代码）
                - 值: 根据 value_col 或标记为 1
        """
        if df is None or len(df) == 0:
            logger.warning("输入数据为空")
            return pd.DataFrame()

        if 'ts_code' not in df.columns or 'trade_date' not in df.columns:
            logger.error("输入数据缺少必需列: ts_code, trade_date")
            return pd.DataFrame()

        # 提取股票列表和日期列表
        if all_stocks is None:
            all_stocks = sorted(df['ts_code'].unique())
            logger.info(f"从数据中提取 {len(all_stocks)} 只股票")

        if all_dates is None:
            all_dates = sorted(df['trade_date'].unique())
            logger.info(f"从数据中提取 {len(all_dates)} 个交易日")

        # 使用 pivot_table 构建矩阵（比 iterrows 快 100+ 倍）
        logger.info("使用 pivot_table 构建矩阵...")

        if value_col and value_col in df.columns:
            # 有值列：使用 pivot_table
            matrix = df.pivot_table(
                index='trade_date',
                columns='ts_code',
                values=value_col,
                aggfunc='first'  # 如果有重复，取第一个值
            )
        else:
            # 无值列：所有存在的记录标记为 1
            df_copy = df.copy()
            df_copy['_value'] = 1
            matrix = df_copy.pivot_table(
                index='trade_date',
                columns='ts_code',
                values='_value',
                aggfunc='first'
            )

        # 对齐到全局日期和股票列表
        logger.info("对齐矩阵到全局日期和股票列表...")
        matrix = matrix.reindex(index=all_dates, columns=all_stocks, fill_value=default_value)

        logger.info(f"矩阵构建完成: {matrix.shape[0]} × {matrix.shape[1]}")
        return matrix

    @staticmethod
    def from_daily_files(
        dates: pd.DatetimeIndex,
        stocks: List[str],
        extractor_func: Callable[[str, pd.DatetimeIndex], np.ndarray],
        desc: str = "处理股票",
        n_jobs: int = 4
    ) -> pd.DataFrame:
        """
        从日线文件构建矩阵（适用于缺失、涨跌停、收益率等）

        Args:
            dates: 全局日期索引
            stocks: 股票代码列表
            extractor_func: 提取函数，输入 (ts_code, dates)，输出 numpy 数组
            desc: 进度条描述
            n_jobs: 并行线程数（默认 4）

        Returns:
            DataFrame: 矩阵
        """
        # 初始化矩阵
        n_dates = len(dates)
        n_stocks = len(stocks)
        matrix = np.zeros((n_dates, n_stocks), dtype=np.float32)

        # 定义单个股票处理函数
        def process_stock(args):
            i, ts_code = args
            try:
                values = extractor_func(ts_code, dates)
                return i, values
            except Exception as e:
                logger.debug(f"处理 {ts_code} 时出错: {e}")
                return i, np.zeros(n_dates, dtype=np.float32)

        # 并行处理
        logger.info(f"使用 {n_jobs} 个线程并行处理 {len(stocks)} 只股票...")
        with ThreadPoolExecutor(max_workers=n_jobs) as executor:
            # 提交所有任务
            futures = {executor.submit(process_stock, (i, ts_code)): (i, ts_code)
                      for i, ts_code in enumerate(stocks)}

            # 收集结果
            with tqdm(total=len(stocks), desc=desc) as pbar:
                for future in as_completed(futures):
                    i, values = future.result()
                    matrix[:, i] = values
                    pbar.update(1)

        # 转换为 DataFrame
        df_matrix = pd.DataFrame(
            matrix,
            index=dates.strftime('%Y%m%d'),
            columns=stocks
        )
        df_matrix.index.name = 'trade_date'

        logger.info(f"矩阵构建完成: {df_matrix.shape[0]} × {df_matrix.shape[1]}")
        return df_matrix

    @staticmethod
    def from_basic_info(
        basic_info: pd.DataFrame,
        dates: pd.DatetimeIndex,
        stocks: List[str],
        condition_func: Callable[[pd.Series, pd.DatetimeIndex], np.ndarray],
        n_jobs: int = 4
    ) -> pd.DataFrame:
        """
        从基础信息构建矩阵（适用于上市天数等）

        Args:
            basic_info: 基础信息 DataFrame（索引为 ts_code）
            dates: 全局日期索引
            stocks: 股票代码列表
            condition_func: 条件函数，输入 (stock_info, dates)，输出 numpy 数组
            n_jobs: 并行线程数（默认 4）

        Returns:
            DataFrame: 矩阵
        """
        # 初始化矩阵（默认为 1）
        matrix = np.ones((len(dates), len(stocks)), dtype=np.int8)

        # 定义单个股票处理函数
        def process_stock(args):
            i, ts_code = args
            if ts_code in basic_info.index:
                stock_info = basic_info.loc[ts_code]
                result = condition_func(stock_info, dates)
                return i, result
            return i, np.ones(len(dates), dtype=np.int8)

        # 并行处理
        logger.info(f"使用 {n_jobs} 个线程并行处理 {len(stocks)} 只股票...")
        with ThreadPoolExecutor(max_workers=n_jobs) as executor:
            # 提交所有任务
            futures = {executor.submit(process_stock, (i, ts_code)): (i, ts_code)
                      for i, ts_code in enumerate(stocks)}

            # 收集结果
            with tqdm(total=len(stocks), desc="计算条件") as pbar:
                for future in as_completed(futures):
                    i, result = future.result()
                    matrix[:, i] = result
                    pbar.update(1)

        # 转换为 DataFrame
        df_matrix = pd.DataFrame(
            matrix,
            index=dates.strftime('%Y%m%d'),
            columns=stocks
        )
        df_matrix.index.name = 'trade_date'

        logger.info(f"矩阵构建完成: {df_matrix.shape[0]} × {df_matrix.shape[1]}")
        return df_matrix

    @staticmethod
    def combine_matrices(
        *matrices: pd.DataFrame,
        operation: str = 'and'
    ) -> pd.DataFrame:
        """
        组合多个矩阵（适用于交易可用性等）

        Args:
            *matrices: 多个矩阵
            operation: 操作类型（'and', 'or', 'sum'）

        Returns:
            DataFrame: 组合后的矩阵
        """
        if len(matrices) == 0:
            logger.error("没有输入矩阵")
            return pd.DataFrame()

        if len(matrices) == 1:
            return matrices[0]

        # 确保所有矩阵维度一致
        base_shape = matrices[0].shape
        for i, matrix in enumerate(matrices[1:], 1):
            if matrix.shape != base_shape:
                logger.error(f"矩阵 {i} 维度不一致: {matrix.shape} vs {base_shape}")
                return pd.DataFrame()

        # 执行操作
        if operation == 'and':
            result = matrices[0].values.astype(bool)
            for matrix in matrices[1:]:
                result &= matrix.values.astype(bool)
            result = result.astype(np.int8)
        elif operation == 'or':
            result = matrices[0].values.astype(bool)
            for matrix in matrices[1:]:
                result |= matrix.values.astype(bool)
            result = result.astype(np.int8)
        elif operation == 'sum':
            result = matrices[0].values.copy()
            for matrix in matrices[1:]:
                result += matrix.values
        else:
            logger.error(f"不支持的操作: {operation}")
            return pd.DataFrame()

        # 转换为 DataFrame
        df_result = pd.DataFrame(
            result,
            index=matrices[0].index,
            columns=matrices[0].columns
        )
        df_result.index.name = 'trade_date'

        logger.info(f"矩阵组合完成: {df_result.shape[0]} × {df_result.shape[1]}")
        return df_result

    @staticmethod
    def align_matrix(
        matrix: pd.DataFrame,
        target_dates: pd.DatetimeIndex,
        target_stocks: List[str],
        fill_value: int = 0
    ) -> pd.DataFrame:
        """
        对齐矩阵到目标日期和股票

        Args:
            matrix: 原始矩阵
            target_dates: 目标日期索引
            target_stocks: 目标股票列表
            fill_value: 填充值

        Returns:
            DataFrame: 对齐后的矩阵
        """
        # 转换日期格式（如果是 DatetimeIndex）
        if hasattr(target_dates, 'strftime'):
            target_date_strs = target_dates.strftime('%Y%m%d')
        else:
            # 已经是字符串索引
            target_date_strs = target_dates

        # 对齐行（日期）和列（股票）
        aligned = matrix.reindex(
            index=target_date_strs,
            columns=target_stocks,
            fill_value=fill_value
        )

        logger.info(f"矩阵对齐完成: {aligned.shape[0]} × {aligned.shape[1]}")
        return aligned
