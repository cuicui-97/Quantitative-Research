#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
数据加载工具函数

提供日线数据加载的通用函数
"""
import logging
import pandas as pd
import numpy as np
from typing import Any, Optional, List, Callable
from config.config import Config

logger = logging.getLogger(__name__)


def load_daily_data(
    ts_code: str,
    dates: pd.DatetimeIndex,
    default_value: Any = None
) -> Optional[pd.DataFrame]:
    """
    加载日线数据的通用函数

    Args:
        ts_code: 股票代码
        dates: 日期索引
        default_value: 缺失时的默认值（None 表示不填充）

    Returns:
        DataFrame: 对齐到 dates 的日线数据，如果文件不存在或读取失败则返回 None
    """
    daily_file = Config.DAILY_DATA_DIR / f'{ts_code}.csv'

    if not daily_file.exists():
        logger.debug(f"日线数据文件不存在: {ts_code}")
        return None

    try:
        df = pd.read_csv(daily_file, dtype={'trade_date': str})

        if df.empty or 'trade_date' not in df.columns:
            logger.debug(f"日线数据为空或缺少 trade_date 列: {ts_code}")
            return None

        # 转换日期并设置索引
        df['trade_date'] = pd.to_datetime(df['trade_date'], format='%Y%m%d')
        df = df.set_index('trade_date')

        # 重新索引到目标日期范围
        if default_value is not None:
            df = df.reindex(dates, fill_value=default_value)
        else:
            df = df.reindex(dates)

        return df

    except Exception as e:
        logger.debug(f"加载 {ts_code} 失败: {e}")
        return None


def load_daily_column(
    ts_code: str,
    dates: pd.DatetimeIndex,
    column: str,
    default_value: Any = 0.0
) -> pd.Series:
    """
    加载日线数据的单列

    Args:
        ts_code: 股票代码
        dates: 日期索引
        column: 列名（如 'close', 'volume'）
        default_value: 缺失时的默认值

    Returns:
        Series: 对齐到 dates 的数据列，如果失败则返回全为 default_value 的 Series
    """
    df = load_daily_data(ts_code, dates, default_value=None)

    if df is None or column not in df.columns:
        # 返回全为默认值的 Series
        return pd.Series(default_value, index=dates)

    # 提取列并填充缺失值
    series = df[column].fillna(default_value)
    return series


def extract_stock_data(
    ts_code: str,
    dates: pd.DatetimeIndex,
    columns: List[str] = None
) -> pd.DataFrame:
    """
    提取股票多列数据

    Args:
        ts_code: 股票代码
        dates: 日期索引
        columns: 需要的列名列表，默认 ['open', 'high', 'low', 'close', 'vol', 'amount']

    Returns:
        DataFrame: 包含指定列的数据，缺失值为 NaN
    """
    if columns is None:
        columns = ['open', 'high', 'low', 'close', 'vol', 'amount']

    df = load_daily_data(ts_code, dates)

    if df is None:
        return pd.DataFrame(index=dates, columns=columns)

    result = pd.DataFrame(index=dates)
    for col in columns:
        if col in df.columns:
            result[col] = df[col].astype(np.float32)
        else:
            result[col] = np.nan

    return result


def get_all_trading_dates() -> pd.DatetimeIndex:
    """
    获取所有交易日

    Returns:
        DatetimeIndex: 交易日列表
    """
    trade_calendar = pd.read_csv(Config.SUPPLEMENTARY_DATA_DIR / 'trade_calendar.csv')
    trade_calendar = trade_calendar[trade_calendar['is_open'] == 1]
    dates = pd.DatetimeIndex(pd.to_datetime(
        trade_calendar['cal_date'].astype(str), format='%Y%m%d'
    ))
    return dates


def get_all_stocks() -> List[str]:
    """
    获取所有股票代码列表

    Returns:
        List[str]: 股票代码列表
    """
    basic_info = pd.read_csv(Config.BASIC_DATA_DIR / 'all_companies_info.csv')
    return basic_info['ts_code'].tolist()


def check_daily_data_exists(ts_code: str) -> bool:
    """
    检查股票的日线数据是否存在

    Args:
        ts_code: 股票代码

    Returns:
        bool: 是否存在
    """
    daily_file = Config.DAILY_DATA_DIR / f'{ts_code}.csv'
    return daily_file.exists()


def build_matrix_from_extractor(
    extractor_func: Callable[[str, pd.DatetimeIndex], pd.Series],
    dates: Optional[pd.DatetimeIndex] = None,
    stocks: Optional[List[str]] = None,
    logger: Optional[logging.Logger] = None
) -> pd.DataFrame:
    """
    从提取函数构建矩阵的通用方法

    Args:
        extractor_func: 提取函数，接收 (ts_code, dates) 返回 Series
        dates: 日期索引，默认从交易日历获取
        stocks: 股票列表，默认从基础信息获取
        logger: 日志对象

    Returns:
        DataFrame: 构建的矩阵 (dates × stocks)
    """
    if dates is None:
        dates = get_all_trading_dates()
    if stocks is None:
        stocks = get_all_stocks()

    if logger:
        logger.info(f"构建矩阵: {len(stocks)} 只股票, {len(dates)} 个交易日")

    # 初始化结果矩阵
    result = pd.DataFrame(np.nan, index=dates.strftime('%Y%m%d'), columns=stocks, dtype=np.float32)

    # 逐股票处理
    total = len(stocks)
    for i, ts_code in enumerate(stocks, 1):
        if logger and i % 500 == 0:
            logger.info(f"处理进度: {i}/{total} ({i/total:.1%})")

        series = extractor_func(ts_code, dates)
        if series is not None and not series.isna().all():
            result[ts_code] = series.values.astype(np.float32)

    if logger:
        logger.info(f"处理完成，共 {total} 只股票")
        valid_ratio = result.notna().sum().sum() / result.size
        logger.info(f"有效数据比例: {valid_ratio:.2%}")

    return result


def load_standard_matrices(
    factor_file: str,
    logger: Optional[logging.Logger] = None
) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    """
    加载标准四矩阵：因子、市值、收益、可交易性

    Args:
        factor_file: 因子矩阵文件名（如 'n_income_attr_p_yoy_matrix.csv'）
        logger: 日志对象

    Returns:
        tuple: (factor_matrix, mv_matrix, return_matrix, tradability_matrix)
    """
    from data_engine.processors.matrix_io import load_matrix

    if logger:
        logger.info("加载标准矩阵...")

    factor_matrix = load_matrix(Config.MATRIX_DATA_DIR / factor_file)
    mv_matrix = load_matrix(Config.MATRIX_DATA_DIR / 'circ_mv_matrix.csv')
    return_matrix = load_matrix(Config.MATRIX_DATA_DIR / 'open_return_matrix.csv')
    tradability_matrix = load_matrix(Config.MATRIX_DATA_DIR / 'tradability_matrix.csv')

    if logger:
        logger.info(f"  因子矩阵: {factor_matrix.shape}")
        logger.info(f"  市值矩阵: {mv_matrix.shape}")
        logger.info(f"  收益率矩阵: {return_matrix.shape}")
        logger.info(f"  可交易矩阵: {tradability_matrix.shape}")

    return factor_matrix, mv_matrix, return_matrix, tradability_matrix


def align_matrices(
    *matrices: pd.DataFrame,
    logger: Optional[logging.Logger] = None
) -> list[pd.DataFrame]:
    """
    统一矩阵对齐：取日期和股票的交集

    Args:
        *matrices: 需要对齐的矩阵
        logger: 日志对象

    Returns:
        list[pd.DataFrame]: 对齐后的矩阵列表
    """
    if len(matrices) < 2:
        return list(matrices)

    # 计算日期交集
    common_dates = matrices[0].index
    for m in matrices[1:]:
        common_dates = common_dates.intersection(m.index)

    # 计算股票交集
    common_stocks = matrices[0].columns
    for m in matrices[1:]:
        common_stocks = common_stocks.intersection(m.columns)

    if logger:
        logger.info(f"对齐后: {len(common_dates)} 个交易日, {len(common_stocks)} 只股票")

    # 返回对齐后的矩阵
    return [m.loc[common_dates, common_stocks] for m in matrices]


def build_price_matrix(
    dates: pd.DatetimeIndex,
    stocks: List[str],
    column: str = 'close',
    logger: Optional[logging.Logger] = None
) -> pd.DataFrame:
    """
    构建价格矩阵（或任意单列矩阵）

    从日线数据文件中提取指定列，构建 dates × stocks 的矩阵。
    这是向量化因子计算的基础。

    Args:
        dates: 日期索引
        stocks: 股票代码列表
        column: 要提取的列名，默认 'close'
        logger: 日志对象

    Returns:
        DataFrame: 价格矩阵 (dates × stocks)

    Example:
        >>> dates = get_all_trading_dates()
        >>> stocks = get_all_stocks()
        >>> close_matrix = build_price_matrix(dates, stocks, 'close', logger)
        >>> vol_matrix = build_price_matrix(dates, stocks, 'vol', logger)
    """
    if logger:
        logger.info(f"构建 {column} 矩阵 ({len(stocks)} 只股票)...")

    matrix = pd.DataFrame(index=dates, columns=stocks, dtype=np.float32)

    total = len(stocks)
    for i, ts_code in enumerate(stocks):
        if logger and i % 1000 == 0:
            logger.info(f"  加载进度: {i}/{total}")

        file_path = Config.DAILY_DATA_DIR / f'{ts_code}.csv'
        if not file_path.exists():
            continue

        try:
            df = pd.read_csv(file_path, index_col='trade_date', parse_dates=True)
            df = df.reindex(dates)
            if column in df.columns:
                matrix[ts_code] = df[column].values
        except Exception:
            continue

    if logger:
        logger.info(f"  {column} 矩阵: {matrix.shape}")

    return matrix


def build_ohlcv_matrices(
    dates: pd.DatetimeIndex,
    stocks: List[str],
    columns: List[str] = None,
    logger: Optional[logging.Logger] = None
) -> dict[str, pd.DataFrame]:
    """
    批量构建OHLCV矩阵

    同时构建多个价格/成交量矩阵，减少文件IO次数。

    Args:
        dates: 日期索引
        stocks: 股票代码列表
        columns: 要提取的列名列表，默认 ['open', 'high', 'low', 'close', 'vol', 'amount']
        logger: 日志对象

    Returns:
        dict[str, DataFrame]: {列名: 矩阵}

    Example:
        >>> dates = get_all_trading_dates()
        >>> stocks = get_all_stocks()
        >>> ohlcv = build_ohlcv_matrices(dates, stocks, logger=logger)
        >>> close_matrix = ohlcv['close']
        >>> vol_matrix = ohlcv['vol']
    """
    if columns is None:
        columns = ['open', 'high', 'low', 'close', 'vol', 'amount']

    if logger:
        logger.info(f"构建OHLCV矩阵 ({len(stocks)} 只股票, {len(columns)} 列)...")

    # 初始化结果字典
    matrices = {col: pd.DataFrame(index=dates, columns=stocks, dtype=np.float32) for col in columns}

    total = len(stocks)
    for i, ts_code in enumerate(stocks):
        if logger and i % 1000 == 0:
            logger.info(f"  加载进度: {i}/{total}")

        file_path = Config.DAILY_DATA_DIR / f'{ts_code}.csv'
        if not file_path.exists():
            continue

        try:
            df = pd.read_csv(file_path, index_col='trade_date', parse_dates=True)
            df = df.reindex(dates)
            for col in columns:
                if col in df.columns:
                    matrices[col][ts_code] = df[col].values
        except Exception:
            continue

    if logger:
        for col, matrix in matrices.items():
            logger.info(f"  {col} 矩阵: {matrix.shape}")

    return matrices


def filter_dates(
    dates: pd.DatetimeIndex,
    start_date: Optional[str] = None,
    end_date: Optional[str] = None
) -> pd.DatetimeIndex:
    """
    根据起止日期筛选交易日

    Args:
        dates: 完整的交易日列表
        start_date: 开始日期（YYYYMMDD格式），包含
        end_date: 结束日期（YYYYMMDD格式），包含

    Returns:
        DatetimeIndex: 筛选后的日期列表

    Example:
        >>> dates = get_all_trading_dates()
        >>> filtered = filter_dates(dates, "20200101", "20201231")
    """
    if start_date:
        dates = dates[dates >= start_date]
    if end_date:
        dates = dates[dates <= end_date]
    return dates
