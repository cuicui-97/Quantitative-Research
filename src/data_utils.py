"""
数据处理工具函数
提供各种数据转换和处理的通用函数
"""
import logging
import pandas as pd
from datetime import datetime, timedelta
from typing import Dict, Set

logger = logging.getLogger(__name__)


def build_st_dict(df_st: pd.DataFrame) -> Dict[str, Set[str]]:
    """
    构建 ST 状态字典: {trade_date: {ts_code}}

    stock_st API 返回的数据已经是按日期展开的，无需填充日期范围

    Args:
        df_st: ST 状态 DataFrame（包含 ts_code, trade_date 字段）

    Returns:
        字典，键为日期，值为该日期处于 ST 状态的股票代码集合
    """
    st_dict = {}

    for _, row in df_st.iterrows():
        ts_code = row['ts_code']
        date_str = str(row['trade_date'])  # 确保是字符串

        # 处理缺失值
        if pd.isna(date_str) or date_str == 'nan':
            continue

        if date_str not in st_dict:
            st_dict[date_str] = set()
        st_dict[date_str].add(ts_code)

    logger.info(f"ST 状态字典构建完成，覆盖 {len(st_dict)} 个日期")
    return st_dict


def build_suspension_dict(df_suspend: pd.DataFrame) -> Dict[str, Set[str]]:
    """
    构建停牌字典: {trade_date: {ts_code}}

    Args:
        df_suspend: 停牌 DataFrame（包含 ts_code, trade_date 字段）
                   注意：Tushare suspend_d API 返回的是 trade_date，不是 suspend_date

    Returns:
        字典，键为日期，值为该日期停牌的股票代码集合
    """
    suspension_dict = {}

    for _, row in df_suspend.iterrows():
        ts_code = row['ts_code']
        trade_date = row['trade_date']

        if pd.isna(trade_date):
            continue

        # 直接使用 trade_date（该日期处于停牌状态）
        date_str = str(trade_date)
        if date_str not in suspension_dict:
            suspension_dict[date_str] = set()
        suspension_dict[date_str].add(ts_code)

    logger.info(f"停牌字典构建完成，覆盖 {len(suspension_dict)} 个日期")
    return suspension_dict


def merge_dicts(*dicts: Dict[str, Set[str]]) -> Dict[str, Set[str]]:
    """
    合并多个日期-股票字典

    Args:
        *dicts: 多个字典，格式为 {date: {ts_codes}}

    Returns:
        合并后的字典
    """
    merged = {}

    for d in dicts:
        for date_str, ts_codes in d.items():
            if date_str not in merged:
                merged[date_str] = set()
            merged[date_str].update(ts_codes)

    return merged


def filter_dict_by_dates(d: Dict[str, Set[str]], start_date: str, end_date: str) -> Dict[str, Set[str]]:
    """
    按日期范围过滤字典

    Args:
        d: 日期-股票字典
        start_date: 开始日期 YYYYMMDD
        end_date: 结束日期 YYYYMMDD

    Returns:
        过滤后的字典
    """
    return {
        date_str: ts_codes
        for date_str, ts_codes in d.items()
        if start_date <= date_str <= end_date
    }


def get_stocks_on_date(d: Dict[str, Set[str]], date: str) -> Set[str]:
    """
    获取指定日期的股票集合

    Args:
        d: 日期-股票字典
        date: 日期 YYYYMMDD

    Returns:
        股票代码集合，如果日期不存在则返回空集合
    """
    return d.get(date, set())


def is_stock_in_dict(d: Dict[str, Set[str]], date: str, ts_code: str) -> bool:
    """
    判断某股票在某日期是否在字典中

    Args:
        d: 日期-股票字典
        date: 日期 YYYYMMDD
        ts_code: 股票代码

    Returns:
        是否存在
    """
    return date in d and ts_code in d[date]


def dict_statistics(d: Dict[str, Set[str]]) -> Dict[str, any]:
    """
    统计字典信息

    Args:
        d: 日期-股票字典

    Returns:
        统计信息字典
    """
    if not d:
        return {
            'total_dates': 0,
            'total_stocks': 0,
            'avg_stocks_per_date': 0,
            'max_stocks_per_date': 0,
            'min_stocks_per_date': 0,
            'date_range': None
        }

    stocks_per_date = [len(ts_codes) for ts_codes in d.values()]
    all_stocks = set()
    for ts_codes in d.values():
        all_stocks.update(ts_codes)

    dates_sorted = sorted(d.keys())

    return {
        'total_dates': len(d),
        'total_stocks': len(all_stocks),
        'avg_stocks_per_date': sum(stocks_per_date) / len(stocks_per_date),
        'max_stocks_per_date': max(stocks_per_date),
        'min_stocks_per_date': min(stocks_per_date),
        'date_range': (dates_sorted[0], dates_sorted[-1])
    }
