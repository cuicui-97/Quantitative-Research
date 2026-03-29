"""
数据加载工具函数

提供日线数据加载的通用函数
"""
import logging
import pandas as pd
from typing import Any
from config.config import Config

logger = logging.getLogger(__name__)


def load_daily_data(
    ts_code: str,
    dates: pd.DatetimeIndex,
    default_value: Any = None
) -> pd.DataFrame:
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
