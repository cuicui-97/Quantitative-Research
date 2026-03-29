"""
数据加载模块

负责加载因子分析所需的各类数据
"""
import pandas as pd
import logging
from pathlib import Path
from typing import Dict

logger = logging.getLogger(__name__)


def load_index_data(supplementary_data_dir: Path) -> Dict[str, pd.Series]:
    """
    加载指数数据

    Args:
        supplementary_data_dir: 补充数据目录路径

    Returns:
        dict: {指数名称: 收益率序列}
    """
    logger.info("加载指数数据...")

    index_returns = {}
    index_codes = {
        '沪深300': '000300.SH',
        '上证50': '000016.SH',
        '中证1000': '000852.SH'
    }

    for name, code in index_codes.items():
        index_file = supplementary_data_dir / f'{code}.csv'
        if index_file.exists():
            df = pd.read_csv(index_file, dtype={'trade_date': str})
            df['trade_date'] = pd.to_datetime(df['trade_date'], format='%Y%m%d')
            df = df.set_index('trade_date').sort_index()

            if 'open' in df.columns:
                returns = df['open'].pct_change()
                index_returns[name] = returns
                logger.info(f"  {name}: {len(returns)} 个交易日")
            else:
                logger.warning(f"  {name}: 缺少开盘价数据")
        else:
            logger.warning(f"  {name}: 数据文件不存在")

    return index_returns
