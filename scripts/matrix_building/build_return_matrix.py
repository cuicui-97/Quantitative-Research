#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
构建收益率矩阵

收益率定义（后复权开盘价计算）：
    Return_t = (Open_t - Open_{t-1}) / Open_{t-1}

含义：
    - 如果在t-1日开盘买入，t日开盘卖出，收益率为Return_t
    - 或者说，t日的收益 = (t日开盘价 - t-1日开盘价) / t-1日开盘价

时间对齐（回测中）：
    - T日收盘后计算因子
    - T+1日开盘买入
    - T+2日开盘卖出
    - 收益 = return_matrix[T+2] = (Open_T+2 - Open_T+1) / Open_T+1
    - 回测引擎用 return_matrix.shift(-1) 来对齐因子和收益

注意：
    - 使用后复权价格(open列)，已考虑分红送股
    - 不包含交易成本
"""
import sys
from pathlib import Path

project_root = Path(__file__).resolve().parent.parent.parent
sys.path.insert(0, str(project_root))

import pandas as pd
import numpy as np
from config.config import Config
from data_engine.processors.matrix_io import save_matrix
from data_engine.processors.data_loader import load_daily_column
from utils import setup_logger


def extract_open_return(ts_code: str, dates: pd.DatetimeIndex) -> pd.Series:
    """
    提取开盘收益率序列

    Args:
        ts_code: 股票代码
        dates: 日期索引

    Returns:
        Series: 开盘收益率，index=dates
    """
    open_price = load_daily_column(ts_code, dates, 'open', default_value=np.nan)

    if open_price.isna().all():
        return pd.Series(index=dates, dtype=np.float32)

    # 计算收益率: Return_t = (Open_t - Open_{t-1}) / Open_{t-1}
    returns = open_price.pct_change()

    return returns.astype(np.float32)


def build_return_matrix(logger=None):
    """
    构建开盘收益率矩阵

    Returns:
        DataFrame: 收益率矩阵 (dates × stocks)
    """
    if logger:
        logger.info("开始构建开盘收益率矩阵...")

    # 加载股票列表
    basic_info = pd.read_csv(Config.BASIC_DATA_DIR / 'all_companies_info.csv')
    all_stocks = basic_info['ts_code'].tolist()

    # 加载交易日历
    trade_calendar = pd.read_csv(Config.SUPPLEMENTARY_DATA_DIR / 'trade_calendar.csv')
    trade_calendar = trade_calendar[trade_calendar['is_open'] == 1]
    all_dates = pd.DatetimeIndex(pd.to_datetime(trade_calendar['cal_date'].astype(str), format='%Y%m%d'))

    if logger:
        logger.info(f"股票数: {len(all_stocks)}, 交易日数: {len(all_dates)}")

    # 初始化结果矩阵
    return_matrix = pd.DataFrame(np.nan, index=all_dates.strftime('%Y%m%d'), columns=all_stocks, dtype=np.float32)

    # 逐股票处理
    total = len(all_stocks)
    for i, ts_code in enumerate(all_stocks, 1):
        if i % 500 == 0 and logger:
            logger.info(f"处理进度: {i}/{total} ({i/total:.1%})")

        returns = extract_open_return(ts_code, all_dates)
        if not returns.isna().all():
            return_matrix[ts_code] = returns.values.astype(np.float32)

    if logger:
        logger.info(f"处理完成，共 {total} 只股票")
        valid_ratio = return_matrix.notna().sum().sum() / return_matrix.size
        logger.info(f"有效数据比例: {valid_ratio:.2%}")

    return return_matrix


def main():
    logger = setup_logger(prefix="return_matrix")
    logger.info("="*60)
    logger.info("构建开盘收益率矩阵")
    logger.info("="*60)
    logger.info("收益率定义: Return_t = (Open_t - Open_{t-1}) / Open_{t-1}")
    logger.info("含义: t-1日开盘买入，t日开盘卖出的收益率")
    logger.info("="*60)

    return_matrix = build_return_matrix(logger=logger)

    # 保存矩阵
    output_file = Config.MATRIX_DATA_DIR / 'open_return_matrix.csv'
    save_matrix(return_matrix, output_file)
    logger.info(f"\n收益率矩阵已保存: {output_file}")
    logger.info(f"矩阵维度: {return_matrix.shape}")

    # 示例验证
    logger.info("\n示例验证 (000001.SZ 平安银行):")
    sample_dates = ['20240103', '20240104', '20240105']
    for d in sample_dates:
        if d in return_matrix.index:
            logger.info(f"  {d}: {return_matrix.loc[d, '000001.SZ']:.4f}")

    logger.info("\n构建完成!")


if __name__ == '__main__':
    main()
