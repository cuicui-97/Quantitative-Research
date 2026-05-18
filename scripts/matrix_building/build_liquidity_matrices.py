#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
构建流动性因子矩阵（向量化版）

因子定义（T日收盘后可计算）：
1. Amihud_20d: 20日平均(|日收益率|/日成交额)，衡量非流动性
2. Turnover_20d: 20日平均换手率(成交量/流通股本)
3. Turnover_Vol_20d: 20日换手率波动率(标准差)
4. Volume_Price_Corr: 20日成交量与收盘价的相关系数

优化点：
- 使用矩阵向量化计算，避免逐股票循环
- 先构建价格/成交额/成交量矩阵，再对整个矩阵做rolling操作

使用方法:
    python scripts/matrix_building/build_liquidity_matrices.py
    python scripts/matrix_building/build_liquidity_matrices.py --start-date 20200101
"""
import argparse
import sys
from pathlib import Path

project_root = Path(__file__).resolve().parent.parent.parent
sys.path.insert(0, str(project_root))

import pandas as pd
import numpy as np

from config.config import Config
from data_engine.processors.matrix_io import load_matrix, save_matrix
from utils import setup_logger




def build_market_value_matrix(dates, stocks, logger):
    """构建流通市值矩阵 (dates x stocks)"""
    logger.info("构建流通市值矩阵...")

    mv_file = Config.SUPPLEMENTARY_DATA_DIR / 'daily_basic.csv'
    if not mv_file.exists():
        logger.warning("  daily_basic.csv 不存在，跳过换手率计算")
        return None

    try:
        mv_df = pd.read_csv(mv_file, dtype={'trade_date': str, 'ts_code': str})
        mv_df['trade_date'] = pd.to_datetime(mv_df['trade_date'], format='%Y%m%d')

        # 筛选有效数据
        mv_df = mv_df[mv_df['ts_code'].isin(stocks)]
        mv_df = mv_df[mv_df['trade_date'].isin(dates)]

        # 构建矩阵
        mv_matrix = mv_df.pivot(index='trade_date', columns='ts_code', values='circ_mv')
        mv_matrix = mv_matrix.reindex(index=dates, columns=stocks)

        logger.info(f"  流通市值矩阵: {mv_matrix.shape}")
        return mv_matrix
    except Exception as e:
        logger.warning(f"  构建市值矩阵失败: {e}")
        return None


def calculate_liquidity_factors(close_matrix, vol_matrix, amount_matrix, mv_matrix):
    """
    向量化计算流动性因子

    Args:
        close_matrix: 收盘价矩阵 (dates x stocks)
        vol_matrix: 成交量矩阵 (dates x stocks)
        amount_matrix: 成交额矩阵 (dates x stocks)
        mv_matrix: 流通市值矩阵 (dates x stocks)，可为None

    Returns:
        Dict[str, DataFrame]: 各因子矩阵
    """
    logger = setup_logger()
    factors = {}

    # 1. Amihud非流动性 = |收益率| / 成交额
    logger.info("  计算 Amihud_20d...")
    returns_matrix = close_matrix.pct_change()
    amihud_daily = np.abs(returns_matrix) / (amount_matrix * 1000 + 1e-10)
    factors['amihud_20d'] = amihud_daily.rolling(20).mean() * 1e9

    # 2&3. 换手率和换手率波动
    logger.info("  计算 Turnover_20d / Turnover_Vol_20d...")
    if mv_matrix is not None:
        # 换手率 = 成交额 / (流通市值 * 100)
        turnover_daily = amount_matrix / (mv_matrix * 100 + 1e-10)
    else:
        # 备用：用成交量标准化
        turnover_daily = vol_matrix / (vol_matrix.rolling(60).mean() + 1e-10)

    factors['turnover_20d'] = turnover_daily.rolling(20).mean()
    factors['turnover_vol_20d'] = turnover_daily.rolling(20).std()

    # 4. 价量相关系数
    logger.info("  计算 VP_Corr_20d...")
    factors['vp_corr_20d'] = vol_matrix.rolling(20).corr(close_matrix)

    return factors


def main():
    parser = argparse.ArgumentParser(description='构建流动性因子矩阵（向量化版）')
    parser.add_argument('--start-date', type=str, default='20150101',
                        help='开始日期（YYYYMMDD格式），默认20150101')
    parser.add_argument('--end-date', type=str, default=None,
                        help='结束日期（YYYYMMDD格式），默认到今天')
    args = parser.parse_args()

    logger = setup_logger(prefix="liquidity_matrices")
    logger.info("=" * 60)
    logger.info("构建流动性因子矩阵（向量化版）")
    logger.info(f"日期范围: {args.start_date} 至 {args.end_date or '今天'}")
    logger.info("=" * 60)

    # 获取日期和股票列表
    from data_engine.processors.data_loader import (
        get_all_trading_dates, get_all_stocks, build_ohlcv_matrices, filter_dates
    )

    dates = get_all_trading_dates()
    dates = filter_dates(dates, args.start_date, args.end_date)

    stocks = get_all_stocks()
    date_strs = dates.strftime('%Y%m%d')

    logger.info(f"交易日: {len(dates)}, 股票: {len(stocks)}")

    # 构建OHLCV矩阵
    ohlcv = build_ohlcv_matrices(dates, stocks, columns=['close', 'vol', 'amount'], logger=logger)
    close_matrix = ohlcv['close']
    vol_matrix = ohlcv['vol']
    amount_matrix = ohlcv['amount']

    # 构建流通市值矩阵
    mv_matrix = build_market_value_matrix(dates, stocks, logger)

    # 向量化计算因子
    logger.info("\n向量化计算因子...")
    factors = calculate_liquidity_factors(close_matrix, vol_matrix, amount_matrix, mv_matrix)

    # 保存结果
    logger.info("\n保存因子矩阵...")
    for name, matrix in factors.items():
        # 转换索引为日期字符串
        matrix.index = date_strs
        output_file = Config.MATRIX_DATA_DIR / f'{name}_matrix.csv'
        save_matrix(matrix, output_file)

        valid_ratio = matrix.notna().sum().sum() / matrix.size
        mean_val = matrix.mean().mean()
        std_val = matrix.std().mean()
        logger.info(f"  {name}: 有效数据 {valid_ratio:.2%}, 均值 {mean_val:.4f}, 标准差 {std_val:.4f}")

    logger.info("\n全部流动性因子矩阵构建完成!")


if __name__ == '__main__':
    main()
