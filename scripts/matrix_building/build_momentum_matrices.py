#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
构建动量/反转/波动率因子矩阵（向量化版）

因子定义（T日收盘后可计算）：
1. Reversal_5d:   -(过去5日累计收益)，预期短期反转
2. Momentum_20d:  过去20日累计收益，预期中期动量
3. Momentum_60d:  过去60日累计收益，预期季度动量
4. Volatility_20d: 20日收益率标准差

优化点：
- 使用矩阵向量化计算，避免逐股票循环
- 先构建价格矩阵，再对整个矩阵做rolling操作

使用方法:
    python scripts/matrix_building/build_momentum_matrices.py
    python scripts/matrix_building/build_momentum_matrices.py --start-date 20200101
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




def calculate_momentum_factors(close_matrix):
    """
    向量化计算动量因子

    Args:
        close_matrix: 价格矩阵 (dates x stocks)

    Returns:
        Dict[str, DataFrame]: 各因子矩阵
    """
    # 计算收益率矩阵
    returns_matrix = close_matrix.pct_change()

    factors = {}

    # 向量化计算（对整个矩阵操作，非逐股票循环）
    factors['reversal_5d'] = -close_matrix.pct_change(5)
    factors['momentum_20d'] = close_matrix.pct_change(20)
    factors['momentum_60d'] = close_matrix.pct_change(60)
    factors['volatility_20d'] = returns_matrix.rolling(20).std()

    return factors


def main():
    parser = argparse.ArgumentParser(description='构建动量/反转/波动率因子矩阵（向量化版）')
    parser.add_argument('--start-date', type=str, default='20150101',
                        help='开始日期（YYYYMMDD格式），默认20150101')
    parser.add_argument('--end-date', type=str, default=None,
                        help='结束日期（YYYYMMDD格式），默认到今天')
    args = parser.parse_args()

    logger = setup_logger(prefix="momentum_matrices")
    logger.info("=" * 60)
    logger.info("构建动量/反转/波动率因子矩阵（向量化版）")
    logger.info(f"日期范围: {args.start_date} 至 {args.end_date or '今天'}")
    logger.info("=" * 60)

    # 获取日期和股票列表
    from data_engine.processors.data_loader import (
        get_all_trading_dates, get_all_stocks, build_price_matrix, filter_dates
    )

    dates = get_all_trading_dates()
    dates = filter_dates(dates, args.start_date, args.end_date)

    stocks = get_all_stocks()
    date_strs = dates.strftime('%Y%m%d')

    logger.info(f"交易日: {len(dates)}, 股票: {len(stocks)}")

    # 构建价格矩阵
    close_matrix = build_price_matrix(dates, stocks, 'close', logger)

    # 向量化计算因子
    logger.info("\n向量化计算因子...")
    factors = calculate_momentum_factors(close_matrix)

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

    logger.info("\n全部动量/反转/波动率因子矩阵构建完成!")


if __name__ == '__main__':
    main()
