#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
构建动量/反转因子矩阵

因子定义（避免未来数据，信号用于次日交易）：
1. Reversal_5d:  -(过去5日累计收益)，预期短期反转
2. Momentum_20d: 过去20日累计收益，预期中期动量
3. Momentum_60d: 过去60日累计收益，预期季度动量

注意：因子值在T日收盘后计算，用于T+1日开盘选股
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


def build_momentum_matrices(logger=None):
    """构建动量/反转因子矩阵"""

    if logger:
        logger.info("开始构建动量/反转因子矩阵...")

    # 加载基础数据
    basic_info = pd.read_csv(Config.BASIC_DATA_DIR / 'all_companies_info.csv')
    all_stocks = basic_info['ts_code'].tolist()

    # 获取交易日历（只取开市日）
    trade_calendar = pd.read_csv(Config.SUPPLEMENTARY_DATA_DIR / 'trade_calendar.csv')
    trade_calendar = trade_calendar[trade_calendar['is_open'] == 1]
    all_dates = pd.DatetimeIndex(pd.to_datetime(trade_calendar['cal_date'].astype(str), format='%Y%m%d'))

    if logger:
        logger.info(f"股票数: {len(all_stocks)}, 交易日数: {len(all_dates)}")

    # 初始化结果矩阵（用NaN填充）
    reversal_5d = pd.DataFrame(np.nan, index=all_dates.strftime('%Y%m%d'), columns=all_stocks, dtype=np.float32)
    momentum_20d = pd.DataFrame(np.nan, index=all_dates.strftime('%Y%m%d'), columns=all_stocks, dtype=np.float32)
    momentum_60d = pd.DataFrame(np.nan, index=all_dates.strftime('%Y%m%d'), columns=all_stocks, dtype=np.float32)

    # 逐股票处理
    total = len(all_stocks)
    for i, ts_code in enumerate(all_stocks, 1):
        if i % 500 == 0 and logger:
            logger.info(f"处理进度: {i}/{total} ({i/total:.1%})")

        close = load_daily_column(ts_code, all_dates, 'close', default_value=np.nan)
        if close.isna().all():
            continue

        # 计算收益率（用于计算累计收益）
        # 注意：因子值在T日计算，表示用T日收盘价能观察到的信号
        # 用于T+1日开盘交易，这符合避免未来数据原则

        # 1. 5日反转 = -(过去5日累计收益)
        # 使用shift(1)确保是用T-5到T-1日的收益（T日收盘后才能知道）
        ret_5d = close.pct_change(5).shift(1)  # T-5到T日的收益，shift后变成T-6到T-1
        reversal_5d[ts_code] = (-ret_5d).values.astype(np.float32)

        # 2. 20日动量
        ret_20d = close.pct_change(20).shift(1)
        momentum_20d[ts_code] = ret_20d.values.astype(np.float32)

        # 3. 60日动量
        ret_60d = close.pct_change(60).shift(1)
        momentum_60d[ts_code] = ret_60d.values.astype(np.float32)

    if logger:
        logger.info(f"处理完成，共 {total} 只股票")

    return {
        'reversal_5d': reversal_5d,
        'momentum_20d': momentum_20d,
        'momentum_60d': momentum_60d,
    }


def main():
    logger = setup_logger(prefix="momentum_matrices")
    logger.info("="*60)
    logger.info("构建动量/反转因子矩阵")
    logger.info("="*60)

    matrices = build_momentum_matrices(logger=logger)

    # 保存矩阵
    factor_info = {
        'reversal_5d': '5日反转因子',
        'momentum_20d': '20日动量因子',
        'momentum_60d': '60日动量因子',
    }

    for key, name in factor_info.items():
        output_file = Config.MATRIX_DATA_DIR / f'{key}_matrix.csv'
        save_matrix(matrices[key], output_file)

        # 统计
        valid_ratio = matrices[key].notna().sum().sum() / matrices[key].size
        mean_val = matrices[key].mean().mean()
        std_val = matrices[key].std().mean()
        logger.info(f"{name}: 有效数据 {valid_ratio:.2%}, 均值 {mean_val:.4f}, 标准差 {std_val:.4f}")

    logger.info("\n全部动量/反转因子矩阵构建完成!")


if __name__ == '__main__':
    main()
