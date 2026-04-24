#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
构建量价因子矩阵

包含因子：
1. Momentum_20d - 20日动量（过去20日累计收益率）
2. Momentum_60d - 60日动量
3. Reversal_5d - 5日反转（短期反转效应）
4. Volatility_20d - 20日波动率（日收益率标准差）
5. Turnover_20d - 20日平均换手率（成交量/流通股本）
6. Amihud - 非流动性指标（|日收益|/日成交额）
"""
import sys
from pathlib import Path

project_root = Path(__file__).resolve().parent.parent.parent
sys.path.insert(0, str(project_root))

import pandas as pd
import numpy as np
from config.config import Config
from data_engine.processors.matrix_io import save_matrix
from data_engine.processors.matrix_builder import MatrixBuilder
from data_engine.processors.data_loader import extract_stock_data
from utils import setup_logger


def build_price_volume_matrices(logger=None):
    """构建所有量价因子矩阵"""

    if logger:
        logger.info("开始构建量价因子矩阵...")

    # 加载基础数据
    basic_info = pd.read_csv(Config.BASIC_DATA_DIR / 'all_companies_info.csv')
    all_stocks = basic_info['ts_code'].tolist()

    # 获取交易日历（只取开市日）
    trade_calendar = pd.read_csv(Config.SUPPLEMENTARY_DATA_DIR / 'trade_calendar.csv')
    trade_calendar = trade_calendar[trade_calendar['is_open'] == 1]
    all_dates = pd.DatetimeIndex(pd.to_datetime(trade_calendar['cal_date'].astype(str), format='%Y%m%d'))

    if logger:
        logger.info(f"股票数: {len(all_stocks)}, 交易日数: {len(all_dates)}")

    # 使用 data_loader 中的通用函数提取数据
    def extract_close_returns(ts_code: str, dates: pd.DatetimeIndex) -> pd.DataFrame:
        """提取收盘价和收益率"""
        df = extract_stock_data(ts_code, dates, columns=['close', 'vol', 'amount'])
        if df['close'].isna().all():
            return pd.DataFrame(index=dates, columns=['close', 'returns', 'vol', 'amount'])

        df['returns'] = df['close'].pct_change()
        return df.rename(columns={'vol': 'vol', 'amount': 'amount'})

    # 初始化矩阵
    matrices = {
        'momentum_20d': pd.DataFrame(index=all_dates.strftime('%Y%m%d'), columns=all_stocks, dtype=np.float32),
        'momentum_60d': pd.DataFrame(index=all_dates.strftime('%Y%m%d'), columns=all_stocks, dtype=np.float32),
        'reversal_5d': pd.DataFrame(index=all_dates.strftime('%Y%m%d'), columns=all_stocks, dtype=np.float32),
        'volatility_20d': pd.DataFrame(index=all_dates.strftime('%Y%m%d'), columns=all_stocks, dtype=np.float32),
        'amihud': pd.DataFrame(index=all_dates.strftime('%Y%m%d'), columns=all_stocks, dtype=np.float32),
    }

    # 逐股票处理
    total = len(all_stocks)
    for i, ts_code in enumerate(all_stocks, 1):
        if i % 500 == 0 and logger:
            logger.info(f"处理进度: {i}/{total} ({i/total:.1%})")

        df = extract_close_returns(ts_code, all_dates)
        if df.empty:
            continue

        returns = df['returns']
        close = df['close']
        amount = df['amount']

        # 1. 20日动量
        matrices['momentum_20d'][ts_code] = (close / close.shift(20) - 1).values.astype(np.float32)

        # 2. 60日动量
        matrices['momentum_60d'][ts_code] = (close / close.shift(60) - 1).values.astype(np.float32)

        # 3. 5日反转（负的5日收益）
        matrices['reversal_5d'][ts_code] = -(close / close.shift(5) - 1).values.astype(np.float32)

        # 4. 20日波动率
        matrices['volatility_20d'][ts_code] = returns.rolling(20).std().values.astype(np.float32)

        # 5. Amihud非流动性（|收益率|/成交额，再取20日均值）
        amihud_daily = np.abs(returns) / (amount + 1e-10)  # 避免除0
        matrices['amihud'][ts_code] = amihud_daily.rolling(20).mean().values.astype(np.float32) * 1e6  # 乘1e6便于阅读

    if logger:
        logger.info(f"处理完成，共 {total} 只股票")

    return matrices


def main():
    logger = setup_logger(prefix="pv_matrices")
    logger.info("="*60)
    logger.info("构建量价因子矩阵")
    logger.info("="*60)

    matrices = build_price_volume_matrices(logger=logger)

    # 保存矩阵
    factor_names = {
        'momentum_20d': '20日动量',
        'momentum_60d': '60日动量',
        'reversal_5d': '5日反转',
        'volatility_20d': '20日波动率',
        'amihud': 'Amihud非流动性',
    }

    for key, name in factor_names.items():
        output_file = Config.MATRIX_DATA_DIR / f'{key}_matrix.csv'
        save_matrix(matrices[key], output_file)

        # 统计
        valid_ratio = matrices[key].notna().sum().sum() / matrices[key].size
        logger.info(f"{name}: 有效数据比例 {valid_ratio:.2%}")

    logger.info("\n全部量价因子矩阵构建完成!")


if __name__ == '__main__':
    main()
