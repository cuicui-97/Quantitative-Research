#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
构建流动性因子矩阵

因子定义（避免未来数据）：
1. Amihud_20d: 20日平均(|日收益率|/日成交额)，衡量非流动性
2. Turnover_20d: 20日平均换手率(成交量/流通股本)
3. Turnover_Vol_20d: 20日换手率波动率(标准差)
4. Volume_Price_Corr: 20日成交量与收盘价的相关系数

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
from data_engine.processors.data_loader import extract_stock_data
from utils import setup_logger


def build_liquidity_matrices(logger=None):
    """构建流动性因子矩阵"""

    if logger:
        logger.info("开始构建流动性因子矩阵...")

    # 加载基础数据
    basic_info = pd.read_csv(Config.BASIC_DATA_DIR / 'all_companies_info.csv')
    all_stocks = basic_info['ts_code'].tolist()

    # 加载流通股本数据（用于计算换手率）
    mv_df = pd.read_csv(Config.SUPPLEMENTARY_DATA_DIR / 'daily_basic.csv', dtype={'trade_date': str})
    mv_df['trade_date'] = pd.to_datetime(mv_df['trade_date'], format='%Y%m%d')
    # 流通市值(万元) / 收盘价 = 流通股本(万股)
    # 或者用 circ_mv / close，但要注意单位

    # 获取交易日历
    trade_calendar = pd.read_csv(Config.SUPPLEMENTARY_DATA_DIR / 'trade_calendar.csv')
    trade_calendar = trade_calendar[trade_calendar['is_open'] == 1]
    all_dates = pd.DatetimeIndex(pd.to_datetime(trade_calendar['cal_date'].astype(str), format='%Y%m%d'))

    if logger:
        logger.info(f"股票数: {len(all_stocks)}, 交易日数: {len(all_dates)}")

    # 初始化结果矩阵
    amihud = pd.DataFrame(np.nan, index=all_dates.strftime('%Y%m%d'), columns=all_stocks, dtype=np.float32)
    turnover = pd.DataFrame(np.nan, index=all_dates.strftime('%Y%m%d'), columns=all_stocks, dtype=np.float32)
    turnover_vol = pd.DataFrame(np.nan, index=all_dates.strftime('%Y%m%d'), columns=all_stocks, dtype=np.float32)
    vp_corr = pd.DataFrame(np.nan, index=all_dates.strftime('%Y%m%d'), columns=all_stocks, dtype=np.float32)

    # 逐股票处理
    total = len(all_stocks)
    for i, ts_code in enumerate(all_stocks, 1):
        if i % 500 == 0 and logger:
            logger.info(f"处理进度: {i}/{total} ({i/total:.1%})")

        df = extract_stock_data(ts_code, all_dates)
        if df['close'].isna().all():
            continue

        close = df['close']
        vol = df['vol']
        amount = df['amount']

        # 日收益率（T日收盘后计算，用于T+1日交易）
        # close.pct_change() 计算 (close_T - close_T-1) / close_T-1
        # T日收盘后，close_T和close_T-1都已知，所以T日收益率是已知的历史数据
        daily_ret = close.pct_change()

        # 1. Amihud非流动性 = |日收益率|/日成交额，20日平均
        # 注意：amount单位是千元，需要乘1000转为元
        # Amihud单位：1/元，乘以1e9便于阅读
        # 当天收益率 / 当天成交额（单位匹配）
        amihud_daily = np.abs(daily_ret) / (amount * 1000 + 1e-10)  # amount(千元) -> 元
        amihud[ts_code] = amihud_daily.rolling(20).mean().values.astype(np.float32) * 1e9

        # 2. 换手率 = 成交量/流通股本，这里用成交额/流通市值近似
        # 需要从daily_basic获取流通市值
        stock_mv = mv_df[mv_df['ts_code'] == ts_code][['trade_date', 'circ_mv']]
        if not stock_mv.empty:
            stock_mv = stock_mv.set_index('trade_date')['circ_mv']
            stock_mv = stock_mv.reindex(all_dates)
            # 换手率 = 成交额(元) / 流通市值(万元) * 10000 = 成交额 / (circ_mv * 10000) * 100 = 成交额 / (circ_mv * 100)
            # 简化：turnover = amount / (circ_mv * 100)  # 单位：%
            turnover_daily = amount / (stock_mv * 100 + 1e-10)
            turnover[ts_code] = turnover_daily.rolling(20).mean().values.astype(np.float32)
            turnover_vol[ts_code] = turnover_daily.rolling(20).std().values.astype(np.float32)
        else:
            # 如果没有市值数据，用成交量标准化
            turnover_daily = vol / (vol.rolling(60).mean() + 1e-10)
            turnover[ts_code] = turnover_daily.rolling(20).mean().values.astype(np.float32)
            turnover_vol[ts_code] = turnover_daily.rolling(20).std().values.astype(np.float32)

        # 3. 成交量与价格相关系数（20日）
        # 使用当天的成交量和收盘价计算相关性
        vp_corr[ts_code] = vol.rolling(20).corr(close).values.astype(np.float32)

    if logger:
        logger.info(f"处理完成，共 {total} 只股票")

    return {
        'amihud_20d': amihud,
        'turnover_20d': turnover,
        'turnover_vol_20d': turnover_vol,
        'vp_corr_20d': vp_corr,
    }


def main():
    logger = setup_logger(prefix="liquidity_matrices")
    logger.info("="*60)
    logger.info("构建流动性因子矩阵")
    logger.info("="*60)

    matrices = build_liquidity_matrices(logger=logger)

    # 保存矩阵
    factor_info = {
        'amihud_20d': 'Amihud非流动性(20日)',
        'turnover_20d': '换手率(20日均)',
        'turnover_vol_20d': '换手率波动(20日)',
        'vp_corr_20d': '价量相关系数(20日)',
    }

    for key, name in factor_info.items():
        output_file = Config.MATRIX_DATA_DIR / f'{key}_matrix.csv'
        save_matrix(matrices[key], output_file)

        # 统计
        valid_ratio = matrices[key].notna().sum().sum() / matrices[key].size
        mean_val = matrices[key].mean().mean()
        std_val = matrices[key].std().mean()
        logger.info(f"{name}: 有效数据 {valid_ratio:.2%}, 均值 {mean_val:.4f}, 标准差 {std_val:.4f}")

    logger.info("\n全部流动性因子矩阵构建完成!")


if __name__ == '__main__':
    main()
