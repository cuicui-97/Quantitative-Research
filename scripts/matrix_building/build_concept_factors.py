#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
构建概念因子矩阵

因子定义：
1. Concept_Momentum_5d: 过去5日概念平均收益率（概念动量）
2. Concept_Flow: 概念资金流向（当日成交额 / 过去20日平均成交额）

使用方法：
    python scripts/matrix_building/build_concept_factors.py
"""
import sys
from pathlib import Path

project_root = Path(__file__).resolve().parent.parent.parent
sys.path.insert(0, str(project_root))

import pandas as pd
import numpy as np
from config.config import Config
from data_engine.processors.matrix_io import load_matrix, save_matrix
from data_engine.processors.data_loader import load_daily_column
from utils import setup_logger


def build_concept_momentum(return_matrix: pd.DataFrame, concept_matrix: pd.DataFrame, window: int = 5, logger=None):
    """
    构建概念动量因子

    逻辑：
    - 对于每个概念，计算其成分股过去N日的平均收益率
    - 将该平均值作为该概念内所有股票的因子值

    Args:
        return_matrix: 收益率矩阵 (dates × stocks)
        concept_matrix: 概念矩阵 (stocks × concepts)，0/1值
        window: 动量计算窗口（日）
        logger: 日志对象

    Returns:
        DataFrame: 概念动量因子矩阵 (dates × stocks)
    """
    if logger:
        logger.info(f"构建概念动量因子（{window}日窗口）...")

    # 计算每只股票的过去window日累计收益
    stock_momentum = return_matrix.rolling(window).mean()

    # 初始化结果矩阵
    concept_momentum = pd.DataFrame(
        np.nan,
        index=return_matrix.index,
        columns=return_matrix.columns,
        dtype=np.float32
    )

    # 对每个概念
    concepts = concept_matrix.columns
    for i, concept in enumerate(concepts):
        if i % 50 == 0 and logger:
            logger.info(f"  处理概念: {i}/{len(concepts)} ({i/len(concepts):.1%})")

        # 获取该概念的成分股
        stocks_in_concept = concept_matrix[concept_matrix[concept] == 1].index.tolist()

        # 过滤出在收益率矩阵中的股票
        valid_stocks = [s for s in stocks_in_concept if s in stock_momentum.columns]

        if len(valid_stocks) < 3:  # 成分股太少，跳过
            continue

        # 计算概念动量（成分股的平均动量）
        concept_mom = stock_momentum[valid_stocks].mean(axis=1)

        # 将该概念动量赋给所有成分股
        for stock in valid_stocks:
            concept_momentum[stock] = concept_mom

    if logger:
        valid_ratio = concept_momentum.notna().sum().sum() / concept_momentum.size
        logger.info(f"  有效数据比例: {valid_ratio:.2%}")

    return concept_momentum


def build_concept_flow(concept_matrix: pd.DataFrame, dates, stocks, window: int = 20, logger=None):
    """
    构建概念资金流向因子

    逻辑：
    - 计算每只股票的成交额变化率（当日 / 过去20日平均）
    - 对于每个概念，计算其成分股的平均成交额变化率
    - 将该平均值作为该概念内所有股票的因子值

    Args:
        concept_matrix: 概念矩阵 (stocks × concepts)
        dates: 日期索引
        stocks: 股票列表
        window: 成交额平均窗口
        logger: 日志对象

    Returns:
        DataFrame: 概念资金流向因子矩阵 (dates × stocks)
    """
    if logger:
        logger.info(f"构建概念资金流向因子（{window}日窗口）...")

    # 首先构建成交额矩阵
    if logger:
        logger.info("  构建成交额矩阵...")

    amount_matrix = pd.DataFrame(np.nan, index=dates, columns=stocks, dtype=np.float32)

    total = len(stocks)
    for i, ts_code in enumerate(stocks):
        if i % 500 == 0 and logger:
            logger.info(f"    进度: {i}/{total} ({i/total:.1%})")

        amount = load_daily_column(ts_code, dates, 'amount', default_value=np.nan)
        if not amount.isna().all():
            amount_matrix[ts_code] = amount.values

    # 计算成交额变化率（当日 / 过去20日平均）
    if logger:
        logger.info("  计算成交额变化率...")

    amount_ma = amount_matrix.rolling(window, min_periods=1).mean()
    flow_ratio = amount_matrix / (amount_ma + 1e-10)  # 避免除0

    # 初始化结果矩阵（index使用原始日期格式，与save_matrix兼容）
    original_dates = return_matrix.index
    concept_flow = pd.DataFrame(
        np.nan,
        index=original_dates,
        columns=stocks,
        dtype=np.float32
    )

    # 对每个概念计算平均资金流向
    concepts = concept_matrix.columns
    for i, concept in enumerate(concepts):
        if i % 50 == 0 and logger:
            logger.info(f"  处理概念资金流向: {i}/{len(concepts)}")

        stocks_in_concept = concept_matrix[concept_matrix[concept] == 1].index.tolist()
        valid_stocks = [s for s in stocks_in_concept if s in flow_ratio.columns]

        if len(valid_stocks) < 3:
            continue

        # 计算概念平均资金流向
        concept_avg_flow = flow_ratio[valid_stocks].mean(axis=1)

        # 赋给所有成分股
        for stock in valid_stocks:
            concept_flow[stock] = concept_avg_flow

    if logger:
        valid_ratio = concept_flow.notna().sum().sum() / concept_flow.size
        logger.info(f"  有效数据比例: {valid_ratio:.2%}")

    return concept_flow


def main():
    logger = setup_logger(prefix="concept_factors")
    logger.info("="*60)
    logger.info("构建概念因子矩阵")
    logger.info("="*60)

    # 加载数据
    logger.info("\n[1/4] 加载数据...")

    return_matrix = load_matrix(Config.MATRIX_DATA_DIR / 'open_return_matrix.csv')
    logger.info(f"  收益率矩阵: {return_matrix.shape}")

    concept_matrix = load_matrix(Config.SUPPLEMENTARY_DATA_DIR / 'concept_stock_matrix.csv')
    logger.info(f"  概念矩阵: {concept_matrix.shape}")

    # 对齐数据
    common_stocks = return_matrix.columns.intersection(concept_matrix.index)
    return_matrix = return_matrix[common_stocks]
    concept_matrix = concept_matrix.loc[common_stocks]

    # 将日期索引转换为DatetimeIndex
    dates = pd.DatetimeIndex(pd.to_datetime(return_matrix.index.astype(str), format='%Y%m%d'))
    stocks = common_stocks.tolist()

    logger.info(f"  对齐后: {len(dates)} 日, {len(stocks)} 只股票, {len(concept_matrix.columns)} 个概念")

    # 1. 构建概念动量因子
    logger.info("\n[2/4] 构建概念动量因子（5日）...")
    concept_momentum_5d = build_concept_momentum(return_matrix, concept_matrix, window=5, logger=logger)
    save_matrix(concept_momentum_5d, Config.MATRIX_DATA_DIR / 'concept_momentum_5d_matrix.csv')
    logger.info("  已保存: concept_momentum_5d_matrix.csv")

    # 2. 构建概念资金流向因子
    logger.info("\n[3/4] 构建概念资金流向因子（20日）...")
    concept_flow = build_concept_flow(concept_matrix, dates, stocks, window=20, logger=logger)
    save_matrix(concept_flow, Config.MATRIX_DATA_DIR / 'concept_flow_matrix.csv')
    logger.info("  已保存: concept_flow_matrix.csv")

    # 统计信息
    logger.info("\n[4/4] 因子统计:")

    # 概念动量分布
    mom_mean = concept_momentum_5d.mean().mean()
    mom_std = concept_momentum_5d.std().mean()
    logger.info(f"  概念动量5日: 均值={mom_mean:.4f}, 标准差={mom_std:.4f}")

    # 资金流向分布
    flow_mean = concept_flow.mean().mean()
    flow_std = concept_flow.std().mean()
    logger.info(f"  概念资金流向: 均值={flow_mean:.4f}, 标准差={flow_std:.4f}")

    logger.info("\n" + "="*60)
    logger.info("概念因子矩阵构建完成!")
    logger.info("="*60)
    logger.info("\n生成的因子矩阵:")
    logger.info("  1. concept_momentum_5d_matrix.csv - 概念动量因子（5日）")
    logger.info("  2. concept_flow_matrix.csv - 概念资金流向因子（20日）")
    logger.info("\n使用建议:")
    logger.info("  - 概念动量：选因子值最高的组（强势概念）")
    logger.info("  - 资金流向：选因子值>1的组（资金流入）")


if __name__ == '__main__':
    main()
