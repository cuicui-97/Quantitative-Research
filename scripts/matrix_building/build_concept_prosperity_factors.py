#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
构建概念景气度基础因子矩阵（PIT正确版本，高性能实现）

因子定义：
1. Concept_Price_Boom: 价格景气 - 概念成分股过去N日平均收益率
2. Concept_Flow_Boom: 资金景气 - 概念成分股成交额(当日/过去M日均值)的平均
3. Concept_Sentiment: 情绪景气 - 概念内上涨家数占比 + 涨停家数占比

概念矩阵格式（PIT正确）：
  - index: 日期 (YYYYMMDD)
  - columns: 股票代码
  - values: 逗号分隔的概念代码，如 '886078,886033'，或 NaN

使用方法：
    python scripts/matrix_building/build_concept_prosperity_factors.py
"""
import sys
from pathlib import Path

project_root = Path(__file__).resolve().parent.parent.parent
sys.path.insert(0, str(project_root))

import pandas as pd
import numpy as np
from collections import defaultdict
from config.config import Config
from data_engine.processors.matrix_io import load_matrix, save_matrix
from data_engine.processors.data_loader import load_daily_column
from utils import setup_logger


def parse_concept_membership_on_date(concept_cell):
    """解析单个单元格的概念列表"""
    if pd.isna(concept_cell):
        return []
    return [c.strip() for c in str(concept_cell).split(',') if c.strip()]


def build_concept_price_boom(return_matrix: pd.DataFrame, concept_matrix: pd.DataFrame,
                             window: int = 5, logger=None):
    """
    构建概念价格景气因子（高性能版本）

    逻辑：
    - 对于每个交易日，根据当天的概念归属关系
    - 计算每个概念成分股过去N日平均收益率的均值
    - 将该均值作为该概念内所有股票的因子值
    """
    if logger:
        logger.info(f"构建概念价格景气因子（{window}日窗口，PIT正确）...")

    # 计算每只股票的过去window日平均收益
    stock_momentum = return_matrix.rolling(window).mean()

    # 结果矩阵用numpy数组存储，最后转成DataFrame
    dates = return_matrix.index.tolist()
    stocks = return_matrix.columns.tolist()
    n_dates = len(dates)
    n_stocks = len(stocks)

    # 股票到列索引的映射
    stock_to_idx = {s: i for i, s in enumerate(stocks)}

    # 初始化结果数组
    result_array = np.full((n_dates, n_stocks), np.nan, dtype=np.float64)
    # 计数数组（用于多概念时求平均）
    count_array = np.zeros((n_dates, n_stocks), dtype=np.int32)

    for i, date in enumerate(dates):
        if i % 200 == 0 and logger:
            logger.info(f"  处理日期: {i}/{n_dates} ({i/n_dates:.1%})")

        # 获取当天的概念归属
        daily_concepts = concept_matrix.loc[date]

        # 构建 概念->股票列表 映射
        concept_to_stocks = defaultdict(list)
        for stock, concept_cell in daily_concepts.dropna().items():
            concepts = parse_concept_membership_on_date(concept_cell)
            for concept in concepts:
                concept_to_stocks[concept].append(stock)

        if len(concept_to_stocks) == 0:
            continue

        # 获取当日所有股票的动量值
        daily_momentum = stock_momentum.loc[date].values

        # 计算每个概念的景气度
        for concept, concept_stocks in concept_to_stocks.items():
            # 获取股票索引
            stock_indices = [stock_to_idx[s] for s in concept_stocks if s in stock_to_idx]
            if len(stock_indices) < 2:
                continue

            # 获取这些股票的动量值
            momentum_values = daily_momentum[stock_indices]
            valid_mask = ~np.isnan(momentum_values)

            if valid_mask.sum() < 2:
                continue

            # 概念价格景气 = 成分股动量均值
            concept_boom = np.nanmean(momentum_values)

            # 批量赋值
            for idx in stock_indices:
                if count_array[i, idx] == 0:
                    result_array[i, idx] = concept_boom
                else:
                    result_array[i, idx] += concept_boom
                count_array[i, idx] += 1

    # 多概念股票取平均
    mask = count_array > 1
    result_array[mask] /= count_array[mask]

    # 转成DataFrame
    result = pd.DataFrame(result_array, index=dates, columns=stocks, dtype=np.float32)

    if logger:
        valid_ratio = result.notna().sum().sum() / result.size
        logger.info(f"  有效数据比例: {valid_ratio:.2%}")

    return result


def build_concept_flow_boom(concept_matrix: pd.DataFrame, dates, stocks,
                            window: int = 20, logger=None):
    """
    构建概念资金景气因子（高性能版本）
    """
    if logger:
        logger.info(f"构建概念资金景气因子（成交额{window}日均值，PIT正确）...")

    # 构建成交额矩阵
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

    # 计算成交额变化率
    if logger:
        logger.info("  计算成交额变化率...")

    amount_ma = amount_matrix.rolling(window, min_periods=1).mean()
    flow_ratio = amount_matrix / (amount_ma + 1e-10)

    # 日期字符串索引（与concept_matrix对齐）
    date_strs = [d.strftime('%Y%m%d') for d in dates]
    flow_ratio.index = date_strs

    # 股票到列索引的映射
    stock_to_idx = {s: i for i, s in enumerate(stocks)}
    n_dates = len(date_strs)
    n_stocks = len(stocks)

    # 初始化结果数组
    result_array = np.full((n_dates, n_stocks), np.nan, dtype=np.float64)
    count_array = np.zeros((n_dates, n_stocks), dtype=np.int32)

    for i, date_str in enumerate(date_strs):
        if i % 200 == 0 and logger:
            logger.info(f"  处理日期: {i}/{n_dates} ({i/n_dates:.1%})")

        # 获取当天的概念归属
        if date_str not in concept_matrix.index:
            continue

        daily_concepts = concept_matrix.loc[date_str]

        concept_to_stocks = defaultdict(list)
        for stock, concept_cell in daily_concepts.dropna().items():
            concepts = parse_concept_membership_on_date(concept_cell)
            for concept in concepts:
                concept_to_stocks[concept].append(stock)

        if len(concept_to_stocks) == 0:
            continue

        # 获取当日所有股票的资金流数据
        daily_flow = flow_ratio.loc[date_str].values

        # 计算每个概念的资金景气
        for concept, concept_stocks in concept_to_stocks.items():
            stock_indices = [stock_to_idx[s] for s in concept_stocks if s in stock_to_idx]
            if len(stock_indices) < 2:
                continue

            flow_values = daily_flow[stock_indices]
            valid_mask = ~np.isnan(flow_values)

            if valid_mask.sum() < 2:
                continue

            concept_flow = np.nanmean(flow_values)

            for idx in stock_indices:
                if count_array[i, idx] == 0:
                    result_array[i, idx] = concept_flow
                else:
                    result_array[i, idx] += concept_flow
                count_array[i, idx] += 1

    # 多概念股票取平均
    mask = count_array > 1
    result_array[mask] /= count_array[mask]

    # 转成DataFrame
    result = pd.DataFrame(result_array, index=date_strs, columns=stocks, dtype=np.float32)

    if logger:
        valid_ratio = result.notna().sum().sum() / result.size
        logger.info(f"  有效数据比例: {valid_ratio:.2%}")

    return result


def build_concept_sentiment(return_matrix: pd.DataFrame, concept_matrix: pd.DataFrame,
                            up_threshold: float = 0.0, limit_threshold: float = 0.095,
                            logger=None):
    """
    构建概念情绪景气因子（高性能版本）
    """
    if logger:
        logger.info(f"构建概念情绪景气因子（上涨>{up_threshold*100:.0f}%, 涨停>{limit_threshold*100:.1f}%）...")

    # 计算上涨和涨停标记
    up_flag = (return_matrix > up_threshold).values.astype(np.float32)
    limit_flag = (return_matrix > limit_threshold).values.astype(np.float32)

    dates = return_matrix.index.tolist()
    stocks = return_matrix.columns.tolist()
    n_dates = len(dates)
    n_stocks = len(stocks)

    stock_to_idx = {s: i for i, s in enumerate(stocks)}

    # 初始化结果数组
    result_array = np.full((n_dates, n_stocks), np.nan, dtype=np.float64)
    count_array = np.zeros((n_dates, n_stocks), dtype=np.int32)

    for i, date in enumerate(dates):
        if i % 200 == 0 and logger:
            logger.info(f"  处理日期: {i}/{n_dates} ({i/n_dates:.1%})")

        # 获取当天的概念归属
        if date not in concept_matrix.index:
            continue

        daily_concepts = concept_matrix.loc[date]

        concept_to_stocks = defaultdict(list)
        for stock, concept_cell in daily_concepts.dropna().items():
            concepts = parse_concept_membership_on_date(concept_cell)
            for concept in concepts:
                concept_to_stocks[concept].append(stock)

        if len(concept_to_stocks) == 0:
            continue

        # 获取当日上涨/涨停数据
        daily_up = up_flag[i]
        daily_limit = limit_flag[i]

        # 计算每个概念的情绪景气
        for concept, concept_stocks in concept_to_stocks.items():
            stock_indices = [stock_to_idx[s] for s in concept_stocks if s in stock_to_idx]
            if len(stock_indices) < 2:
                continue

            up_values = daily_up[stock_indices]
            limit_values = daily_limit[stock_indices]

            # 情绪景气 = (上涨占比 + 涨停占比) / 2
            up_ratio = np.nanmean(up_values)
            limit_ratio = np.nanmean(limit_values)
            sentiment = (up_ratio + limit_ratio) / 2

            for idx in stock_indices:
                if count_array[i, idx] == 0:
                    result_array[i, idx] = sentiment
                else:
                    result_array[i, idx] += sentiment
                count_array[i, idx] += 1

    # 多概念股票取平均
    mask = count_array > 1
    result_array[mask] /= count_array[mask]

    # 转成DataFrame
    result = pd.DataFrame(result_array, index=dates, columns=stocks, dtype=np.float32)

    if logger:
        valid_ratio = result.notna().sum().sum() / result.size
        mean_val = result.mean().mean()
        logger.info(f"  有效数据比例: {valid_ratio:.2%}, 全局均值: {mean_val:.4f}")

    return result


def main():
    logger = setup_logger(prefix="concept_prosperity")
    logger.info("="*60)
    logger.info("构建概念景气度基础因子（PIT正确版本，高性能）")
    logger.info("="*60)

    # 加载数据
    logger.info("\n[1/5] 加载数据...")

    return_matrix = load_matrix(Config.MATRIX_DATA_DIR / 'open_return_matrix.csv')
    logger.info(f"  收益率矩阵: {return_matrix.shape}")

    # 加载概念标签矩阵（日期 × 股票，值为概念列表）
    concept_matrix_file = Config.SUPPLEMENTARY_DATA_DIR / 'concept_stock_matrix.csv'
    concept_matrix = pd.read_csv(concept_matrix_file, index_col=0, dtype=str)
    logger.info(f"  概念标签矩阵: {concept_matrix.shape}")
    logger.info(f"  概念矩阵格式: 日期×股票，值为逗号分隔的概念代码")

    # 对齐数据
    common_dates = return_matrix.index.intersection(concept_matrix.index)
    common_stocks = return_matrix.columns.intersection(concept_matrix.columns)

    return_matrix = return_matrix.loc[common_dates, common_stocks]
    concept_matrix = concept_matrix.loc[common_dates, common_stocks]

    # 将日期索引转换为DatetimeIndex（用于load_daily_column）
    dates = pd.DatetimeIndex(pd.to_datetime(return_matrix.index.astype(str), format='%Y%m%d'))
    stocks = common_stocks.tolist()

    logger.info(f"  对齐后: {len(common_dates)} 日, {len(stocks)} 只股票")

    # 1. 构建概念价格景气因子
    logger.info("\n[2/5] 构建概念价格景气因子...")
    concept_price_boom = build_concept_price_boom(
        return_matrix, concept_matrix, window=5, logger=logger
    )
    save_matrix(concept_price_boom, Config.MATRIX_DATA_DIR / 'concept_price_boom_matrix.csv')
    logger.info("  已保存: concept_price_boom_matrix.csv")

    # 2. 构建概念资金景气因子
    logger.info("\n[3/5] 构建概念资金景气因子...")
    concept_flow_boom = build_concept_flow_boom(
        concept_matrix, dates, stocks, window=20, logger=logger
    )
    save_matrix(concept_flow_boom, Config.MATRIX_DATA_DIR / 'concept_flow_boom_matrix.csv')
    logger.info("  已保存: concept_flow_boom_matrix.csv")

    # 3. 构建概念情绪景气因子
    logger.info("\n[4/5] 构建概念情绪景气因子...")
    concept_sentiment = build_concept_sentiment(
        return_matrix, concept_matrix,
        up_threshold=0.0, limit_threshold=0.095, logger=logger
    )
    save_matrix(concept_sentiment, Config.MATRIX_DATA_DIR / 'concept_sentiment_matrix.csv')
    logger.info("  已保存: concept_sentiment_matrix.csv")

    # 统计信息
    logger.info("\n[5/5] 因子统计:")

    # 价格景气统计
    price_mean = concept_price_boom.mean().mean()
    price_std = concept_price_boom.std().mean()
    logger.info(f"  价格景气: 均值={price_mean:.4f}, 标准差={price_std:.4f}")

    # 资金景气统计
    flow_mean = concept_flow_boom.mean().mean()
    flow_std = concept_flow_boom.std().mean()
    logger.info(f"  资金景气: 均值={flow_mean:.4f}, 标准差={flow_std:.4f}")

    # 情绪景气统计
    sent_mean = concept_sentiment.mean().mean()
    sent_std = concept_sentiment.std().mean()
    logger.info(f"  情绪景气: 均值={sent_mean:.4f}, 标准差={sent_std:.4f}")

    logger.info("\n" + "="*60)
    logger.info("概念景气度基础因子构建完成!")
    logger.info("="*60)
    logger.info("\n生成的因子矩阵:")
    logger.info("  1. concept_price_boom_matrix.csv - 价格景气（5日收益率均值）")
    logger.info("  2. concept_flow_boom_matrix.csv - 资金景气（成交额/20日均值）")
    logger.info("  3. concept_sentiment_matrix.csv - 情绪景气（上涨+涨停占比）")
    logger.info("\nPIT正确性:")
    logger.info("  - 概念归属按每日实际关系计算")
    logger.info("  - 概念list_date之前不含该概念成分股")


if __name__ == '__main__':
    main()
