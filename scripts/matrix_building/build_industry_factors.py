#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
构建行业因子矩阵

因子定义：
1. Industry_Momentum_20d: 过去20日行业平均收益率（个股所属行业的动量）
2. Industry_Rank: 行业动量在所有行业中的排名（0-100）
3. 行业内标准化: 将现有因子在行业内做z-score标准化

行业数据来自: basic/all_companies_info.csv 的 industry 字段
"""
import sys
from pathlib import Path

project_root = Path(__file__).resolve().parent.parent.parent
sys.path.insert(0, str(project_root))

import pandas as pd
import numpy as np
from config.config import Config
from data_engine.processors.matrix_io import load_matrix, save_matrix
from utils import setup_logger


def build_industry_momentum_matrix(return_matrix: pd.DataFrame, industry_map: dict, window: int = 20, logger=None):
    """
    构建行业动量因子矩阵

    Args:
        return_matrix: 收益率矩阵 (dates × stocks)
        industry_map: 股票到行业的映射字典 {ts_code: industry}
        window: 动量计算窗口（日）
        logger: 日志对象

    Returns:
        DataFrame: 行业动量因子矩阵
    """
    if logger:
        logger.info(f"构建行业动量因子（{window}日窗口）...")

    # 计算每只股票的过去window日累计收益
    stock_momentum = return_matrix.rolling(window).mean()

    # 获取所有行业
    all_industries = set(industry_map.values())
    if '' in all_industries:
        all_industries.remove('')  # 移除空行业

    if logger:
        logger.info(f"  共 {len(all_industries)} 个行业")

    # 初始化行业动量矩阵
    industry_momentum = pd.DataFrame(np.nan, index=return_matrix.index, columns=return_matrix.columns)

    # 对每个日期，计算各行业平均动量，再赋给个股
    for date in return_matrix.index:
        if pd.isna(stock_momentum.loc[date]).all():
            continue

        # 计算各行业平均动量
        industry_avg = {}
        for industry in all_industries:
            # 获取该行业所有股票
            stocks_in_industry = [s for s, ind in industry_map.items() if ind == industry and s in stock_momentum.columns]
            if stocks_in_industry:
                industry_avg[industry] = stock_momentum.loc[date, stocks_in_industry].mean()

        # 将行业动量赋给个股
        for stock in return_matrix.columns:
            if stock in industry_map and industry_map[stock] in industry_avg:
                industry_momentum.loc[date, stock] = industry_avg[industry_map[stock]]

    if logger:
        valid_ratio = industry_momentum.notna().sum().sum() / industry_momentum.size
        logger.info(f"  有效数据比例: {valid_ratio:.2%}")

    return industry_momentum


def build_industry_rank_matrix(industry_momentum: pd.DataFrame, logger=None):
    """
    构建行业排名因子（行业动量在所有行业中的分位数排名）

    Args:
        industry_momentum: 行业动量因子矩阵
        logger: 日志对象

    Returns:
        DataFrame: 行业排名因子矩阵（0-100分位）
    """
    if logger:
        logger.info("构建行业排名因子...")

    # 对每个日期，计算该股票行业动量在横截面的排名
    industry_rank = industry_momentum.rank(axis=1, pct=True) * 100

    if logger:
        logger.info(f"  排名范围: 0-100分位")

    return industry_rank


def build_within_industry_zscore(factor_matrix: pd.DataFrame, return_matrix: pd.DataFrame, industry_map: dict, logger=None):
    """
    构建行业内标准化因子（z-score）

    对每个因子，在每个行业内做z-score标准化
    z = (x - mean) / std

    Args:
        factor_matrix: 原始因子矩阵
        return_matrix: 收益率矩阵（用于对齐）
        industry_map: 股票到行业的映射
        logger: 日志对象

    Returns:
        DataFrame: 行业内标准化后的因子矩阵
    """
    if logger:
        logger.info("构建行业内标准化因子...")

    # 获取所有行业
    all_industries = set(industry_map.values())
    if '' in all_industries:
        all_industries.remove('')

    # 初始化结果矩阵
    zscore_matrix = pd.DataFrame(np.nan, index=factor_matrix.index, columns=factor_matrix.columns)

    # 对每个日期
    for date in factor_matrix.index:
        # 对每个行业
        for industry in all_industries:
            # 获取该行业所有股票
            stocks_in_industry = [s for s, ind in industry_map.items() if ind == industry and s in factor_matrix.columns]

            if len(stocks_in_industry) < 3:  # 行业股票太少，跳过
                continue

            # 获取该行业因子值
            industry_factors = factor_matrix.loc[date, stocks_in_industry]

            # 计算z-score
            mean_val = industry_factors.mean()
            std_val = industry_factors.std()

            if std_val > 0:
                zscore_matrix.loc[date, stocks_in_industry] = (industry_factors - mean_val) / std_val

    if logger:
        valid_ratio = zscore_matrix.notna().sum().sum() / zscore_matrix.size
        logger.info(f"  有效数据比例: {valid_ratio:.2%}")

    return zscore_matrix


def main():
    logger = setup_logger(prefix="industry_factors")
    logger.info("="*60)
    logger.info("构建行业因子矩阵")
    logger.info("="*60)

    # 加载行业映射
    logger.info("加载行业数据...")
    basic_info = pd.read_csv(Config.BASIC_DATA_DIR / 'all_companies_info.csv')
    industry_map = dict(zip(basic_info['ts_code'], basic_info['industry']))
    logger.info(f"  共 {len(industry_map)} 只股票，{len(set(industry_map.values()))} 个行业")

    # 加载收益率矩阵
    logger.info("加载收益率矩阵...")
    return_matrix = load_matrix(Config.MATRIX_DATA_DIR / 'open_return_matrix.csv')

    # 1. 构建行业动量因子（20日）
    logger.info("\n[1/3] 构建行业动量因子...")
    industry_momentum_20d = build_industry_momentum_matrix(return_matrix, industry_map, window=20, logger=logger)
    save_matrix(industry_momentum_20d, Config.MATRIX_DATA_DIR / 'industry_momentum_20d_matrix.csv')
    logger.info("  已保存: industry_momentum_20d_matrix.csv")

    # 2. 构建行业排名因子
    logger.info("\n[2/3] 构建行业排名因子...")
    industry_rank = build_industry_rank_matrix(industry_momentum_20d, logger=logger)
    save_matrix(industry_rank, Config.MATRIX_DATA_DIR / 'industry_rank_matrix.csv')
    logger.info("  已保存: industry_rank_matrix.csv")

    # 3. 构建行业内标准化因子（以Amihud为例）
    logger.info("\n[3/3] 构建行业内标准化Amihud因子...")
    amihud_matrix = load_matrix(Config.MATRIX_DATA_DIR / 'amihud_20d_matrix.csv')
    amihud_zscore = build_within_industry_zscore(amihud_matrix, return_matrix, industry_map, logger=logger)
    save_matrix(amihud_zscore, Config.MATRIX_DATA_DIR / 'amihud_20d_zscore_matrix.csv')
    logger.info("  已保存: amihud_20d_zscore_matrix.csv")

    logger.info("\n" + "="*60)
    logger.info("行业因子矩阵构建完成！")
    logger.info("="*60)
    logger.info("\n生成的矩阵：")
    logger.info("  1. industry_momentum_20d_matrix.csv - 行业动量因子（20日）")
    logger.info("  2. industry_rank_matrix.csv - 行业排名因子（0-100分位）")
    logger.info("  3. amihud_20d_zscore_matrix.csv - 行业内标准化Amihud因子")


if __name__ == '__main__':
    main()
