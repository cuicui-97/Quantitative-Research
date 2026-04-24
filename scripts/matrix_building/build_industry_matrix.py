#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
构建申万行业标签矩阵（一级 + 二级）

前置条件：已运行 scripts/data_fetching/fetch_industry.py

输出：
    stockdata/matrices/industry_l1_matrix.csv  （dates × stocks，值为一级行业名称）
    stockdata/matrices/industry_l2_matrix.csv  （dates × stocks，值为二级行业名称）

使用方式（在因子分析脚本中）：
    from data_engine.processors.matrix_io import load_matrix
    from config.config import Config

    industry_matrix = load_matrix(Config.INDUSTRY_L1_MATRIX_FILE)

    # 限定在目标行业（不在目标行业的股票视为不可交易）
    target_industries = ['银行', '非银金融']
    not_in_industry = (~industry_matrix.isin(target_industries)).astype(int)
    tradable_filtered = (tradability_matrix | not_in_industry).clip(0, 1)
"""
import sys
from pathlib import Path

project_root = Path(__file__).resolve().parent.parent.parent
sys.path.insert(0, str(project_root))

from config.config import Config
from data_engine.processors.industry_matrix_builder import IndustryMatrixBuilder
from utils import setup_logger


def main():
    logger = setup_logger(prefix="matrix")

    if not Config.INDUSTRY_DATA_FILE.exists():
        logger.error(f"行业成分数据不存在: {Config.INDUSTRY_DATA_FILE}")
        logger.error("请先运行: python scripts/data_fetching/fetch_industry.py")
        return

    logger.info("开始构建行业标签矩阵...")
    results = IndustryMatrixBuilder.build()

    l1 = results['l1']
    l2 = results['l2']
    logger.info(f"一级行业矩阵: {l1.shape}，行业数: {l1.stack().nunique()}")
    logger.info(f"二级行业矩阵: {l2.shape}，行业数: {l2.stack().nunique()}")
    logger.info("完成。")


if __name__ == '__main__':
    main()
