#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
市值因子分析（重构版）

使用 factor_engine 模块进行单因子分析
"""
import sys
from pathlib import Path

# 添加项目根目录到 Python 路径
project_root = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(project_root))

from config.config import Config
from data_engine.processors.matrix_io import load_matrix
from utils import setup_logger
from factor_engine import SingleFactorAnalyzer


def main():
    """主函数"""
    logger = setup_logger()

    # 1. 加载数据
    logger.info("加载矩阵数据...")
    mv_matrix = load_matrix(Config.MATRIX_DATA_DIR / 'circ_mv_matrix.csv')
    return_matrix = load_matrix(Config.MATRIX_DATA_DIR / 'open_return_matrix.csv')
    tradability_matrix = load_matrix(Config.MATRIX_DATA_DIR / 'tradability_matrix.csv')

    logger.info(f"  流通市值矩阵: {mv_matrix.shape}")
    logger.info(f"  收益率矩阵: {return_matrix.shape}")
    logger.info(f"  可交易矩阵: {tradability_matrix.shape}")

    # 2. 对齐矩阵
    logger.info("对齐矩阵...")
    common_dates = (mv_matrix.index
                   .intersection(return_matrix.index)
                   .intersection(tradability_matrix.index))
    common_stocks = (mv_matrix.columns
                    .intersection(return_matrix.columns)
                    .intersection(tradability_matrix.columns))

    logger.info(f"  对齐后: {len(common_dates)} 个交易日, {len(common_stocks)} 只股票")

    mv = mv_matrix.loc[common_dates, common_stocks]
    returns = return_matrix.loc[common_dates, common_stocks]
    tradable = tradability_matrix.loc[common_dates, common_stocks]

    # 3. 创建分析器并运行
    # 注意：市值因子本身就是市值，所以市值加权和等权会有不同的经济含义
    analyzer = SingleFactorAnalyzer(
        factor_name='MV',
        factor_matrix=mv,
        return_matrix=returns,
        tradability_matrix=tradable,
        mv_matrix=mv,  # 使用市值矩阵进行加权
        n_groups=10,
        logger=logger
    )

    # 4. 运行分析
    output_dir = Config.DATA_DIR / 'factor_analysis_results' / 'mv_factor'
    results = analyzer.run_analysis(output_dir=output_dir, save_results=True)


if __name__ == '__main__':
    main()
