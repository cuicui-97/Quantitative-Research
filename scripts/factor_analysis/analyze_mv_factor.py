#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
市值因子分析

特性：
1. 测试时间：2015年至今
2. 合并市值加权和等权重展示
3. 添加指数基准对比（沪深300、上证50、中证1000）
"""
import sys
from pathlib import Path

project_root = Path(__file__).resolve().parent.parent.parent
sys.path.insert(0, str(project_root))

import pandas as pd
from config.config import Config
from data_engine.processors.matrix_io import load_matrix
from utils import setup_logger
from factor_engine import SingleFactorAnalyzer
from factor_engine.backtest.data_loader import load_index_data
from factor_engine.backtest.visualization import (
    plot_combined_returns,
    plot_combined_statistics
)


def main():
    """主函数"""
    logger = setup_logger(prefix="factor")

    logger.info("="*60)
    logger.info("市值因子分析（2015年至今，含指数基准对比）")
    logger.info("="*60)

    # 加载矩阵数据
    logger.info("\n加载矩阵数据...")
    mv_circ_matrix = load_matrix(Config.MATRIX_DATA_DIR / 'circ_mv_matrix.csv')
    return_matrix = load_matrix(Config.MATRIX_DATA_DIR / 'open_return_matrix.csv')
    tradability_matrix = load_matrix(Config.MATRIX_DATA_DIR / 'tradability_matrix.csv')

    logger.info(f"  流通市值矩阵: {mv_circ_matrix.shape}")
    logger.info(f"  收益率矩阵: {return_matrix.shape}")
    logger.info(f"  可交易矩阵: {tradability_matrix.shape}")

    # 对齐矩阵
    logger.info("\n对齐矩阵...")
    common_dates = (mv_circ_matrix.index
                   .intersection(return_matrix.index)
                   .intersection(tradability_matrix.index))
    common_stocks = (mv_circ_matrix.columns
                    .intersection(return_matrix.columns)
                    .intersection(tradability_matrix.columns))

    logger.info(f"  对齐后: {len(common_dates)} 个交易日, {len(common_stocks)} 只股票")

    mv_circ = mv_circ_matrix.loc[common_dates, common_stocks]
    returns = return_matrix.loc[common_dates, common_stocks]
    tradable = tradability_matrix.loc[common_dates, common_stocks]

    # 筛选日期范围
    start_date = '2015-01-01'
    start = pd.to_datetime(start_date)
    date_mask = pd.to_datetime(mv_circ.index, format='%Y%m%d') >= start

    mv_filtered = mv_circ.loc[date_mask]
    returns_filtered = returns.loc[date_mask]
    tradable_filtered = tradable.loc[date_mask]

    logger.info(f"\n筛选后数据范围: {len(mv_filtered)} 个交易日, {len(mv_filtered.columns)} 只股票")

    # 加载指数数据
    index_returns = load_index_data(Config.SUPPLEMENTARY_DATA_DIR)

    # 创建分析器
    logger.info("\n创建因子分析器...")
    analyzer = SingleFactorAnalyzer(
        factor_name='MV',
        factor_matrix=mv_filtered,
        return_matrix=returns_filtered,
        tradability_matrix=tradable_filtered,
        mv_matrix=mv_filtered,
        n_groups=10,
        logger=logger
    )

    # 运行分析
    output_dir = Config.DATA_DIR / 'factor_analysis_results' / 'mv_factor'
    output_dir.mkdir(parents=True, exist_ok=True)

    logger.info("\n运行因子分析...")
    results = analyzer.run_analysis(output_dir=output_dir, save_results=True)

    # 绘制增强版图表
    plot_combined_returns(
        group_returns_equal=results['group_returns_equal'],
        group_returns_mv=results['group_returns_mv'],
        index_returns=index_returns,
        factor_name='MV',
        output_dir=output_dir,
        start_date=start_date
    )

    plot_combined_statistics(
        stats_equal=results['stats_equal'],
        stats_mv=results['stats_mv'],
        factor_name='MV',
        output_dir=output_dir
    )

    logger.info("\n" + "="*60)
    logger.info("市值因子分析完成")
    logger.info("="*60)
    logger.info(f"结果保存目录: {output_dir.relative_to(Config.DATA_DIR.parent)}")


if __name__ == '__main__':
    main()
