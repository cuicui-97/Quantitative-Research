#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
EBITDA同比增速因子分析（含/不含交易成本）
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
from factor_engine.backtest.visualization import plot_combined_returns, plot_combined_statistics


def run_analysis(enable_cost: bool, logger):
    factor_name = 'EBITDAYoY'
    cost_tag = 'with_cost' if enable_cost else 'no_cost'

    logger.info(f"\n{'='*60}")
    logger.info(f"EBITDA同比增速因子分析（{'含' if enable_cost else '不含'}交易成本）")
    logger.info(f"{'='*60}")

    # 加载矩阵
    yoy_matrix = load_matrix(Config.MATRIX_DATA_DIR / 'ebitda_yoy_matrix.csv')
    mv_circ_matrix = load_matrix(Config.MATRIX_DATA_DIR / 'circ_mv_matrix.csv')
    return_matrix = load_matrix(Config.MATRIX_DATA_DIR / 'open_return_matrix.csv')
    tradability_matrix = load_matrix(Config.MATRIX_DATA_DIR / 'tradability_matrix.csv')

    logger.info(f"  EBITDA YoY矩阵: {yoy_matrix.shape}")
    logger.info(f"  流通市值矩阵: {mv_circ_matrix.shape}")
    logger.info(f"  收益率矩阵: {return_matrix.shape}")
    logger.info(f"  可交易矩阵: {tradability_matrix.shape}")

    # 对齐
    common_dates = (yoy_matrix.index
                    .intersection(return_matrix.index)
                    .intersection(tradability_matrix.index)
                    .intersection(mv_circ_matrix.index))
    common_stocks = (yoy_matrix.columns
                     .intersection(return_matrix.columns)
                     .intersection(tradability_matrix.columns)
                     .intersection(mv_circ_matrix.columns))

    yoy = yoy_matrix.loc[common_dates, common_stocks]
    mv = mv_circ_matrix.loc[common_dates, common_stocks]
    returns = return_matrix.loc[common_dates, common_stocks]
    tradable = tradability_matrix.loc[common_dates, common_stocks]

    logger.info(f"  对齐后: {len(common_dates)} 个交易日, {len(common_stocks)} 只股票")

    # 加载指数
    index_returns = load_index_data(Config.SUPPLEMENTARY_DATA_DIR)

    # 创建分析器
    analyzer = SingleFactorAnalyzer(
        factor_name=factor_name,
        factor_matrix=yoy,
        return_matrix=returns,
        tradability_matrix=tradable,
        mv_matrix=mv,
        n_groups=10,
        enable_transaction_cost=enable_cost,
        commission_rate=Config.COMMISSION_RATE,
        stamp_duty_rate=Config.STAMP_DUTY_RATE,
        slippage_rate=Config.SLIPPAGE_RATE,
        logger=logger
    )

    output_dir = Config.DATA_DIR / 'factor_analysis_results' / f'ebitda_yoy_{cost_tag}'
    output_dir.mkdir(parents=True, exist_ok=True)

    results = analyzer.run_analysis(output_dir=output_dir, save_results=True)

    plot_combined_returns(
        group_returns_equal=results['group_returns_equal'],
        group_returns_mv=results['group_returns_mv'],
        index_returns=index_returns,
        factor_name=factor_name,
        output_dir=output_dir,
        start_date='2015-01-01'
    )

    plot_combined_statistics(
        stats_equal=results['stats_equal'],
        stats_mv=results['stats_mv'],
        factor_name=factor_name,
        output_dir=output_dir
    )

    logger.info(f"结果保存: {output_dir.relative_to(Config.DATA_DIR.parent)}")
    return results


def main():
    logger = setup_logger(prefix="factor")

    run_analysis(enable_cost=False, logger=logger)
    run_analysis(enable_cost=True, logger=logger)

    logger.info("\n全部完成。")


if __name__ == '__main__':
    main()
