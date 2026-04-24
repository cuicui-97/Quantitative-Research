#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
概念景气度因子回测分析

同时测试三个基础因子：
1. Concept_Price_Boom - 价格景气（过去5日收益均值）
2. Concept_Flow_Boom - 资金景气（成交额/20日均值）
3. Concept_Sentiment - 情绪景气（上涨+涨停占比）

使用方法：
    python scripts/factor_analysis/analyze_concept_prosperity_factors.py
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


def analyze_single_factor(factor_name, factor_matrix, return_matrix, tradable,
                          mv_matrix, index_returns, output_dir, logger, start_date):
    """分析单个因子"""
    logger.info(f"\n{'='*60}")
    logger.info(f"{factor_name} 因子分析")
    logger.info('='*60)

    # 筛选日期范围
    start = pd.to_datetime(start_date)
    date_mask = pd.to_datetime(factor_matrix.index.astype(str), format='%Y%m%d') >= start

    factor_filtered = factor_matrix.loc[date_mask]
    returns_filtered = return_matrix.loc[date_mask]
    tradable_filtered = tradable.loc[date_mask]
    mv_filtered = mv_matrix.loc[date_mask]

    logger.info(f"筛选后数据: {len(factor_filtered)} 个交易日")

    # 创建分析器
    analyzer = SingleFactorAnalyzer(
        factor_name=factor_name,
        factor_matrix=factor_filtered,
        return_matrix=returns_filtered,
        tradability_matrix=tradable_filtered,
        mv_matrix=mv_filtered,
        n_groups=10,
        logger=logger
    )

    # 运行分析
    factor_output_dir = output_dir / factor_name.lower()
    factor_output_dir.mkdir(parents=True, exist_ok=True)

    results = analyzer.run_analysis(output_dir=factor_output_dir, save_results=True)

    # 绘制图表
    plot_combined_returns(
        group_returns_equal=results['group_returns_equal'],
        group_returns_mv=results['group_returns_mv'],
        index_returns=index_returns,
        factor_name=factor_name,
        output_dir=factor_output_dir,
        start_date=start_date
    )

    plot_combined_statistics(
        stats_equal=results['stats_equal'],
        stats_mv=results['stats_mv'],
        factor_name=factor_name,
        output_dir=factor_output_dir
    )

    # 提取关键指标
    stats = results['stats_equal']
    long_short_return = stats.loc['G1-G10', '累计收益率'] if 'G1-G10' in stats.index else 0
    long_short_sharpe = stats.loc['G1-G10', '夏普比率'] if 'G1-G10' in stats.index else 0

    logger.info(f"\n{factor_name} 关键结果:")
    logger.info(f"  Long-Short 累计收益: {long_short_return:.2%}")
    logger.info(f"  Long-Short 夏普比率: {long_short_sharpe:.2f}")
    logger.info(f"  IC均值: {results['ic_ir']['mean_ic']:.4f}")
    logger.info(f"  ICIR: {results['ic_ir']['ir']:.4f}")

    return {
        'factor_name': factor_name,
        'long_short_return': long_short_return,
        'long_short_sharpe': long_short_sharpe,
        'ic_mean': results['ic_ir']['mean_ic'],
        'ic_ir': results['ic_ir']['ir'],
        'results': results
    }


def main():
    logger = setup_logger(prefix="concept_prosperity")

    logger.info("="*70)
    logger.info("概念景气度因子回测分析（热门概念策略）")
    logger.info("="*70)

    # 加载数据
    logger.info("\n[1/4] 加载矩阵数据...")

    price_boom = load_matrix(Config.MATRIX_DATA_DIR / 'concept_price_boom_matrix.csv')
    flow_boom = load_matrix(Config.MATRIX_DATA_DIR / 'concept_flow_boom_matrix.csv')
    sentiment = load_matrix(Config.MATRIX_DATA_DIR / 'concept_sentiment_matrix.csv')

    mv_circ = load_matrix(Config.MATRIX_DATA_DIR / 'circ_mv_matrix.csv')
    returns = load_matrix(Config.MATRIX_DATA_DIR / 'open_return_matrix.csv')
    tradable = load_matrix(Config.MATRIX_DATA_DIR / 'tradability_matrix.csv')

    logger.info(f"  价格景气矩阵: {price_boom.shape}")
    logger.info(f"  资金景气矩阵: {flow_boom.shape}")
    logger.info(f"  情绪景气矩阵: {sentiment.shape}")
    logger.info(f"  流通市值矩阵: {mv_circ.shape}")
    logger.info(f"  收益率矩阵: {returns.shape}")
    logger.info(f"  可交易矩阵: {tradable.shape}")

    # 对齐所有矩阵
    logger.info("\n[2/4] 对齐矩阵...")

    common_dates = (price_boom.index
                   .intersection(flow_boom.index)
                   .intersection(sentiment.index)
                   .intersection(mv_circ.index)
                   .intersection(returns.index)
                   .intersection(tradable.index))

    common_stocks = (price_boom.columns
                    .intersection(flow_boom.columns)
                    .intersection(sentiment.columns)
                    .intersection(mv_circ.columns)
                    .intersection(returns.columns)
                    .intersection(tradable.columns))

    logger.info(f"  对齐后: {len(common_dates)} 个交易日, {len(common_stocks)} 只股票")

    price_boom = price_boom.loc[common_dates, common_stocks]
    flow_boom = flow_boom.loc[common_dates, common_stocks]
    sentiment = sentiment.loc[common_dates, common_stocks]
    mv_circ = mv_circ.loc[common_dates, common_stocks]
    returns = returns.loc[common_dates, common_stocks]
    tradable = tradable.loc[common_dates, common_stocks]

    # 加载指数数据
    logger.info("\n[3/4] 加载指数数据...")
    index_returns = load_index_data(Config.SUPPLEMENTARY_DATA_DIR)

    # 输出目录
    output_dir = Config.DATA_DIR / 'factor_analysis_results' / 'concept_prosperity'
    output_dir.mkdir(parents=True, exist_ok=True)

    start_date = '2015-01-01'

    # 分别分析三个因子
    logger.info("\n[4/4] 开始因子分析...")

    results_summary = []

    # 1. 价格景气因子
    result1 = analyze_single_factor(
        'Concept_Price_Boom',
        price_boom, returns, tradable, mv_circ,
        index_returns, output_dir, logger, start_date
    )
    results_summary.append(result1)

    # 2. 情绪景气因子
    result3 = analyze_single_factor(
        'Concept_Sentiment',
        sentiment, returns, tradable, mv_circ,
        index_returns, output_dir, logger, start_date
    )
    results_summary.append(result3)

    # 汇总输出
    logger.info("\n" + "="*70)
    logger.info("概念景气度因子对比汇总")
    logger.info("="*70)
    logger.info(f"{'因子名称':<20} {'L-S收益':>10} {'L-S夏普':>10} {'IC均值':>10} {'ICIR':>10}")
    logger.info("-"*70)

    for r in results_summary:
        logger.info(f"{r['factor_name']:<20} {r['long_short_return']:>9.2%} {r['long_short_sharpe']:>10.2f} {r['ic_mean']:>10.4f} {r['ic_ir']:>10.4f}")

    logger.info("\n" + "="*70)
    logger.info("分析完成!")
    logger.info("="*70)
    logger.info(f"结果保存目录: {output_dir.relative_to(Config.DATA_DIR.parent)}")

    # 策略逻辑说明
    logger.info("\n策略逻辑说明:")
    logger.info("  - Top组(G1): 买入热门概念的股票（追热点策略）")
    logger.info("  - Bottom组(G10): 买入冷门概念的股票（逆向策略）")
    logger.info("  - Long-Short: 做多热门 + 做空冷门")
    logger.info("\n预期效果:")
    logger.info("  - 如果追热点有效: G1收益 > G10，IC为正")
    logger.info("  - 如果追热点无效（过热反转）: G10收益 > G1，IC为负")


if __name__ == '__main__':
    main()
