#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
统一因子分析脚本

支持全市场和微盘股的因子分析，通过参数控制

用法:
    # 分析单个因子在全市场和微盘股的表现
    python analyze_factor_unified.py --factor Amihud_20d --matrix amihud_20d_matrix.csv --pools all_stocks,microcap

    # 批量分析多个因子
    python analyze_factor_unified.py --config factor_list.json
"""
import sys
from pathlib import Path

project_root = Path(__file__).resolve().parent.parent.parent
sys.path.insert(0, str(project_root))

import argparse
import json
import pandas as pd
from typing import List, Dict

from config.config import Config
from factor_engine.backtest.factor_analyzer import (
    UnifiedFactorAnalyzer,
    FactorConfig,
    create_stock_pool_config
)
from utils import setup_logger


# 预定义的因子列表
PREDEFINED_FACTORS = {
    'Amihud_20d': FactorConfig('Amihud_20d', 'amihud_20d_matrix.csv'),
    'Turnover_20d': FactorConfig('Turnover_20d', 'turnover_20d_matrix.csv'),
    'Turnover_Vol_20d': FactorConfig('Turnover_Vol_20d', 'turnover_vol_20d_matrix.csv'),
    'VP_Corr_20d': FactorConfig('VP_Corr_20d', 'vp_corr_20d_matrix.csv'),
    'Reversal_5d': FactorConfig('Reversal_5d', 'reversal_5d_matrix.csv'),
    'Momentum_20d': FactorConfig('Momentum_20d', 'momentum_20d_matrix.csv'),
    'Momentum_60d': FactorConfig('Momentum_60d', 'momentum_60d_matrix.csv'),
    'NetProfitYoY': FactorConfig('NetProfitYoY', 'n_income_attr_p_yoy_matrix.csv'),
    'RevenueYoY': FactorConfig('RevenueYoY', 'total_revenue_yoy_matrix.csv'),
    'PB': FactorConfig('PB', 'pb_matrix.csv'),
}


def run_factor_analysis(
    factor_config: FactorConfig,
    pool_types: List[str],
    enable_cost: bool = False,
    n_groups: int = 10,
    logger=None
) -> Dict[str, Dict]:
    """
    运行因子分析

    Args:
        factor_config: 因子配置
        pool_types: 股票池类型列表，如 ['all_stocks', 'microcap']
        enable_cost: 是否启用交易成本
        n_groups: 分组数
        logger: 日志对象

    Returns:
        各股票池的分析结果摘要
    """
    results_summary = {}
    cost_tag = 'with_cost' if enable_cost else 'no_cost'

    for pool_type in pool_types:
        # 创建股票池配置
        stock_pool_config = create_stock_pool_config(pool_type)

        # 创建分析器
        analyzer = UnifiedFactorAnalyzer(
            factor_config=factor_config,
            stock_pool_config=stock_pool_config,
            n_groups=n_groups,
            enable_transaction_cost=enable_cost,
            logger=logger
        )

        # 设置输出目录
        output_dir = Config.DATA_DIR / 'factor_analysis_results' / pool_type / f'{factor_config.name.lower()}_{cost_tag}'

        # 运行分析
        results = analyzer.run_analysis(
            output_dir=output_dir,
            save_results=True,
            plot_results=True,
            start_date='2015-01-01'
        )

        # 提取关键指标
        ls_stats_equal = results['stats_equal'].loc[f'Long-Short ({n_groups}-1)']

        results_summary[pool_type] = {
            'factor': factor_config.name,
            'pool': pool_type,
            'annual_return': ls_stats_equal['annual_return'],
            'sharpe_ratio': ls_stats_equal['sharpe_ratio'],
            'max_drawdown': ls_stats_equal['max_drawdown'],
            'ic_mean': results['ic_series'].mean(),
        }

    return results_summary


def print_summary(summary: Dict[str, Dict], logger):
    """打印汇总结果"""
    logger.info("\n" + "=" * 80)
    logger.info("分析结果汇总")
    logger.info("=" * 80)

    # 转换为DataFrame便于展示
    df = pd.DataFrame.from_dict(summary, orient='index')
    df = df.sort_values('sharpe_ratio', ascending=False)

    for idx, row in df.iterrows():
        logger.info(
            f"{row['factor']:20s} | {row['pool']:15s} | "
            f"年化={row['annual_return']:7.2f}% | 夏普={row['sharpe_ratio']:5.2f} | "
            f"回撤={row['max_drawdown']:6.2f}% | IC={row['ic_mean']:+.4f}"
        )

    logger.info("=" * 80)


def main():
    parser = argparse.ArgumentParser(description='统一因子分析')
    parser.add_argument('--factor', type=str, help='因子名称（预定义列表中的名称）')
    parser.add_argument('--matrix', type=str, help='因子矩阵文件名（如果不是预定义因子）')
    parser.add_argument('--pools', type=str, default='all_stocks,microcap',
                       help='股票池类型，逗号分隔，如 all_stocks,microcap')
    parser.add_argument('--with-cost', action='store_true', help='启用交易成本')
    parser.add_argument('--n-groups', type=int, default=10, help='分组数量')
    parser.add_argument('--config', type=str, help='批量分析配置文件（JSON格式）')

    args = parser.parse_args()

    # 设置日志
    logger = setup_logger(prefix="factor_analysis")
    logger.info("=" * 80)
    logger.info("统一因子分析")
    logger.info("=" * 80)

    # 解析股票池
    pool_types = [p.strip() for p in args.pools.split(',')]

    # 获取因子配置列表
    if args.config:
        # 从配置文件加载
        with open(args.config, 'r') as f:
            config = json.load(f)
        factor_configs = [FactorConfig(**fc) for fc in config['factors']]
    elif args.factor:
        # 使用预定义或自定义因子
        if args.factor in PREDEFINED_FACTORS:
            factor_configs = [PREDEFINED_FACTORS[args.factor]]
        elif args.matrix:
            factor_configs = [FactorConfig(args.factor, args.matrix)]
        else:
            raise ValueError(f"未知因子 {args.factor}，请提供 --matrix 参数或添加至预定义列表")
    else:
        # 默认分析所有预定义因子
        factor_configs = list(PREDEFINED_FACTORS.values())

    logger.info(f"将分析 {len(factor_configs)} 个因子")
    logger.info(f"股票池: {pool_types}")
    logger.info(f"交易成本: {'启用' if args.with_cost else '禁用'}")

    # 运行分析
    all_summaries = {}

    for factor_config in factor_configs:
        logger.info(f"\n{'#' * 80}")
        logger.info(f"# 分析因子: {factor_config.name}")
        logger.info(f"{'#' * 80}")

        summary = run_factor_analysis(
            factor_config=factor_config,
            pool_types=pool_types,
            enable_cost=args.with_cost,
            n_groups=args.n_groups,
            logger=logger
        )

        all_summaries[factor_config.name] = summary

    # 打印总汇总
    logger.info("\n" + "=" * 80)
    logger.info("所有因子分析完成")
    logger.info("=" * 80)

    for factor_name, summary in all_summaries.items():
        print_summary(summary, logger)

    logger.info("\n全部完成！")


if __name__ == '__main__':
    main()
