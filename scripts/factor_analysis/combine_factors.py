#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
简单因子组合分析
- 等权组合
- IC加权组合
- IR加权组合
"""
import sys
from pathlib import Path

project_root = Path(__file__).resolve().parent.parent.parent
sys.path.insert(0, str(project_root))

import pandas as pd
import numpy as np
from config.config import Config
from data_engine.processors.matrix_io import load_matrix
from utils import setup_logger
from factor_engine import SingleFactorAnalyzer
from factor_engine.backtest.data_loader import load_index_data
from factor_engine.backtest.visualization import plot_combined_returns, plot_combined_statistics


def calculate_ic_series(factor_matrix: pd.DataFrame, return_matrix: pd.DataFrame) -> pd.Series:
    """计算因子的IC序列（每日截面秩相关）"""
    common_dates = factor_matrix.index.intersection(return_matrix.index)
    common_stocks = factor_matrix.columns.intersection(return_matrix.columns)

    f = factor_matrix.loc[common_dates, common_stocks]
    r = return_matrix.loc[common_dates, common_stocks]

    ic_list = []
    for date in common_dates:
        f_row = f.loc[date]
        r_row = r.loc[date]
        # 对齐非空值
        valid = f_row.notna() & r_row.notna()
        if valid.sum() >= 10:
            ic = f_row[valid].rank().corr(r_row[valid].rank(), method='spearman')
            ic_list.append({'date': date, 'ic': ic})

    return pd.DataFrame(ic_list).set_index('date')['ic']


def combine_factors_equal(factors: list[pd.DataFrame]) -> pd.DataFrame:
    """等权组合：各因子排名后平均"""
    ranked = [f.rank(axis=1) for f in factors]
    return sum(ranked) / len(ranked)


def combine_factors_ic_weighted(factors: list[pd.DataFrame], returns: pd.DataFrame,
                                lookback: int = 252) -> pd.DataFrame:
    """IC加权组合：用近期IC均值加权"""
    # 计算各因子的IC序列
    ic_series = [calculate_ic_series(f, returns) for f in factors]

    # 滚动计算IC均值作为权重
    weights = [ic.rolling(lookback, min_periods=60).mean().iloc[-1] for ic in ic_series]
    weights = np.array(weights)
    weights = np.maximum(weights, 0)  # 只保留正IC的因子

    if weights.sum() == 0:
        logger.warning("所有因子IC均为负，使用等权")
        weights = np.ones(len(factors)) / len(factors)
    else:
        weights = weights / weights.sum()

    logger.info(f"IC权重: {dict(zip(['净利润YoY', '营收YoY'], [f'{w:.3f}' for w in weights]))}")

    # 排名后加权
    ranked = [f.rank(axis=1) for f in factors]
    combined = sum(w * r for w, r in zip(weights, ranked))
    return combined


def combine_factors_ir_weighted(factors: list[pd.DataFrame], returns: pd.DataFrame,
                                lookback: int = 252) -> pd.DataFrame:
    """IR加权组合：用近期IR（IC均值/标准差）加权"""
    ic_series = [calculate_ic_series(f, returns) for f in factors]

    # 滚动计算IR
    irs = []
    for ic in ic_series:
        ic_mean = ic.rolling(lookback, min_periods=60).mean().iloc[-1]
        ic_std = ic.rolling(lookback, min_periods=60).std().iloc[-1]
        ir = ic_mean / ic_std if ic_std > 0 else 0
        irs.append(max(ir, 0))  # 只保留正IR

    weights = np.array(irs)
    if weights.sum() == 0:
        logger.warning("所有因子IR均为负，使用等权")
        weights = np.ones(len(factors)) / len(factors)
    else:
        weights = weights / weights.sum()

    logger.info(f"IR权重: {dict(zip(['净利润YoY', '营收YoY'], [f'{w:.3f}' for w in weights]))}")

    ranked = [f.rank(axis=1) for f in factors]
    combined = sum(w * r for w, r in zip(weights, ranked))
    return combined


def run_combined_analysis(combined_factor: pd.DataFrame, method_name: str, enable_cost: bool, logger):
    """运行组合因子分析"""
    cost_tag = 'with_cost' if enable_cost else 'no_cost'

    logger.info(f"\n{'='*60}")
    logger.info(f"组合因子分析 ({method_name}) - {'含成本' if enable_cost else '不含成本'}")
    logger.info(f"{'='*60}")

    # 加载数据
    mv_circ_matrix = load_matrix(Config.MATRIX_DATA_DIR / 'circ_mv_matrix.csv')
    return_matrix = load_matrix(Config.MATRIX_DATA_DIR / 'open_return_matrix.csv')
    tradability_matrix = load_matrix(Config.MATRIX_DATA_DIR / 'tradability_matrix.csv')

    # 对齐
    common_dates = (combined_factor.index
                    .intersection(return_matrix.index)
                    .intersection(tradability_matrix.index)
                    .intersection(mv_circ_matrix.index))
    common_stocks = (combined_factor.columns
                     .intersection(return_matrix.columns)
                     .intersection(tradability_matrix.columns)
                     .intersection(mv_circ_matrix.columns))

    factor = combined_factor.loc[common_dates, common_stocks]
    mv = mv_circ_matrix.loc[common_dates, common_stocks]
    returns = return_matrix.loc[common_dates, common_stocks]
    tradable = tradability_matrix.loc[common_dates, common_stocks]

    logger.info(f"  对齐后: {len(common_dates)} 个交易日, {len(common_stocks)} 只股票")

    # 加载指数
    index_returns = load_index_data(Config.SUPPLEMENTARY_DATA_DIR)

    # 创建分析器
    analyzer = SingleFactorAnalyzer(
        factor_name=f'Combined_{method_name}',
        factor_matrix=factor,
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

    output_dir = Config.DATA_DIR / 'factor_analysis_results' / f'combined_{method_name.lower()}_{cost_tag}'
    output_dir.mkdir(parents=True, exist_ok=True)

    results = analyzer.run_analysis(output_dir=output_dir, save_results=True)

    plot_combined_returns(
        group_returns_equal=results['group_returns_equal'],
        group_returns_mv=results['group_returns_mv'],
        index_returns=index_returns,
        factor_name=f'Combined_{method_name}',
        output_dir=output_dir,
        start_date='2015-01-01'
    )

    plot_combined_statistics(
        stats_equal=results['stats_equal'],
        stats_mv=results['stats_mv'],
        factor_name=f'Combined_{method_name}',
        output_dir=output_dir
    )

    # 输出核心指标
    stats = results['stats_equal']
    ls_stats = stats.loc['Long-Short (10-1)']
    logger.info(f"\n{method_name} L-S 表现:")
    logger.info(f"  年化收益: {ls_stats['annual_return']:.2f}%")
    logger.info(f"  夏普比率: {ls_stats['sharpe_ratio']:.2f}")
    logger.info(f"  最大回撤: {ls_stats['max_drawdown']:.2f}%")

    return results


def main():
    global logger
    logger = setup_logger(prefix="combine")

    logger.info("="*60)
    logger.info("因子组合分析")
    logger.info("="*60)

    # 加载两个因子
    logger.info("加载因子矩阵...")
    profit_yoy = load_matrix(Config.MATRIX_DATA_DIR / 'n_income_attr_p_yoy_matrix.csv')
    revenue_yoy = load_matrix(Config.MATRIX_DATA_DIR / 'total_revenue_yoy_matrix.csv')
    return_matrix = load_matrix(Config.MATRIX_DATA_DIR / 'open_return_matrix.csv')

    logger.info(f"  净利润YoY: {profit_yoy.shape}")
    logger.info(f"  营收YoY: {revenue_yoy.shape}")

    # 计算单因子IC（用于展示）
    logger.info("\n计算单因子IC...")
    ic_profit = calculate_ic_series(profit_yoy, return_matrix)
    ic_revenue = calculate_ic_series(revenue_yoy, return_matrix)

    logger.info(f"  净利润YoY - IC均值: {ic_profit.mean():.4f}, ICIR: {ic_profit.mean()/ic_profit.std():.3f}")
    logger.info(f"  营收YoY   - IC均值: {ic_revenue.mean():.4f}, ICIR: {ic_revenue.mean()/ic_revenue.std():.3f}")

    factors = [profit_yoy, revenue_yoy]

    # 三种组合方法
    methods = [
        ('Equal', combine_factors_equal),
        ('IC_Weighted', lambda fs: combine_factors_ic_weighted(fs, return_matrix)),
        ('IR_Weighted', lambda fs: combine_factors_ir_weighted(fs, return_matrix)),
    ]

    for method_name, combine_fn in methods:
        logger.info(f"\n{'='*60}")
        logger.info(f"方法: {method_name}")
        logger.info(f"{'='*60}")

        # 构建组合因子
        combined = combine_fn(factors)

        # 分析（不含成本）
        run_combined_analysis(combined, method_name, enable_cost=False, logger=logger)

    logger.info("\n全部完成!")


if __name__ == '__main__':
    main()
