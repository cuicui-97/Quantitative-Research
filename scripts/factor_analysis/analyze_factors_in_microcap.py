#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
微盘股内因子分析

在微盘股范围内测试各类因子的效果
"""
import sys
from pathlib import Path

project_root = Path(__file__).resolve().parent.parent.parent
sys.path.insert(0, str(project_root))

import pandas as pd
from data_engine.core.factor_matrix import FactorMatrix
from factor_engine.backtest.backtest_engine import BacktestEngine, BacktestConfig
from config.config import Config


def run_microcap_factor_analysis(factor_name: str, factor_file: str, enable_cost: bool):
    """在微盘股内运行单个因子分析"""
    cost_tag = 'with_cost' if enable_cost else 'no_cost'

    print(f"\n{'='*60}")
    print(f"{factor_name} 微盘股分析（{'含成本' if enable_cost else '不含'}）")
    print(f"{'='*60}")

    # 加载矩阵
    factor = FactorMatrix.from_csv(Config.MATRIX_DATA_DIR / factor_file, name=factor_name)
    returns = FactorMatrix.from_csv(Config.DATA_DIR / 'matrices' / 'open_return_matrix.csv', name='returns')
    tradable = FactorMatrix.from_csv(Config.DATA_DIR / 'matrices' / 'tradability_matrix.csv', name='tradable')
    mv = FactorMatrix.from_csv(Config.MATRIX_DATA_DIR / 'circ_mv_matrix.csv', name='circ_mv')
    microcap = FactorMatrix.from_csv(Config.MATRIX_DATA_DIR / 'microcap_matrix.csv', name='microcap')

    # 创建引擎
    engine = BacktestEngine(
        config=BacktestConfig(
            n_groups=10,
            enable_cost=enable_cost,
            commission_rate=Config.COMMISSION_RATE,
            stamp_duty_rate=Config.STAMP_DUTY_RATE,
            slippage_rate=Config.SLIPPAGE_RATE,
            long_short=True
        )
    )

    engine.load_factor(factor)
    engine.load_returns(returns)
    engine.load_tradable(tradable)
    engine.load_mv(mv)
    engine.load_index_component(microcap, name='microcap')

    result = engine.run()

    output_dir = Config.DATA_DIR / 'factor_analysis_results' / 'microcap' / f'{factor_name.lower()}_{cost_tag}'
    result.generate_report(output_dir)

    # 返回核心指标
    if result.stats_equal is not None and 'Long-Short' in result.stats_equal.index:
        ls_stats = result.stats_equal.loc['Long-Short']
        return {
            'factor': factor_name,
            'cost': cost_tag,
            'ann_ret': ls_stats['annual_return'],
            'sharpe': ls_stats['sharpe'],
            'max_dd': ls_stats['max_drawdown'],
            'ic_mean': result.ic_mean
        }
    return None


def main():
    print("="*70)
    print("微盘股内因子分析")
    print("="*70)

    # 定义要测试的因子
    factors = [
        ('NetProfitYoY', 'n_income_attr_p_yoy_matrix.csv'),
        ('RevenueYoY', 'total_revenue_yoy_matrix.csv'),
        ('OperateProfitYoY', 'operate_profit_yoy_matrix.csv'),
        ('PB', 'pb_matrix.csv'),
    ]

    summary = []

    for factor_name, matrix_file in factors:
        try:
            print(f"\n{'#'*70}")
            print(f"# 开始分析: {factor_name} (微盘股)")
            print(f"{'#'*70}")

            # 不含成本
            result_no_cost = run_microcap_factor_analysis(factor_name, matrix_file, False)
            if result_no_cost:
                summary.append(result_no_cost)

            # 含成本
            result_with_cost = run_microcap_factor_analysis(factor_name, matrix_file, True)
            if result_with_cost:
                summary.append(result_with_cost)

        except Exception as e:
            print(f"分析 {factor_name} 失败: {e}")
            continue

    # 输出汇总
    print(f"\n\n{'='*70}")
    print("微盘股内因子汇总对比")
    print(f"{'='*70}")

    if summary:
        summary_df = pd.DataFrame(summary)
        summary_df = summary_df.sort_values('sharpe', ascending=False)

        print(f"{'因子':<20} {'成本':<10} {'年化收益':>10} {'夏普':>8} {'最大回撤':>10}")
        print("-" * 70)
        for _, row in summary_df.iterrows():
            print(f"{row['factor']:<20} {row['cost']:<10} {row['ann_ret']:>10.2f}% {row['sharpe']:>8.2f} {row['max_dd']:>10.2f}%")

    print("\n全部完成！")


if __name__ == '__main__':
    main()
