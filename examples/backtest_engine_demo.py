#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
BacktestEngine 使用示例

演示如何使用新的回测引擎进行因子回测
"""
import sys
from pathlib import Path

project_root = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(project_root))

import logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)

from data_engine.core.factor_matrix import FactorMatrix
from factor_engine.backtest.backtest_engine import BacktestEngine, BacktestConfig
from config.config import Config


def demo_simple_backtest():
    """简单回测示例 - 生成完整报告"""
    print("\n" + "=" * 70)
    print("示例1: 简单回测（含可视化）")
    print("=" * 70)

    # 加载因子矩阵
    factor = FactorMatrix.from_csv(
        Config.MATRIX_DATA_DIR / 'net_profit_yoy_matrix.csv',
        name='net_profit_yoy'
    )

    # 加载收益矩阵
    returns = FactorMatrix.from_csv(
        Config.DATA_DIR / 'matrices' / 'open_return_matrix.csv',
        name='returns'
    )

    # 加载可交易矩阵
    tradable = FactorMatrix.from_csv(
        Config.DATA_DIR / 'matrices' / 'tradability_matrix.csv',
        name='tradable'
    )

    # 加载市值矩阵
    mv = FactorMatrix.from_csv(
        Config.MATRIX_DATA_DIR / 'circ_mv_matrix.csv',
        name='circ_mv'
    )

    # 创建回测引擎
    engine = BacktestEngine(
        config=BacktestConfig(
            n_groups=10,
            enable_cost=False,
            long_short=True
        )
    )

    # 加载矩阵
    engine.load_factor(factor)
    engine.load_returns(returns)
    engine.load_tradable(tradable)
    engine.load_mv(mv)

    # 运行回测
    result = engine.run()

    # 生成完整报告（数据+图表）
    output_dir = Config.DATA_DIR / 'backtest_results' / 'net_profit_yoy'
    result.generate_report(output_dir)

    print(f"\n完整报告已生成: {output_dir}")
    print(f"  - CSV 数据文件")
    print(f"  - 累计收益曲线图")
    print(f"  - 统计指标柱状图")


def demo_index_component_backtest():
    """指数成分股回测示例"""
    print("\n" + "=" * 70)
    print("示例2: 中证1000成分股回测")
    print("=" * 70)

    # 加载因子
    factor = FactorMatrix.from_csv(
        Config.MATRIX_DATA_DIR / 'pb_matrix.csv',
        name='pb'
    )

    # 加载收益
    returns = FactorMatrix.from_csv(
        Config.DATA_DIR / 'matrices' / 'open_return_matrix.csv',
        name='returns'
    )

    # 加载可交易性
    tradable = FactorMatrix.from_csv(
        Config.DATA_DIR / 'matrices' / 'tradability_matrix.csv',
        name='tradable'
    )

    # 加载市值
    mv = FactorMatrix.from_csv(
        Config.MATRIX_DATA_DIR / 'circ_mv_matrix.csv',
        name='circ_mv'
    )

    # 加载中证1000成分股矩阵
    index_file = Config.MATRIX_DATA_DIR / '中证1000_matrix.csv'
    if index_file.exists():
        index_component = FactorMatrix.from_csv(index_file, name='zz1000')
    else:
        print(f"中证1000矩阵不存在，跳过此示例")
        return

    # 创建引擎
    engine = BacktestEngine(
        config=BacktestConfig(n_groups=10)
    )

    # 加载矩阵（包括指数成分股）
    engine.load_factor(factor)
    engine.load_returns(returns)
    engine.load_tradable(tradable)
    engine.load_mv(mv)
    engine.load_index_component(index_component, name='zz1000')

    # 运行
    result = engine.run()

    # 生成报告
    output_dir = Config.DATA_DIR / 'backtest_results' / 'pb_zz1000'
    result.generate_report(output_dir)

    print(f"\n中证1000成分股回测完成: {output_dir}")


def demo_compare_factors():
    """多因子对比回测"""
    print("\n" + "=" * 70)
    print("示例3: 多因子对比")
    print("=" * 70)

    # 定义要对比的因子
    factors = [
        ('pb', 'pb_matrix.csv'),
        ('momentum_20d', 'momentum_20d_matrix.csv'),
        ('net_profit_yoy', 'net_profit_yoy_matrix.csv'),
    ]

    # 加载通用数据
    returns = FactorMatrix.from_csv(
        Config.DATA_DIR / 'matrices' / 'open_return_matrix.csv',
        name='returns'
    )
    tradable = FactorMatrix.from_csv(
        Config.DATA_DIR / 'matrices' / 'tradability_matrix.csv',
        name='tradable'
    )
    mv = FactorMatrix.from_csv(
        Config.MATRIX_DATA_DIR / 'circ_mv_matrix.csv',
        name='circ_mv'
    )

    results_summary = []

    for factor_name, filename in factors:
        factor_path = Config.MATRIX_DATA_DIR / filename
        if not factor_path.exists():
            print(f"  跳过 {factor_name}: 文件不存在")
            continue

        print(f"\n回测因子: {factor_name}")

        factor = FactorMatrix.from_csv(factor_path, name=factor_name)

        engine = BacktestEngine(config=BacktestConfig(n_groups=10))
        engine.load_factor(factor)
        engine.load_returns(returns)
        engine.load_tradable(tradable)
        engine.load_mv(mv)

        result = engine.run()

        # 生成报告
        output_dir = Config.DATA_DIR / 'backtest_results' / f'{factor_name}_comparison'
        result.generate_report(output_dir)

        # 收集结果
        if result.stats_equal is not None and 'Long-Short' in result.stats_equal.index:
            ls_stats = result.stats_equal.loc['Long-Short']
            results_summary.append({
                'factor': factor_name,
                'annual_return': ls_stats['annual_return'],
                'sharpe': ls_stats['sharpe'],
                'max_drawdown': ls_stats['max_drawdown'],
                'ic_mean': result.ic_mean,
                'ic_ir': result.ic_ir
            })

    # 输出对比表
    print("\n" + "=" * 70)
    print("多因子对比结果")
    print("=" * 70)

    if results_summary:
        import pandas as pd
        summary_df = pd.DataFrame(results_summary)
        summary_df = summary_df.sort_values('sharpe', ascending=False)

        print(f"{'因子':<20} {'年化收益':>10} {'夏普':>8} {'最大回撤':>10} {'IC均值':>8} {'IC_IR':>8}")
        print("-" * 70)

        for _, row in summary_df.iterrows():
            print(f"{row['factor']:<20} {row['annual_return']:>10.2f}% {row['sharpe']:>8.2f} "
                  f"{row['max_drawdown']:>10.2f}% {row['ic_mean']:>8.4f} {row['ic_ir']:>8.2f}")


if __name__ == '__main__':
    # 运行示例
    demo_simple_backtest()
    # demo_index_component_backtest()  # 需要中证1000矩阵
    # demo_compare_factors()

    print("\n" + "=" * 70)
    print("所有示例完成！")
    print("=" * 70)
