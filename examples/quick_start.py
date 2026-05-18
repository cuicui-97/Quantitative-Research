#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
BacktestEngine 快速开始

最简单的使用方式
"""
import sys
from pathlib import Path

project_root = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(project_root))

from data_engine.core.factor_matrix import FactorMatrix
from factor_engine.backtest.backtest_engine import BacktestEngine, BacktestConfig
from config.config import Config


def main():
    # 1. 加载数据（FactorMatrix 封装 DataFrame）
    factor = FactorMatrix.from_csv(
        Config.MATRIX_DATA_DIR / 'pb_matrix.csv',
        name='pb'
    )
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
        name='mv'
    )

    # 2. 创建引擎
    engine = BacktestEngine(
        config=BacktestConfig(n_groups=10)
    )

    # 3. 加载矩阵
    engine.load_factor(factor)
    engine.load_returns(returns)
    engine.load_tradable(tradable)
    engine.load_mv(mv)

    # 4. 运行回测
    result = engine.run()

    # 5. 生成完整报告（数据 + 图表）
    output_dir = Config.DATA_DIR / 'backtest_results' / 'pb_report'
    result.generate_report(output_dir)

    print(f"\n报告已生成: {output_dir}")
    print("  - CSV 数据文件")
    print("  - cumulative_returns.html (交互式累计收益曲线)")
    print("  - statistics.html (交互式统计指标)")


if __name__ == '__main__':
    main()
