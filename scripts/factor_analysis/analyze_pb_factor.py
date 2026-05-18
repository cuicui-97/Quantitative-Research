#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
PB 因子分析
"""
import sys
from pathlib import Path

project_root = Path(__file__).resolve().parent.parent.parent
sys.path.insert(0, str(project_root))

from data_engine.core.factor_matrix import FactorMatrix
from factor_engine.backtest.backtest_engine import BacktestEngine, BacktestConfig
from config.config import Config


def main():
    print("\n" + "="*60)
    print("PB 因子分析")
    print("="*60 + "\n")

    # 加载数据
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
        name='circ_mv'
    )

    # 创建引擎
    engine = BacktestEngine(
        config=BacktestConfig(n_groups=10, enable_cost=False, long_short=True)
    )

    engine.load_factor(factor)
    engine.load_returns(returns)
    engine.load_tradable(tradable)
    engine.load_mv(mv)

    # 运行回测
    result = engine.run()

    # 生成报告
    output_dir = Config.DATA_DIR / 'factor_analysis_results' / 'pb'
    result.generate_report(output_dir)

    print(f"\n分析完成！结果保存到: {output_dir}")


if __name__ == '__main__':
    main()
