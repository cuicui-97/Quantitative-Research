#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
净利润同比增速因子分析
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
    print("净利润同比增速因子分析")
    print("="*60 + "\n")

    # 加载数据
    factor = FactorMatrix.from_csv(
        Config.MATRIX_DATA_DIR / 'net_profit_yoy_matrix.csv',
        name='net_profit_yoy'
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

    # 创建引擎（含成本和不含成本）
    for enable_cost in [False, True]:
        cost_tag = 'with_cost' if enable_cost else 'no_cost'
        print(f"\n{'='*60}")
        print(f"回测: {'含' if enable_cost else '不含'}交易成本")
        print(f"{'='*60}")

        engine = BacktestEngine(
            config=BacktestConfig(
                n_groups=10,
                enable_cost=enable_cost,
                commission_rate=0.0003,
                stamp_duty_rate=0.001,
                slippage_rate=0.001
            )
        )

        engine.load_factor(factor)
        engine.load_returns(returns)
        engine.load_tradable(tradable)
        engine.load_mv(mv)

        result = engine.run()

        output_dir = Config.DATA_DIR / 'factor_analysis_results' / f'net_profit_yoy_{cost_tag}'
        result.generate_report(output_dir)

    print(f"\n分析完成！")


if __name__ == '__main__':
    main()
