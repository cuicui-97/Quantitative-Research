#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
因子分析脚本模板

使用方法：复制此文件并修改配置部分
"""
import sys
from pathlib import Path

project_root = Path(__file__).resolve().parent.parent.parent
sys.path.insert(0, str(project_root))

from data_engine.core.factor_matrix import FactorMatrix
from factor_engine.backtest.backtest_engine import BacktestEngine, BacktestConfig
from config.config import Config

# ==================== 配置部分 ====================
FACTOR_NAME = "因子名称"          # 如: "pb", "momentum_20d"
FACTOR_FILE = "因子矩阵文件名"     # 如: "pb_matrix.csv"
OUTPUT_SUBDIR = "输出子目录名"     # 如: "pb_analysis"

# 可选：股票池筛选（指数成分股）
# INDEX_FILE = "中证1000_matrix.csv"  # 设为 None 表示全市场
# INDEX_NAME = "zz1000"
INDEX_FILE = None
INDEX_NAME = None

# 回测配置
N_GROUPS = 10
ENABLE_COST = False
# =================================================


def main():
    """运行因子分析"""
    print(f"\n{'='*60}")
    print(f"因子分析: {FACTOR_NAME}")
    print(f"{'='*60}\n")

    # 加载数据
    factor = FactorMatrix.from_csv(
        Config.MATRIX_DATA_DIR / FACTOR_FILE,
        name=FACTOR_NAME
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
        config=BacktestConfig(
            n_groups=N_GROUPS,
            enable_cost=ENABLE_COST,
            long_short=True
        )
    )

    # 加载矩阵
    engine.load_factor(factor)
    engine.load_returns(returns)
    engine.load_tradable(tradable)
    engine.load_mv(mv)

    # 加载指数成分股（如果配置）
    if INDEX_FILE:
        index_path = Config.MATRIX_DATA_DIR / INDEX_FILE
        if index_path.exists():
            index_component = FactorMatrix.from_csv(index_path, name=INDEX_NAME)
            engine.load_index_component(index_component, name=INDEX_NAME)
        else:
            print(f"警告: 指数文件不存在 {index_path}")

    # 运行回测
    result = engine.run()

    # 生成报告
    output_dir = Config.DATA_DIR / 'factor_analysis_results' / OUTPUT_SUBDIR
    result.generate_report(output_dir)

    print(f"\n分析完成！结果保存到: {output_dir}")


if __name__ == '__main__':
    main()
