#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
构建利润表因子矩阵（原始值，无winsorize）

默认输出原始因子值，winsorize建议在回测时处理

用法：
  python scripts/matrix_building/build_income_matrices.py
  python scripts/matrix_building/build_income_matrices.py --winsorize-pct 0.01
"""
import argparse
import sys
from pathlib import Path

project_root = Path(__file__).resolve().parent.parent.parent
sys.path.insert(0, str(project_root))

from data_engine.processors.financial_matrix_builder import FinancialMatrixBuilder
from utils import setup_logger


def main():
    parser = argparse.ArgumentParser(description='构建利润表YoY矩阵')
    parser.add_argument('--no-winsorize', action='store_true',
                        help='禁用Winsorize处理，保留原始值')
    parser.add_argument('--winsorize-pct', type=float, default=0.0,
                        help='Winsorize百分位 (默认: 0，即不处理；建议回测时用0.01)')
    args = parser.parse_args()

    winsorize_pct = args.winsorize_pct

    logger = setup_logger(prefix="matrix")
    logger.info(f"Winsorize设置: {'保留原始值' if winsorize_pct == 0 else f'{winsorize_pct*100:.1f}%'}")

    builder = FinancialMatrixBuilder()

    # 有完整季度数据的利润表指标
    fields = [
        ('n_income_attr_p', '归母净利润'),
        ('total_revenue', '营业总收入'),
        ('revenue', '营业收入'),
        ('operate_profit', '营业利润'),
        ('n_income', '净利润'),
        ('total_profit', '利润总额'),
        ('admin_exp', '管理费用'),
        ('fin_exp', '财务费用'),
        ('sell_exp', '销售费用'),
    ]

    for field, name in fields:
        logger.info(f"\n{'='*60}")
        logger.info(f"构建 {name} ({field}) 同比增速矩阵...")
        logger.info(f"{'='*60}")
        yoy = builder.yoy(field, winsorize_pct=winsorize_pct)
        logger.info(f"{name} YoY矩阵: {yoy.shape}, 非空率: {yoy.notna().mean().mean():.1%}")

    logger.info("\n全部完成！")


if __name__ == '__main__':
    main()
