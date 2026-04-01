#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
构建利润表因子矩阵

用法：
  python scripts/matrix_building/build_income_matrices.py
"""
import sys
from pathlib import Path

project_root = Path(__file__).resolve().parent.parent.parent
sys.path.insert(0, str(project_root))

from data_engine.processors.financial_matrix_builder import FinancialMatrixBuilder
from utils import setup_logger


def main():
    logger = setup_logger()

    builder = FinancialMatrixBuilder()

    # 归母净利润单季 PIT 矩阵
    logger.info("构建归母净利润单季矩阵...")
    pit = builder.pit_single_quarter('n_income_attr_p')
    logger.info(f"单季矩阵: {pit.shape}, 非空率: {pit.notna().mean().mean():.1%}")

    # 归母净利润同比增速矩阵
    logger.info("构建归母净利润同比增速矩阵...")
    yoy = builder.yoy('n_income_attr_p')
    logger.info(f"同比矩阵: {yoy.shape}, 非空率: {yoy.notna().mean().mean():.1%}")


if __name__ == '__main__':
    main()
