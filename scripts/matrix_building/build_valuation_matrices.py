#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
构建估值矩阵（市值和PB）

从 daily_basic.csv 中提取并保存：
- circ_mv_matrix.csv - 流通市值矩阵（万元）
- total_mv_matrix.csv - 总市值矩阵（万元）
- pb_matrix.csv - 市净率矩阵

使用 MatrixBuilder.from_long_format() 方法从长格式数据构建矩阵
"""
import sys
from pathlib import Path

# 添加项目根目录到 Python 路径
project_root = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(project_root))

import argparse
import pandas as pd
from config.config import Config
from data_engine.processors.matrix_builder import MatrixBuilder
from data_engine.processors.matrix_io import save_matrix, matrix_statistics
from utils import setup_logger, get_trade_dates, get_all_stocks


def parse_args():
    """解析命令行参数"""
    parser = argparse.ArgumentParser(
        description='构建估值矩阵（市值和PB）',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
可用的矩阵类型:
  circ_mv     - 流通市值矩阵（万元）
  total_mv    - 总市值矩阵（万元）
  pb          - 市净率矩阵
  all         - 所有矩阵（默认）

示例:
  # 构建所有矩阵
  python scripts/build_valuation_matrices.py

  # 只构建流通市值矩阵
  python scripts/build_valuation_matrices.py --matrices circ_mv

  # 指定日期范围
  python scripts/build_valuation_matrices.py --start-date 20200101 --end-date 20201231

  # 只包含上市股票
  python scripts/build_valuation_matrices.py --list-status L
        """
    )
    parser.add_argument(
        '--start-date',
        type=str,
        default='20150101',
        help='开始日期 YYYYMMDD（默认 20150101）'
    )
    parser.add_argument(
        '--end-date',
        type=str,
        default='20261231',
        help='结束日期 YYYYMMDD（默认 20261231）'
    )
    parser.add_argument(
        '--list-status',
        type=str,
        choices=['L', 'D', 'ALL'],
        default='L',
        help='股票状态（L=上市, D=退市, ALL=全部，默认 L）'
    )
    parser.add_argument(
        '--matrices',
        nargs='+',
        choices=['circ_mv', 'total_mv', 'pb', 'all'],
        default=['all'],
        help='要构建的矩阵类型（默认 all）'
    )
    parser.add_argument(
        '--output-dir',
        type=Path,
        default=Config.MATRIX_DATA_DIR,
        help='输出目录（默认 data/matrices）'
    )
    return parser.parse_args()


def load_daily_basic(start_date: str, end_date: str, logger) -> pd.DataFrame:
    """
    读取 daily_basic 数据并过滤日期范围

    Args:
        start_date: 开始日期 YYYYMMDD
        end_date: 结束日期 YYYYMMDD
        logger: 日志记录器

    Returns:
        DataFrame: 包含 ts_code, trade_date, circ_mv, total_mv, pb 列
    """
    logger.info(f"读取 daily_basic 数据 ({start_date} ~ {end_date})...")

    file_path = Config.SUPPLEMENTARY_DATA_DIR / 'daily_basic.csv'

    if not file_path.exists():
        logger.error(f"文件不存在: {file_path}")
        return pd.DataFrame()

    # 只读取需要的列
    usecols = ['ts_code', 'trade_date', 'circ_mv', 'total_mv', 'pb']

    # 指定 dtype
    dtype = {
        'ts_code': str,
        'trade_date': str,
        'circ_mv': float,
        'total_mv': float,
        'pb': float
    }

    df = pd.read_csv(file_path, usecols=usecols, dtype=dtype)

    # 过滤日期范围
    df = df[(df['trade_date'] >= start_date) & (df['trade_date'] <= end_date)]

    logger.info(f"读取完成，共 {len(df):,} 条记录")
    return df


def build_valuation_matrix(
    df: pd.DataFrame,
    dates: list,
    stocks: list,
    value_column: str,
    matrix_name: str,
    logger
) -> pd.DataFrame:
    """
    从 daily_basic 构建估值矩阵

    Args:
        df: daily_basic DataFrame
        dates: 全局交易日期列表（字符串格式）
        stocks: 股票代码列表
        value_column: 值列名（'circ_mv', 'total_mv', 'pb'）
        matrix_name: 矩阵名称（用于日志）
        logger: 日志记录器

    Returns:
        DataFrame: 矩阵（行=日期，列=股票）
    """
    logger.info(f"构建 {matrix_name}...")

    # 使用 MatrixBuilder.from_long_format
    # 注意：参数名是 value_col 而不是 value_column
    matrix = MatrixBuilder.from_long_format(
        df=df,
        value_col=value_column,
        all_dates=dates,
        all_stocks=stocks,
        default_value=float('nan')  # 缺失值用 NaN
    )

    # 统计信息
    non_nan_count = matrix.notna().sum().sum()
    total_cells = matrix.size
    non_nan_ratio = (non_nan_count / total_cells) * 100 if total_cells > 0 else 0

    logger.info(f"  非空率: {non_nan_ratio:.2f}%")

    # 只对非空值计算统计量
    if non_nan_count > 0:
        logger.info(f"  最小值: {matrix.min().min():.2f}")
        logger.info(f"  最大值: {matrix.max().max():.2f}")
        logger.info(f"  中位数: {matrix.median().median():.2f}")

    return matrix


def main():
    """主函数"""
    args = parse_args()
    logger = setup_logger(prefix="matrix")

    logger.info("=" * 80)
    logger.info("开始构建估值矩阵")
    logger.info("=" * 80)

    # 1. 读取 daily_basic 数据
    df_daily_basic = load_daily_basic(args.start_date, args.end_date, logger)

    if df_daily_basic.empty:
        logger.error("无法读取 daily_basic 数据，退出")
        return

    # 2. 获取全局日期和股票列表
    logger.info("获取交易日历和股票列表...")
    dates = get_trade_dates(args.start_date, args.end_date)
    stocks = get_all_stocks(list_status=args.list_status)

    # 转换日期为字符串格式（与 daily_basic 一致）
    date_strs = [d.strftime('%Y%m%d') for d in dates]

    logger.info(f"交易日数: {len(date_strs)}, 股票数: {len(stocks)}")
    logger.info(f"预期矩阵维度: {len(date_strs)} × {len(stocks)}")

    # 3. 构建矩阵
    matrices_to_build = {
        'circ_mv': ('circ_mv', '流通市值矩阵', 'circ_mv_matrix.csv'),
        'total_mv': ('total_mv', '总市值矩阵', 'total_mv_matrix.csv'),
        'pb': ('pb', '市净率矩阵', 'pb_matrix.csv')
    }

    selected = args.matrices if 'all' not in args.matrices else matrices_to_build.keys()

    for key in selected:
        if key not in matrices_to_build:
            continue

        value_col, name, filename = matrices_to_build[key]

        logger.info("-" * 80)

        matrix = build_valuation_matrix(
            df=df_daily_basic,
            dates=date_strs,
            stocks=stocks,
            value_column=value_col,
            matrix_name=name,
            logger=logger
        )

        # 4. 保存矩阵
        output_file = args.output_dir / filename
        save_matrix(matrix, output_file)
        logger.info(f"  已保存: {output_file}")

        # 5. 输出统计信息
        matrix_statistics(matrix, name)

    logger.info("=" * 80)
    logger.info("✓ 所有矩阵构建完成")
    logger.info("=" * 80)


if __name__ == '__main__':
    main()
