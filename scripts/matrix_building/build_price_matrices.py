#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
构建价格和成交量矩阵

从日线数据中提取并保存：
- open_matrix.csv - 开盘价矩阵（不复权）
- close_matrix.csv - 收盘价矩阵（不复权）
- high_matrix.csv - 最高价矩阵（不复权）
- low_matrix.csv - 最低价矩阵（不复权）
- volume_matrix.csv - 成交量矩阵（手）

- open_hfq_matrix.csv - 开盘价矩阵（后复权）
- close_hfq_matrix.csv - 收盘价矩阵（后复权）
- high_hfq_matrix.csv - 最高价矩阵（后复权）
- low_hfq_matrix.csv - 最低价矩阵（后复权）

一次性读取，避免重复 IO
"""
import sys
from pathlib import Path

# 添加项目根目录到 Python 路径
project_root = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(project_root))

import argparse
import numpy as np
import pandas as pd
from config.config import Config
from data_engine.processors.matrix_builder import MatrixBuilder
from data_engine.processors.matrix_io import save_matrix
from utils import setup_logger, get_trade_dates, get_all_stocks, format_date_range


def parse_args():
    """解析命令行参数"""
    parser = argparse.ArgumentParser(
        description='构建价格和成交量矩阵',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
可用的矩阵类型:
  open        - 开盘价矩阵（不复权）
  close       - 收盘价矩阵（不复权）
  high        - 最高价矩阵（不复权）
  low         - 最低价矩阵（不复权）
  volume      - 成交量矩阵
  open_hfq    - 开盘价矩阵（后复权）
  close_hfq   - 收盘价矩阵（后复权）
  high_hfq    - 最高价矩阵（后复权）
  low_hfq     - 最低价矩阵（后复权）
  all         - 所有矩阵（默认）

示例:
  # 构建所有矩阵
  python scripts/build_price_matrices.py

  # 只构建开盘价和收盘价
  python scripts/build_price_matrices.py --matrices open close

  # 自定义线程数
  python scripts/build_price_matrices.py --n-jobs 8
        """
    )
    parser.add_argument(
        '--matrices',
        nargs='+',
        choices=['open', 'close', 'high', 'low', 'volume',
                'open_hfq', 'close_hfq', 'high_hfq', 'low_hfq', 'all'],
        default=['all'],
        help='要构建的矩阵类型（默认 all）'
    )
    parser.add_argument(
        '--n-jobs',
        type=int,
        default=4,
        help='并行线程数（默认 4）'
    )
    return parser.parse_args()


def build_price_matrix(
    dates: pd.DatetimeIndex,
    stocks: list,
    field_name: str,
    desc: str,
    n_jobs: int = 4
) -> pd.DataFrame:
    """
    构建价格/成交量矩阵

    Args:
        dates: 全局日期索引
        stocks: 股票代码列表
        field_name: 字段名（open_raw, close_raw, high_raw, low_raw, vol, open, close, high, low）
        desc: 进度条描述
        n_jobs: 并行线程数

    Returns:
        DataFrame: 价格/成交量矩阵
    """
    logger.info(f"构建{desc}矩阵...")

    def extractor_func(ts_code: str, dates: pd.DatetimeIndex) -> np.ndarray:
        """提取指定字段"""
        daily_file = Config.DAILY_DATA_DIR / f'{ts_code}.csv'
        if not daily_file.exists():
            return np.full(len(dates), np.nan, dtype=np.float32)

        try:
            df_daily = pd.read_csv(daily_file, dtype={'trade_date': str})
            if df_daily.empty or 'trade_date' not in df_daily.columns:
                return np.full(len(dates), np.nan, dtype=np.float32)

            df_daily['trade_date'] = pd.to_datetime(df_daily['trade_date'], format='%Y%m%d')
            df_daily = df_daily.set_index('trade_date').reindex(dates)

            # 提取字段
            if field_name in df_daily.columns:
                return df_daily[field_name].values.astype(np.float32)
            else:
                return np.full(len(dates), np.nan, dtype=np.float32)
        except Exception as e:
            logger.debug(f"处理 {ts_code} 失败: {e}")
            return np.full(len(dates), np.nan, dtype=np.float32)

    matrix = MatrixBuilder.from_daily_files(
        dates=dates,
        stocks=stocks,
        extractor_func=extractor_func,
        desc=desc,
        n_jobs=n_jobs
    )

    return matrix


def main():
    """主函数"""
    args = parse_args()

    # 设置日志
    logger = setup_logger()
    logger.info("=" * 60)
    logger.info("价格和成交量矩阵构建脚本启动")
    logger.info("=" * 60)

    # 确保目录存在
    Config.MATRIX_DATA_DIR.mkdir(parents=True, exist_ok=True)

    try:
        # 获取交易日期
        logger.info("获取交易日期...")
        dates = get_trade_dates()
        dates_dt = pd.to_datetime(dates, format='%Y%m%d')
        logger.info(format_date_range(dates))

        # 获取所有股票（包括退市）
        logger.info("获取股票列表...")
        all_stocks = get_all_stocks(list_status='ALL')
        logger.info(f"所有股票数量: {len(all_stocks)}（包括已退市）")

        # 确定要构建的矩阵
        matrices_to_build = args.matrices
        if 'all' in matrices_to_build:
            matrices_to_build = ['open', 'close', 'high', 'low', 'volume',
                                'open_hfq', 'close_hfq', 'high_hfq', 'low_hfq']

        logger.info(f"要构建的矩阵: {', '.join(matrices_to_build)}")
        logger.info(f"并行线程数: {args.n_jobs}")
        logger.info("=" * 60)

        # 字段映射
        field_mapping = {
            'open': ('open_raw', '开盘价（不复权）'),
            'close': ('close_raw', '收盘价（不复权）'),
            'high': ('high_raw', '最高价（不复权）'),
            'low': ('low_raw', '最低价（不复权）'),
            'volume': ('vol', '成交量'),
            'open_hfq': ('open', '开盘价（后复权）'),
            'close_hfq': ('close', '收盘价（后复权）'),
            'high_hfq': ('high', '最高价（后复权）'),
            'low_hfq': ('low', '最低价（后复权）'),
        }

        # 构建矩阵
        for i, matrix_name in enumerate(matrices_to_build, 1):
            logger.info(f"\n[{i}/{len(matrices_to_build)}] 构建 {matrix_name} 矩阵")
            logger.info("-" * 60)

            field_name, desc = field_mapping[matrix_name]

            # 构建矩阵
            matrix = build_price_matrix(
                dates=dates_dt,
                stocks=all_stocks,
                field_name=field_name,
                desc=desc,
                n_jobs=args.n_jobs
            )

            # 保存矩阵
            output_file = Config.MATRIX_DATA_DIR / f'{matrix_name}_matrix.csv'
            save_matrix(matrix, output_file)

            # 统计
            non_nan_ratio = (~matrix.isna()).sum().sum() / matrix.size
            logger.info(f"非空比例: {non_nan_ratio:.2%}")
            logger.info(f"保存到: {output_file}")

        # 完成
        logger.info("\n" + "=" * 60)
        logger.info("所有矩阵构建完成")
        logger.info("=" * 60)
        logger.info(f"输出目录: {Config.MATRIX_DATA_DIR}")
        logger.info(f"矩阵数量: {len(matrices_to_build)}")
        logger.info("=" * 60)

        return 0

    except Exception as e:
        logger.error(f"构建矩阵失败: {str(e)}", exc_info=True)
        return 1


if __name__ == '__main__':
    # 导入 logger（必须在 setup_logger 之后）
    import logging
    logger = logging.getLogger(__name__)

    sys.exit(main())
