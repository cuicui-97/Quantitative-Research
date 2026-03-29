#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
批量构建所有矩阵 - 使用统一的 MatrixProcessor

按顺序构建：
1. ST 状态矩阵
2. 停牌状态矩阵
3. 上市天数矩阵
4. 数据缺失矩阵
5. 涨跌停矩阵
6. 交易可用性矩阵
7. 开盘收益率矩阵

支持选择性构建：通过 --matrices 参数指定要构建的矩阵

ST矩阵数据源说明：
  - st_status.csv 包含完整历史数据（API数据 + namechange提取的历史数据）
  - 如需更新完整ST数据，请运行：
    1. python scripts/data_fetching/fetch_st_status.py         # 更新API数据
    2. python scripts/data_fetching/fetch_namechange_history.py # 更新名称变更历史
    3. python scripts/data_fetching/fetch_and_merge_st_data.py  # 融合数据到st_status.csv
"""
import sys
from pathlib import Path

# 添加项目根目录到 Python 路径
project_root = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(project_root))

import argparse
import pandas as pd
from config.config import Config
from data_engine.processors.matrix_processor import MatrixProcessor
from data_engine.processors.matrix_io import save_matrix
from utils import setup_logger, get_trade_dates, get_all_stocks, format_date_range


def parse_args():
    """解析命令行参数"""
    parser = argparse.ArgumentParser(
        description='批量构建矩阵',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
可用的矩阵类型:
  st              - ST 状态矩阵
  suspension      - 停牌状态矩阵
  listing_days    - 上市天数矩阵
  missing_data    - 数据缺失矩阵
  limit           - 涨跌停矩阵
  tradability     - 交易可用性矩阵
  return          - 开盘收益率矩阵
  all             - 所有矩阵（默认）

示例:
  # 构建所有矩阵
  python scripts/build_all_matrices.py

  # 只构建 ST 矩阵
  python scripts/build_all_matrices.py --matrices st

  # 只构建开盘收益率矩阵
  python scripts/build_all_matrices.py --matrices return

  # 构建多个矩阵
  python scripts/build_all_matrices.py --matrices st suspension limit tradability

  # 指定最小上市天数
  python scripts/build_all_matrices.py --min-listing-days 90
        """
    )
    parser.add_argument(
        '--matrices',
        nargs='+',
        choices=['st', 'suspension', 'listing_days', 'missing_data', 'limit', 'tradability', 'return', 'all'],
        default=['all'],
        help='要构建的矩阵类型（可指定多个）'
    )
    parser.add_argument(
        '--min-listing-days',
        type=int,
        default=180,
        help='最小上市天数（默认 180 天）'
    )
    parser.add_argument(
        '--n-jobs',
        type=int,
        default=4,
        help='并行线程数（默认 4）'
    )
    return parser.parse_args()


def main():
    """主函数"""
    args = parse_args()

    # 设置日志
    logger = setup_logger()
    logger.info("=" * 60)
    logger.info("批量构建矩阵（使用统一 MatrixProcessor）")
    logger.info("=" * 60)

    # 确定要构建的矩阵
    if 'all' in args.matrices:
        matrices_to_build = ['st', 'suspension', 'listing_days', 'missing_data', 'limit', 'tradability', 'return']
    else:
        matrices_to_build = args.matrices

    logger.info(f"要构建的矩阵: {', '.join(matrices_to_build)}")
    logger.info("=" * 60)

    try:
        # 1. 读取基础数据
        basic_file = Config.BASIC_DATA_DIR / 'all_companies_info.csv'
        if not basic_file.exists():
            logger.error(f"基础数据文件不存在: {basic_file}")
            logger.error("请先运行: python scripts/fetch_basic_data.py")
            return 1

        logger.info(f"读取基础数据: {basic_file}")
        basic_info = pd.read_csv(basic_file, dtype={'list_date': str, 'delist_date': str})

        # 2. 获取所有股票列表（包括已退市的）
        all_stocks = get_all_stocks(list_status='ALL')
        logger.info(f"所有股票数量: {len(all_stocks)}（包括已退市）")

        # 统计各状态股票数量
        listed_count = (basic_info['list_status'] == 'L').sum()
        delisted_count = (basic_info['list_status'] == 'D').sum()
        logger.info(f"  - 上市: {listed_count} 只")
        logger.info(f"  - 退市: {delisted_count} 只")

        # 3. 获取交易日期
        logger.info("获取全局交易日期...")
        dates = get_trade_dates()
        logger.info(f"交易日期: {format_date_range(dates)}")

        # 4. 检查 ST 状态矩阵是否存在（仅在需要时检查）
        st_matrix_file = Config.MATRIX_DATA_DIR / 'st_matrix.csv'
        if 'st' not in matrices_to_build and not st_matrix_file.exists():
            logger.warning(f"ST 状态矩阵不存在: {st_matrix_file}")
            logger.warning("如需构建，运行: python scripts/build_all_matrices.py --matrices st")
            logger.info("继续构建其他矩阵...")

        # 5. 检查停牌状态矩阵是否存在（仅在需要时检查）
        suspension_matrix_file = Config.MATRIX_DATA_DIR / 'suspension_matrix.csv'
        if 'suspension' not in matrices_to_build and not suspension_matrix_file.exists():
            logger.warning(f"停牌状态矩阵不存在: {suspension_matrix_file}")
            logger.warning("如需构建，运行: python scripts/build_all_matrices.py --matrices suspension")
            logger.info("继续构建其他矩阵...")

        # 6. 初始化矩阵计算器
        logger.info("\n" + "=" * 60)
        logger.info("初始化矩阵处理器...")
        logger.info("=" * 60)
        processor = MatrixProcessor(basic_info=basic_info)

        # 7. 构建各个矩阵
        logger.info("\n" + "=" * 60)
        logger.info("开始构建矩阵")
        logger.info("=" * 60)

        total = len(matrices_to_build)
        current = 0
        built_matrices = []

        # 7.0 ST 状态矩阵
        if 'st' in matrices_to_build:
            current += 1
            logger.info(f"\n[{current}/{total}] 构建 ST 状态矩阵...")
            st_input_file = Config.SUPPLEMENTARY_DATA_DIR / 'st_status.csv'
            if not st_input_file.exists():
                logger.error(f"ST 状态数据文件不存在: {st_input_file}")
                logger.error("请先运行: python scripts/fetch_st_status.py")
                return 1

            st_matrix = processor.build_st_matrix(st_file=st_input_file, all_stocks=all_stocks)
            output_file = Config.MATRIX_DATA_DIR / 'st_matrix.csv'
            st_matrix.to_csv(output_file)
            logger.info(f"✓ ST 状态矩阵已保存: {output_file}")
            built_matrices.append('st_matrix.csv - ST 状态矩阵')

        # 7.0.1 停牌状态矩阵
        if 'suspension' in matrices_to_build:
            current += 1
            logger.info(f"\n[{current}/{total}] 构建停牌状态矩阵...")
            suspension_input_file = Config.SUPPLEMENTARY_DATA_DIR / 'suspension_status.csv'
            if not suspension_input_file.exists():
                logger.error(f"停牌状态数据文件不存在: {suspension_input_file}")
                logger.error("请先运行: python scripts/fetch_suspension_status.py")
                return 1

            suspension_matrix = processor.build_suspension_matrix(suspension_file=suspension_input_file, all_stocks=all_stocks)
            output_file = Config.MATRIX_DATA_DIR / 'suspension_matrix.csv'
            suspension_matrix.to_csv(output_file)
            logger.info(f"✓ 停牌状态矩阵已保存: {output_file}")
            built_matrices.append('suspension_matrix.csv - 停牌状态矩阵')

        # 7.1 上市天数矩阵
        if 'listing_days' in matrices_to_build:
            current += 1
            logger.info(f"\n[{current}/{total}] 构建上市天数矩阵...")
            listing_days_matrix = processor.build_listing_days_matrix(
                dates=dates,
                stocks=all_stocks,
                min_listing_days=args.min_listing_days,
                n_jobs=args.n_jobs
            )
            output_file = Config.MATRIX_DATA_DIR / 'listing_days_matrix.csv'
            listing_days_matrix.to_csv(output_file)
            logger.info(f"✓ 上市天数矩阵已保存: {output_file}")
            built_matrices.append('listing_days_matrix.csv - 上市天数矩阵')

        # 7.2 数据缺失矩阵
        if 'missing_data' in matrices_to_build:
            current += 1
            logger.info(f"\n[{current}/{total}] 构建数据缺失矩阵...")
            missing_data_matrix = processor.build_missing_data_matrix(
                dates=dates,
                stocks=all_stocks,
                n_jobs=args.n_jobs
            )
            output_file = Config.MATRIX_DATA_DIR / 'missing_data_matrix.csv'
            missing_data_matrix.to_csv(output_file)
            logger.info(f"✓ 数据缺失矩阵已保存: {output_file}")
            built_matrices.append('missing_data_matrix.csv - 数据缺失矩阵')

        # 7.3 涨跌停矩阵
        if 'limit' in matrices_to_build:
            current += 1
            logger.info(f"\n[{current}/{total}] 构建涨跌停矩阵...")
            limit_matrix = processor.build_limit_matrix(
                dates=dates,
                stocks=all_stocks,
                n_jobs=args.n_jobs
            )
            output_file = Config.MATRIX_DATA_DIR / 'limit_matrix.csv'
            limit_matrix.to_csv(output_file)
            logger.info(f"✓ 涨跌停矩阵已保存: {output_file}")
            built_matrices.append('limit_matrix.csv - 涨跌停矩阵')

        # 7.4 交易可用性矩阵（完整版本）
        if 'tradability' in matrices_to_build:
            current += 1
            logger.info(f"\n[{current}/{total}] 构建交易可用性矩阵...")
            tradability_matrix = processor.build_tradability_matrix(
                dates=dates,
                stocks=all_stocks,
                save_intermediate=False,  # 不保存中间矩阵（已经单独保存了）
                n_jobs=args.n_jobs
            )
            built_matrices.append('tradability_matrix.csv - 交易可用性矩阵')

        # 7.5 开盘收益率矩阵
        if 'return' in matrices_to_build:
            current += 1
            logger.info(f"\n[{current}/{total}] 构建开盘收益率矩阵...")
            open_return_matrix = processor.build_return_matrix(
                dates=dates,
                stocks=all_stocks,
                n_jobs=args.n_jobs
            )
            built_matrices.append('open_return_matrix.csv - 开盘收益率矩阵')

        # 8. 输出总结
        logger.info("\n" + "=" * 60)
        logger.info("矩阵构建完成")
        logger.info("=" * 60)
        logger.info(f"输出目录: {Config.MATRIX_DATA_DIR}")
        logger.info(f"\n本次构建的矩阵 ({len(built_matrices)} 个):")
        for i, matrix_name in enumerate(built_matrices, 1):
            logger.info(f"  {i}. {matrix_name}")

        logger.info("=" * 60)

        return 0

    except Exception as e:
        logger.error(f"构建矩阵失败: {str(e)}", exc_info=True)
        return 1


if __name__ == '__main__':
    sys.exit(main())
