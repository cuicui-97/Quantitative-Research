#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
因子构建脚本 v3.0 - 使用统一构建框架

优势：
1. 全面矩阵化计算（无逐股票循环）
2. 自动中间结果缓存
3. 支持声明式配置（YAML）
4. 严格防未来函数

使用方法：
    # 从配置文件构建
    python build_factors_v3.py --config config/factors/momentum.yaml

    # 使用预定义构建器
    python build_factors_v3.py --type momentum --start-date 20200101

    # 构建所有标准因子
    python build_factors_v3.py --type all
"""
import sys
from pathlib import Path

project_root = Path(__file__).resolve().parent.parent.parent
sys.path.insert(0, str(project_root))

import argparse
import logging
from pathlib import Path

from data_engine.core.unified_factor_builder import (
    UnifiedFactorBuilder,
    MomentumFactorBuilder,
    LiquidityFactorBuilder,
    build_all_factors
)
from utils import setup_logger


def main():
    parser = argparse.ArgumentParser(
        description='构建因子矩阵 v3.0（统一框架）',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
示例:
  # 从配置文件构建
  python build_factors_v3.py --config config/factors/momentum.yaml

  # 只构建动量因子
  python build_factors_v3.py --type momentum

  # 构建所有标准因子
  python build_factors_v3.py --type all --start-date 20200101

  # 指定日期范围
  python build_factors_v3.py --type all --start-date 20200101 --end-date 20231231
        """
    )

    parser.add_argument(
        '--config',
        type=Path,
        help='因子配置文件路径（YAML格式）'
    )

    parser.add_argument(
        '--type',
        choices=['momentum', 'liquidity', 'all'],
        default='all',
        help='要构建的因子类型（默认: all）'
    )

    parser.add_argument(
        '--start-date',
        type=str,
        default='20150101',
        help='开始日期 (YYYYMMDD格式，默认: 20150101)'
    )

    parser.add_argument(
        '--end-date',
        type=str,
        default=None,
        help='结束日期 (YYYYMMDD格式，默认: 今天)'
    )

    parser.add_argument(
        '--dry-run',
        action='store_true',
        help='试运行模式：计算但不保存结果'
    )

    parser.add_argument(
        '--list-cache',
        action='store_true',
        help='显示缓存统计信息'
    )

    args = parser.parse_args()

    # 设置日志
    logger = setup_logger(prefix="factor_builder_v3")

    logger.info("=" * 70)
    logger.info("因子构建 v3.0（统一框架）")
    logger.info("=" * 70)

    # 从配置文件构建
    if args.config:
        if not args.config.exists():
            logger.error(f"配置文件不存在: {args.config}")
            sys.exit(1)

        logger.info(f"从配置文件构建: {args.config}")
        logger.info(f"日期: {args.start_date} ~ {args.end_date or '今天'}")

        builder = UnifiedFactorBuilder(
            config_file=args.config,
            start_date=args.start_date,
            end_date=args.end_date
        )
        results = builder.build(save=not args.dry_run)

        logger.info(f"\n构建完成: {len(results)} 个因子")
        for name in results:
            logger.info(f"  - {name}")

        if args.list_cache:
            stats = builder.computation_graph.get_cache_stats()
            logger.info(f"\n缓存统计: 命中 {stats['hits']}, 未命中 {stats['misses']}, 大小 {stats['size']}")

    # 从预定义构建器构建
    else:
        types_to_build = ['momentum', 'liquidity'] if args.type == 'all' else [args.type]

        logger.info(f"类型: {', '.join(types_to_build)}")
        logger.info(f"日期: {args.start_date} ~ {args.end_date or '今天'}")
        logger.info(f"保存: {'否' if args.dry_run else '是'}")
        logger.info("=" * 70)

        all_results = build_all_factors(
            factor_types=types_to_build,
            start_date=args.start_date,
            end_date=args.end_date
        )

        logger.info("\n" + "=" * 70)
        logger.info(f"全部完成: {len(all_results)} 个因子")
        logger.info("=" * 70)


if __name__ == '__main__':
    main()
