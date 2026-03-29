#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
Fama 三因子数据获取脚本

一键获取所有 Fama 三因子所需的数据：
1. 日度基本面数据（市值、PE、PB）
2. 指数日线数据（沪深300）
3. Shibor 利率数据
"""
import sys
from pathlib import Path

# 添加项目根目录到 Python 路径
project_root = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(project_root))

import argparse
from config.config import Config
from data_engine.api.tushare_api import TushareAPI
from data_engine.fetchers.daily_basic_fetcher import DailyBasicFetcher
from data_engine.fetchers.index_data_fetcher import IndexDataFetcher
from data_engine.fetchers.risk_free_rate_fetcher import RiskFreeRateFetcher
from utils import setup_logger


def parse_args():
    """解析命令行参数"""
    parser = argparse.ArgumentParser(
        description='Fama 三因子数据获取脚本',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
示例:
  # 获取全部历史数据
  python scripts/fetch_fama_data.py --start-date 20050101 --end-date 20261231

  # 只获取特定数据
  python scripts/fetch_fama_data.py --data-types daily_basic index

  # 强制刷新（重新获取所有数据）
  python scripts/fetch_fama_data.py --force-refresh
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
        '--data-types',
        nargs='+',
        choices=['daily_basic', 'index', 'shibor', 'all'],
        default=['all'],
        help='要获取的数据类型（默认 all）'
    )
    parser.add_argument(
        '--index-code',
        type=str,
        default='000300.SH',
        help='指数代码（默认 000300.SH 沪深300）'
    )
    parser.add_argument(
        '--force-refresh',
        action='store_true',
        help='强制刷新（忽略已有文件，重新获取所有数据）'
    )
    return parser.parse_args()


def main():
    """主函数"""
    args = parse_args()

    # 设置日志
    logger = setup_logger()
    logger.info("=" * 60)
    logger.info("Fama 三因子数据获取脚本启动")
    logger.info("=" * 60)

    # 确定要获取的数据类型
    if 'all' in args.data_types:
        data_types = ['daily_basic', 'index', 'shibor']
    else:
        data_types = args.data_types

    logger.info(f"要获取的数据类型: {', '.join(data_types)}")
    logger.info(f"日期范围: {args.start_date} ~ {args.end_date}")
    logger.info(f"强制刷新: {'是' if args.force_refresh else '否'}")
    logger.info("=" * 60)

    try:
        # 1. 初始化 API
        logger.info("\n[步骤 1/4] 初始化 Tushare API...")
        api = TushareAPI()
        logger.info("✓ API 初始化成功")

        # 2. 获取日度基本面数据
        if 'daily_basic' in data_types:
            logger.info("\n[步骤 2/4] 获取日度基本面数据（市值、PE、PB）...")
            logger.info("-" * 60)
            fetcher = DailyBasicFetcher(api)
            df_daily_basic = fetcher.fetch_daily_basic(
                start_date=args.start_date,
                end_date=args.end_date,
                force_refresh=args.force_refresh
            )
            logger.info(f"✓ 日度基本面数据获取完成，共 {len(df_daily_basic)} 条记录")
        else:
            logger.info("\n[步骤 2/4] 跳过日度基本面数据")

        # 3. 获取指数日线数据
        if 'index' in data_types:
            logger.info(f"\n[步骤 3/4] 获取指数日线数据（{args.index_code}）...")
            logger.info("-" * 60)
            fetcher = IndexDataFetcher(api)
            df_index = fetcher.fetch_index_daily(
                ts_code=args.index_code,
                start_date=args.start_date,
                end_date=args.end_date,
                force_refresh=args.force_refresh
            )
            logger.info(f"✓ 指数日线数据获取完成，共 {len(df_index)} 条记录")
        else:
            logger.info("\n[步骤 3/4] 跳过指数日线数据")

        # 4. 获取 Shibor 利率数据
        if 'shibor' in data_types:
            logger.info("\n[步骤 4/4] 获取 Shibor 利率数据...")
            logger.info("-" * 60)
            fetcher = RiskFreeRateFetcher(api)
            df_shibor = fetcher.fetch_shibor(
                start_date=args.start_date,
                end_date=args.end_date,
                force_refresh=args.force_refresh
            )
            logger.info(f"✓ Shibor 利率数据获取完成，共 {len(df_shibor)} 条记录")
        else:
            logger.info("\n[步骤 4/4] 跳过 Shibor 利率数据")

        # 5. 输出统计摘要
        logger.info("\n" + "=" * 60)
        logger.info("数据获取完成")
        logger.info("=" * 60)
        logger.info(f"输出目录: {Config.SUPPLEMENTARY_DATA_DIR}")
        logger.info("\n数据文件:")

        if 'daily_basic' in data_types:
            daily_basic_file = Config.SUPPLEMENTARY_DATA_DIR / 'daily_basic.csv'
            if daily_basic_file.exists():
                size_mb = daily_basic_file.stat().st_size / (1024 * 1024)
                logger.info(f"  1. daily_basic.csv - 日度基本面数据 ({size_mb:.2f} MB)")

        if 'index' in data_types:
            index_file = Config.SUPPLEMENTARY_DATA_DIR / f'index_daily_{args.index_code}.csv'
            if index_file.exists():
                size_mb = index_file.stat().st_size / (1024 * 1024)
                logger.info(f"  2. index_daily_{args.index_code}.csv - 指数日线数据 ({size_mb:.2f} MB)")

        if 'shibor' in data_types:
            shibor_file = Config.SUPPLEMENTARY_DATA_DIR / 'shibor.csv'
            if shibor_file.exists():
                size_mb = shibor_file.stat().st_size / (1024 * 1024)
                logger.info(f"  3. shibor.csv - Shibor 利率数据 ({size_mb:.2f} MB)")

        logger.info("\n下一步:")
        logger.info("  - 运行验证脚本: python scripts/validate_fama_data.py")
        logger.info("  - 构建因子（开发中）: python scripts/build_fama_factors.py")
        logger.info("=" * 60)

        return 0

    except Exception as e:
        logger.error(f"数据获取失败: {str(e)}", exc_info=True)
        return 1


if __name__ == '__main__':
    sys.exit(main())
