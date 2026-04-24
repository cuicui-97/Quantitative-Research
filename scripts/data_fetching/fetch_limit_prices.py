#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
获取涨跌停价格数据

使用 Tushare stk_limit 接口获取全市场股票的涨跌停价格
支持增量更新和断点续传
"""
import sys
from pathlib import Path

# 添加项目根目录到 Python 路径
project_root = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(project_root))

import argparse
from config.config import Config
from data_engine.api.tushare_api import TushareAPI
from data_engine.fetchers.limit_fetcher import LimitFetcher
from utils import setup_logger, get_trade_dates, format_date_range


def parse_args():
    """解析命令行参数"""
    parser = argparse.ArgumentParser(
        description='获取涨跌停价格数据',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
示例:
  # 获取所有交易日的涨跌停价格
  python scripts/fetch_limit_prices.py

  # 获取指定日期范围的数据
  python scripts/fetch_limit_prices.py --start-date 20100101 --end-date 20261231

  # 指定保存间隔（每 100 个交易日保存一次）
  python scripts/fetch_limit_prices.py --save-interval 100

说明:
  - 支持增量更新：自动识别已有数据，只获取缺失的日期
  - 支持断点续传：每 N 个交易日保存一次进度
  - 数据保存在: data/supplementary/limit_prices.csv
        """
    )
    parser.add_argument(
        '--start-date',
        type=str,
        help='开始日期（格式：YYYYMMDD）'
    )
    parser.add_argument(
        '--end-date',
        type=str,
        help='结束日期（格式：YYYYMMDD）'
    )
    parser.add_argument(
        '--save-interval',
        type=int,
        default=50,
        help='保存间隔（每 N 个交易日保存一次，默认 50）'
    )
    return parser.parse_args()


def main():
    """主函数"""
    args = parse_args()

    # 设置日志
    logger = setup_logger(prefix="fetch")
    logger.info("=" * 60)
    logger.info("涨跌停价格数据获取脚本启动")
    logger.info("=" * 60)

    try:
        # 初始化 API
        logger.info("初始化 Tushare API...")
        api = TushareAPI()

        # 获取交易日期
        logger.info("获取交易日历...")
        trade_dates = get_trade_dates()
        trade_dates_str = [d.strftime('%Y%m%d') for d in trade_dates]
        logger.info(f"交易日期: {format_date_range(trade_dates)}")

        # 初始化 Fetcher
        fetcher = LimitFetcher(api=api)

        # 获取并保存数据
        logger.info("\n" + "=" * 60)
        logger.info("开始获取涨跌停价格数据")
        logger.info("=" * 60)

        output_file = fetcher.fetch_and_save(
            trade_dates=trade_dates_str,
            start_date=args.start_date,
            end_date=args.end_date,
            save_interval=args.save_interval
        )

        logger.info("\n" + "=" * 60)
        logger.info("涨跌停价格数据获取完成")
        logger.info("=" * 60)
        logger.info(f"输出文件: {output_file}")
        logger.info("=" * 60)

        return 0

    except KeyboardInterrupt:
        logger.warning("\n用户中断操作")
        return 1
    except Exception as e:
        logger.error(f"获取涨跌停价格数据失败: {str(e)}", exc_info=True)
        return 1


if __name__ == '__main__':
    sys.exit(main())
