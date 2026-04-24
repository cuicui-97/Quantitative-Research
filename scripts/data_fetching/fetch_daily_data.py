#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
日线数据抓取脚本

功能：
- 批量抓取所有股票的日线行情数据（后复权）
- 支持断点续传（跳过已存在的文件）
- 支持分批抓取（指定起始索引和批次大小）
- 一股一文件存储

使用方法：
    # 全量抓取（断点续传）
    python scripts/fetch_daily_data.py

    # 指定起始位置和批次大小
    python scripts/fetch_daily_data.py --start-index 1000 --batch-size 500

    # 强制重新抓取（不跳过已存在文件）
    python scripts/fetch_daily_data.py --no-skip-existing
"""
import sys
import argparse
from pathlib import Path

# 添加项目根目录到 Python 路径
project_root = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(project_root))

import pandas as pd
from config.config import Config
from data_engine.api.tushare_api import TushareAPI
from data_engine.fetchers.daily_fetcher import DailyDataFetcher
from utils import setup_logger


def parse_args():
    """解析命令行参数"""
    parser = argparse.ArgumentParser(
        description='批量抓取 A 股日线数据（三种复权类型：不复权、前复权、后复权）',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
示例:
  # 全量抓取（断点续传）
  python scripts/fetch_daily_data.py

  # 从第 1000 只股票开始，抓取 500 只
  python scripts/fetch_daily_data.py --start-index 1000 --batch-size 500

  # 强制重新抓取（覆盖已存在文件）
  python scripts/fetch_daily_data.py --no-skip-existing
        """
    )

    parser.add_argument(
        '--start-index',
        type=int,
        default=0,
        help='起始索引（从第 N 只股票开始，默认 0）'
    )

    parser.add_argument(
        '--batch-size',
        type=int,
        default=None,
        help='批次大小（不指定则抓取全部）'
    )

    parser.add_argument(
        '--skip-existing',
        dest='skip_existing',
        action='store_true',
        default=True,
        help='跳过已存在的文件（默认开启，支持断点续传）'
    )

    parser.add_argument(
        '--no-skip-existing',
        dest='skip_existing',
        action='store_false',
        help='不跳过已存在的文件（强制重新抓取）'
    )

    return parser.parse_args()


def main():
    """主函数"""
    # 1. 解析命令行参数
    args = parse_args()

    # 2. 初始化日志系统（配置根logger，让所有模块的日志都能输出）
    logger = setup_logger(prefix="fetch")
    logger.info("=" * 60)
    logger.info("日线数据抓取脚本启动")
    logger.info("=" * 60)

    try:
        # 3. 确保目录存在
        Config.ensure_dirs()

        # 4. 读取基础数据文件
        basic_data_file = Config.BASIC_DATA_FILE
        if not basic_data_file.exists():
            logger.error(
                f"基础数据文件不存在: {basic_data_file}\n"
                f"请先运行 scripts/fetch_basic_data.py 获取基础数据"
            )
            return 1

        logger.info(f"正在读取基础数据: {basic_data_file}")
        # 指定日期字段为字符串类型，避免被自动转换为整数
        df_stocks = pd.read_csv(
            basic_data_file,
            dtype={
                'list_date': str,
                'delist_date': str,
                'ipo_date': str,
                'issue_date': str,
                'setup_date': str
            }
        )
        logger.info(f"读取到 {len(df_stocks)} 只股票")

        # 5. 检查必要字段
        required_fields = ['ts_code', 'list_date']
        missing_fields = [f for f in required_fields if f not in df_stocks.columns]
        if missing_fields:
            logger.error(f"基础数据缺少必要字段: {missing_fields}")
            return 1

        # 6. 创建 Tushare API 实例
        logger.info("正在初始化 Tushare API...")
        api = TushareAPI()

        # 7. 测试连接
        logger.info("正在测试连接...")
        if not api.test_connection():
            logger.error("连接测试失败，请检查 TOKEN 和 API_URL 配置")
            return 1

        # 8. 创建日线数据抓取器（传入基础信息）
        logger.info("正在创建日线数据抓取器...")
        fetcher = DailyDataFetcher(api, basic_info_df=df_stocks)

        # 9. 批量抓取数据
        logger.info("开始批量抓取日线数据...")
        result = fetcher.fetch_all_stocks(
            stock_list_df=df_stocks,
            data_dir=Config.DAILY_DATA_DIR,
            skip_existing=args.skip_existing,
            start_index=args.start_index,
            batch_size=args.batch_size
        )

        # 10. 输出最终统计
        logger.info("=" * 60)
        logger.info("日线数据抓取完成！")
        logger.info(f"数据保存目录: {Config.DAILY_DATA_DIR}")
        logger.info(f"成功: {result['success_count']}")
        logger.info(f"跳过: {result['skip_count']}")
        logger.info(f"失败: {result['fail_count']}")

        if result['fail_count'] > 0:
            logger.warning(
                f"\n有 {result['fail_count']} 只股票抓取失败，"
                f"请查看日志了解详情"
            )

        logger.info("=" * 60)

        return 0

    except Exception as e:
        logger.error(f"执行失败: {e}", exc_info=True)
        return 1


if __name__ == '__main__':
    sys.exit(main())
