#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
获取利润表数据（PIT 设计）

用法：
  # 初次全量获取（从2005年至今）
  python scripts/data_fetching/fetch_income.py

  # 增量更新（最近2个季度）
  python scripts/data_fetching/fetch_income.py --update
"""
import sys
import argparse
from pathlib import Path

project_root = Path(__file__).resolve().parent.parent.parent
sys.path.insert(0, str(project_root))

from data_engine.api.tushare_api import TushareAPI
from data_engine.fetchers.income_fetcher import IncomeFetcher
from utils import setup_logger


def main():
    parser = argparse.ArgumentParser(description='获取利润表数据')
    parser.add_argument('--update', action='store_true', help='增量更新模式（只拉取最近2个季度）')
    parser.add_argument('--start-year', type=int, default=2005, help='全量获取起始年份（默认2005）')
    parser.add_argument('--quarters', type=int, default=2, help='增量更新回溯季度数（默认2）')
    args = parser.parse_args()

    logger = setup_logger()
    api = TushareAPI()
    fetcher = IncomeFetcher(api)

    if args.update:
        logger.info(f"增量更新模式：最近 {args.quarters} 个季度")
        df = fetcher.update(n_quarters=args.quarters)
    else:
        logger.info(f"全量获取模式：从 {args.start_year} 年至今")
        df = fetcher.fetch_all(start_year=args.start_year)

    logger.info(f"完成。共 {len(df)} 条记录")
    if len(df) > 0:
        logger.info(f"报告期范围: {df['end_date'].min()} ~ {df['end_date'].max()}")
        logger.info(f"公告日期范围: {df['ann_date'].min()} ~ {df['ann_date'].max()}")
        # 验证 PIT：检查是否存在同一报告期的多个版本
        dup = df.groupby(['ts_code', 'end_date']).size()
        multi_version = (dup > 1).sum()
        logger.info(f"存在多版本的 (ts_code, end_date) 组合: {multi_version} 个（PIT 验证）")


# if __name__ == '__main__':
main()
