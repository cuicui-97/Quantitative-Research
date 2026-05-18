#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
东方财富板块数据获取脚本（使用dc系列接口）

数据来源：
1. dc_index - 板块列表（6000个板块）
2. dc_member - 板块成分股（带日期维度）
3. dc_daily - 板块日线行情

输出文件：
- dc_index_list.csv - 板块列表
- dc_members/ - 各板块成分股（按日期分目录）
- dc_daily/ - 板块日线行情

使用方法：
    python fetch_dc_data.py
    python fetch_dc_data.py --trade-date 20260424
    python fetch_dc_data.py --fetch-daily --start-date 20250101
"""
import argparse
import sys
import time
from pathlib import Path
from datetime import datetime, timedelta

project_root = Path(__file__).resolve().parent.parent.parent
sys.path.insert(0, str(project_root))

import pandas as pd
from config.config import Config
from data_engine.api.tushare_api import TushareAPI
from utils import setup_logger


def fetch_dc_index(api, trade_date=None, logger=None):
    """获取板块列表"""
    if logger:
        logger.info("获取板块列表（dc_index）...")
        if trade_date:
            logger.info(f"  指定日期: {trade_date}")
        else:
            logger.info("  使用最新日期")

    try:
        if trade_date:
            df = api.call_api('dc_index', trade_date=trade_date)
        else:
            df = api.call_api('dc_index')
        # 清理板块代码
        df['ts_code'] = df['ts_code'].str.replace('.DC', '', regex=False)

        if logger:
            logger.info(f"  获取到 {len(df)} 个板块")
            if 'idx_type' in df.columns:
                logger.info(f"  类型分布: {df['idx_type'].value_counts().to_dict()}")

        return df
    except Exception as e:
        logger.error(f"  获取失败: {e}")
        return None


def fetch_dc_members(api, ts_code, start_date=None, end_date=None, logger=None):
    """获取板块成分股（支持日期范围）"""
    try:
        params = {'ts_code': f"{ts_code}.DC"}
        if start_date:
            params['start_date'] = start_date
        if end_date:
            params['end_date'] = end_date
        df = api.call_api('dc_member', **params)
        return df
    except Exception as e:
        if logger:
            logger.warning(f"  获取 {ts_code} 成分股失败: {e}")
        return None


def fetch_dc_daily_range(api, start_date, end_date, logger=None):
    """获取板块日线行情（日期范围，单次最多约6000条）"""
    try:
        df = api.call_api('dc_daily', start_date=start_date, end_date=end_date)
        return df
    except Exception as e:
        if logger:
            logger.warning(f"  获取 {start_date}-{end_date} 日线失败: {e}")
        return None


def main():
    parser = argparse.ArgumentParser(description='获取东方财富板块数据')
    parser.add_argument('--trade-date', type=str, help='指定交易日（默认最新）')
    parser.add_argument('--start-date', type=str, default='20250101', help='日线开始日期')
    parser.add_argument('--end-date', type=str, help='日线结束日期（默认今天）')
    parser.add_argument('--fetch-members', action='store_true', help='获取成分股')
    parser.add_argument('--fetch-daily', action='store_true', help='获取日线行情')
    parser.add_argument('--limit', type=int, default=100, help='限制板块数量（默认100）')
    args = parser.parse_args()

    end_date = args.end_date or datetime.now().strftime('%Y%m%d')
    logger = setup_logger(prefix="dc_data")

    logger.info("=" * 60)
    logger.info("东方财富板块数据获取")
    logger.info("=" * 60)

    api = TushareAPI()
    output_dir = Config.SUPPLEMENTARY_DATA_DIR / 'dc'
    output_dir.mkdir(parents=True, exist_ok=True)

    # 1. 获取板块列表
    index_df = fetch_dc_index(api, trade_date=args.trade_date, logger=logger)
    if index_df is None:
        return

    # 保存板块列表
    index_file = output_dir / 'dc_index_list.csv'
    index_df.to_csv(index_file, index=False, encoding='utf-8-sig')
    logger.info(f"板块列表已保存: {index_file}")

    # 限制数量
    if args.limit and len(index_df) > args.limit:
        logger.info(f"限制处理前 {args.limit} 个板块")
        index_df = index_df.head(args.limit)

    # 2. 获取成分股
    if args.fetch_members:
        logger.info("\n获取板块成分股（dc_member）...")
        members_dir = output_dir / 'members'
        members_dir.mkdir(exist_ok=True)

        for idx, row in index_df.iterrows():
            ts_code = row['ts_code']
            name = row['name']

            if idx % 10 == 0:
                logger.info(f"  进度: {idx}/{len(index_df)} - {ts_code} {name}")

            members = fetch_dc_members(api, ts_code, logger=logger)
            if members is not None and not members.empty:
                # 添加板块信息
                members['concept_code'] = ts_code
                members['concept_name'] = name

                # 按日期保存
                for trade_date, group in members.groupby('trade_date'):
                    date_dir = members_dir / str(trade_date)
                    date_dir.mkdir(exist_ok=True)
                    file_path = date_dir / f"{ts_code}.csv"
                    group.to_csv(file_path, index=False, encoding='utf-8-sig')

            # 限流
            if idx % 50 == 49:
                time.sleep(1)

        logger.info(f"成分股数据已保存到: {members_dir}")

    # 3. 获取日线行情（按日期范围循环获取）
    if args.fetch_daily:
        logger.info("\n获取板块日线行情（dc_daily）...")
        logger.info(f"  日期范围: {args.start_date} 至 {end_date}")
        logger.info("  使用日期范围分批获取（单次最多约6天数据）")

        daily_dir = output_dir / 'daily'
        daily_dir.mkdir(exist_ok=True)

        # 分批获取：每次5天（约5000条，留余量）
        batch_days = 5
        current = datetime.strptime(args.start_date, '%Y%m%d')
        end = datetime.strptime(end_date, '%Y%m%d')

        all_data = []
        batch_num = 0

        while current <= end:
            batch_start = current.strftime('%Y%m%d')
            batch_end_dt = min(current + timedelta(days=batch_days-1), end)
            batch_end = batch_end_dt.strftime('%Y%m%d')

            batch_num += 1
            logger.info(f"  批次 {batch_num}: {batch_start} - {batch_end}")

            daily = fetch_dc_daily_range(api, batch_start, batch_end, logger=logger)
            if daily is not None and not daily.empty:
                # 清理板块代码
                daily['ts_code'] = daily['ts_code'].str.replace('.DC', '', regex=False)
                all_data.append(daily)
                logger.info(f"    获取 {len(daily)} 条记录")

                # 按日期分文件保存
                for trade_date, group in daily.groupby('trade_date'):
                    file_path = daily_dir / f"{trade_date}.csv"
                    group.to_csv(file_path, index=False, encoding='utf-8-sig')
            else:
                logger.warning(f"    无数据")

            # 移动到下一批次
            current = batch_end_dt + timedelta(days=1)

            # 限流
            if batch_num % 10 == 0:
                time.sleep(1)

        logger.info(f"日线数据已保存到: {daily_dir}")
        if all_data:
            total = sum(len(d) for d in all_data)
            logger.info(f"  总计: {total} 条记录")

    logger.info("\n" + "=" * 60)
    logger.info("完成!")
    logger.info("=" * 60)


if __name__ == '__main__':
    main()
