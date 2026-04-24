#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
获取历史分钟数据（5分钟K线）

Tushare API: pro_bar
- freq: "5min" 表示5分钟K线
- 需要600积分以上权限

使用方法:
    python scripts/data_fetching/fetch_minute_data.py --ts-code 000001.SZ --start 20240101 --end 20240131
"""
import sys
from pathlib import Path

project_root = Path(__file__).resolve().parent.parent.parent
sys.path.insert(0, str(project_root))

import argparse
import pandas as pd
from datetime import datetime
from config.config import Config
from data_engine.api.tushare_api import TushareAPI
from utils import setup_logger


def fetch_minute_data(api: TushareAPI, ts_code: str, start_date: str, end_date: str, freq: str = '5min'):
    """
    获取单只股票的分钟级历史数据

    Args:
        api: TushareAPI实例
        ts_code: 股票代码，如'000001.SZ'
        start_date: 开始日期，格式YYYYMMDD
        end_date: 结束日期，格式YYYYMMDD
        freq: 频率，'1min','5min','15min','30min','60min'

    Returns:
        DataFrame: 分钟K线数据
    """
    logger = setup_logger(prefix="minute_data")

    # 转换日期格式为datetime
    start_dt = f"{start_date[:4]}-{start_date[4:6]}-{start_date[6:]} 09:30:00"
    end_dt = f"{end_date[:4]}-{end_date[4:6]}-{end_date[6:]} 15:00:00"

    logger.info(f"获取 {ts_code} 的{freq}数据 ({start_date} ~ {end_date})")

    try:
        # 使用stk_mins API获取分钟数据（需单独开权限）
        df = api.call_api('stk_mins',
                         ts_code=ts_code,
                         freq=freq,
                         start_date=start_dt,
                         end_date=end_dt)

        if df is not None and len(df) > 0:
            logger.info(f"  获取到 {len(df)} 条记录")
            return df
        else:
            logger.warning(f"  无数据返回")
            return None

    except Exception as e:
        logger.error(f"  获取失败: {e}")
        return None


def main():
    parser = argparse.ArgumentParser(description='获取历史分钟数据')
    parser.add_argument('--ts-code', type=str, default='000001.SZ',
                       help='股票代码，如000001.SZ')
    parser.add_argument('--start', type=str, default='20240101',
                       help='开始日期，格式YYYYMMDD')
    parser.add_argument('--end', type=str, default='20240131',
                       help='结束日期，格式YYYYMMDD')
    parser.add_argument('--freq', type=str, default='5min',
                       choices=['1min', '5min', '15min', '30min', '60min'],
                       help='数据频率')
    args = parser.parse_args()

    logger = setup_logger(prefix="minute_data")
    logger.info("="*60)
    logger.info("历史分钟数据获取")
    logger.info("="*60)

    # 初始化API
    api = TushareAPI()

    # 获取数据
    df = fetch_minute_data(api, args.ts_code, args.start, args.end, args.freq)

    if df is not None:
        logger.info("\n数据预览:")
        print(df.head(10).to_string())

        # 保存到文件
        output_dir = Config.DATA_DIR / 'minute_data'
        output_dir.mkdir(parents=True, exist_ok=True)

        output_file = output_dir / f"{args.ts_code.replace('.', '_')}_{args.freq}_{args.start}_{args.end}.csv"
        df.to_csv(output_file, index=False, encoding='utf-8-sig')
        logger.info(f"\n数据已保存: {output_file}")
    else:
        logger.error("\n获取数据失败，可能原因:")
        logger.error("  1. 积分不足（需要600积分以上）")
        logger.error("  2. 超过试用次数限制")
        logger.error("  3. 日期范围过大")


if __name__ == '__main__':
    main()
