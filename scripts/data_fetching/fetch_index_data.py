#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
获取指数数据

下载沪深300、上证50、中证1000的日线数据
"""
import sys
from pathlib import Path

# 添加项目根目录到 Python 路径
project_root = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(project_root))

import pandas as pd
from config.config import Config
from data_engine.api.tushare_api import TushareAPI
from data_engine.utils import setup_logger
import time


def fetch_index_data(api: TushareAPI, ts_code: str, start_date: str = '20150101'):
    """
    获取指数日线数据

    Args:
        api: TushareAPI 实例
        ts_code: 指数代码
        start_date: 起始日期

    Returns:
        DataFrame: 指数日线数据
    """
    logger = setup_logger()
    logger.info(f"获取 {ts_code} 的日线数据（从 {start_date} 开始）...")

    try:
        # 调用 Tushare API
        df = api.fetch_index_daily(ts_code=ts_code, start_date=start_date)

        if df is None or df.empty:
            logger.warning(f"  未获取到数据")
            return pd.DataFrame()

        logger.info(f"  获取到 {len(df)} 条记录")

        # 按日期排序
        df = df.sort_values('trade_date')

        return df

    except Exception as e:
        logger.error(f"  获取失败: {e}")
        return pd.DataFrame()


def main():
    """主函数"""
    logger = setup_logger()

    logger.info("=" * 60)
    logger.info("获取指数数据")
    logger.info("=" * 60)

    # 初始化 API
    api = TushareAPI()

    # 指数列表
    indices = {
        '沪深300': '000300.SH',
        '上证50': '000016.SH',
        '中证1000': '000852.SH'
    }

    # 确保输出目录存在
    output_dir = Config.SUPPLEMENTARY_DATA_DIR
    output_dir.mkdir(parents=True, exist_ok=True)

    # 下载每个指数
    for name, ts_code in indices.items():
        logger.info(f"\n处理 {name} ({ts_code})...")

        # 获取数据
        df = fetch_index_data(api, ts_code, start_date='20150101')

        if not df.empty:
            # 保存到 supplementary 目录
            output_file = output_dir / f'{ts_code}.csv'
            df.to_csv(output_file, index=False)
            logger.info(f"  已保存: {output_file}")
        else:
            logger.warning(f"  跳过 {name}（无数据）")

        # 避免请求过快
        time.sleep(0.5)

    logger.info("\n" + "=" * 60)
    logger.info("指数数据获取完成")
    logger.info("=" * 60)
    logger.info(f"输出目录: {output_dir}")


if __name__ == '__main__':
    main()
