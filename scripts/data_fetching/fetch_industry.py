#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
获取申万行业成分数据

输出：stockdata/supplementary/industry_members.csv
"""
import sys
from pathlib import Path

project_root = Path(__file__).resolve().parent.parent.parent
sys.path.insert(0, str(project_root))

from config.config import Config
from data_engine.api.tushare_api import TushareAPI
from data_engine.fetchers.industry_fetcher import IndustryFetcher
from utils import setup_logger


def main():
    logger = setup_logger(prefix="fetch")

    api = TushareAPI()
    fetcher = IndustryFetcher(api)

    logger.info("开始获取申万行业成分数据...")
    df = fetcher.fetch_all()

    if df.empty:
        logger.error("获取失败，未保存任何数据")
        return

    logger.info(f"完成。共 {len(df)} 条记录，已保存到 {Config.INDUSTRY_DATA_FILE}")


if __name__ == '__main__':
    main()
