#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
基础数据抓取脚本

功能：
- 抓取所有 A 股上市公司的基础信息
- 合并多个数据源（stock_basic, stock_company, new_share）
- 保存到单个 CSV 文件

使用方法：
    python scripts/fetch_basic_data.py
"""
import sys
from pathlib import Path

# 添加项目根目录到 Python 路径
project_root = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(project_root))

from config.config import Config
from data_engine.api.tushare_api import TushareAPI
from data_engine.fetchers.basic_fetcher import BasicDataFetcher
from utils import setup_logger


def main():
    """主函数"""
    # 1. 初始化日志系统（配置根logger，让所有模块的日志都能输出）
    logger = setup_logger()
    logger.info("=" * 60)
    logger.info("基础数据抓取脚本启动")
    logger.info("=" * 60)

    try:
        # 2. 确保目录存在
        Config.ensure_dirs()

        # 3. 创建 Tushare API 实例
        logger.info("正在初始化 Tushare API...")
        api = TushareAPI()

        # 4. 测试连接
        logger.info("正在测试连接...")
        if not api.test_connection():
            logger.error("连接测试失败，请检查 TOKEN 和 API_URL 配置")
            return 1

        # 5. 创建基础数据抓取器
        logger.info("正在创建基础数据抓取器...")
        fetcher = BasicDataFetcher(api)

        # 6. 抓取并合并数据
        logger.info("正在抓取基础数据...")
        df = fetcher.merge_all_data()

        # 7. 保存数据
        output_file = Config.BASIC_DATA_FILE
        logger.info(f"正在保存数据到: {output_file}")
        df.to_csv(output_file, index=False, encoding='utf-8-sig')

        # 8. 输出统计信息
        logger.info("=" * 60)
        logger.info("基础数据抓取完成！")
        logger.info(f"文件路径: {output_file}")
        logger.info(f"总记录数: {len(df)}")
        logger.info(f"总字段数: {len(df.columns)}")
        logger.info(f"文件大小: {output_file.stat().st_size / 1024 / 1024:.2f} MB")
        logger.info("=" * 60)

        # 9. 显示前几行数据
        logger.info("\n数据预览（前 5 行）:")
        logger.info(f"\n{df.head()}")

        return 0

    except Exception as e:
        logger.error(f"执行失败: {e}", exc_info=True)
        return 1


if __name__ == '__main__':
    sys.exit(main())
