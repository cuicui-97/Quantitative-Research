"""
ST 状态数据获取脚本
获取股票 ST 状态历史数据并保存为 CSV
"""
import sys
from pathlib import Path

# 添加项目根目录到 Python 路径
project_root = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(project_root))

import argparse
from datetime import datetime

from config.config import Config
from src.api.tushare_api import TushareAPI
from src.fetchers.st_fetcher import STFetcher
from src.utils import setup_logger


def parse_args():
    """解析命令行参数"""
    parser = argparse.ArgumentParser(description='获取 ST 状态历史数据')
    parser.add_argument(
        '--start-date',
        type=str,
        default='20160101',
        help='开始日期 (YYYYMMDD)，默认 20160101（stock_st API 数据起始日期）'
    )
    parser.add_argument(
        '--end-date',
        type=str,
        help='结束日期 (YYYYMMDD)，默认为当前日期'
    )
    parser.add_argument(
        '--force-refresh',
        action='store_true',
        help='强制刷新数据（忽略已有文件）'
    )
    return parser.parse_args()


def main():
    """主函数"""
    args = parse_args()

    # 设置日志
    logger = setup_logger()
    logger.info("=" * 60)
    logger.info("ST 状态数据获取脚本启动")
    logger.info("=" * 60)

    # 确保目录存在
    Config.SUPPLEMENTARY_DATA_DIR.mkdir(parents=True, exist_ok=True)

    try:
        # 确定日期范围
        start_date = args.start_date
        end_date = args.end_date or datetime.now().strftime('%Y%m%d')

        # stock_st API 数据从 20160101 开始
        if start_date < '20160101':
            logger.warning(f"注意: stock_st API 数据从 20160101 开始，起始日期已调整为 20160101")
            start_date = '20160101'

        logger.info(f"日期范围: {start_date} ~ {end_date}")

        # 初始化 Tushare API
        logger.info("初始化 Tushare API...")
        api = TushareAPI(Config.TUSHARE_TOKEN, Config.TUSHARE_API_URL)

        # 获取 ST 状态数据
        logger.info("开始获取 ST 状态数据...")
        st_fetcher = STFetcher(api)
        df_st = st_fetcher.fetch_st_status(start_date, end_date, args.force_refresh)

        # 输出统计信息
        logger.info("\n" + "=" * 60)
        logger.info("ST 状态数据获取完成")
        logger.info("=" * 60)
        logger.info(f"输出文件: {Config.SUPPLEMENTARY_DATA_DIR / 'st_status.csv'}")
        logger.info(f"记录数: {len(df_st)}")
        if len(df_st) > 0:
            logger.info(f"涉及股票数: {df_st['ts_code'].nunique()}")
            logger.info(f"日期范围: {df_st['trade_date'].min()} ~ {df_st['trade_date'].max()}")
        logger.info("=" * 60)

        return 0

    except Exception as e:
        logger.error(f"获取 ST 状态数据失败: {str(e)}", exc_info=True)
        return 1


if __name__ == '__main__':
    sys.exit(main())
