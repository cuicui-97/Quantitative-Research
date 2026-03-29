"""
停牌信息获取脚本
获取股票停牌信息并保存为 CSV
"""
import sys
from pathlib import Path

# 添加项目根目录到 Python 路径
project_root = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(project_root))

import argparse
from datetime import datetime

from config.config import Config
from data_engine.api.tushare_api import TushareAPI
from data_engine.fetchers.suspension_fetcher import SuspensionFetcher
from data_engine.utils import setup_logger


def parse_args():
    """解析命令行参数"""
    parser = argparse.ArgumentParser(description='获取停牌信息')
    parser.add_argument(
        '--start-date',
        type=str,
        default='20100101',
        help='开始日期 (YYYYMMDD)，默认 20000101'
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
    logger.info("停牌信息获取脚本启动")
    logger.info("=" * 60)

    # 确保目录存在
    Config.SUPPLEMENTARY_DATA_DIR.mkdir(parents=True, exist_ok=True)

    try:
        # 确定日期范围
        start_date = args.start_date
        end_date = args.end_date or datetime.now().strftime('%Y%m%d')
        logger.info(f"日期范围: {start_date} ~ {end_date}")

        # 初始化 Tushare 客户端
        logger.info("初始化 Tushare 客户端...")
        api = TushareAPI(Config.TUSHARE_TOKEN, Config.TUSHARE_API_URL)

        # 获取停牌信息
        logger.info("开始获取停牌信息...")
        suspension_fetcher = SuspensionFetcher(api)
        df_suspend = suspension_fetcher.fetch_suspension_status(start_date, end_date, args.force_refresh)

        # 输出统计信息
        logger.info("\n" + "=" * 60)
        logger.info("停牌信息获取完成")
        logger.info("=" * 60)
        logger.info(f"输出文件: {Config.SUPPLEMENTARY_DATA_DIR / 'suspension_status.csv'}")
        logger.info(f"记录数: {len(df_suspend)}")
        if len(df_suspend) > 0:
            logger.info(f"涉及股票数: {df_suspend['ts_code'].nunique()}")
            logger.info(f"日期范围: {df_suspend['trade_date'].min()} ~ {df_suspend['trade_date'].max()}")
        logger.info("=" * 60)

        return 0

    except Exception as e:
        logger.error(f"获取停牌信息失败: {str(e)}", exc_info=True)
        return 1


if __name__ == '__main__':
    sys.exit(main())
