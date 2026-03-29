"""
构建交易日历缓存文件
从日线数据中提取所有交易日并保存到本地缓存
"""
import sys
from pathlib import Path

# 添加项目根目录到 Python 路径
project_root = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(project_root))

import os
import pandas as pd
from dotenv import load_dotenv
from config.config import Config
from data_engine.api.tushare_api import TushareAPI
from data_engine.utils.trade_calendar import TradeCalendar
from data_engine.utils import setup_logger

# 加载环境变量
load_dotenv()


def main():
    """构建交易日历缓存"""
    logger = setup_logger()
    logger.info("=" * 60)
    logger.info("交易日历缓存构建脚本启动")
    logger.info("=" * 60)

    # 初始化客户端
    token = os.getenv('TUSHARE_TOKEN')
    api_url = os.getenv('TUSHARE_API_URL')

    if not token or not api_url:
        logger.error("请在 .env 文件中配置 TUSHARE_TOKEN 和 TUSHARE_API_URL")
        return 1

    api = TushareAPI(token, api_url)
    calendar = TradeCalendar(api)

    # 从日线数据中提取所有交易日
    logger.info("\n步骤 1: 从日线数据中提取交易日")
    daily_dir = Config.DAILY_DATA_DIR

    if not daily_dir.exists():
        logger.error(f"日线数据目录不存在: {daily_dir}")
        logger.error("请先运行: python scripts/fetch_daily_data.py")
        return 1

    # 读取所有日线文件的日期
    dates = set()
    daily_files = list(daily_dir.glob('*.csv'))

    if not daily_files:
        logger.error(f"日线数据目录为空: {daily_dir}")
        return 1

    logger.info(f"找到 {len(daily_files)} 个日线文件")
    logger.info("正在提取交易日...")

    # 采样读取（100个文件足够覆盖所有交易日）
    import random
    sample_size = min(100, len(daily_files))
    sample_files = random.sample(daily_files, sample_size)

    for i, file in enumerate(sample_files, 1):
        if i % 20 == 0 or i == 1:
            logger.info(f"  [{i}/{sample_size}] 读取 {file.name}")
        try:
            df = pd.read_csv(file, usecols=['trade_date'], dtype={'trade_date': str})
            dates.update(df['trade_date'].tolist())
        except Exception as e:
            logger.warning(f"读取文件失败 {file.name}: {e}")
            continue

    dates_sorted = sorted(dates)
    logger.info(f"提取到 {len(dates_sorted)} 个交易日")
    logger.info(f"日期范围: {dates_sorted[0]} ~ {dates_sorted[-1]}")

    # 构造交易日历 DataFrame
    logger.info("\n步骤 2: 构建交易日历数据")

    # 生成完整的日期范围（包括非交易日）
    from datetime import datetime, timedelta

    start_date = datetime.strptime(dates_sorted[0], '%Y%m%d')
    end_date = datetime.strptime(dates_sorted[-1], '%Y%m%d')

    all_dates = []
    current = start_date
    while current <= end_date:
        all_dates.append(current.strftime('%Y%m%d'))
        current += timedelta(days=1)

    logger.info(f"完整日期范围: {len(all_dates)} 天（包括非交易日）")

    # 构造 DataFrame
    df_calendar = pd.DataFrame({
        'cal_date': all_dates,
        'is_open': [1 if d in dates else 0 for d in all_dates],
        'exchange': [''] * len(all_dates)
    })

    # 保存到文件
    logger.info("\n步骤 3: 保存交易日历缓存")
    output_file = Config.SUPPLEMENTARY_DATA_DIR / 'trade_calendar.csv'
    df_calendar.to_csv(output_file, index=False)

    logger.info(f"交易日历已保存: {output_file}")
    logger.info(f"总记录数: {len(df_calendar)}")
    logger.info(f"交易日数: {df_calendar['is_open'].sum()}")
    logger.info(f"非交易日数: {len(df_calendar) - df_calendar['is_open'].sum()}")

    # 验证
    logger.info("\n步骤 4: 验证缓存文件")
    calendar_new = TradeCalendar(api)
    test_dates = calendar_new.get_trade_dates(dates_sorted[0], dates_sorted[-1])
    logger.info(f"验证: 读取到 {len(test_dates)} 个交易日")

    if len(test_dates) == len(dates_sorted):
        logger.info("✓ 验证成功！")
    else:
        logger.warning(f"⚠ 验证失败：预期 {len(dates_sorted)} 个，实际 {len(test_dates)} 个")

    logger.info("\n" + "=" * 60)
    logger.info("交易日历缓存构建完成！")
    logger.info("=" * 60)

    return 0


if __name__ == '__main__':
    sys.exit(main())
