"""
日期相关工具函数
"""
import logging
import pandas as pd
from typing import Optional, List
from datetime import datetime
from config.config import Config


def get_trade_dates(
    start_date: Optional[str] = None,
    end_date: Optional[str] = None,
    api=None
) -> pd.DatetimeIndex:
    """
    获取交易日期（优先级：本地缓存 > API > 日线数据挖掘）

    Args:
        start_date: 开始日期 YYYYMMDD（可选）
        end_date: 结束日期 YYYYMMDD（可选）
        api: TushareAPI 实例（可选，用于 API 调用）

    Returns:
        pd.DatetimeIndex: 交易日期索引（升序排列）
    """
    logger = logging.getLogger(__name__)

    # 方法 1: 从本地交易日历缓存读取（最快）
    trade_cal_file = Config.SUPPLEMENTARY_DATA_DIR / 'trade_calendar.csv'
    if trade_cal_file.exists():
        logger.info(f"从本地交易日历缓存读取: {trade_cal_file}")
        try:
            df_cal = pd.read_csv(trade_cal_file, dtype={'cal_date': str})
            # 过滤交易日（is_open == 1）
            trade_dates = df_cal[df_cal['is_open'] == 1]['cal_date'].tolist()

            # 转换为 datetime
            dates = pd.to_datetime(trade_dates, format='%Y%m%d')

            # 过滤日期范围
            if start_date:
                start_dt = pd.to_datetime(start_date, format='%Y%m%d')
                dates = dates[dates >= start_dt]
            if end_date:
                end_dt = pd.to_datetime(end_date, format='%Y%m%d')
                dates = dates[dates <= end_dt]

            dates = dates.sort_values()
            logger.info(f"从本地缓存获取到 {len(dates)} 个交易日")
            logger.info(f"日期范围: {dates.min()} ~ {dates.max()}")
            return dates

        except Exception as e:
            logger.warning(f"读取本地交易日历失败: {e}，尝试其他方法")

    # 方法 2: 调用 API 获取（需要提供 api 参数）
    if api is not None:
        logger.info("从 Tushare API 获取交易日历...")
        try:
            from data_engine.trade_calendar import TradeCalendar

            trade_calendar = TradeCalendar(api)
            dates_list = trade_calendar.get_trade_dates(
                start_date or '19900101',
                end_date or datetime.now().strftime('%Y%m%d')
            )

            dates = pd.to_datetime(dates_list, format='%Y%m%d')
            logger.info(f"从 API 获取到 {len(dates)} 个交易日")
            logger.info(f"日期范围: {dates.min()} ~ {dates.max()}")
            return dates

        except Exception as e:
            logger.warning(f"API 获取交易日历失败: {e}，尝试其他方法")

    # 方法 3: 从日线数据中挖掘（最后的备选方案）
    logger.info("从日线数据中挖掘交易日期...")
    daily_dir = Config.DAILY_DATA_DIR
    if not daily_dir.exists():
        logger.error(f"日线数据目录不存在: {daily_dir}")
        raise FileNotFoundError(f"无法获取交易日期，日线数据目录不存在: {daily_dir}")

    # 从前 50 只股票中提取日期（足够覆盖所有交易日）
    all_dates = set()
    csv_files = list(daily_dir.glob('*.csv'))

    if not csv_files:
        logger.error(f"日线数据目录为空: {daily_dir}")
        raise FileNotFoundError(f"无法获取交易日期，日线数据目录为空: {daily_dir}")

    for csv_file in csv_files[:50]:
        try:
            df = pd.read_csv(csv_file, dtype={'trade_date': str})
            if 'trade_date' in df.columns:
                all_dates.update(df['trade_date'].tolist())
        except Exception as e:
            logger.debug(f"读取 {csv_file.name} 失败: {e}")
            continue

    if not all_dates:
        logger.error("无法从日线数据中提取交易日期")
        raise ValueError("无法获取交易日期，所有方法都失败")

    # 转换为 datetime 并排序
    dates = pd.to_datetime(sorted(all_dates), format='%Y%m%d')

    # 过滤日期范围
    if start_date:
        start_dt = pd.to_datetime(start_date, format='%Y%m%d')
        dates = dates[dates >= start_dt]
    if end_date:
        end_dt = pd.to_datetime(end_date, format='%Y%m%d')
        dates = dates[dates <= end_dt]

    logger.info(f"从日线数据挖掘到 {len(dates)} 个交易日")
    logger.info(f"日期范围: {dates.min()} ~ {dates.max()}")

    return dates


def get_all_stocks(list_status: str = 'L') -> List[str]:
    """
    获取所有股票代码

    Args:
        list_status: 上市状态
            - 'L': 上市
            - 'D': 退市
            - 'P': 暂停上市
            - 'ALL': 所有股票（包括上市、退市、暂停上市）

    Returns:
        List[str]: 股票代码列表
    """
    logger = logging.getLogger(__name__)

    basic_file = Config.BASIC_DATA_DIR / 'all_companies_info.csv'
    if not basic_file.exists():
        logger.error(f"基础数据文件不存在: {basic_file}")
        raise FileNotFoundError(f"基础数据文件不存在: {basic_file}")

    df = pd.read_csv(basic_file)

    if list_status == 'ALL':
        # 获取所有股票（包括上市、退市、暂停上市）
        stocks = df['ts_code'].tolist()
        logger.info(f"获取到 {len(stocks)} 只股票（所有状态）")
    else:
        # 获取指定状态的股票
        stocks = df[df['list_status'] == list_status]['ts_code'].tolist()
        logger.info(f"获取到 {len(stocks)} 只股票（list_status={list_status}）")

    return stocks


def format_date_range(dates: pd.DatetimeIndex) -> str:
    """
    格式化日期范围为字符串

    Args:
        dates: 日期索引

    Returns:
        str: 格式化的日期范围字符串
    """
    if len(dates) == 0:
        return "无日期"

    return f"{dates.min().strftime('%Y-%m-%d')} ~ {dates.max().strftime('%Y-%m-%d')} (共 {len(dates)} 个交易日)"
