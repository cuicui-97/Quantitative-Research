"""
交易日历工具模块
提供交易日查询、日期范围生成等通用功能
"""
import logging
import pandas as pd
from datetime import datetime, timedelta
from typing import List, Optional
from pathlib import Path

from data_engine.api.tushare_api import TushareAPI
from config.config import Config

logger = logging.getLogger(__name__)


class TradeCalendar:
    """交易日历管理类"""

    def __init__(self, api: TushareAPI):
        """
        初始化交易日历

        Args:
            api: Tushare API 实例
        """
        self.api = api
        self._cache = {}  # 内存缓存
        self.cache_file = Config.SUPPLEMENTARY_DATA_DIR / 'trade_calendar.csv'

        # 确保目录存在
        Config.SUPPLEMENTARY_DATA_DIR.mkdir(parents=True, exist_ok=True)

        # 加载本地缓存
        self._load_local_cache()

    def get_trade_dates(self, start_date: str, end_date: str, exchange: str = '') -> List[str]:
        """
        获取指定日期范围内的交易日列表

        优先级：内存缓存 > 本地文件 > trade_cal API > 日线数据提取

        Args:
            start_date: 开始日期 YYYYMMDD
            end_date: 结束日期 YYYYMMDD
            exchange: 交易所代码（SSE-上交所, SZSE-深交所, 空字符串-全部）

        Returns:
            交易日列表（排序后的 YYYYMMDD 字符串列表）
        """
        # 1. 检查内存缓存
        cache_key = f"{start_date}_{end_date}_{exchange}"
        if cache_key in self._cache:
            logger.debug(f"从内存缓存读取交易日: {cache_key}")
            return self._cache[cache_key]

        # 2. 检查本地文件缓存
        local_dates = self._get_from_local_cache(start_date, end_date, exchange)
        if local_dates:
            logger.info(f"从本地文件读取到 {len(local_dates)} 个交易日")
            self._cache[cache_key] = local_dates
            return local_dates

        logger.info(f"获取交易日历: {start_date} ~ {end_date}")

        try:
            # 3. 尝试调用 trade_cal API
            df = self.api.fetch_trade_cal(
                exchange=exchange,
                start_date=start_date,
                end_date=end_date,
                is_open='1'
            )

            if df is not None and len(df) > 0:
                # 筛选交易日（is_open == 1）
                trade_dates = df[df['is_open'] == 1]['cal_date'].astype(str).tolist()
                trade_dates_sorted = sorted(trade_dates)

                # 保存到本地缓存
                self._save_to_local_cache(df, exchange)

                # 缓存结果
                self._cache[cache_key] = trade_dates_sorted

                logger.info(f"获取到 {len(trade_dates_sorted)} 个交易日（来自 trade_cal API）")
                return trade_dates_sorted
            else:
                logger.warning(f"trade_cal API 返回空数据，尝试从日线数据提取")

        except Exception as e:
            logger.warning(f"trade_cal API 调用失败: {e}，尝试从日线数据提取")

        # 4. 降级方案 - 从日线数据文件中提取交易日
        trade_dates = self._extract_trade_dates_from_daily_data(start_date, end_date)

        if trade_dates:
            # 保存到本地缓存（构造 DataFrame）
            df_calendar = pd.DataFrame({
                'cal_date': trade_dates,
                'is_open': [1] * len(trade_dates),
                'exchange': [exchange] * len(trade_dates)
            })
            self._save_to_local_cache(df_calendar, exchange)

            # 缓存结果
            self._cache[cache_key] = trade_dates
            logger.info(f"获取到 {len(trade_dates)} 个交易日（来自日线数据）")
            return trade_dates

        logger.error(f"无法获取交易日历: {start_date} ~ {end_date}")
        return []

    def _extract_trade_dates_from_daily_data(self, start_date: str, end_date: str) -> List[str]:
        """
        从日线数据文件中提取交易日

        Args:
            start_date: 开始日期 YYYYMMDD
            end_date: 结束日期 YYYYMMDD

        Returns:
            交易日列表
        """
        from config.config import Config

        daily_dir = Config.DAILY_DATA_DIR

        if not daily_dir.exists():
            logger.warning(f"日线数据目录不存在: {daily_dir}")
            return []

        # 从所有日线文件中提取交易日
        dates = set()
        daily_files = list(daily_dir.glob('*.csv'))

        if not daily_files:
            logger.warning(f"日线数据目录为空: {daily_dir}")
            return []

        # 采样 100 个文件（足够覆盖所有交易日）
        import random
        sample_size = min(100, len(daily_files))
        sample_files = random.sample(daily_files, sample_size)

        logger.info(f"从 {sample_size} 个日线文件中提取交易日...")

        for file in sample_files:
            try:
                df = pd.read_csv(file, usecols=['trade_date'], dtype={'trade_date': str})
                dates.update(df['trade_date'].tolist())
            except Exception as e:
                logger.debug(f"读取文件失败 {file.name}: {e}")
                continue

        # 筛选日期范围
        dates_filtered = sorted([d for d in dates if start_date <= d <= end_date])

        return dates_filtered

    def is_trade_date(self, date: str, exchange: str = '') -> bool:
        """
        判断指定日期是否为交易日

        Args:
            date: 日期 YYYYMMDD
            exchange: 交易所代码

        Returns:
            是否为交易日
        """
        # 获取当天的交易日历
        trade_dates = self.get_trade_dates(date, date, exchange)
        return date in trade_dates

    def get_previous_trade_date(self, date: str, n: int = 1, exchange: str = '') -> Optional[str]:
        """
        获取指定日期之前的第 n 个交易日

        Args:
            date: 基准日期 YYYYMMDD
            n: 往前推 n 个交易日
            exchange: 交易所代码

        Returns:
            交易日 YYYYMMDD，如果不存在则返回 None
        """
        # 往前推 n*2 天作为查询范围（保守估计）
        start = (datetime.strptime(date, '%Y%m%d') - timedelta(days=n * 2)).strftime('%Y%m%d')
        trade_dates = self.get_trade_dates(start, date, exchange)

        # 找到小于指定日期的交易日
        prev_dates = [d for d in trade_dates if d < date]

        if len(prev_dates) >= n:
            return prev_dates[-n]
        else:
            logger.warning(f"无法找到 {date} 之前的第 {n} 个交易日")
            return None

    def get_next_trade_date(self, date: str, n: int = 1, exchange: str = '') -> Optional[str]:
        """
        获取指定日期之后的第 n 个交易日

        Args:
            date: 基准日期 YYYYMMDD
            n: 往后推 n 个交易日
            exchange: 交易所代码

        Returns:
            交易日 YYYYMMDD，如果不存在则返回 None
        """
        # 往后推 n*2 天作为查询范围
        end = (datetime.strptime(date, '%Y%m%d') + timedelta(days=n * 2)).strftime('%Y%m%d')
        trade_dates = self.get_trade_dates(date, end, exchange)

        # 找到大于指定日期的交易日
        next_dates = [d for d in trade_dates if d > date]

        if len(next_dates) >= n:
            return next_dates[n - 1]
        else:
            logger.warning(f"无法找到 {date} 之后的第 {n} 个交易日")
            return None

    def _load_local_cache(self):
        """加载本地缓存文件到内存"""
        if not self.cache_file.exists():
            logger.debug("本地交易日历缓存文件不存在")
            return

        try:
            df = pd.read_csv(self.cache_file, dtype={'cal_date': str, 'exchange': str})
            logger.info(f"加载本地交易日历缓存: {len(df)} 条记录，日期范围 {df['cal_date'].min()} ~ {df['cal_date'].max()}")
        except Exception as e:
            logger.warning(f"加载本地缓存失败: {e}")

    def _get_from_local_cache(self, start_date: str, end_date: str, exchange: str) -> List[str]:
        """
        从本地缓存文件中获取交易日

        Args:
            start_date: 开始日期 YYYYMMDD
            end_date: 结束日期 YYYYMMDD
            exchange: 交易所代码

        Returns:
            交易日列表，如果缓存不完整则返回空列表
        """
        if not self.cache_file.exists():
            return []

        try:
            df = pd.read_csv(self.cache_file, dtype={'cal_date': str, 'exchange': str})

            # 筛选交易所
            if exchange:
                df = df[df['exchange'] == exchange]

            # 筛选日期范围
            df = df[(df['cal_date'] >= start_date) & (df['cal_date'] <= end_date)]

            # 只返回交易日（is_open == 1）
            trade_dates = df[df['is_open'] == 1]['cal_date'].tolist()

            return sorted(trade_dates)

        except Exception as e:
            logger.warning(f"从本地缓存读取失败: {e}")
            return []

    def _save_to_local_cache(self, df_new: pd.DataFrame, exchange: str):
        """
        保存交易日历到本地缓存文件（增量更新）

        Args:
            df_new: 新的交易日历数据
            exchange: 交易所代码
        """
        if df_new is None or len(df_new) == 0:
            return

        try:
            # 确保新数据包含必要字段
            if 'exchange' not in df_new.columns:
                df_new['exchange'] = exchange

            # 如果本地文件存在，合并数据
            if self.cache_file.exists():
                df_existing = pd.read_csv(self.cache_file, dtype={'cal_date': str, 'exchange': str})
                df_combined = pd.concat([df_existing, df_new], ignore_index=True)
                # 去重（保留最新的记录）
                df_combined = df_combined.drop_duplicates(subset=['cal_date', 'exchange'], keep='last')
            else:
                df_combined = df_new

            # 按日期排序
            df_combined = df_combined.sort_values('cal_date')

            # 保存到文件
            df_combined.to_csv(self.cache_file, index=False)
            logger.info(f"交易日历已保存到本地缓存: {len(df_combined)} 条记录")

        except Exception as e:
            logger.warning(f"保存本地缓存失败: {e}")

    def clear_cache(self):
        """清空内存缓存"""
        self._cache.clear()
        logger.info("交易日历内存缓存已清空")

    def clear_local_cache(self):
        """删除本地缓存文件"""
        if self.cache_file.exists():
            self.cache_file.unlink()
            logger.info(f"本地缓存文件已删除: {self.cache_file}")
        self._cache.clear()


def generate_date_range(start_date: str, end_date: str) -> List[str]:
    """
    生成日期范围（包括非交易日）

    用作 trade_cal API 不可用时的降级方案

    Args:
        start_date: 开始日期 YYYYMMDD
        end_date: 结束日期 YYYYMMDD

    Returns:
        日期列表
    """
    start = datetime.strptime(start_date, '%Y%m%d')
    end = datetime.strptime(end_date, '%Y%m%d')

    dates = []
    current = start
    while current <= end:
        dates.append(current.strftime('%Y%m%d'))
        current += timedelta(days=1)

    return dates
