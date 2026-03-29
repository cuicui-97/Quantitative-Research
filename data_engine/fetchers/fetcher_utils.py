"""
Fetcher 通用工具函数和装饰器

提供增量更新和断点续传的装饰器实现
"""
import logging
import time
import pandas as pd
from pathlib import Path
from functools import wraps
from typing import List


def incremental_update(
    output_filename: str,
    date_column: str = 'trade_date',
    unique_keys: List[str] = None
):
    """
    增量更新装饰器

    自动处理增量更新逻辑：检查已有数据，识别缺失范围，调用被装饰函数获取数据

    Args:
        output_filename: 输出文件名（相对于 output_dir）
        date_column: 日期列名（默认 'trade_date'）
        unique_keys: 去重键列表（如果为 None，则使用 [date_column]）

    用法:
        @incremental_update('daily_basic.csv', unique_keys=['ts_code', 'trade_date'])
        def fetch_daily_basic(self, start_date, end_date, force_refresh=False):
            return self._fetch_by_date_range(start_date, end_date)
    """
    if unique_keys is None:
        unique_keys = [date_column]

    def decorator(func):
        @wraps(func)
        def wrapper(self, start_date, end_date, force_refresh=False, **kwargs):
            logger = self.logger
            output_file = self.output_dir / output_filename

            # 如果文件存在且不强制刷新，智能增量更新
            if output_file.exists() and not force_refresh:
                logger.info(f"数据文件已存在，从文件读取: {output_file}")
                existing_df = pd.read_csv(output_file, dtype={date_column: str})

                if len(existing_df) > 0:
                    existing_min_date = existing_df[date_column].min()
                    existing_max_date = existing_df[date_column].max()
                    logger.info(f"现有数据日期范围: {existing_min_date} ~ {existing_max_date}")

                    # 检查是否需要补充数据
                    missing_ranges = []

                    # 1. 检查是否需要补充历史数据
                    if start_date < existing_min_date:
                        missing_ranges.append((start_date, existing_min_date))
                        logger.info(f"需要补充历史数据: {start_date} ~ {existing_min_date}")

                    # 2. 检查是否需要补充最新数据
                    if end_date > existing_max_date:
                        missing_ranges.append((existing_max_date, end_date))
                        logger.info(f"需要补充最新数据: {existing_max_date} ~ {end_date}")

                    # 如果有缺失范围，进行增量更新
                    if missing_ranges:
                        logger.info(f"开始补充缺失数据（共 {len(missing_ranges)} 个范围）")
                        new_dfs = [existing_df]

                        for i, (range_start, range_end) in enumerate(missing_ranges, 1):
                            logger.info(f"[{i}/{len(missing_ranges)}] 获取 {range_start} ~ {range_end} 的数据")
                            new_df = func(self, range_start, range_end, force_refresh=False, **kwargs)
                            if len(new_df) > 0:
                                new_dfs.append(new_df)

                        # 合并所有数据并去重
                        combined_df = pd.concat(new_dfs, ignore_index=True)
                        combined_df = combined_df.drop_duplicates(subset=unique_keys, keep='last')
                        combined_df = combined_df.sort_values(date_column).reset_index(drop=True)

                        # 保存更新后的数据
                        combined_df.to_csv(output_file, index=False)
                        logger.info(f"增量更新完成，总记录数: {len(combined_df)}")
                        logger.info(f"更新后日期范围: {combined_df[date_column].min()} ~ {combined_df[date_column].max()}")
                        return combined_df
                    else:
                        logger.info(f"现有数据已覆盖请求范围 ({start_date} ~ {end_date})，无需更新")
                        return existing_df

                return existing_df

            # 全量获取
            logger.info(f"开始获取数据 ({start_date} ~ {end_date})")
            df = func(self, start_date, end_date, force_refresh=force_refresh, **kwargs)

            # 保存到文件
            if len(df) > 0:
                df.to_csv(output_file, index=False)
                logger.info(f"数据已保存: {output_file}, 共 {len(df)} 条记录")
            else:
                logger.warning(f"未获取到数据")

            return df

        return wrapper
    return decorator


def with_checkpoint(
    temp_filename: str,
    checkpoint_interval: int = 50,
    api_delay: float = 0.3
):
    """
    断点续传装饰器

    自动处理断点续传逻辑：检查临时文件，过滤已获取日期，循环获取剩余数据

    Args:
        temp_filename: 临时文件名（相对于 output_dir）
        checkpoint_interval: 保存间隔（默认 50）
        api_delay: API 调用间隔（秒，默认 0.3）

    用法:
        @with_checkpoint('daily_basic_temp.csv')
        def _fetch_by_date_range(self, start_date, end_date):
            dates = self.trade_calendar.get_trade_dates(start_date, end_date)
            dates_reversed = sorted(dates, reverse=True)

            # 返回 (dates_list, fetch_single_func)
            return dates_reversed, self._fetch_single_date_with_retry
    """
    def decorator(func):
        @wraps(func)
        def wrapper(self, start_date, end_date, **kwargs):
            logger = self.logger
            temp_file = self.output_dir / temp_filename

            # 调用被装饰函数，获取日期列表和单日期获取函数
            result = func(self, start_date, end_date, **kwargs)

            # 支持两种返回格式：
            # 1. (dates_list, fetch_single_func) - 使用断点续传
            # 2. DataFrame - 直接返回数据（不使用断点续传）
            if isinstance(result, tuple) and len(result) == 2:
                dates_list, fetch_single_func = result
            else:
                # 直接返回 DataFrame，不使用断点续传
                return result

            # 检查是否有临时文件（断点续传）
            existing_dates = set()
            if temp_file.exists():
                logger.info(f"发现临时文件，加载已有数据: {temp_file}")
                df_existing = pd.read_csv(temp_file, dtype={'trade_date': str})
                existing_dates = set(df_existing['trade_date'].unique())
                logger.info(f"已有 {len(existing_dates)} 个日期的数据")
                all_records = [df_existing]
            else:
                all_records = []

            # 过滤掉已获取的日期
            remaining_dates = [d for d in dates_list if d not in existing_dates]

            if not remaining_dates:
                logger.info("所有日期的数据已获取完毕")
                if all_records:
                    return pd.concat(all_records, ignore_index=True)
                else:
                    return pd.DataFrame()

            logger.info(f"剩余需要查询 {len(remaining_dates)} 个交易日（从 {remaining_dates[0]} 开始）")

            try:
                for i, date_str in enumerate(remaining_dates, 1):
                    if i % 10 == 0 or i == 1:
                        logger.info(f"[{i}/{len(remaining_dates)}] 获取 {date_str} 的数据")

                    df = fetch_single_func(date_str)

                    if df is not None and len(df) > 0:
                        # 确保包含 trade_date 字段
                        if 'trade_date' not in df.columns:
                            df['trade_date'] = date_str
                        all_records.append(df)
                    else:
                        logger.debug(f"  {date_str} 无数据")

                    # 每获取 N 个日期，保存一次临时文件
                    if i % checkpoint_interval == 0 and all_records:
                        df_temp = pd.concat(all_records, ignore_index=True)
                        df_temp.to_csv(temp_file, index=False)
                        logger.info(f"  ✓ 临时保存: 已获取 {len(existing_dates) + i} 个日期，共 {len(df_temp)} 条记录")
                    elif i % checkpoint_interval == 0:
                        logger.info(f"  ○ 第 {i} 次检查点: 暂无数据需要保存")

                    # API 限流
                    time.sleep(api_delay)

            except KeyboardInterrupt:
                logger.warning("用户中断操作，保存已获取的数据...")
                if all_records:
                    df_partial = pd.concat(all_records, ignore_index=True)
                    df_partial.to_csv(temp_file, index=False)
                    logger.info(f"部分数据已保存到临时文件: {temp_file}")
                    logger.info(f"已获取 {len(df_partial['trade_date'].unique())} 个日期的数据")
                    logger.info("下次运行时将从断点继续")
                    return df_partial
                raise

            except Exception as e:
                logger.error(f"获取数据时发生错误: {e}")
                # 保存已获取的数据
                if all_records:
                    df_partial = pd.concat(all_records, ignore_index=True)
                    df_partial.to_csv(temp_file, index=False)
                    logger.info(f"部分数据已保存到临时文件: {temp_file}")
                    logger.info(f"已获取 {len(df_partial['trade_date'].unique())} 个日期的数据")
                    logger.info("下次运行时将从断点继续")
                    return df_partial
                raise

            if not all_records:
                logger.warning("未获取到任何数据")
                return pd.DataFrame()

            # 合并所有记录
            df_all = pd.concat(all_records, ignore_index=True)
            logger.info(f"共获取 {len(df_all)} 条记录，覆盖 {len(df_all['trade_date'].unique())} 个交易日")

            # 删除临时文件
            if temp_file.exists():
                temp_file.unlink()
                logger.info("临时文件已删除")

            return df_all

        return wrapper
    return decorator
