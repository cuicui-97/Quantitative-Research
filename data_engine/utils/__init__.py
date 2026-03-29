"""
统一的工具函数模块

所有工具函数通过此模块导出，保持向后兼容
"""
from .retry import retry_on_error
from .logger import setup_logger
from .date import get_trade_dates, get_all_stocks, format_date_range
from .data import (
    build_st_dict,
    build_suspension_dict,
    merge_dicts,
    filter_dict_by_dates,
    get_stocks_on_date,
    is_stock_in_dict,
    dict_statistics
)
from .file import ensure_directory
from .trade_calendar import TradeCalendar

__all__ = [
    # 重试和日志
    'retry_on_error',
    'setup_logger',
    # 日期工具
    'get_trade_dates',
    'get_all_stocks',
    'format_date_range',
    'TradeCalendar',
    # 数据处理
    'build_st_dict',
    'build_suspension_dict',
    'merge_dicts',
    'filter_dict_by_dates',
    'get_stocks_on_date',
    'is_stock_in_dict',
    'dict_statistics',
    # 文件操作
    'ensure_directory',
]
