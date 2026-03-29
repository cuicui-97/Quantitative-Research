"""
Fetcher 基类

提供所有 Fetcher 的通用初始化逻辑
"""
from abc import ABC
import logging
from pathlib import Path

from config.config import Config
from data_engine.api.tushare_api import TushareAPI
from utils.trade_calendar import TradeCalendar


class BaseFetcher(ABC):
    """所有 Fetcher 的基类"""

    def __init__(self, api: TushareAPI, use_output_dir: bool = False):
        """
        初始化 Fetcher

        Args:
            api: TushareAPI 实例
            use_output_dir: 是否需要 output_dir 和 trade_calendar
                           - True: 创建 output_dir 和 trade_calendar（用于需要保存文件的 Fetcher）
                           - False: 只初始化 api 和 logger（用于只返回数据的 Fetcher）
        """
        self.api = api
        self.logger = logging.getLogger(self.__class__.__name__)

        if use_output_dir:
            self.output_dir = Config.SUPPLEMENTARY_DATA_DIR
            self.output_dir.mkdir(parents=True, exist_ok=True)
            self.trade_calendar = TradeCalendar(api)
