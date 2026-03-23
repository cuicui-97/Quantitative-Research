"""
Tushare 客户端封装模块
提供统一的 Tushare Pro API 调用接口
"""
import time
import logging
import tushare as ts
from config.config import Config


class TushareClient:
    """
    Tushare Pro API 客户端封装

    特性：
    - 强制注入 TOKEN 和 API_URL（访问私有网关的关键）
    - 自动速率控制（防止频率限制）
    - 统一异常处理
    - 详细日志记录
    """

    def __init__(self, token=None, api_url=None):
        """
        初始化 Tushare 客户端

        Args:
            token: Tushare TOKEN（默认使用配置）
            api_url: API URL（默认使用配置）
        """
        self.token = token or Config.TUSHARE_TOKEN
        self.api_url = api_url or Config.TUSHARE_API_URL
        self.last_request_time = 0
        self.logger = logging.getLogger(__name__)

        # 初始化 Tushare Pro API
        self.pro = ts.pro_api(self.token)

        # ==================== 核心注入：必须保证以下赋值 ====================
        # 这两行代码是访问私有网关的关键，缺少会导致无法获取数据
        self.pro._DataApi__token = self.token
        self.pro._DataApi__http_url = self.api_url
        # ================================================================

        self.logger.info(
            f"Tushare 客户端初始化成功 (API: {self.api_url})"
        )

    def _rate_limit(self):
        """
        速率限制控制

        确保每次 API 调用之间至少间隔 REQUEST_INTERVAL 秒
        """
        elapsed = time.time() - self.last_request_time
        if elapsed < Config.REQUEST_INTERVAL:
            sleep_time = Config.REQUEST_INTERVAL - elapsed
            time.sleep(sleep_time)
        self.last_request_time = time.time()

    def call_api(self, api_name, **kwargs):
        """
        统一 API 调用接口

        Args:
            api_name: API 方法名（如 'stock_basic', 'daily' 等）
            **kwargs: API 参数

        Returns:
            pandas.DataFrame: API 返回的数据

        Raises:
            Exception: API 调用失败时抛出异常
        """
        # 速率控制
        self._rate_limit()

        try:
            # 获取 API 方法
            api_func = getattr(self.pro, api_name)

            # 调用 API
            result = api_func(**kwargs)

            # 记录成功日志
            self.logger.debug(
                f"API 调用成功: {api_name}, 参数: {kwargs}, "
                f"返回行数: {len(result) if result is not None else 0}"
            )

            return result

        except Exception as e:
            # 记录失败日志
            self.logger.error(
                f"API 调用失败: {api_name}, 参数: {kwargs}, 错误: {e}"
            )
            raise

    def test_connection(self):
        """
        测试连接是否正常

        Returns:
            bool: 连接正常返回 True，否则返回 False
        """
        try:
            # 尝试获取股票基础信息（只获取 1 条）
            df = self.call_api('stock_basic', list_status='L', limit=1)
            if df is not None and len(df) > 0:
                self.logger.info("连接测试成功")
                return True
            else:
                self.logger.warning("连接测试失败：返回数据为空")
                return False
        except Exception as e:
            self.logger.error(f"连接测试失败: {e}")
            return False
