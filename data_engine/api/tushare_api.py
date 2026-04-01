"""
Tushare API 调用层

所有方法都是对 Tushare API 的直接封装，只负责：
1. 调用 API
2. 速率控制
3. 日志记录
4. 基础数据处理（如排序）

不包含业务逻辑（循环、合并、复杂转换等）
"""
import logging
import time
import pandas as pd
import tushare as ts
from config.config import Config

logger = logging.getLogger(__name__)


class TushareAPI:
    """Tushare API 统一调用层"""

    def __init__(self, token: str = None, api_url: str = None):
        """
        初始化 Tushare API 客户端

        Args:
            token: Tushare Token
            api_url: API URL（私有网关）
        """
        self.token = token or Config.TUSHARE_TOKEN
        self.api_url = api_url or Config.TUSHARE_API_URL

        # 初始化 Pro API
        self.pro = ts.pro_api(self.token)

        # ==================== 核心注入：必须保证以下赋值 ====================
        # 这两行代码是访问私有网关的关键，缺少会导致无法获取数据
        self.pro._DataApi__token = self.token
        self.pro._DataApi__http_url = self.api_url
        # ================================================================

        logger.info(f"Tushare API 初始化成功 (API: {self.api_url})")

        self._last_call_time = 0

    def _rate_limit(self):
        """速率控制（每次调用间隔 0.3 秒）"""
        current_time = time.time()
        elapsed = current_time - self._last_call_time

        min_interval = 0.3  # 最小调用间隔（秒）
        if elapsed < min_interval:
            sleep_time = min_interval - elapsed
            time.sleep(sleep_time)

        self._last_call_time = time.time()

    def _handle_api_error(self, api_name: str, error: Exception):
        """
        统一处理 API 错误，提供友好的错误提示

        Args:
            api_name: API 名称
            error: 异常对象
        """
        error_msg = str(error)

        # 连接错误
        if "Connection refused" in error_msg or "Failed to establish" in error_msg:
            logger.error(f"调用 {api_name} 失败: 无法连接到 API 服务器 ({self.api_url})")
            logger.error("可能原因:")
            logger.error("  1. 服务器维护中")
            logger.error("  2. 网络连接问题")
            logger.error("  3. 端口被防火墙封禁")
            logger.error("建议:")
            logger.error("  1. 稍后重试（等待 10-30 分钟）")
            logger.error(f"  2. 测试连接: ping {self.api_url.replace('http://', '').replace('https://', '')}")
            logger.error("  3. 联系 API 服务提供方")

        # 超时错误
        elif "timeout" in error_msg.lower() or "timed out" in error_msg.lower():
            logger.error(f"调用 {api_name} 失败: 请求超时")
            logger.error("可能原因: 网络延迟或服务器响应慢")
            logger.error("建议: 稍后重试")

        # 请求频率超限
        elif "请求上限" in error_msg or "rate limit" in error_msg.lower():
            logger.error(f"调用 {api_name} 失败: API 请求频率超限")
            logger.error("建议: 等待 5-10 分钟后重试")

        # Token 或参数错误
        elif "无效" in error_msg or "invalid" in error_msg.lower():
            logger.error(f"调用 {api_name} 失败: Token 或参数无效")
            logger.error("建议:")
            logger.error("  1. 检查 .env 文件中的 TUSHARE_TOKEN")
            logger.error("  2. 检查 .env 文件中的 TUSHARE_API_URL")
            logger.error("  3. 确认 Token 是否过期")

        # 权限错误
        elif "权限" in error_msg or "permission" in error_msg.lower():
            logger.error(f"调用 {api_name} 失败: 权限不足")
            logger.error("建议: 检查 Tushare 账户积分和 API 权限")

        # 其他错误
        else:
            logger.error(f"调用 {api_name} 失败: {error}")

    # ==================== 基础数据 API ====================

    def fetch_stock_basic(self, list_status: str = 'L', fields: str = None) -> pd.DataFrame:
        """
        获取股票基础信息

        Args:
            list_status: 上市状态 'L'上市 'D'退市 'P'暂停上市
            fields: 返回字段

        Returns:
            DataFrame: 股票基础信息
        """
        self._rate_limit()
        logger.debug(f"调用 stock_basic: list_status={list_status}")

        try:
            df = self.pro.stock_basic(list_status=list_status, fields=fields)

            if df is None or len(df) == 0:
                logger.warning(f"stock_basic 返回空数据: list_status={list_status}")
                return pd.DataFrame()

            logger.info(f"获取到 {len(df)} 条股票基础信息")
            return df

        except Exception as e:
            logger.error(f"调用 stock_basic 失败: {e}")
            raise

    def fetch_stock_company(self, exchange: str = None, fields: str = None) -> pd.DataFrame:
        """
        获取上市公司基本信息

        Args:
            exchange: 交易所 'SSE'上交所 'SZSE'深交所 'BSE'北交所
            fields: 返回字段

        Returns:
            DataFrame: 公司信息
        """
        self._rate_limit()
        logger.debug(f"调用 stock_company: exchange={exchange}")

        try:
            df = self.pro.stock_company(exchange=exchange, fields=fields)

            if df is None or len(df) == 0:
                logger.warning(f"stock_company 返回空数据: exchange={exchange}")
                return pd.DataFrame()

            logger.info(f"获取到 {len(df)} 条公司信息")
            return df

        except Exception as e:
            logger.error(f"调用 stock_company 失败: {e}")
            raise

    def fetch_new_share(self, start_date: str = None, end_date: str = None) -> pd.DataFrame:
        """
        获取 IPO 新股列表

        Args:
            start_date: 开始日期 YYYYMMDD
            end_date: 结束日期 YYYYMMDD

        Returns:
            DataFrame: IPO 信息
        """
        self._rate_limit()
        logger.debug(f"调用 new_share: {start_date} ~ {end_date}")

        try:
            df = self.pro.new_share(start_date=start_date, end_date=end_date)

            if df is None or len(df) == 0:
                logger.warning("new_share 返回空数据")
                return pd.DataFrame()

            logger.info(f"获取到 {len(df)} 条 IPO 信息")
            return df

        except Exception as e:
            logger.error(f"调用 new_share 失败: {e}")
            raise

    # ==================== 日线数据 API ====================

    def fetch_daily_bar(
        self,
        ts_code: str,
        start_date: str,
        end_date: str,
        adj: str = None
    ) -> pd.DataFrame:
        """
        获取单只股票的日线数据

        Args:
            ts_code: 股票代码
            start_date: 开始日期 YYYYMMDD
            end_date: 结束日期 YYYYMMDD
            adj: 复权类型 None(不复权)/'qfq'(前复权)/'hfq'(后复权)

        Returns:
            DataFrame: 日线数据（按日期升序）
        """
        self._rate_limit()
        logger.debug(f"调用 pro_bar: ts_code={ts_code}, adj={adj}")

        try:
            df = ts.pro_bar(
                ts_code=ts_code,
                adj=adj,
                start_date=start_date,
                end_date=end_date,
                api=self.pro
            )

            if df is None or len(df) == 0:
                logger.warning(f"pro_bar 返回空数据: {ts_code}, adj={adj}")
                return pd.DataFrame()

            # 按日期升序排列（API 返回的是倒序）
            df = df.sort_values('trade_date').reset_index(drop=True)

            logger.debug(f"获取到 {len(df)} 条日线数据")
            return df

        except Exception as e:
            logger.error(f"调用 pro_bar 失败: {e}")
            raise

    # ==================== ST 状态 API ====================

    def fetch_stock_st(self, ts_code: str = None, trade_date: str = None) -> pd.DataFrame:
        """
        获取 ST 股票列表

        Args:
            ts_code: 股票代码（可选）
            trade_date: 交易日期 YYYYMMDD（可选）

        Returns:
            DataFrame: ST 状态数据
        """
        self._rate_limit()
        logger.debug(f"调用 stock_st: ts_code={ts_code}, trade_date={trade_date}")

        try:
            df = self.pro.stock_st(ts_code=ts_code, trade_date=trade_date)

            if df is None or len(df) == 0:
                logger.debug(f"stock_st 返回空数据")
                return pd.DataFrame()

            logger.debug(f"获取到 {len(df)} 条 ST 状态记录")
            return df

        except Exception as e:
            self._handle_api_error('stock_st', e)
            raise

    def fetch_namechange(
        self,
        ts_code: str = None,
        start_date: str = None,
        end_date: str = None
    ) -> pd.DataFrame:
        """
        获取股票名称变更历史（包含ST状态变更）

        Args:
            ts_code: 股票代码（可选）
            start_date: 开始日期 YYYYMMDD（可选）
            end_date: 结束日期 YYYYMMDD（可选）

        Returns:
            DataFrame: 名称变更记录
        """
        self._rate_limit()
        logger.debug(f"调用 namechange: ts_code={ts_code}, start_date={start_date}, end_date={end_date}")

        try:
            df = self.pro.namechange(
                ts_code=ts_code,
                start_date=start_date,
                end_date=end_date
            )

            if df is None or len(df) == 0:
                logger.debug(f"namechange 返回空数据")
                return pd.DataFrame()

            logger.debug(f"获取到 {len(df)} 条名称变更记录")
            return df

        except Exception as e:
            self._handle_api_error('namechange', e)
            raise

    # ==================== 停牌信息 API ====================

    def fetch_suspend_d(
        self,
        ts_code: str = None,
        trade_date: str = None,
        suspend_type: str = None
    ) -> pd.DataFrame:
        """
        获取股票停牌信息

        Args:
            ts_code: 股票代码（可选）
            trade_date: 交易日期 YYYYMMDD（查询该日期处于停牌状态的股票）
            suspend_type: 停牌类型 'S'停牌（可选）

        Returns:
            DataFrame: 停牌信息（包含 ts_code, trade_date, suspend_timing, suspend_type）
        """
        self._rate_limit()
        logger.debug(f"调用 suspend_d: ts_code={ts_code}, trade_date={trade_date}, suspend_type={suspend_type}")

        try:
            df = self.pro.suspend_d(
                ts_code=ts_code,
                trade_date=trade_date,
                suspend_type=suspend_type
            )

            if df is None or len(df) == 0:
                logger.debug(f"suspend_d 返回空数据")
                return pd.DataFrame()

            logger.debug(f"获取到 {len(df)} 条停牌记录")
            return df

        except Exception as e:
            logger.error(f"调用 suspend_d 失败: {e}")
            raise

    # ==================== 涨跌停价格 API ====================

    def fetch_stk_limit(self, trade_date: str = None, ts_code: str = None) -> pd.DataFrame:
        """
        获取全市场股票涨跌停价格数据

        Args:
            trade_date: 交易日期 YYYYMMDD（查询该日期的涨跌停价格）
            ts_code: 股票代码（可选，不指定则返回全部）

        Returns:
            DataFrame: 涨跌停价格数据（包含 ts_code, trade_date, up_limit, down_limit）
        """
        self._rate_limit()
        logger.debug(f"调用 stk_limit: trade_date={trade_date}, ts_code={ts_code}")

        try:
            df = self.pro.stk_limit(trade_date=trade_date, ts_code=ts_code)

            if df is None or len(df) == 0:
                logger.debug(f"stk_limit 返回空数据: trade_date={trade_date}")
                return pd.DataFrame()

            logger.debug(f"获取到 {len(df)} 条涨跌停价格记录")
            return df

        except Exception as e:
            logger.error(f"调用 stk_limit 失败: {e}")
            raise

    # ==================== 交易日历 API ====================

    def fetch_trade_cal(
        self,
        exchange: str = 'SSE',
        start_date: str = None,
        end_date: str = None,
        is_open: str = None
    ) -> pd.DataFrame:
        """
        获取交易日历

        Args:
            exchange: 交易所 'SSE'/'SZSE'/'BSE'
            start_date: 开始日期 YYYYMMDD
            end_date: 结束日期 YYYYMMDD
            is_open: 是否交易 '0'休市 '1'交易

        Returns:
            DataFrame: 交易日历
        """
        self._rate_limit()
        logger.debug(f"调用 trade_cal: {start_date} ~ {end_date}")

        try:
            df = self.pro.trade_cal(
                exchange=exchange,
                start_date=start_date,
                end_date=end_date,
                is_open=is_open
            )

            if df is None or len(df) == 0:
                logger.warning(f"trade_cal 返回空数据")
                return pd.DataFrame()

            logger.info(f"获取到 {len(df)} 条交易日历记录")
            return df

        except Exception as e:
            logger.error(f"调用 trade_cal 失败: {e}")
            raise

    # ==================== Fama 三因子数据 API ====================

    def fetch_daily_basic(
        self,
        ts_code: str = None,
        trade_date: str = None,
        start_date: str = None,
        end_date: str = None,
        fields: str = None
    ) -> pd.DataFrame:
        """
        获取日度基本面数据（市值、PE、PB 等）

        Args:
            ts_code: 股票代码（可选，不指定则返回全市场）
            trade_date: 交易日期 YYYYMMDD
            start_date: 开始日期 YYYYMMDD
            end_date: 结束日期 YYYYMMDD
            fields: 返回字段

        Returns:
            DataFrame: 日度基本面数据（包含 ts_code, trade_date, total_mv, circ_mv, pe, pb, ps 等）

        Note:
            单次最多返回 6000 条记录
        """
        self._rate_limit()
        logger.debug(f"调用 daily_basic: ts_code={ts_code}, trade_date={trade_date}")

        try:
            df = self.pro.daily_basic(
                ts_code=ts_code,
                trade_date=trade_date,
                start_date=start_date,
                end_date=end_date,
                fields=fields
            )

            if df is None or len(df) == 0:
                logger.debug(f"daily_basic 返回空数据")
                return pd.DataFrame()

            logger.info(f"获取到 {len(df)} 条日度基本面数据")
            return df

        except Exception as e:
            self._handle_api_error('daily_basic', e)
            raise

    def fetch_index_daily(
        self,
        ts_code: str,
        start_date: str = None,
        end_date: str = None,
        fields: str = None
    ) -> pd.DataFrame:
        """
        获取指数日线数据

        Args:
            ts_code: 指数代码（如 '000300.SH' 沪深300）
            start_date: 开始日期 YYYYMMDD
            end_date: 结束日期 YYYYMMDD
            fields: 返回字段

        Returns:
            DataFrame: 指数日线数据（包含 ts_code, trade_date, open, high, low, close, vol, amount）

        Note:
            单次最多返回 8000 条记录
        """
        self._rate_limit()
        logger.debug(f"调用 index_daily: ts_code={ts_code}, start_date={start_date}, end_date={end_date}")

        try:
            df = self.pro.index_daily(
                ts_code=ts_code,
                start_date=start_date,
                end_date=end_date,
                fields=fields
            )

            if df is None or len(df) == 0:
                logger.warning(f"index_daily 返回空数据: ts_code={ts_code}")
                return pd.DataFrame()

            logger.info(f"获取到 {len(df)} 条指数日线数据")
            return df

        except Exception as e:
            self._handle_api_error('index_daily', e)
            raise

    def fetch_shibor(
        self,
        start_date: str = None,
        end_date: str = None,
        fields: str = None
    ) -> pd.DataFrame:
        """
        获取 Shibor 利率数据

        Args:
            start_date: 开始日期 YYYYMMDD
            end_date: 结束日期 YYYYMMDD
            fields: 返回字段

        Returns:
            DataFrame: Shibor 利率数据（包含 date, rate1w, rate2w, rate1m, rate3m, rate6m, rate9m, rate1y）

        Note:
            包含 1周、2周、1月、3月、6月、9月、1年 等期限利率
        """
        self._rate_limit()
        logger.debug(f"调用 shibor: start_date={start_date}, end_date={end_date}")

        try:
            df = self.pro.shibor(
                start_date=start_date,
                end_date=end_date,
                fields=fields
            )

            if df is None or len(df) == 0:
                logger.warning(f"shibor 返回空数据")
                return pd.DataFrame()

            logger.info(f"获取到 {len(df)} 条 Shibor 利率数据")
            return df

        except Exception as e:
            self._handle_api_error('shibor', e)
            raise

    # ==================== 财务报表 API ====================

    def fetch_income(self, period: str, report_type: str = None) -> pd.DataFrame:
        """
        获取利润表数据（全市场，按报告期）

        使用 income_vip 接口，需要 5000 积分

        Args:
            period: 报告期 YYYYMMDD（如 '20201231' 为2020年报）
            report_type: 报告类型（None=不过滤，返回所有类型）

        Returns:
            DataFrame: 利润表数据
        """
        self._rate_limit()
        logger.debug(f"调用 income_vip: period={period}, report_type={report_type}")

        fields = (
            'ts_code,ann_date,f_ann_date,end_date,report_type,comp_type,end_type,'
            'basic_eps,diluted_eps,'
            'total_revenue,revenue,'
            'total_cogs,oper_cost,sell_exp,admin_exp,fin_exp,fin_exp_int_exp,fin_exp_int_inc,'
            'rd_exp,assets_impair_loss,credit_impa_loss,'
            'operate_profit,non_oper_income,non_oper_exp,'
            'total_profit,income_tax,'
            'n_income,n_income_attr_p,minority_gain,'
            'net_after_nr_lp_correct,'
            'ebit,ebitda,'
            'update_flag'
        )

        try:
            df = self.pro.income_vip(period=period, report_type=report_type, fields=fields)

            if df is None or len(df) == 0:
                logger.debug(f"income_vip 返回空数据: period={period}, report_type={report_type}")
                return pd.DataFrame()

            logger.debug(f"获取到 {len(df)} 条利润表记录: period={period}, report_type={report_type}")
            return df

        except Exception as e:
            self._handle_api_error('income_vip', e)
            raise

    # ==================== 通用 API 调用 ====================

    def call_api(self, api_name: str, **kwargs) -> pd.DataFrame:
        """
        通用 API 调用方法（兼容旧代码）

        Args:
            api_name: API 名称（如 'stock_basic', 'daily' 等）
            **kwargs: API 参数

        Returns:
            DataFrame: API 返回数据
        """
        self._rate_limit()
        logger.debug(f"调用 {api_name}: {kwargs}")

        try:
            api_func = getattr(self.pro, api_name)
            df = api_func(**kwargs)

            if df is None or len(df) == 0:
                logger.debug(f"{api_name} 返回空数据")
                return pd.DataFrame()

            logger.debug(f"{api_name} 返回 {len(df)} 条记录")
            return df

        except AttributeError:
            logger.error(f"API 方法不存在: {api_name}")
            raise
        except Exception as e:
            logger.error(f"调用 {api_name} 失败: {e}")
            raise

    def test_connection(self) -> bool:
        """
        测试连接

        Returns:
            bool: 连接是否正常
        """
        try:
            logger.info("测试 Tushare API 连接...")
            df = self.fetch_stock_basic(list_status='L')

            if df is not None and len(df) > 0:
                logger.info(f"连接测试成功")
                return True
            else:
                logger.error("连接测试失败：返回空数据")
                return False

        except Exception as e:
            logger.error(f"连接测试失败: {e}")
            return False
