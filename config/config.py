"""
配置管理模块
统一管理所有配置项，包括 API 配置、路径配置、抓取配置等
"""
import os
from pathlib import Path
from dotenv import load_dotenv

# 加载环境变量
load_dotenv()


class Config:
    """配置类"""

    # ==================== API 配置 ====================
    TUSHARE_TOKEN = os.getenv('TUSHARE_TOKEN')
    if not TUSHARE_TOKEN:
        raise ValueError(
            "未找到 TUSHARE_TOKEN 环境变量。\n"
            "请在 .env 文件中设置：\n"
            "TUSHARE_TOKEN=your_token_here"
        )

    TUSHARE_API_URL = os.getenv('TUSHARE_API_URL')
    if not TUSHARE_API_URL:
        raise ValueError(
            "未找到 TUSHARE_API_URL 环境变量。\n"
            "请在 .env 文件中设置：\n"
            "TUSHARE_API_URL=your_api_url_here"
        )

    # ==================== 路径配置 ====================
    # 项目根目录
    BASE_DIR = Path(__file__).resolve().parent.parent

    # 数据目录
    DATA_DIR = BASE_DIR / 'data'
    BASIC_DATA_DIR = DATA_DIR / 'basic'
    DAILY_DATA_DIR = DATA_DIR / 'daily'
    LOG_DIR = DATA_DIR / 'logs'
    SUPPLEMENTARY_DATA_DIR = DATA_DIR / 'supplementary'  # 补充数据目录
    MATRIX_DATA_DIR = DATA_DIR / 'matrices'  # 矩阵输出目录

    # 基础数据文件路径
    BASIC_DATA_FILE = BASIC_DATA_DIR / 'all_companies_info.csv'

    # ==================== 抓取配置 ====================
    # 重试配置
    RETRY_TIMES = 3  # 最大重试次数
    RETRY_DELAY = 2  # 初始重试延迟（秒）
    RETRY_BACKOFF = 2  # 重试延迟倍数（指数退避）

    # API 调用配置
    REQUEST_INTERVAL = 0.3  # API 调用间隔（秒），防止频率限制

    # 批量抓取配置
    DEFAULT_BATCH_SIZE = None  # 默认批次大小（None 表示全部抓取）
    SKIP_EXISTING = True  # 默认跳过已存在的文件（断点续传）

    # ==================== 日志配置 ====================
    LOG_LEVEL = 'INFO'  # 日志级别
    LOG_FORMAT = '%(asctime)s - %(levelname)s - %(name)s - %(message)s'  # 日志格式
    LOG_DATE_FORMAT = '%Y-%m-%d %H:%M:%S'  # 日志时间格式

    @classmethod
    def ensure_dirs(cls):
        """确保所有必要的目录存在"""
        for dir_path in [cls.BASIC_DATA_DIR, cls.DAILY_DATA_DIR, cls.LOG_DIR]:
            dir_path.mkdir(parents=True, exist_ok=True)

    @classmethod
    def get_log_file(cls):
        """获取当前日志文件路径"""
        from datetime import datetime
        log_filename = f"fetch_{datetime.now().strftime('%Y%m%d')}.log"
        return cls.LOG_DIR / log_filename
