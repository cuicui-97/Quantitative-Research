"""
工具函数模块
提供重试装饰器、日志配置等通用功能
"""
import time
import logging
from functools import wraps
from pathlib import Path
from config.config import Config


def retry_on_error(max_retries=None, delay=None, backoff=None):
    """
    重试装饰器，支持指数退避

    Args:
        max_retries: 最大重试次数（默认使用配置）
        delay: 初始延迟时间（秒，默认使用配置）
        backoff: 延迟倍数（默认使用配置）
    """
    max_retries = max_retries or Config.RETRY_TIMES
    delay = delay or Config.RETRY_DELAY
    backoff = backoff or Config.RETRY_BACKOFF

    def decorator(func):
        @wraps(func)
        def wrapper(*args, **kwargs):
            retries = 0
            current_delay = delay
            logger = logging.getLogger(func.__module__)

            while retries < max_retries:
                try:
                    return func(*args, **kwargs)
                except Exception as e:
                    retries += 1
                    if retries >= max_retries:
                        logger.error(
                            f"{func.__name__} 失败，已达最大重试次数 {max_retries}: {e}"
                        )
                        raise

                    logger.warning(
                        f"{func.__name__} 失败，{current_delay}秒后重试 "
                        f"({retries}/{max_retries}): {e}"
                    )
                    time.sleep(current_delay)
                    current_delay *= backoff

        return wrapper
    return decorator


def setup_logger(name=None, log_file=None, level=None):
    """
    配置日志系统

    Args:
        name: 日志记录器名称（默认为 root）
        log_file: 日志文件路径（默认使用配置）
        level: 日志级别（默认使用配置）

    Returns:
        logging.Logger: 配置好的日志记录器
    """
    # 确保日志目录存在
    Config.ensure_dirs()

    # 设置日志文件路径
    if log_file is None:
        log_file = Config.get_log_file()

    # 设置日志级别
    if level is None:
        level = getattr(logging, Config.LOG_LEVEL)

    # 创建日志记录器
    logger = logging.getLogger(name)
    logger.setLevel(level)

    # 避免重复添加处理器
    if logger.handlers:
        return logger

    # 创建文件处理器
    file_handler = logging.FileHandler(log_file, encoding='utf-8')
    file_handler.setLevel(level)

    # 创建控制台处理器
    console_handler = logging.StreamHandler()
    console_handler.setLevel(level)

    # 创建格式化器
    formatter = logging.Formatter(
        Config.LOG_FORMAT,
        datefmt=Config.LOG_DATE_FORMAT
    )
    file_handler.setFormatter(formatter)
    console_handler.setFormatter(formatter)

    # 添加处理器
    logger.addHandler(file_handler)
    logger.addHandler(console_handler)

    return logger


def ensure_directory(path):
    """
    确保目录存在

    Args:
        path: 目录路径（str 或 Path）
    """
    Path(path).mkdir(parents=True, exist_ok=True)
