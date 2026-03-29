"""
重试装饰器
"""
import time
import logging
from functools import wraps
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
