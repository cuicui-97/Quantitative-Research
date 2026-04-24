"""
日志配置
"""
import logging
from config.config import Config


def setup_logger(name=None, log_file=None, level=None, prefix: str = 'run'):
    """
    配置日志系统

    Args:
        name: 日志记录器名称（默认为 root）
        log_file: 日志文件路径（默认使用配置）
        level: 日志级别（默认使用配置）
        prefix: 日志文件名前缀，如 'fetch'、'factor'（默认 'run'）

    Returns:
        logging.Logger: 配置好的日志记录器
    """
    # 确保日志目录存在
    Config.ensure_dirs()

    # 设置日志文件路径
    if log_file is None:
        log_file = Config.get_log_file(prefix=prefix)

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
