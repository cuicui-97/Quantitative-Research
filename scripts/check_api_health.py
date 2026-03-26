"""
API 健康检查工具
快速测试 Tushare API 是否可用
"""
import sys
from pathlib import Path

project_root = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(project_root))

import os
from dotenv import load_dotenv
from src.api.tushare_api import TushareAPI
from src.utils import setup_logger

load_dotenv()


def main():
    """API 健康检查"""
    logger = setup_logger()
    logger.info("=" * 60)
    logger.info("Tushare API 健康检查")
    logger.info("=" * 60)

    # 读取配置
    token = os.getenv('TUSHARE_TOKEN')
    api_url = os.getenv('TUSHARE_API_URL')

    if not token or not api_url:
        logger.error("错误: 未找到 TUSHARE_TOKEN 或 TUSHARE_API_URL")
        logger.error("请检查 .env 文件配置")
        return 1

    logger.info(f"\n配置信息:")
    logger.info(f"  Token: {token[:20]}... (长度: {len(token)})")
    logger.info(f"  API URL: {api_url}")

    # 创建客户端
    try:
        api = TushareAPI(token, api_url)
        logger.info("\n✓ 客户端创建成功")
    except Exception as e:
        logger.error(f"\n✗ 客户端创建失败: {e}")
        return 1

    # 测试关键 API（直接调用专用方法）
    logger.info("\n测试 API 调用:")

    test_cases = [
        {
            'name': 'stock_basic (基础信息)',
            'func': lambda: api.fetch_stock_basic(list_status='L')
        },
        {
            'name': 'trade_cal (交易日历)',
            'func': lambda: api.fetch_trade_cal(
                exchange='SSE',
                start_date='20260301',
                end_date='20260331',
                is_open='1'
            )
        },
        {
            'name': 'stock_st (ST 状态)',
            'func': lambda: api.fetch_stock_st(trade_date='20260323')
        },
        {
            'name': 'daily (日线数据)',
            'func': lambda: api.fetch_daily_bar(
                ts_code='000001.SZ',
                start_date='20260301',
                end_date='20260331',
                adj='hfq'
            )
        },
    ]

    success_count = 0

    for test in test_cases:
        logger.info(f"\n  测试: {test['name']}")
        try:
            df = test['func']()

            if df is not None and len(df) > 0:
                logger.info(f"    ✓ 成功: 返回 {len(df)} 条记录")
                logger.info(f"    列: {df.columns.tolist()}")
                success_count += 1
            else:
                logger.warning(f"    ⚠ 返回空数据")

        except Exception as e:
            logger.error(f"    ✗ 失败: {e}")

    # 总结
    logger.info("\n" + "=" * 60)
    logger.info("检查结果:")
    logger.info("=" * 60)

    if success_count == len(test_cases):
        logger.info(f"✓ 所有测试通过 ({success_count}/{len(test_cases)})")
        logger.info("\nAPI 服务正常，可以继续数据获取！")
        logger.info("\n建议执行:")
        logger.info("  python scripts/fetch_st_status.py --start-date 20000101 --end-date 20261231")
        return 0

    elif success_count > 0:
        logger.warning(f"⚠ 部分测试通过 ({success_count}/{len(test_cases)})")
        logger.info("\n部分 API 可用，可以使用可用的 API 获取数据")
        return 0

    else:
        logger.error(f"✗ 所有测试失败 (0/{len(test_cases)})")
        logger.error("\nAPI 服务暂时不可用")
        logger.error("\n可能原因:")
        logger.error("  1. API 服务器维护中")
        logger.error("  2. 网络连接问题")
        logger.error("  3. 服务器临时故障")
        logger.error("\n建议:")
        logger.error("  1. 稍后重试（1-2小时后）")
        logger.error("  2. 检查网络连接: ping lianghua.nanyangqiankun.top")
        logger.error("  3. 联系 API 服务提供方")
        logger.error("\n重试命令:")
        logger.error("  python scripts/check_api_health.py")

        return 1


if __name__ == '__main__':
    sys.exit(main())
