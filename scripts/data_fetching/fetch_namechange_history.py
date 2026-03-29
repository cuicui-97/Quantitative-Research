#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
获取所有股票的名称变更历史

第一步：获取并保存所有股票的名称变更记录
"""
import sys
from pathlib import Path

# 添加项目根目录到 Python 路径
project_root = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(project_root))

import pandas as pd
from tqdm import tqdm

from config.config import Config
from data_engine.api.tushare_api import TushareAPI
from data_engine.utils import setup_logger


def main():
    """主函数"""
    logger = setup_logger()
    logger.info("=" * 80)
    logger.info("获取所有股票的名称变更历史")
    logger.info("=" * 80)

    try:
        # 1. 初始化 API
        logger.info("初始化 Tushare API...")
        api = TushareAPI()

        # 2. 获取所有股票列表（包含退市股票）
        logger.info("获取股票列表...")
        # 优先读取剩余股票列表，如果不存在则读取全部
        remaining_file = Config.SUPPLEMENTARY_DATA_DIR / 'remaining_stocks_for_namechange.csv'
        if remaining_file.exists():
            df_stocks = pd.read_csv(remaining_file, dtype={'ts_code': str})
            logger.info(f"读取剩余股票列表: {len(df_stocks)} 只")
        else:
            # 读取全部股票（包含退市）
            all_companies_file = Config.BASIC_DATA_DIR / 'all_companies_info.csv'
            if all_companies_file.exists():
                df_stocks = pd.read_csv(all_companies_file, dtype={'ts_code': str})
                logger.info(f"读取全部股票（含退市）: {len(df_stocks)} 只")
            else:
                # 兜底：从API获取在市股票
                df_stocks = api.fetch_stock_basic(list_status='L')
                logger.info(f"从API获取在市股票: {len(df_stocks)} 只")

        # 3. 检查是否已有数据
        output_file = Config.SUPPLEMENTARY_DATA_DIR / 'stock_namechange_history.csv'

        if output_file.exists():
            logger.info(f"发现已有数据文件: {output_file}")
            df_existing = pd.read_csv(output_file, dtype={'ts_code': str})
            existing_stocks = set(df_existing['ts_code'].unique())
            logger.info(f"已有 {len(existing_stocks)} 只股票的名称变更记录")

            # 筛选出未获取的股票
            remaining_stocks = df_stocks[~df_stocks['ts_code'].isin(existing_stocks)]
            logger.info(f"还需获取 {len(remaining_stocks)} 只股票")

            all_name_changes = [df_existing]
            df_to_fetch = remaining_stocks
        else:
            logger.info("未发现已有数据，将获取所有股票")
            all_name_changes = []
            df_to_fetch = df_stocks

        if len(df_to_fetch) == 0:
            logger.info("所有股票的名称变更历史已获取完毕")
            return 0

        # 4. 逐只股票获取名称变更历史
        logger.info("=" * 80)
        logger.info(f"开始获取名称变更历史（共 {len(df_to_fetch)} 只股票）...")
        logger.info("=" * 80)

        failed_stocks = []
        save_interval = 100  # 每100只股票保存一次

        for idx, (_, row) in enumerate(tqdm(df_to_fetch.iterrows(), total=len(df_to_fetch), desc="获取名称变更")):
            ts_code = row['ts_code']

            try:
                # 获取该股票的名称变更历史
                df_changes = api.fetch_namechange(ts_code=ts_code)

                if not df_changes.empty:
                    all_name_changes.append(df_changes)

            except Exception as e:
                logger.warning(f"{ts_code}: 获取失败 - {e}")
                failed_stocks.append(ts_code)
                continue

            # 定期保存进度
            if (idx + 1) % save_interval == 0:
                df_temp = pd.concat(all_name_changes, ignore_index=True)
                df_temp.to_csv(output_file, index=False, encoding='utf-8-sig')
                logger.info(f"  ✓ 进度保存: 已获取 {idx + 1}/{len(df_to_fetch)} 只股票，共 {len(df_temp)} 条记录")

        # 5. 合并并保存所有记录
        if all_name_changes:
            df_all = pd.concat(all_name_changes, ignore_index=True)

            # 去重（可能有重复记录）
            df_all = df_all.drop_duplicates(subset=['ts_code', 'name', 'start_date'], keep='first')

            # 保存
            df_all.to_csv(output_file, index=False, encoding='utf-8-sig')

            logger.info("=" * 80)
            logger.info("完成")
            logger.info("=" * 80)
            logger.info(f"输出文件: {output_file}")
            logger.info(f"总记录数: {len(df_all)} 条")
            logger.info(f"涉及股票: {df_all['ts_code'].nunique()} 只")

            # 统计有名称变更的股票
            stocks_with_changes = df_all.groupby('ts_code').size()
            logger.info(f"有名称变更的股票: {len(stocks_with_changes)} 只")
            logger.info(f"平均每只股票变更次数: {stocks_with_changes.mean():.2f} 次")

            # 失败统计
            if failed_stocks:
                logger.warning(f"\n获取失败的股票: {len(failed_stocks)} 只")
                logger.warning(f"失败股票: {', '.join(failed_stocks[:10])}" +
                             (f" ... (还有{len(failed_stocks)-10}只)" if len(failed_stocks) > 10 else ""))
        else:
            logger.warning("未获取到任何名称变更记录")

        logger.info("=" * 80)
        logger.info("✓ 第一步完成：名称变更历史已保存")
        logger.info("下一步：运行 extract_st_from_namechange.py 提取ST状态")
        logger.info("=" * 80)

        return 0

    except KeyboardInterrupt:
        logger.warning("\n用户中断操作")
        logger.info("已保存部分数据，下次运行将从断点继续")
        return 1
    except Exception as e:
        logger.error(f"执行失败: {str(e)}", exc_info=True)
        return 1


if __name__ == '__main__':
    sys.exit(main())
