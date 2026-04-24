#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
概念板块数据获取与整合脚本

功能：
1. 从Tushare获取概念列表（ths_index）
2. 获取每个概念的成分股（ths_member）
3. 整合为统一格式（长表和矩阵）
4. 支持增量更新（只获取新增或变化的概念）

使用方法：
    # 全量获取
    python fetch_and_build_concept_data.py

    # 增量更新
    python fetch_and_build_concept_data.py --incremental

    # 强制重新获取
    python fetch_and_build_concept_data.py --force

输出文件：
    - concept_list.csv: 概念列表
    - concept_stock_long.csv: 股票-概念关系（长表）
    - concept_stock_matrix.csv: 概念×股票矩阵（0/1）
"""
import sys
from pathlib import Path

project_root = Path(__file__).resolve().parent.parent.parent
sys.path.insert(0, str(project_root))

import argparse
import pandas as pd
import numpy as np
from datetime import datetime
from config.config import Config
from data_engine.api.tushare_api import TushareAPI
from data_engine.processors.matrix_io import save_matrix
from utils import setup_logger


def fetch_concept_list(api, logger=None):
    """获取概念列表"""
    if logger:
        logger.info("获取概念列表...")

    try:
        df = api.call_api('ths_index')

        # 只保留概念（type=N），过滤掉基础板块（type=BB）
        df = df[df['type'] == 'N'].copy()

        # 清理概念代码
        df['ts_code'] = df['ts_code'].str.replace('.TI', '', regex=False)

        if logger:
            logger.info(f"  获取到 {len(df)} 个概念")

        return df[['ts_code', 'name', 'count', 'exchange', 'list_date']]

    except Exception as e:
        if logger:
            logger.error(f"  获取失败: {e}")
        return None


def fetch_concept_members(api, concept_code, logger=None):
    """获取单个概念的成分股"""
    try:
        df = api.call_api('ths_member', ts_code=f"{concept_code}.TI")
        return df
    except Exception as e:
        if logger:
            logger.warning(f"  获取 {concept_code} 成分股失败: {e}")
        return None


def build_concept_matrix(constituents_list, all_stocks, all_dates, concept_list_df, logger=None):
    """
    构建概念-股票时序矩阵（考虑概念list_date）

    逻辑：
    - 概念只在 list_date 之后才有效
    - 输出矩阵格式：index=日期(YYYYMMDD), columns=股票, 值=所属概念列表(逗号分隔)
    """
    if logger:
        logger.info("\n构建概念-股票时序矩阵...")

    # 获取概念list_date映射
    concept_list_date = dict(zip(
        concept_list_df['ts_code'].astype(str),
        pd.to_datetime(concept_list_df['list_date'].astype(str), format='%Y%m%d')
    ))

    # 获取所有概念代码
    all_concepts = sorted(list(set([c['concept_code'] for c in constituents_list])))

    if logger:
        logger.info(f"  概念数: {len(all_concepts)}, 股票数: {len(all_stocks)}, 交易日: {len(all_dates)}")

    # 转换为日期数组便于比较
    dates_dt = pd.to_datetime(all_dates)
    date_strs = [d.strftime('%Y%m%d') for d in dates_dt]

    # 为每个(日期, 股票)记录所属概念列表
    # 数据结构: {(date, stock): [concept1, concept2, ...]}
    membership = {}

    # 按概念分组处理
    constituents_by_concept = {}
    for item in constituents_list:
        concept = item['concept_code']
        if concept not in constituents_by_concept:
            constituents_by_concept[concept] = []
        constituents_by_concept[concept].append(item['con_code'])

    for concept, stocks in constituents_by_concept.items():
        if logger and len(membership) % 10000 == 0:
            logger.info(f"  处理概念: {concept} ({len(stocks)}只股票)")

        # 获取该概念的生效日期
        list_date = concept_list_date.get(concept)
        if list_date is None:
            logger.warning(f"  概念 {concept} 无list_date，跳过")
            continue

        # 只在该概念list_date之后的交易日填充
        valid_dates_mask = dates_dt >= list_date
        valid_dates = [date_strs[i] for i, valid in enumerate(valid_dates_mask) if valid]

        if len(valid_dates) == 0:
            continue

        # 填充membership
        for stock in stocks:
            if stock not in all_stocks:
                continue
            for date_str in valid_dates:
                key = (date_str, stock)
                if key not in membership:
                    membership[key] = []
                membership[key].append(concept)

    if logger:
        logger.info(f"  共 {len(membership)} 个(日期,股票)有概念归属")

    # 构建DataFrame
    # 先构建长格式数据
    rows = []
    for (date_str, stock), concepts in membership.items():
        rows.append({
            'trade_date': date_str,
            'ts_code': stock,
            'concepts': ','.join(sorted(concepts)),
            'concept_count': len(concepts)
        })

    if len(rows) == 0:
        logger.warning("  无有效概念归属数据")
        return pd.DataFrame(index=date_strs, columns=all_stocks, dtype=object)

    long_df = pd.DataFrame(rows)

    # pivot为宽格式：日期 × 股票
    concept_names_matrix = long_df.pivot(
        index='trade_date',
        columns='ts_code',
        values='concepts'
    )

    # reindex到完整的日期和股票
    concept_names_matrix = concept_names_matrix.reindex(index=date_strs, columns=all_stocks)

    if logger:
        coverage = concept_names_matrix.notna().sum().sum() / (len(date_strs) * len(all_stocks))
        avg_concepts = long_df['concept_count'].mean() if len(long_df) > 0 else 0
        logger.info(f"  矩阵非空比例: {coverage:.2%}")
        logger.info(f"  平均每只股票属于 {avg_concepts:.1f} 个概念")

    return concept_names_matrix


def main():
    parser = argparse.ArgumentParser(description='概念板块数据获取与整合')
    parser.add_argument('--incremental', action='store_true',
                       help='增量更新（只获取新增概念）')
    parser.add_argument('--force', action='store_true',
                       help='强制重新获取所有数据')
    args = parser.parse_args()

    logger = setup_logger(prefix="concept_data")
    logger.info("="*60)
    logger.info("概念板块数据获取与整合")
    logger.info("="*60)

    # 初始化API
    api = TushareAPI()

    # 输出目录
    output_dir = Config.SUPPLEMENTARY_DATA_DIR
    output_dir.mkdir(parents=True, exist_ok=True)

    # 加载现有数据（用于增量更新）
    existing_concepts = set()
    if args.incremental and (output_dir / 'concept_list.csv').exists():
        existing_df = pd.read_csv(output_dir / 'concept_list.csv')
        existing_concepts = set(existing_df['ts_code'].tolist())
        logger.info(f"增量更新模式，已有 {len(existing_concepts)} 个概念")

    # 1. 获取概念列表
    concept_list = fetch_concept_list(api, logger=logger)
    if concept_list is None:
        logger.error("获取概念列表失败")
        return

    # 保存概念列表
    list_file = output_dir / 'concept_list.csv'
    concept_list.to_csv(list_file, index=False, encoding='utf-8-sig')
    logger.info(f"概念列表已保存: {list_file}")

    # 2. 获取每个概念的成分股
    logger.info("\n获取概念成分股...")

    # 确定需要获取的概念
    if args.force or not args.incremental:
        concepts_to_fetch = concept_list
    else:
        # 只获取新增概念
        concepts_to_fetch = concept_list[~concept_list['ts_code'].isin(existing_concepts)]
        logger.info(f"新增概念: {len(concepts_to_fetch)} 个")

    if len(concepts_to_fetch) == 0:
        logger.info("没有新概念需要获取")
        return

    # 获取成分股
    constituents_list = []
    total = len(concepts_to_fetch)

    for idx, row in concepts_to_fetch.iterrows():
        ts_code = row['ts_code']
        name = row['name']

        if idx % 50 == 0:
            logger.info(f"  进度: {idx}/{total} ({idx/total:.1%})")

        members = fetch_concept_members(api, ts_code, logger=logger)

        if members is not None and len(members) > 0:
            for _, member_row in members.iterrows():
                constituents_list.append({
                    'concept_code': ts_code,
                    'concept_name': name,
                    'con_code': member_row['con_code'],
                    'con_name': member_row['con_name']
                })

    logger.info(f"  共获取 {len(constituents_list)} 条成分股记录")

    # 3. 整合数据
    logger.info("\n整合数据...")

    # 加载所有股票列表
    basic_info = pd.read_csv(Config.BASIC_DATA_DIR / 'all_companies_info.csv')
    all_stocks = basic_info['ts_code'].tolist()

    # 获取交易日历（用于矩阵索引）
    trade_calendar = pd.read_csv(Config.SUPPLEMENTARY_DATA_DIR / 'trade_calendar.csv')
    trade_calendar = trade_calendar[trade_calendar['is_open'] == 1]
    all_dates = pd.DatetimeIndex(pd.to_datetime(trade_calendar['cal_date'].astype(str), format='%Y%m%d'))

    # 构建矩阵（考虑list_date）
    concept_names_matrix = build_concept_matrix(
        constituents_list, all_stocks, all_dates, concept_list, logger=logger
    )

    # 保存概念标签矩阵（日期 × 股票，值为概念代码列表）
    matrix_file = output_dir / 'concept_stock_matrix.csv'
    concept_names_matrix.to_csv(matrix_file, encoding='utf-8-sig')
    logger.info(f"概念标签矩阵已保存: {matrix_file}")
    logger.info(f"  维度: {concept_names_matrix.shape} (日期 × 股票)")
    logger.info(f"  格式: 值为逗号分隔的概念代码，如 '886078,886033'")

    # 构建并保存长格式
    df_long = pd.DataFrame(constituents_list)
    long_file = output_dir / 'concept_stock_long.csv'
    df_long.to_csv(long_file, index=False, encoding='utf-8-sig')
    logger.info(f"长格式数据已保存: {long_file}")
    logger.info(f"  记录数: {len(df_long)}")

    # 显示热门概念统计（基于最新日期）
    logger.info("\n热门概念统计（最新交易日）:")
    hot_concepts = ['886078', '886033', '885728', '886067', '886032', '886019']
    concept_name_map = dict(zip(concept_list['ts_code'], concept_list['name']))
    concept_list_date = dict(zip(
        concept_list['ts_code'].astype(str),
        pd.to_datetime(concept_list['list_date'].astype(str), format='%Y%m%d')
    ))

    # 统计最新日期的概念覆盖
    latest_date = concept_names_matrix.index[-1]
    latest_data = concept_names_matrix.loc[latest_date]

    for code in hot_concepts:
        # 统计包含该概念的股票数
        count = latest_data.dropna().apply(lambda x: code in str(x).split(',') if x else False).sum()
        name = concept_name_map.get(code, '')
        list_date = concept_list_date.get(code, 'N/A')
        logger.info(f"  {code} ({name}): {count} 只股票 (list_date: {list_date.strftime('%Y%m%d') if hasattr(list_date, 'strftime') else list_date})"
)

    logger.info("\n" + "="*60)
    logger.info("完成!")
    logger.info("="*60)
    logger.info(f"\n生成的文件:")
    logger.info(f"  1. {list_file.name} - 概念列表（含list_date）")
    logger.info(f"  2. {matrix_file.name} - 概念标签矩阵（日期×股票，PIT正确）")
    logger.info(f"     格式: 值为逗号分隔的概念代码，如 '886078,886033'")
    logger.info(f"  3. {long_file.name} - 长格式数据（原始成分股列表）")


if __name__ == '__main__':
    main()
