"""
ST数据处理工具模块

提供ST股票判断、时间段提取、每日数据展开等功能
"""
import pandas as pd


def is_st_name(name: str) -> bool:
    """
    判断股票名称是否为ST股票

    Args:
        name: 股票名称

    Returns:
        bool: 是否为ST股票

    Examples:
        >>> is_st_name('ST华锦')
        True
        >>> is_st_name('*ST华锦')
        True
        >>> is_st_name('平安银行')
        False
    """
    if pd.isna(name):
        return False

    name = str(name).upper()

    # ST、*ST、S*ST、SST、S
    st_patterns = ['ST', '*ST', 'S*ST', 'SST']

    for pattern in st_patterns:
        if pattern in name:
            return True

    return False


def extract_st_periods(df_changes: pd.DataFrame) -> pd.DataFrame:
    """
    从名称变更记录中提取ST状态时间段

    Args:
        df_changes: 单只股票的名称变更记录
            必须包含列: ts_code, name, start_date, end_date

    Returns:
        DataFrame: ST状态时间段
            列: ts_code, name, st_type, entry_dt, remove_dt
    """
    if df_changes.empty:
        return pd.DataFrame(columns=['ts_code', 'name', 'st_type', 'entry_dt', 'remove_dt'])

    # 筛选ST相关的变更
    st_changes = df_changes[df_changes['name'].apply(is_st_name)].copy()

    if st_changes.empty:
        return pd.DataFrame(columns=['ts_code', 'name', 'st_type', 'entry_dt', 'remove_dt'])

    # 按开始日期排序
    st_changes = st_changes.sort_values('start_date')

    # 提取ST类型
    def extract_st_type(name):
        name = str(name).upper()
        if '*ST' in name or 'S*ST' in name:
            return '*ST'
        elif 'SST' in name:
            return 'SST'
        elif 'ST' in name:
            return 'ST'
        elif 'S' in name:
            return 'S'
        return 'ST'

    st_changes['st_type'] = st_changes['name'].apply(extract_st_type)

    # 重命名列
    result = st_changes[['ts_code', 'name', 'st_type', 'start_date', 'end_date']].copy()
    result.columns = ['ts_code', 'name', 'st_type', 'entry_dt', 'remove_dt']

    return result


def expand_st_to_daily(df_st_periods: pd.DataFrame, trade_dates: list) -> pd.DataFrame:
    """
    将ST时间段展开为每日数据

    Args:
        df_st_periods: ST状态时间段
            必须包含列: ts_code, name, st_type, entry_dt, remove_dt
        trade_dates: 所有交易日列表 (格式: YYYYMMDD字符串)

    Returns:
        DataFrame: 每日ST状态
            列: trade_date, ts_code, name, st_type
    """
    if df_st_periods.empty:
        return pd.DataFrame(columns=['trade_date', 'ts_code', 'name', 'st_type'])

    records = []

    for _, row in df_st_periods.iterrows():
        ts_code = row['ts_code']
        name = row['name']
        st_type = row['st_type']
        entry_dt = row['entry_dt']
        remove_dt = row['remove_dt']

        # 确定结束日期
        if pd.isna(remove_dt) or remove_dt == '' or remove_dt == 'None':
            # 如果没有结束日期，认为到现在都是ST
            end_date = trade_dates[-1]
        else:
            end_date = remove_dt

        # 筛选在时间段内的交易日
        for trade_date in trade_dates:
            if entry_dt <= trade_date <= end_date:
                records.append({
                    'trade_date': trade_date,
                    'ts_code': ts_code,
                    'name': name,
                    'st_type': st_type
                })

    return pd.DataFrame(records)
