import os
import numpy as np
import pandas as pd
import struct
import multiprocessing as mp
from collections import defaultdict
import re
from typing import Dict, List, Tuple, Any, Optional, Union
from pathlib import Path
import zipfile
import shutil
import tempfile
from tqdm import tqdm
import zlib

# Binary format optimizations for single stock asset:
# Total record size: 54 bytes
binary_format = {
    'sync': ('bool', False),
    'date': ('uint8', True),
    # seconds only - uint16 sufficient for seconds in day
    'time_s': ('uint16', True),
    # 0.01 precision - int16 sufficient for stock prices
    'latest_price_tick': ('int16', True),
    # max 255 trades per second - uint8 sufficient
    'trade_count': ('uint8', False),
    'turnover': ('uint32', False),  # RMB amounts - uint32 sufficient
    'volume': ('uint16', False),  # unit is 100 shares - uint16 sufficient
    'bid_price_ticks': ('int16[5]', True),  # 0.01 precision - int16 sufficient
    # unit is 100 shares - uint16 sufficient
    'bid_volumes': ('uint16[5]', False),
    'ask_price_ticks': ('int16[5]', True),  # 0.01 precision - int16 sufficient
    # unit is 100 shares - uint16 sufficient
    'ask_volumes': ('uint16[5]', False),
    'direction': ('uint8', False),
}

# struct format string generation helper (extensible)
type_to_struct_fmt: Dict[str, str] = {
    'bool': '?',
    'uint8': 'B',
    'int8': 'b',
    'uint16': 'H',
    'int16': 'h',
    'uint32': 'I',
    'int32': 'i',
    'uint64': 'Q',
    'int64': 'q',
}

max_trade_count = np.iinfo(np.uint8).max
max_turnover = np.iinfo(np.uint32).max


def get_struct_fmt(fmt_dict: Dict[str, Tuple[str, bool]]) -> str:
    fmt = ''
    for k, (t, is_diff) in fmt_dict.items():
        if '[' in t:
            base_type, length = t.rstrip(']').split('[')
            length = int(length)
            fmt += type_to_struct_fmt[base_type] * length
        else:
            fmt += type_to_struct_fmt[t]
    return fmt


record_struct_fmt = get_struct_fmt(binary_format)
record_struct = struct.Struct(record_struct_fmt)


def price_to_tick(price: float, tick: float = 0.01) -> int:
    return int(round(price / tick))


def type_to_np_dtype(t: str) -> np.dtype:
    # Simple mapping
    mapping = {
        'bool': np.bool_,
        'uint8': np.uint8,
        'int8': np.int8,
        'uint16': np.uint16,
        'int16': np.int16,
        'uint32': np.uint32,
        'int32': np.int32,
        'uint64': np.uint64,
        'int64': np.int64,
        'float32': np.float32,
        'float64': np.float64,
    }
    if '[' in t:
        base = t.split('[')[0]
        return mapping[base]
    else:
        return mapping[t]


# Cache expensive operations
CHINESE_NUMS: List[str] = ['一', '二', '三', '四', '五']
BID_ASK_MAPPING: Dict[str, Tuple[str, str]] = {
    '买': ('bid', '买'),
    '卖': ('ask', '卖')
}

# Pre-computed numpy dtype for structured array
STRUCTURED_DTYPE: List[Tuple[str, Any, ...]] = []
for k, (t, is_diff) in binary_format.items():
    if '[' in t:
        base_type, length = t.rstrip(']').split('[')
        length = int(length)
        np_type = type_to_np_dtype(base_type)
        STRUCTURED_DTYPE.append((k, np_type, length))
    else:
        np_type = type_to_np_dtype(t)
        STRUCTURED_DTYPE.append((k, np_type))

# Optimal CSV reading dtypes - eliminates redundant type conversions
CSV_DTYPES: Dict[str, Any] = {
    '市场代码': 'category',
    '证券代码': 'category',
    '时间': 'object',  # Will be converted to datetime
    '最新价': np.float64,
    '成交笔数': np.uint8,
    '成交额': np.float64,
    '成交量': np.uint16,
    '方向': 'category',  # More memory efficient for categorical data
    # Bid prices and volumes
    '买一价': np.float64, '买二价': np.float64, '买三价': np.float64, '买四价': np.float64, '买五价': np.float64,
    '买一量': np.uint16, '买二量': np.uint16, '买三量': np.uint16, '买四量': np.uint16, '买五量': np.uint16,
    # Ask prices and volumes
    '卖一价': np.float64, '卖二价': np.float64, '卖三价': np.float64, '卖四价': np.float64, '卖五价': np.float64,
    '卖一量': np.uint16, '卖二量': np.uint16, '卖三量': np.uint16, '卖四量': np.uint16, '卖五量': np.uint16,
}

# Pre-compute diff fields list for maximum efficiency
DIFF_FIELDS: List[str] = [
    k for k, (_, is_diff) in binary_format.items() if is_diff]


def parse_snapshot_df(df: pd.DataFrame) -> pd.DataFrame:
    """
    Preprocess DataFrame: calculate all necessary fields efficiently
    Data types are already optimized from CSV reading - minimal conversions needed
    """
    # Input validation
    required_columns = ['时间', '最新价', '方向', '成交笔数', '成交额', '成交量']
    missing_cols = [col for col in required_columns if col not in df.columns]
    assert not missing_cols, f"Missing required columns: {missing_cols}"

    # Optimized datetime processing - data already in correct types
    datetime_col = pd.to_datetime(df['时间'], cache=True)
    df['date_int'] = datetime_col.dt.day.astype(np.uint8)

    # Highly efficient time calculation - direct numpy operations (seconds only)
    time_components = datetime_col.dt
    df['time_s'] = (time_components.hour * 3600 +
                    time_components.minute * 60 +
                    time_components.second).astype(np.uint16)

    # Direct price tick conversion with 0.01 precision - multiply by 100
    df['latest_price_tick'] = np.round(df['最新价'] * 100.0).astype(np.int16)

    # Ultra-efficient vectorized bid/ask processing - data already in correct types
    for side in ['买', '卖']:
        # All price and volume columns are already optimal types from CSV reading
        for idx, cn in enumerate(CHINESE_NUMS, 1):
            price_col = f'{side}{cn}价'
            volume_col = f'{side}{cn}量'
            if price_col in df.columns and volume_col in df.columns:
                # Direct operation with 0.01 precision - multiply by 100
                df[f'{side}{idx}价_tick'] = np.round(
                    df[price_col] * 100.0).astype(np.int16)
                # Volume columns already uint16 from CSV reading - just rename
                df[f'{side}{idx}量'] = df[volume_col]

    # Optimized direction mapping - handle categorical data properly
    direction_map: Dict[Union[str, int], int] = {'买': 0, 'B': 0, 0: 0, '0': 0,
                                                 '卖': 1, 'S': 1, 1: 1, '1': 1, '-': 2}
    # Convert to string first to avoid categorical issues, then map
    direction_series = df['方向'].astype(str).map(
        direction_map).fillna(2).astype(np.uint8)
    df['direction'] = direction_series

    return df


def diagnose_df(df: pd.DataFrame, col_name: str) -> None:
    col = df[col_name]
    # Only debug if there are problematic values
    has_nan = col.isna().any()
    has_inf = np.isinf(col).any()
    has_negative = (col < 0).any()
    if has_nan or has_inf or has_negative:
        print(f"WARNING: Found problematic values in {col_name} column:")
        print(f"  Has NaN: {has_nan} ({col.isna().sum()} values)")
        print(f"  Has inf: {has_inf} ({np.isinf(col).sum()} values)")
        print(f"  Has negative: {has_negative} ({(col < 0).sum()} values)")
        print(f"  Sample problematic values: {col[(col.isna()) | (np.isinf(col)) | (col < 0)].head(5).tolist()}")
        print(f"  Min value: {col.min()}")
        print(f"  Max value: {col.max()}")


def encode_month_snapshots(df: pd.DataFrame) -> bytes:
    """
    Encode entire month's snapshots, return compressed bytes
    Extremely efficient for C++ parser - single decompression per month
    """
    n = len(df)
    if n == 0:
        return b''

    # Validate input
    assert n > 0, "DataFrame cannot be empty"

    # Preprocess DataFrame first
    df = parse_snapshot_df(df)

    # Create numpy structured array using pre-computed dtype with optimal memory layout
    arr = np.empty(n, dtype=STRUCTURED_DTYPE)

    # Fill sync field efficiently - only first record is sync
    arr['sync'] = False
    arr['sync'][0] = True

    # Fill base fields with zero-copy operations - data already in optimal types
    arr['date'] = df['date_int'].values
    arr['time_s'] = df['time_s'].values
    arr['latest_price_tick'] = df['latest_price_tick'].values
    # Convert to uint8 for trade count (sufficient for trades per second)
    arr['trade_count'] = np.clip(df['成交笔数'].values, 0, max_trade_count).astype(np.uint8)
    # Turnover in RMB - uint32 sufficient (no multiplication needed)
    arr['turnover'] = np.clip(df['成交额'].values, 0, max_turnover).astype(np.uint32)

    # Volume in units of 100 shares - uint16 sufficient
    arr['volume'] = df['成交量'].values.astype(np.uint16)
    arr['direction'] = df['direction'].values

    # Ultra-efficiently fill bid/ask arrays - data already in optimal types from CSV reading
    for side, prefix in BID_ASK_MAPPING.values():
        # Direct vectorized assignment - no conversions needed
        price_cols = [f"{prefix}{i+1}价_tick" for i in range(5)]
        volume_cols = [f"{prefix}{i+1}量" for i in range(5)]

        # Zero-copy assignment since data is already in correct types
        arr[f'{side}_price_ticks'] = df[price_cols].values
        arr[f'{side}_volumes'] = df[volume_cols].values

    # Apply differential encoding efficiently using pre-computed diff fields
    # More efficient across entire month for better compression
    for field in DIFF_FIELDS:
        # In-place differential encoding for maximum efficiency
        if len(arr[field].shape) > 1:  # Array field
            np.subtract(arr[field][1:], arr[field][:-1], out=arr[field][1:])
        else:  # Scalar field
            np.subtract(arr[field][1:], arr[field][:-1], out=arr[field][1:])

    # np.set_printoptions(threshold=np.inf)
    # print(arr[:100])
    # assert False

    # Convert structured array to bytes and compress once for entire month
    raw_bytes = arr.tobytes()
    compressed_bytes = zlib.compress(raw_bytes, level=6)  # Optimal compression for tick data

    return compressed_bytes


def extract_symbol_from_filename(filename: str) -> Optional[str]:
    """Extract symbol code from filename (case-insensitive for symbol prefix)"""
    # Match sh600000_20250603.csv or SH600000_20250603.csv format
    match = re.match(r'([a-z]{2}\d{6})_\d{8}\.csv', filename, re.IGNORECASE)
    if match:
        # normalize to lower-case for consistency
        return match.group(1).lower()
    return None


def group_files_by_symbol(month_folder_path: Union[str, Path]) -> Dict[str, List[str]]:
    """Group files by symbol from nested daily directories"""
    symbol_files = defaultdict(list)

    # Walk through all subdirectories in the month folder
    for root, dirs, files in os.walk(month_folder_path):
        for filename in files:
            if not filename.endswith('.csv'):
                continue
            symbol = extract_symbol_from_filename(filename)
            if symbol:
                # Store full relative path from month folder
                rel_path = os.path.relpath(os.path.join(
                    root, filename), month_folder_path)
                symbol_files[symbol].append(rel_path)

    return symbol_files


def process_symbol_files(args: Tuple[str, List[str], Union[str, Path], Union[str, Path], str, int, int]) -> str:
    """Process all files for a single symbol - combine daily data then compress once"""
    symbol, files, month_folder_path, output_dir, month_name, current_idx, total_symbols = args

    # Input validation
    assert symbol and isinstance(symbol, str), "Symbol must be a non-empty string"
    assert files and isinstance(files, list), "Files must be a non-empty list"

    # Create month subdirectory under year directory
    month_output_dir = os.path.join(output_dir, month_name)
    os.makedirs(month_output_dir, exist_ok=True)
    output_path = os.path.join(month_output_dir, f"{symbol}.bin")

    # Sort files by date (using the full path for proper sorting)
    sorted_files = sorted(files)

    # Collect all daily data first
    month_dataframes = []
    for rel_path in sorted_files:
        file_path = os.path.join(month_folder_path, rel_path)
        # Ultra-efficient CSV reading with pre-defined optimal dtypes
        df = pd.read_csv(file_path, encoding='gbk',
                         dtype=CSV_DTYPES, low_memory=False)
        month_dataframes.append(df)

    # Combine all daily data into single dataframe
    if month_dataframes:
        combined_df = pd.concat(month_dataframes, ignore_index=True)
        record_count = len(combined_df)

        # Encode entire month's data and compress once
        month_data_bytes = encode_month_snapshots(combined_df)

        # Include record count in filename for efficient C++ parsing
        output_path_with_count = os.path.join(month_output_dir, f"{symbol}_{record_count}.bin")

        # Write compressed month data
        with open(output_path_with_count, 'wb') as output_bin:
            output_bin.write(month_data_bytes)

    return symbol


def process_month_folder(month_folder_path: str, asset_symbol: str) -> None:
    """Process one month folder for a single asset and output single binary file"""
    month_folder = Path(month_folder_path)
    asset_symbol = asset_symbol.lower()
    
    if not month_folder.exists():
        raise ValueError(f"Month folder does not exist: {month_folder_path}")

    # Group files by symbol
    symbol_files = group_files_by_symbol(month_folder)

    if asset_symbol not in symbol_files:
        print(f"No data found for asset {asset_symbol} in {month_folder}")
        return

    files = sorted(symbol_files[asset_symbol])
    print(f"Processing asset: {asset_symbol}, {len(files)} files")

    # Collect daily data for this asset
    dataframes = []
    for rel_path in files:
        file_path = month_folder / rel_path
        df = pd.read_csv(file_path, encoding='gbk', dtype=CSV_DTYPES, low_memory=False)
        dataframes.append(df)

    if not dataframes:
        print("No data found to process")
        return

    combined_df = pd.concat(dataframes, ignore_index=True)
    record_count = len(combined_df)
    print(f"Total records: {record_count}")

    # Encode and compress
    month_data_bytes = encode_month_snapshots(combined_df)

    # Generate output filename
    output_file = Path(f"{asset_symbol}_{record_count}.bin")
    with open(output_file, 'wb') as output_bin:
        output_bin.write(month_data_bytes)

    print(f"Output written to: {output_file}")
    print(f"Compressed size: {len(month_data_bytes)} bytes")


if __name__ == '__main__':
    # Specify your parameters here
    month_folder_path = "D:/data/A_stock/A_L1/2017/201701"
    asset_symbol = "SH600000"

    print(f"Processing month folder: {month_folder_path}")
    print(f"Asset symbol: {asset_symbol}")

    process_month_folder(month_folder_path, asset_symbol)
    print("Processing completed!")
