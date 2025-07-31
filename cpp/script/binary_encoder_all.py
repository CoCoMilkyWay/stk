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

        try:
            # Encode entire month's data and compress once
            month_data_bytes = encode_month_snapshots(combined_df)

            # Include record count in filename for efficient C++ parsing
            output_path_with_count = os.path.join(month_output_dir, f"{symbol}_{record_count}.bin")

            # Write compressed month data
            with open(output_path_with_count, 'wb') as output_bin:
                output_bin.write(month_data_bytes)
        except ValueError as e:
            if "time data" in str(e):
                print(f"SKIPPING {symbol} in {month_name}: datetime parsing failed - {e}")
                return f"{symbol} (SKIPPED)"
            else:
                raise

    return symbol


def decompress_zip_file(args: Tuple[Path, Path, str]) -> Tuple[str, Path]:
    """Decompress a single zip file and return month name and extracted folder path"""
    zip_file, temp_path, year = args
    
    # Convert "201701" to "2017_01" format
    zip_stem = zip_file.stem  # e.g., "201701" from "201701.zip"
    if len(zip_stem) == 6:  # Format: YYYYMM
        year_part = zip_stem[:4]
        month = zip_stem[4:6]
        month_name = f"{year_part}_{month}"
    else:
        month_name = zip_stem  # Fallback to original name if format doesn't match

    # Create temp directory for this month
    month_temp_dir = temp_path / month_name
    month_temp_dir.mkdir(exist_ok=True)

    # Decompress the zip and flatten the structure
    print(f"Decompressing {zip_file.name} to {month_temp_dir}")
    with zipfile.ZipFile(zip_file, 'r') as zip_ref:
        # Extract all files, but skip the top-level directory structure
        for item in zip_ref.namelist():
            # Skip the root directory (e.g., "201301/")
            if item.endswith('/'):
                continue
            
            # Extract to month_temp_dir, removing the top-level directory prefix
            # e.g., "201301/20130104/file.csv" -> "20130104/file.csv"
            relative_path = '/'.join(item.split('/')[1:])
            if relative_path:
                # Create the target directory structure
                target_path = month_temp_dir / relative_path
                target_path.parent.mkdir(parents=True, exist_ok=True)
                
                # Extract directly to the flattened path
                with zip_ref.open(item) as source:
                    with open(target_path, 'wb') as target:
                        shutil.copyfileobj(source, target)

    # The extracted_month_folder is now the month_temp_dir itself
    return month_name, month_temp_dir


def process_all_zips(snapshot_zip_dir: str, output_dir: str) -> None:
    """
    Process all zip files from snapshot directory to binary format
    """
    snapshot_zip_path = Path(snapshot_zip_dir)
    output_path = Path(output_dir)

    if not snapshot_zip_path.exists():
        raise ValueError(
            f"Snapshot zip directory does not exist: {snapshot_zip_dir}")

    # Create output directory
    output_path.mkdir(parents=True, exist_ok=True)

    # Get all year folders
    year_folders = [f for f in snapshot_zip_path.iterdir() if f.is_dir()]
    year_folders.sort()

    if not year_folders:
        print(f"No year folders found in {snapshot_zip_path}")
        return

    print(
        f"Found {len(year_folders)} year folders: {[f.name for f in year_folders]}")

    num_processes = mp.cpu_count()

    # Process each year folder
    for year_folder in year_folders:
        year = year_folder.name
        print(f"\nProcessing year {year}...")

        # Use root output directory directly (no year subdirectory)
        year_output_dir = output_path

        # Get all zip files in the year folder
        zip_files = list(year_folder.glob("*.zip"))
        zip_files.sort()  # Process in chronological order

        if not zip_files:
            print(f"No zip files found in {year_folder}")
            continue

        print(f"Found {len(zip_files)} month zip files in {year}")

        # Create local temp directory for this year
        local_temp_dir = Path("temp_processing")
        local_temp_dir.mkdir(exist_ok=True)
        temp_path = local_temp_dir

        # Parallel decompress all zip files for the year
        print(f"Parallel decompressing {len(zip_files)} zip files...")
        decompress_args = [(zip_file, temp_path, year) for zip_file in zip_files]
        
        with mp.Pool(processes=num_processes) as pool:
            decompress_results = list(tqdm(
                pool.imap(decompress_zip_file, decompress_args),
                total=len(zip_files),
                desc=f"Decompressing {year}",
                position=0,
                leave=False
            ))

        print(f"Successfully decompressed all {len(zip_files)} zip files for {year}")

        # Process each month sequentially (but with parallel symbol processing)
        for month_name, extracted_month_folder in decompress_results:
            print(f"Processing {month_name}...")

            # Group files by symbol
            symbol_files = group_files_by_symbol(extracted_month_folder)

            if not symbol_files:
                print(f"No valid CSV files found in {extracted_month_folder}")
                continue

            print(f"Found {len(symbol_files)} symbols in {extracted_month_folder}")

            # Process each symbol with multiprocessing and progress bar for the month
            total_symbols = len(symbol_files)

            # Prepare arguments for multiprocessing
            process_args = []
            for i, (symbol, files) in enumerate(symbol_files.items()):
                process_args.append(
                    (symbol, files, extracted_month_folder, year_output_dir, month_name, i, total_symbols))

            # Use multiprocessing with progress bar
            with mp.Pool(processes=num_processes) as pool:
                # Use imap to maintain order and enable progress tracking
                results = list(tqdm(
                    pool.imap(process_symbol_files, process_args),
                    total=total_symbols,
                    desc=f"Processing {month_name}",
                    position=0,
                    leave=False
                ))

            print(f"Successfully processed {month_name}")

        # Clean up the local temp directory after processing all months
        if temp_path.exists():
            shutil.rmtree(temp_path)
            print(f"Cleaned up local temp directory: {temp_path}")


if __name__ == '__main__':

    # Configuration
    # snapshot_zip_dir = "D:/data/A_stock/A_L1"
    # output_dir = "D:/data/A_stock/A_L1_binary"
    
    snapshot_zip_dir = "/home/chuyin/work/data/A_stock/snapshot_zip"
    output_dir = "/home/chuyin/work/data/A_stock/snapshot_binary"

    print("Starting to process all zip files...")
    print(f"Input directory: {snapshot_zip_dir}")
    print(f"Output directory: {output_dir}")

    process_all_zips(snapshot_zip_dir, output_dir)

    print("\nAll processing completed!")
