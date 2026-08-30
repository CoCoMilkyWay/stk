#!/usr/bin/env python3
"""
Check which trading days (from config/stock_days.json) have no archive file.

Usage:
    python check_missing_trading_days.py
"""

import json
from collections import defaultdict
from datetime import date
from pathlib import Path

from repair_utils import get_project_root, resolve_archive_path

ARCHIVE_BASE_DIR = "/media/chuyin/Disk/data/L2"

# Archives are sometimes downloaded in a different format and not yet
# converted to .rar (see fix_zip_to_rar.py / fix_7z_to_rar.py). Check those
# too, so we don't report "missing" for files that just need conversion.
OTHER_EXTENSIONS = [".zip", ".7z"]

# If the calendar file itself doesn't reach today, dates after its last entry
# are invisible to this check (not "checked and OK", just never looked at).
CALENDAR_STALE_THRESHOLD_DAYS = 7


def main():
    stock_days_path = get_project_root() / "config" / "stock_days.json"
    assert stock_days_path.exists(
    ), f"Missing calendar file: {stock_days_path}"

    with open(stock_days_path) as f:
        stock_days = json.load(f)

    calendar_last_date = max(d for d, _ in stock_days)
    today = date.today()
    calendar_last = date(*map(int, calendar_last_date.split('-')))
    stale_days = (today - calendar_last).days
    if stale_days > CALENDAR_STALE_THRESHOLD_DAYS:
        print(f"[!] WARNING: config/stock_days.json only goes up to {calendar_last_date}, "
              f"but today is {today.isoformat()} ({stale_days} days stale).")
        print(f"[!] Dates after {calendar_last_date} are NOT checked below - update "
              f"stock_days.json first (Task Database -> Data Overview -> update).\n")

    trading_dates = [d.replace('-', '')
                     for d, is_trading in stock_days if is_trading == '1']
    trading_dates.sort()

    truly_missing = []
    pending_conversion = defaultdict(list)  # ext -> [dates]

    for d in trading_dates:
        rar_path = resolve_archive_path(ARCHIVE_BASE_DIR, d)
        if rar_path.exists():
            continue

        other_ext = next((ext for ext in OTHER_EXTENSIONS
                          if rar_path.with_suffix(ext).exists()), None)
        if other_ext:
            pending_conversion[other_ext].append(d)
        else:
            truly_missing.append(d)

    print(f"Trading days checked: {len(trading_dates)}")
    print(f"Truly missing (no archive in any format): {len(truly_missing)}")
    for ext, dates in sorted(pending_conversion.items()):
        print(f"Pending conversion ({ext} -> .rar): {len(dates)}")
    print()

    def print_tree(dates, label):
        if not dates:
            return
        print(f"{label}:")
        tree = defaultdict(lambda: defaultdict(list))
        for d in dates:
            tree[d[:4]][d[4:6]].append(d[6:8])
        for year in sorted(tree):
            print(f"  {year}")
            for month in sorted(tree[year]):
                print(f"    {month}")
                print(f"      {' '.join(sorted(tree[year][month]))}")
        print()

    print_tree(truly_missing, "Truly missing")
    for ext, dates in sorted(pending_conversion.items()):
        print_tree(dates, f"Pending conversion ({ext})")


if __name__ == '__main__':
    main()
