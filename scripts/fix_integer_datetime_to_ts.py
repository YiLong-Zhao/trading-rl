#!/usr/bin/env python3
"""
Fix CSVs where 'datetime' column contains integer sequence (0,1,2,...)
by replacing it with actual trading-minute timestamps constructed from
a start/end date range.

Usage:
    python scripts/fix_integer_datetime_to_ts.py data/605069_1m_sh.csv --start 2025-10-01 --end 2026-01-27

This will produce data/605069_1m_sh_clean.csv by default.
"""
import argparse
from pathlib import Path
import pandas as pd
import numpy as np

def trading_minutes_for_days(days):
    parts = []
    for d in days:
        morning = pd.date_range(f"{d} 09:30", f"{d} 11:30", freq="1min")
        afternoon = pd.date_range(f"{d} 13:00", f"{d} 15:00", freq="1min")
        parts.append(morning.append(afternoon))
    if parts:
        return pd.DatetimeIndex(np.concatenate([p.values for p in parts]))
    else:
        return pd.DatetimeIndex([])

def extend_days_until_length(days, required_len):
    # extend by adding more business days after the last day until total minutes >= required_len
    cur_days = list(days)
    while True:
        ts = trading_minutes_for_days(cur_days)
        if len(ts) >= required_len:
            return ts
        # extend by one business day after last
        last = pd.to_datetime(cur_days[-1])
        # next business day (skip weekends) by adding 1 day until weekday <5
        nxt = last + pd.Timedelta(days=1)
        while nxt.weekday() >= 5:
            nxt += pd.Timedelta(days=1)
        cur_days.append(nxt.strftime("%Y-%m-%d"))

def build_minutes_sequence(start_date, end_date, required_len):
    # initial business days (Mon-Fri)
    days = pd.bdate_range(start=start_date, end=end_date, freq='C').strftime("%Y-%m-%d").tolist()
    ts = trading_minutes_for_days(days)
    if len(ts) >= required_len:
        return ts[:required_len]
    # extend forward if not enough
    ts_ext = extend_days_until_length(days, required_len)
    return ts_ext[:required_len]

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("infile", help="input CSV with integer 'datetime' column")
    parser.add_argument("--start", required=True, help="start date YYYY-MM-DD (used to build trading minutes)")
    parser.add_argument("--end", required=True, help="end date YYYY-MM-DD (used to build trading minutes; may be extended)")
    parser.add_argument("--out", default=None, help="output csv path (default: infile stem + _ts.csv in same folder)")
    args = parser.parse_args()

    p = Path(args.infile)
    if not p.exists():
        raise SystemExit(f"输入文件不存在: {p}")

    df = pd.read_csv(p)
    if 'datetime' not in df.columns:
        raise SystemExit("输入文件没有 'datetime' 列，无法处理。")

    # check if datetime column is integer-like sequence
    try:
        sample_vals = df['datetime'].dropna().astype(int).head(10).tolist()
        is_int_seq = all(isinstance(x, (int, np.integer)) for x in sample_vals)
    except Exception:
        is_int_seq = False

    if not is_int_seq:
        raise SystemExit("检测到 'datetime' 列似乎不是整数序列；请确认输入文件格式。")

    n = len(df)
    print(f"输入行数: {n}，将基于给定日期构建至少 {n} 个分钟时间戳...")

    ts = build_minutes_sequence(args.start, args.end, n)
    if len(ts) < n:
        raise SystemExit("无法构建足够的分钟时间戳，请检查 start/end 或数据是否超出合理范围。")

    # assign new datetime index
    df['datetime'] = ts[:n]
    df = df.set_index('datetime').sort_index()
    # ensure ohlcv columns present
    out_path = Path(args.out) if args.out else p.parent / (p.stem + "_ts.csv")
    df.to_csv(out_path)
    print("已保存：", out_path)
    print("新索引范围：", df.index.min(), "->", df.index.max(), " 共行:", len(df))

if __name__ == "__main__":
    main()