#!/usr/bin/env python3
"""
按交易日逐日下载 A 股分钟线（akshare），在下载阶段把日期与时间合并成完整的 datetime，
然后合并所有日的数据并保存为 data/{symbol}_{freq}m_{prefix}_ts.csv。

用法示例：
  python scripts/download_akshare_minute.py --symbol 605069 --start 2025-10-01 --end 2026-01-27 --freq 1

说明：
- 脚本会优先尝试带 'sh' 前缀（适用于以 6 开头的代码），失败则尝试 'sz'。
- 对于 akshare 返回的时间字段（可能为 "时间" 仅含时分，或含完整日期时间），脚本会尝试智能解析并合成标准 pandas datetime。
- 若某日调用失败，脚本会记录并继续下一日，最终合并成功的部分并保存。
"""
import argparse
from pathlib import Path
import time
import pandas as pd


try:
    import akshare as ak
except Exception as e:
    raise SystemExit("请先安装 akshare：python -m pip install akshare\n详细错误：" + str(e))


def trading_days_between(start_date, end_date):
    s = pd.to_datetime(start_date)
    e = pd.to_datetime(end_date)
    days = pd.bdate_range(s, e, freq='C')  # Mon-Fri 简单近似
    return [d.strftime("%Y-%m-%d") for d in days]


def parse_and_normalize_minute_df(df_raw, day_str):
    """
    将 akshare 返回的一日或多日分钟数据标准化为包含 datetime (pd.Timestamp)、open,high,low,close,volume 的 DataFrame。
    - day_str: 下载请求对应的日期字符串（YYYY-MM-DD），用于把只有时间的 '时间' 列合并成完整 datetime。
    返回：df 或 None（无法解析时）
    """
    if df_raw is None or df_raw.empty:
        return None
    df = df_raw.copy()

    # Normalise column names (lower-case)
    cols = {c: c for c in df.columns}
    # make a mapping from lowercase chinese/english to actual column name
    lower_map = {c.lower(): c for c in df.columns}

    # Typical akshare column names: '时间','开盘','最高','最低','收盘','成交量'
    # Or english: 'datetime','open','high','low','close','volume'
    # Try to detect datetime-like column
    datetime_col = None
    for cand in ['datetime', 'time', '时间', 'date']:
        if cand in lower_map:
            datetime_col = lower_map[cand]
            break

    # If there is an index that is time like, reset index to get it
    if datetime_col is None:
        # try reset_index
        try:
            df = df.reset_index()
            for cand in ['datetime', 'time', '时间', 'date']:
                if cand in [c.lower() for c in df.columns]:
                    datetime_col = [c for c in df.columns if c.lower() == cand][0]
                    break
        except Exception:
            pass

    # If we still don't find, give up
    if datetime_col is None:
        return None

    # Clean whitespace
    df[datetime_col] = df[datetime_col].astype(str).str.strip()

    # If value contains a date (e.g., '2025-10-01 09:30:00'), parse directly
    sample = df[datetime_col].dropna().astype(str).head(5).tolist()
    contains_date = any(len(s.split()) > 1 or '-' in s for s in sample)

    if contains_date:
        # parse as full datetime
        try:
            df['datetime'] = pd.to_datetime(df[datetime_col], errors='coerce', infer_datetime_format=True)
        except Exception:
            df['datetime'] = pd.to_datetime(df[datetime_col], errors='coerce')
    else:
        # assume datetime_col contains only times like '09:30' or '09:30:00'
        # combine with provided day_str
        try:
            df['datetime'] = pd.to_datetime(day_str + ' ' + df[datetime_col], errors='coerce', infer_datetime_format=True)
        except Exception:
            df['datetime'] = pd.to_datetime(day_str + ' ' + df[datetime_col], errors='coerce')

    # If parsing failed (many NaT), try more heuristics
    if df['datetime'].isna().mean() > 0.5:
        # Try to interpret as numeric seconds/milliseconds from midnight (less likely)
        try:
            nums = pd.to_numeric(df[datetime_col], errors='coerce')
            if nums.notna().any():
                # numbers might be seconds since midnight: convert to timedelta and add day
                df['datetime'] = pd.to_datetime(day_str) + pd.to_timedelta(nums, unit='s')
        except Exception:
            pass

    # Drop rows that failed parsing
    df = df.dropna(subset=['datetime'])
    if df.empty:
        return None

    # Ensure standard OHLCV names exist, map chinese names
    rename_map = {}
    if '开盘' in df.columns:
        rename_map['开盘'] = 'open'
    if '最高' in df.columns:
        rename_map['最高'] = 'high'
    if '最低' in df.columns:
        rename_map['最低'] = 'low'
    if '收盘' in df.columns:
        rename_map['收盘'] = 'close'
    if '成交量' in df.columns:
        rename_map['成交量'] = 'volume'
    # english name fixups
    for en in ['open', 'high', 'low', 'close', 'volume']:
        if en not in df.columns and en.capitalize() in df.columns:
            rename_map[en.capitalize()] = en

    if rename_map:
        df = df.rename(columns=rename_map)

    # If close column in chinese like '收盘价' or other variants
    # fallback mapping based on lowercase matches
    lowermap = {c.lower(): c for c in df.columns}
    for want in ['open','high','low','close','volume']:
        if want not in df.columns and want in lowermap:
            df = df.rename(columns={lowermap[want]: want})

    # Keep only datetime + OHLCV if exist
    keep = ['datetime'] + [c for c in ['open','high','low','close','volume'] if c in df.columns]
    df = df[keep].copy()
    # Set datetime as index
    df = df.set_index('datetime').sort_index()

    # Ensure numeric types
    for c in ['open','high','low','close','volume']:
        if c in df.columns:
            df[c] = pd.to_numeric(df[c], errors='coerce')

    # Drop rows with NA in price columns
    price_cols = [c for c in ['open','high','low','close'] if c in df.columns]
    df = df.dropna(subset=price_cols)

    return df


def download_minute_range_and_save(symbol_code, start_date, end_date, freq=1, out_dir="data", sleep=0.4):
    out_dir = Path(out_dir)
    out_dir.mkdir(parents=True, exist_ok=True)

    prefixes = ["sh", "sz"] if symbol_code.startswith("6") else ["sz", "sh"]
    days = trading_days_between(start_date, end_date)

    last_successful = None
    tried_prefixes = set()
    for prefix in prefixes:
        tried_prefixes.add(prefix)
        ak_symbol = f"{prefix}{symbol_code}"
        print(f"尝试前缀 {prefix}，标的 {ak_symbol}，按天下载 {len(days)} 个交易日，freq={freq}m")
        frames = []
        failed_days = []
        for d in days:
            try:
                # ak.stock_zh_a_minute 有时返回当日分钟（带时间或仅带时分）
                period = str(freq)
                df_raw = ak.stock_zh_a_minute(symbol=ak_symbol, period=period, adjust="")
                # parse and normalize
                df_day = parse_and_normalize_minute_df(df_raw, d)
                if df_day is None or df_day.empty:
                    # 有时 akshare 返回空或格式不同；记录并继��
                    print(f"  {ak_symbol} 日 {d} 未返回有效数据，跳过")
                    failed_days.append(d)
                    time.sleep(sleep)
                    continue
                # Filter to that day if returned data covers multiple days
                df_day = df_day[(df_day.index.date == pd.to_datetime(d).date())]
                if df_day.empty:
                    print(f"  {ak_symbol} 日 {d} 解析后无该日数据，跳过")
                    failed_days.append(d)
                    time.sleep(sleep)
                    continue
                frames.append(df_day)
                last_successful = prefix
                time.sleep(sleep)
            except Exception as e:
                print(f"  {ak_symbol} 日 {d} 下载失败: {e}")
                failed_days.append(d)
                time.sleep(sleep)

        if frames:
            # 合并并去重
            result = pd.concat(frames).sort_index()
            result = result[~result.index.duplicated(keep='first')]
            out_path = out_dir / f"{symbol_code}_{freq}m_{prefix}_ts.csv"
            result.to_csv(out_path)
            print(f"使用前缀 {prefix} 成功，已保存 {len(result)} 行到 {out_path}")
            return str(out_path)

        else:
            print(f"前缀 {prefix} 未能获取到有效数据，尝试下一个前缀（若有）...")

    raise RuntimeError(f"所有前缀 {tried_prefixes} 都未获取到有效分钟数据。请检查 symbol 或改用 tushare/券商数据源。")



def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--symbol", required=True, help="股票代码，不带交易所前缀，例如 605069")
    parser.add_argument("--start", required=True, help="起始日期 YYYY-MM-DD")
    parser.add_argument("--end", required=True, help="结束日期 YYYY-MM-DD")
    parser.add_argument("--freq", default=1, type=int, help="分钟频率，例如 1、5、15")
    parser.add_argument("--out", default="data", help="输出目录，默认 data/")
    parser.add_argument("--sleep", default=0.4, type=float, help="每次请求后的 sleep 秒数，避免限流")
    args = parser.parse_args()

    print("开始下载：", args.symbol, args.start, "->", args.end, f"freq={args.freq}m")
    try:
        out_path = download_minute_range_and_save(args.symbol, args.start, args.end, freq=args.freq, out_dir=args.out, sleep=args.sleep)
        print("下载并保存完成，文件：", out_path)
    except Exception as e:
        print("下载失败：", e)
        raise SystemExit(1)


if __name__ == "__main__":
    main()


