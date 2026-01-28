# name=scripts/prepare_minute_csv.py
import pandas as pd
from pathlib import Path
import argparse

def normalize_csv(in_path, out_path=None):
    p = Path(in_path)
    if out_path is None:
        out_path = p.parent / (p.stem + "_clean.csv")
    df = pd.read_csv(p)
    # try common datetime column names
    dt_candidates = [c for c in df.columns if c.lower() in ('datetime','time','日期','时间','date','time')]
    if not dt_candidates:
        raise ValueError("无法识别 datetime 列，��始列名：" + ",".join(df.columns))
    dt_col = dt_candidates[0]
    df[dt_col] = pd.to_datetime(df[dt_col])
    df = df.set_index(dt_col).sort_index()
    # map chinese column names if present
    rename_map = {}
    cols_lower = {c.lower():c for c in df.columns}
    if '开盘' in df.columns or '开盘价' in cols_lower:
        for k,v in [('开盘','open'),('开盘价','open'),('收盘','close'),('收盘价','close'),
                    ('最高','high'),('最低','low'),('成交量','volume')]:
            if k in df.columns:
                rename_map[k] = v
            elif k in cols_lower:
                rename_map[cols_lower[k]] = v
    # also handle english names with different casing
    for k in ['open','high','low','close','volume']:
        if k not in df.columns and k.capitalize() in df.columns:
            rename_map[k.capitalize()] = k
    if rename_map:
        df = df.rename(columns=rename_map)
    # keep only desired columns if available
    keep = [c for c in ['open','high','low','close','volume'] if c in df.columns]
    df = df[keep].copy()
    # forward/backfill small gaps, then drop rows with any NA
    df = df.ffill().bfill().dropna()
    df.to_csv(out_path)
    print("Saved cleaned csv:", out_path, "rows:", len(df))
    return out_path

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("infile")
    parser.add_argument("--out", default=None)
    args = parser.parse_args()
    normalize_csv(args.infile, args.out)