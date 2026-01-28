import pandas as pd
from pathlib import Path

def load_minute_csv(path_or_df):
    """
    Load minute-level OHLCV data from a CSV path or DataFrame.
    Expected columns (case-insensitive): datetime, open, high, low, close, volume
    datetime must be parseable; returned index is pd.DatetimeIndex.
    """
    if isinstance(path_or_df, pd.DataFrame):
        df = path_or_df.copy()
    else:
        df = pd.read_csv(path_or_df)
    # normalize column names
    df.columns = [c.lower() for c in df.columns]
    # try common names
    dt_col = 'datetime' if 'datetime' in df.columns else 'date' if 'date' in df.columns else df.columns[0]
    df[dt_col] = pd.to_datetime(df[dt_col])
    df = df.set_index(dt_col).sort_index()
    # keep expected columns if present (fallback to available)
    keep = []
    for c in ['open','high','low','close','volume']:
        if c in df.columns:
            keep.append(c)
    df = df[keep].astype(float)
    return df

def resample_to_minutes(df, rule='1T'):
    """
    Resample higher-frequency data to specified minute frequency (pandas rule, e.g., '1T', '5T').
    """
    ohlc = {
        'open': 'first',
        'high': 'max',
        'low': 'min',
        'close': 'last',
        'volume': 'sum'
    }
    cols = [c for c in ['open','high','low','close','volume'] if c in df.columns]
    ohlc = {c: ohlc[c] for c in cols}
    df_resampled = df.resample(rule).apply(ohlc).dropna()
    return df_resampled

if __name__ == "__main__":
    p = Path("../data/sample_minute.csv")
    if p.exists():
        df = load_minute_csv(p)
        print(df.head())
    else:
        print("请把分钟级 CSV 放到 data/ 目录并命名为 sample_minute.csv，或运行 scripts/generate_sample_data.py 生成合成样本进行测试。")