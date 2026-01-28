"""
生成一个合成的分钟级 OHLCV CSV，用于快速跑通 pipeline（A 股通常交易日 09:30-11:30 & 13:00-15:00）。
输出 path: data/sample_minute.csv
"""
import pandas as pd
import numpy as np
from pathlib import Path

def generate_single_day(date="2025-01-02"):
    # 生成交易日的分钟时间索引（简单版，不考虑午休以外的假期）
    morning = pd.date_range(f"{date} 09:30", f"{date} 11:30", freq="1min")
    afternoon = pd.date_range(f"{date} 13:00", f"{date} 15:00", freq="1min")
    idx = morning.append(afternoon)
    n = len(idx)
    # 生成价格随机游走
    price = 100 + np.cumsum(np.random.normal(scale=0.02, size=n))
    # 构造 OHLCV
    open_p = price + np.random.normal(scale=0.005, size=n)
    close_p = price + np.random.normal(scale=0.005, size=n)
    high_p = np.maximum(open_p, close_p) + np.abs(np.random.normal(scale=0.01, size=n))
    low_p = np.minimum(open_p, close_p) - np.abs(np.random.normal(scale=0.01, size=n))
    volume = np.random.randint(100, 1000, size=n)
    df = pd.DataFrame({
        "datetime": idx,
        "open": open_p,
        "high": high_p,
        "low": low_p,
        "close": close_p,
        "volume": volume
    })
    return df

def generate_multiple_days(n_days=5, start_date="2025-01-02"):
    start = pd.to_datetime(start_date)
    frames = []
    for i in range(n_days):
        d = (start + pd.Timedelta(days=i)).strftime("%Y-%m-%d")
        frames.append(generate_single_day(d))
    df = pd.concat(frames).reset_index(drop=True)
    return df

if __name__ == "__main__":
    Path("../data/data").mkdir(parents=True, exist_ok=True)
    df = generate_multiple_days(n_days=10, start_date="2025-01-02")
    df.to_csv("data/sample_minute.csv", index=False)
    print("已生成 data/sample_minute.csv，样本行数：", len(df))