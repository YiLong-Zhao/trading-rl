import sys
import pathlib
import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
from src.data_loader import load_minute_csv
from src.market_env import MarketEnv

# 自动选择 CSV：优先使用 data/*_clean.csv，其次 data/sample_minute.csv
proj_root = pathlib.Path(__file__).resolve().parent.parent
data_dir = proj_root / "data"
out_dir = proj_root / "outputs"
out_dir.mkdir(parents=True, exist_ok=True)

clean_files = sorted(list(data_dir.glob("*_clean.csv")))
if clean_files:
    CSV_PATH = str(clean_files[-1])
else:
    sample = data_dir / "sample_minute.csv"
    if sample.exists():
        CSV_PATH = str(sample)
    else:
        print("没有找到 *_clean.csv 或 sample_minute.csv，请先生成或下载数据至 data/ 目录。")
        sys.exit(1)

print("使用数据文件：", CSV_PATH)

# 加载并准备数据
df = load_minute_csv(CSV_PATH)
df['return'] = df['close'].pct_change().fillna(0)
print("样本区间：", df.index.min(), "到", df.index.max())
print("样本行数：", len(df))

# 保存价格图
plt.figure(figsize=(12,4))
plt.plot(df.index, df['close'], label='close')
plt.title("Price")
plt.xlabel("datetime")
plt.ylabel("price")
plt.grid(True)
price_fig = out_dir / "price.png"
plt.tight_layout()
plt.savefig(price_fig)
plt.close()
print("已保存价格图：", price_fig)

# 特征工程（简单 SMA 示例）
SMA_SHORT = 5
SMA_LONG = 20
df['sma_short'] = df['close'].rolling(SMA_SHORT).mean()
df['sma_long'] = df['close'].rolling(SMA_LONG).mean()
df = df.dropna()

# 向量化 SMA 交叉策略
signal = np.where(df['sma_short'] > df['sma_long'], 1, -1)
positions = pd.Series(signal, index=df.index).shift(1).fillna(0)
returns = df['return'] * positions
# 交易成本示例
TRANSACTION_COST = 0.0005
pos_change = positions.diff().abs().fillna(0)
returns_net = returns - pos_change * TRANSACTION_COST
equity = (1 + returns_net).cumprod()

# 计算简单统计（总收益、最大回撤）
final_equity = equity.iloc[-1]
total_return = final_equity - 1.0

def max_drawdown(cum_returns):
    roll_max = cum_returns.cummax()
    drawdown = (cum_returns - roll_max) / roll_max
    return drawdown.min()

maxdd = max_drawdown(equity)

print(f"策略最后净值: {final_equity:.6f}, 总收益: {total_return:.6f}, 最大回撤: {maxdd:.6f}")

# 保存净值图
plt.figure(figsize=(10,4))
plt.plot(equity.index, equity.values, label='SMA equity')
plt.title("Equity Curve (SMA)")
plt.xlabel("datetime")
plt.ylabel("equity")
plt.grid(True)
equity_fig = out_dir / "equity.png"
plt.tight_layout()
plt.savefig(equity_fig)
plt.close()
print("已保存净值图：", equity_fig)

# 构造 RL 环境并快速测试 env 接口
features = df[['return','sma_short','sma_long']].values
prices = df['close'].values
T = min(2000, len(prices))
env = MarketEnv(features[:T], prices[:T], window=30, transaction_cost=TRANSACTION_COST)

obs = env.reset()
for _ in range(3):
    o, r, d, info = env.step(env.action_space.sample())
print("env 小测试通过")

# 保存简单报告
report_path = out_dir / "report.txt"
with open(report_path, "w", encoding="utf-8") as f:
    f.write(f"data_file: {CSV_PATH}\n")
    f.write(f"rows: {len(df)}\n")
    f.write(f"sample_range: {df.index.min()} -> {df.index.max()}\n")
    f.write(f"sma_short: {SMA_SHORT}, sma_long: {SMA_LONG}\n")
    f.write(f"final_equity: {final_equity:.6f}\n")
    f.write(f"total_return: {total_return:.6f}\n")
    f.write(f"max_drawdown: {maxdd:.6f}\n")
print("已保存报告：", report_path)