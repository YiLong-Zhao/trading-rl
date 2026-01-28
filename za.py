
import pandas as pd
p = "data/605069_1m_sh_clean.csv"   # 或你实际的清洗文件名
df = pd.read_csv(p, nrows=5)
print("raw columns:", list(df.columns))
# 如果有 datetime 列，打印前几值
for c in df.columns:
    if "time" in c.lower() or "date" in c.lower() or "datetime" in c.lower():
        print("Sample datetime column values (first 10):")
        print(df[c].head(10))
# 读取为 index（如果已是 index，替换为你的文件）
df2 = pd.read_csv(p, parse_dates=True, index_col=0)
print("After parse index dtype:", df2.index.dtype)
print("Index sample:", df2.index[:5])
print("Index type of first element:", type(df2.index[0]))