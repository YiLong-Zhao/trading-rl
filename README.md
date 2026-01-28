# trading-rl

这是基于分钟级数据的 RL 交易项目。包含数据下载、数据清洗、baseline、MarketEnv、PPO 训练脚本和 notebooks。

快速开始：
1. 创建并激活 Python 环境（conda 或 venv）。
2. 安装依赖：`pip install -r requirements.txt`
3. 生成或下载数据（scripts/download_akshare_minute.py / scripts/generate_sample_data.py）
4. 运行 EDA：`python notebooks/01_minute_eda_and_baseline.py`
5. 训练：`python scripts/train_ppo.py --config ...`

数据/模型/输出位置（均在 .gitignore 中被忽略）：
- data/
- models/
- outputs/

更多说明请查看项目内各脚本头部注释。