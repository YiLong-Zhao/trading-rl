import numpy as np
import gym
from gym import spaces

class MarketEnv(gym.Env):
    """
    简化的交易环境（单标的，分钟级）。
    Obs: 最近 N bars 的特征（例如 close returns, sma_short, sma_long, volume）
    Action: Discrete(3) -> 0: hold, 1: long, 2: short
    Reward: pnl change between steps (minus transaction cost when position changes)
    """
    metadata = {'render.modes': ['human']}

    def __init__(self, features, prices, window=30, transaction_cost=0.0005):
        super().__init__()
        self.features = features
        self.prices = np.array(prices, dtype=float)
        self.window = window
        self.transaction_cost = transaction_cost
        self.T = len(self.prices)
        self.observation_space = spaces.Box(low=-np.inf, high=np.inf, shape=(window, features.shape[1]), dtype=np.float32)
        self.action_space = spaces.Discrete(3)
        self.reset()

    def reset(self):
        self.t = self.window
        self.position = 0
        self.last_price = self.prices[self.t - 1]
        self.done = False
        return self._get_obs()

    def _get_obs(self):
        return self.features[self.t - self.window:self.t].astype(np.float32)

    def step(self, action):
        if self.done:
            raise RuntimeError("Environment done. Call reset().")
        pos_map = {0: 0, 1: 1, 2: -1}
        new_pos = pos_map[int(action)]
        price = self.prices[self.t]
        ret = (price - self.last_price) / self.last_price
        pnl = self.position * ret
        tc = 0.0
        if new_pos != self.position:
            tc = abs(new_pos - self.position) * self.transaction_cost
        reward = pnl - tc
        self.position = new_pos
        self.last_price = price
        self.t += 1
        if self.t >= self.T:
            self.done = True
        obs = self._get_obs() if not self.done else np.zeros_like(self._get_obs())
        info = {'pnl': pnl, 'tc': tc}
        return obs, reward, self.done, info

    def render(self, mode='human'):
        print(f"t={self.t}, pos={self.position}, last_price={self.last_price:.4f}")