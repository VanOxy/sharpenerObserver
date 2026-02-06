import gymnasium as gym
from gymnasium import spaces
import numpy as np
from config import Config

# --- AI Params ---
AI_TOP_N = Config.AI_TOP_N                  # Кол-во лучших уровней цен, которые передаются точно
AI_TAIL_BINS = Config.AI_TAIL_BINS          # Кол-во «корзин» для дальних уровней.
AI_TAIL_MAX_BPS = Config.AI_TAIL_MAX_BPS    # Хвост охватывает 5% движения цены

class SharpenerEnv(gym.Env):
    def __init__(self, s_cap=64):
        super().__init__()
        self.s_cap = s_cap
        extra_len = 0
        
        # Описываем пространство наблюдений по SnapshotPacker
        self.observation_space = spaces.Dict({
            # (S_cap, 6) -> O, H, L, C, V, Count
            "bars": spaces.Box(low=-np.inf, high=np.inf, shape=(s_cap, 6), dtype=np.float32),
            # (S_cap, 2, 60) -> Bids/Asks Px
            "depth_top_px": spaces.Box(low=0, high=np.inf, shape=(s_cap, 2, AI_TOP_N), dtype=np.float32),
            # (S_cap, 2, 60) -> Bids/Asks Qty
            "depth_top_qty": spaces.Box(low=0, high=np.inf, shape=(s_cap, 2, AI_TOP_N), dtype=np.float32),
            # (S_cap, 2, 64) -> Tail Qty Bins
            "depth_tail_qty": spaces.Box(low=0, high=np.inf, shape=(s_cap, 2, AI_TAIL_BINS), dtype=np.float32),
            # фичи стакана (6 значений: imb_k, spread, microprice и т.д.)
            "depth_feats": spaces.Box(low=-np.inf, high=np.inf, shape=(s_cap, 6), dtype=np.float32),
            # экстра фичи (если есть)
            "extra_feats": spaces.Box(low=-np.inf, high=np.inf, shape=(s_cap, extra_len), dtype=np.float32),
            # (S_cap,) -> 1.0 если токен активен, 0.0 если пустой слот
            "mask": spaces.Box(low=0, high=1, shape=(s_cap,), dtype=np.float32)
        })

        # Action Space: 
        # Допустим: 0 = Ничего, 1..S_cap = Buy монету i, S_cap+1..2*S_cap = Sell монету i
        self.action_space = spaces.Discrete(1 + s_cap * 2)

        self.state = None

    def reset(self, seed=None, options=None):
        super().reset(seed=seed)
        # В реальном времени reset вызывается редко, 
        # но для SB3 нам нужно вернуть пустой стейт
        return self._get_empty_state(), {}

    def step(self, action):
        # 1. Применяем действие через Orchestrator -> Broker
        # 2. Ждем следующего тика (1 сек)
        # 3. Получаем новый payload
        # 4. Считаем Reward (профит/убыток)
        
        # Это "заглушка", логику наполним вместе с Оркестратором
        reward = 0.0
        terminated = False
        truncated = False
        info = {}

        return self.state, reward, terminated, truncated, info

    def _get_empty_state(self):
        # Создаем нулевой вектор по форме observation_space
        return {
            "bars": np.zeros((self.s_cap, 6), dtype=np.float32),
            "depth_top_px": np.zeros((self.s_cap, 2, AI_TOP_N), dtype=np.float32),
            "depth_top_qty": np.zeros((self.s_cap, 2, AI_TOP_N), dtype=np.float32),
            "depth_tail_qty": np.zeros((self.s_cap, 2, AI_TAIL_BINS), dtype=np.float32),
            "depth_feats": np.zeros((self.s_cap, 6), dtype=np.float32),
            "extra_feats": np.zeros((self.s_cap, 0), dtype=np.float32),
            "mask": np.zeros((self.s_cap,), dtype=np.float32),
        }