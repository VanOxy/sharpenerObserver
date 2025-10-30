# file: paper_broker.py
from dataclasses import dataclass
import csv, os
from typing import Dict, Optional

@dataclass
class Position:
    qty: float = 0.0         # >0 long, <0 short
    avg_px: float = 0.0      # средняя цена входа (по модулю qty)

class PaperBroker:
    def __init__(self, csv_path: str = "trades.csv", taker_fee_rate: float = 0.00055):
        self.csv_path = csv_path
        self.taker_fee_rate = taker_fee_rate
        self.cash = 0.0
        self.realized_pnl = 0.0
        self.pos: Dict[str, Position] = {}
        self._ensure_header()

    def _ensure_header(self):
        need_header = not os.path.exists(self.csv_path) or os.path.getsize(self.csv_path) == 0
        if need_header:
            with open(self.csv_path, "w", newline="") as f:
                w = csv.writer(f)
                w.writerow([
                    "ts","symbol","side","size","fill_price","fee",
                    "pos_qty_after","pos_avg_px_after","cash_after","realized_pnl_total"
                ])

    @staticmethod
    def _best_bid_ask(dom_sym: dict) -> Optional[tuple]:
        bids = dom_sym.get("bids") or []
        asks = dom_sym.get("asks") or []
        if not bids or not asks:
            return None
        best_bid = bids[0]["px"]
        best_ask = asks[0]["px"]
        return best_bid, best_ask

    def _exec_fill(self, symbol: str, side: str, size: float, price: float):
        """Обновляет позицию, кэш и реализованный PnL.
           side in {"buy","sell"}; size > 0; price > 0
        """
        assert side in ("buy", "sell")
        filled_value = size * price
        fee = filled_value * self.taker_fee_rate

        p = self.pos.setdefault(symbol, Position())
        qty_prev, avg_prev = p.qty, p.avg_px

        # знак сделки
        sgn = 1.0 if side == "buy" else -1.0
        qty_new = qty_prev + sgn * size

        realized = 0.0
        if qty_prev == 0.0 or (qty_prev > 0 and sgn > 0) or (qty_prev < 0 and sgn < 0):
            # наращиваем ту же сторону → обновляем среднюю
            # новая средняя = взвешенная по объёму (по модулю)
            if abs(qty_new) > 1e-12:
                p.avg_px = (abs(qty_prev) * avg_prev + size * price) / abs(qty_new)
        else:
            # закрываем часть/всю старую позицию → считаем реализованный PnL на закрываемый объём
            close_qty = min(abs(qty_prev), size)
            # если закрываем long (qty_prev>0), pnl = (price - avg_prev) * close_qty
            # если закрываем short (qty_prev<0), pnl = (avg_prev - price) * close_qty
            realized = (price - avg_prev) * close_qty * (1.0 if qty_prev > 0 else -1.0)
            # если перевернулись — остаток открывает новую позицию с avg=price
            if abs(size) > abs(qty_prev) + 1e-12:
                rem = abs(size) - abs(qty_prev)
                p.avg_px = price  # новый якорь
            elif abs(qty_new) < 1e-12:
                # полностью закрылись
                p.avg_px = 0.0

            self.realized_pnl += realized

        p.qty = qty_new

        # кэш: покупка тратит, продажа приносит (комиссия всегда минус)
        if side == "buy":
            self.cash -= filled_value + fee
        else:
            self.cash += filled_value - fee

        return fee, realized

    def execute_market(self, ts: int, symbol: str, side: str, size: float, dom_sym: dict):
        ba = self._best_bid_ask(dom_sym)
        if ba is None:
            return False  # нет котировок → игнор
        best_bid, best_ask = ba
        price = best_ask if side == "buy" else best_bid

        fee, realized = self._exec_fill(symbol, side, size, price)

        with open(self.csv_path, "a", newline="") as f:
            w = csv.writer(f)
            p = self.pos[symbol]
            w.writerow([
                ts, symbol, side, f"{size:.8f}", f"{price:.2f}", f"{fee:.8f}",
                f"{p.qty:.8f}", f"{p.avg_px:.2f}", f"{self.cash:.2f}", f"{self.realized_pnl:.2f}"
            ])
        return True
