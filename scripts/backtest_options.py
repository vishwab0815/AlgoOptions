"""
scripts/backtest_options.py - Backtest the NIFTY option selling strategy.

This script connects to DhanHQ to download historical 1-minute data for the
active NIFTY options, resamples it to 5-minute candles, and simulates the
GREEN->RED->RED Heikin-Ashi entry and exit logic over that data.
"""
import asyncio
import logging
import math
import sys
from datetime import date, datetime, timedelta, timezone
from zoneinfo import ZoneInfo

import pandas as pd

from algopilot.options.config import load_options_config
from algopilot.options.dhan_client import OptionsDhanClient
from algopilot.options.engine import resolve_band, LegState, _LEGS
from algopilot.strategy.signal_engine import SignalEngine
from algopilot.strategy.direction import SHORT
from algopilot.options.position import trailing_exit_level, OpenPosition

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(name)s: %(message)s", stream=sys.stdout)
logger = logging.getLogger("backtest")
_IST = ZoneInfo("Asia/Kolkata")

_SIGNAL_ENGINE = SignalEngine()

class BacktestSimulator:
    def __init__(self, config):
        self.config = config
        from algopilot.core.candle_builder import CandleBuilder
        from algopilot.core.heikin_ashi import HeikinAshiEngine
        from algopilot.core.indicators import IndicatorState
        
        self.legs = {
            side: LegState(
                option_type=side,
                candle_builder=CandleBuilder(
                    symbol=side, exchange_segment="NSE_FNO", security_id="dummy", 
                    timeframe_seconds=config.candle_timeframe_secs
                ),
                ha_engine=HeikinAshiEngine(),
                indicators=IndicatorState()
            ) for side in _LEGS
        }
        self.active_side = "ANY"
        self.balance = config.paper_capital
        self.trades = []

    def run_simulation(self, df_ce: pd.DataFrame, df_pe: pd.DataFrame):
        # Merge the two dataframes on timestamp so we can step through time
        df_ce = df_ce.copy()
        df_ce.columns = [f"ce_{c}" for c in df_ce.columns]
        df_pe = df_pe.copy()
        df_pe.columns = [f"pe_{c}" for c in df_pe.columns]
        
        # Outer join to ensure we don't drop timestamps where only one side traded
        df = pd.merge(df_ce, df_pe, left_index=True, right_index=True, how="outer").fillna(method="ffill").dropna()
        
        logger.info(f"Starting simulation over {len(df)} 5-minute candles (from {df.index[0]} to {df.index[-1]})")
        
        for ts, row in df.iterrows():
            squareoff_due = ts.hour >= 15
            
            for side in _LEGS:
                leg = self.legs[side]
                prefix = side.lower() + "_"
                
                # Check for exits FIRST on the open position using this candle's data
                if leg.position is not None:
                    # In a real backtest with 5m candles, we use the candle high as the extreme
                    # because we don't have intra-candle 1s ticks here.
                    premium_high = row[prefix + "high"]
                    premium_close = row[prefix + "close"]
                    
                    if squareoff_due:
                        self._exit(side, leg, premium_close, "EOD_SQUAREOFF", ts)
                        continue
                        
                    leg.position.advance_ratchet(premium_high, self.config.ratchet)
                    
                    if leg.position.is_ratchet_triggered(premium_high, self.config.ratchet):
                        self._exit(side, leg, premium_close, "PROFIT_RATCHET", ts)
                        continue
                        
                    if leg.position.is_cover_level_triggered(premium_high):
                        self._exit(side, leg, premium_close, "TRAILING_STOP", ts)
                        continue

                # Now process the candle for pattern entries
                candle_dict = {
                    "open": row[prefix + "open"],
                    "high": row[prefix + "high"],
                    "low": row[prefix + "low"],
                    "close": row[prefix + "close"],
                    "volume": row[prefix + "volume"]
                }
                
                ha_row = leg.ha_engine.append_candle(candle_dict)
                ind_snap = leg.indicators.update(
                    high=float(ha_row["ha_high"]), low=float(ha_row["ha_low"]), close=float(ha_row["ha_close"])
                )
                leg.ha_engine.update_last_indicators(ema=ind_snap.ema, atr=ind_snap.atr, rsi=ind_snap.rsi)
                candle_index = len(leg.ha_engine.df) - 1
                
                if leg.position is not None:
                    trailing = trailing_exit_level(leg.ha_engine.df, candle_index)
                    if trailing is not None:
                        leg.position.cover_level = trailing
                
                decision, new_level, new_stage = _SIGNAL_ENGINE.evaluate_breakout(
                    ha_open=float(ha_row["ha_open"]), ha_high=float(ha_row["ha_high"]),
                    ha_close=float(ha_row["ha_close"]), ha_low=float(ha_row["ha_low"]),
                    target_breakout_level=leg.target_level, setup_stage=leg.setup_stage, rules=SHORT,
                )
                leg.target_level = new_level
                leg.setup_stage = new_stage
                
                # Check for entry
                if (side == self.active_side or self.active_side == "ANY") and leg.position is None and decision.signal == "SELL" and not squareoff_due:
                    # RSI gate removed per user request
                    premium_close = row[prefix + "close"]
                    initial_cover = trailing_exit_level(leg.ha_engine.df, candle_index) or 0.0
                    self._enter(side, leg, premium_close, initial_cover, ts)

    def _enter(self, side: str, leg: LegState, entry_price: float, initial_cover: float, ts: pd.Timestamp):
        # Simplified sizing for backtest: assume fixed margin of 100k per lot for NIFTY
        per_lot_margin = 100000.0 
        lots = math.floor(self.balance / self.config.max_concurrent / per_lot_margin)
        lots = min(lots, self.config.max_lots_per_trade)
        if lots < 1:
            return
            
        qty = lots * self.config.lot_size
        leg.position = OpenPosition(
            symbol=side, entry_price=entry_price, qty=qty, cover_level=initial_cover
        )
        self.balance -= (per_lot_margin * lots)
        logger.info(f"[{ts}] SIMULATED SELL {side} @ {entry_price:.2f} | qty={qty}")

    def _exit(self, side: str, leg: LegState, exit_price: float, reason: str, ts: pd.Timestamp):
        pos = leg.position
        pnl = round(pos.rules.pnl(pos.entry_price, exit_price, pos.qty), 2)
        # Release margin + add pnl
        per_lot_margin = 100000.0
        lots = pos.qty // self.config.lot_size
        self.balance += (per_lot_margin * lots) + pnl
        
        logger.info(f"[{ts}] SIMULATED BUY-COVER {side} @ {exit_price:.2f} | P&L: Rs {pnl:.2f} | {reason} | Balance: Rs {self.balance:.0f}")
        self.trades.append({"entry_time": getattr(pos, 'entry_time', ts), "exit_time": ts, "side": side, "pnl": pnl, "reason": reason})
        
        leg.position = None
        self.active_side = "PE" if side == "CE" else "CE"


async def main():
    config = load_options_config()
    client = OptionsDhanClient(config.client_id, config.access_token)
    
    try:
        # Resolve static index for now (NIFTY near 23300)
        spot = 23300.0
        put_strike, call_strike = resolve_band(spot)
        logger.info(f"Backtesting strikes: {put_strike} PE and {call_strike} CE.")
        
        expiries = client.get_expiry_list(config.nifty_security_id, config.nifty_index_segment)
        if not expiries:
            logger.error("No expiries found.")
            return
            
        expiry = expiries[0]
        logger.info(f"Using nearest expiry: {expiry}")
        
        ce_contract = client.resolve_contract(expiry, call_strike, "CE")
        pe_contract = client.resolve_contract(expiry, put_strike, "PE")
        
        if not ce_contract or not pe_contract:
            logger.error("Could not resolve contracts.")
            return
            
        logger.info(f"Resolved CE: {ce_contract.security_id} | PE: {pe_contract.security_id}")
        
        # Download historical data for last 3 days
        to_date = date.today().strftime("%Y-%m-%d")
        from_date = (date.today() - timedelta(days=3)).strftime("%Y-%m-%d")
        
        logger.info(f"Downloading historical data from {from_date} to {to_date}...")
        
        def fetch_df(sec_id):
            from dhanhq import dhanhq, DhanContext
            dhan_client = dhanhq(DhanContext(
                client_id=config.client_id, 
                access_token=config.access_token.get_secret()
            ))
            
            resp = dhan_client.intraday_minute_data(
                security_id=sec_id, exchange_segment=config.option_exchange_segment,
                instrument_type="OPTIDX", from_date=from_date, to_date=to_date, interval=5
            )
            if resp.get("status") == "success" and "data" in resp:
                data = resp["data"]
                # data is {"start_Time": [...], "open": [...], ...}
                df = pd.DataFrame(data)
                
                # Check for empty data despite success status
                if df.empty:
                    return df
                    
                df["timestamp_dt"] = pd.to_datetime(df["timestamp"], unit='s', utc=True).dt.tz_convert('Asia/Kolkata')
                df.set_index("timestamp_dt", inplace=True)
                df.sort_index(inplace=True)
                
                # Drop original timestamp float column
                df.drop("timestamp", axis=1, inplace=True, errors="ignore")
                return df
            else:
                logger.warning(f"Fetch failed for {sec_id}: {resp}")
            return pd.DataFrame()
            
        df_ce = await asyncio.to_thread(fetch_df, ce_contract.security_id)
        df_pe = await asyncio.to_thread(fetch_df, pe_contract.security_id)
        
        if df_ce.empty or df_pe.empty:
            logger.error(f"Failed to fetch historical data. Check if your DhanHQ subscription allows OPTIDX historical data. CE empty: {df_ce.empty}, PE empty: {df_pe.empty}")
            return
            
        simulator = BacktestSimulator(config)
        simulator.run_simulation(df_ce, df_pe)
        
        wins = sum(1 for t in simulator.trades if t["pnl"] > 0)
        total = len(simulator.trades)
        total_pnl = sum(t["pnl"] for t in simulator.trades)
        logger.info("===" * 15)
        logger.info(f"BACKTEST COMPLETE")
        logger.info(f"Total Trades : {total}")
        logger.info(f"Win Rate     : {wins/total*100:.1f}%" if total > 0 else "Win Rate     : N/A")
        logger.info(f"Total P&L    : Rs {total_pnl:.2f}")
        logger.info(f"Final Balance: Rs {simulator.balance:.2f}")
        logger.info("===" * 15)
        
    finally:
        client.close()

if __name__ == "__main__":
    asyncio.run(main())
