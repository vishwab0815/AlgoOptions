"""
tests/test_kite.py — offline tests of the Zerodha Kite Connect layer and the engine on top of it.

    python tests/test_kite.py

No network, no real orders, never the production ledger: Kite's HTTP API is a
fake (FakeKite) that keeps an order book the way Kite does, the WebSocket parser
is fed packets built to Kite's byte layout, and every engine gets a temporary
ledger. Exit code 0 = all passed.
"""
from __future__ import annotations

import asyncio
import hashlib
import json
import logging
import os
import struct
import sys
import tempfile
import time
from dataclasses import replace
from datetime import datetime, timedelta, timezone
from pathlib import Path
from zoneinfo import ZoneInfo

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))
os.chdir(ROOT)
os.environ.update({"KITE_API_KEY": "testkey", "KITE_ACCESS_TOKEN": "testtoken", "KITE_LOGIN_AT": ""})

import algopilot.options.engine as E  # noqa: E402
from algopilot.core.candle_builder import Candle  # noqa: E402
from algopilot.options import kite_auth  # noqa: E402
from algopilot.options.broker import KiteBroker, OrderUpdateStream, engine_status, normalize_order  # noqa: E402
from algopilot.options.config import load_options_config  # noqa: E402
from algopilot.options.instruments import InstrumentRegistry, parse_instruments  # noqa: E402
from algopilot.options.kite_api import KiteHTTP, KiteReply  # noqa: E402
from algopilot.options.kite_client import OptionsKiteClient  # noqa: E402
from algopilot.options.websocket import parse_binary, split_packets, tick_time  # noqa: E402
from algopilot.utils.secrets import SecretStr  # noqa: E402

IST = ZoneInfo("Asia/Kolkata")
TMP = Path(tempfile.mkdtemp())
RESULTS = []
LOGS = []


class _Grab(logging.Handler):
    def emit(self, r):
        LOGS.append((r.levelname, r.getMessage()))


logging.getLogger("algopilot").addHandler(_Grab())
logging.getLogger("algopilot").setLevel(logging.INFO)


def check(name, cond):
    RESULTS.append((name, bool(cond)))


# ── the real rows of Kite's instrument list (26-Oct-2026) ────────────────────
NFO_CSV = """instrument_token,exchange_token,tradingsymbol,name,last_price,expiry,strike,tick_size,lot_size,instrument_type,segment,exchange
12468226,48704,NIFTY26OCTFUT,"NIFTY",0,2026-10-27,0,0.1,65,FUT,NFO-FUT,NFO
13168642,51440,NIFTY26OCT25000CE,"NIFTY",0,2026-10-27,25000,0.05,65,CE,NFO-OPT,NFO
13168898,51441,NIFTY26OCT25000PE,"NIFTY",0,2026-10-27,25000,0.05,65,PE,NFO-OPT,NFO
13169154,51442,NIFTY26OCT25100CE,"NIFTY",0,2026-10-27,25100,0.05,65,CE,NFO-OPT,NFO
13169410,51443,NIFTY26OCT25100PE,"NIFTY",0,2026-10-27,25100,0.05,65,PE,NFO-OPT,NFO
13908994,54332,NIFTY26N0325000CE,"NIFTY",0,2026-11-03,25000,0.05,65,CE,NFO-OPT,NFO
13909250,54333,NIFTY26N0325000PE,"NIFTY",0,2026-11-03,25000,0.05,65,PE,NFO-OPT,NFO
14000002,55000,BANKNIFTY26OCT50000CE,"BANKNIFTY",0,2026-10-27,50000,0.05,30,CE,NFO-OPT,NFO
"""
NSE_CSV = """instrument_token,exchange_token,tradingsymbol,name,last_price,expiry,strike,tick_size,lot_size,instrument_type,segment,exchange
256265,1001,NIFTY 50,"NIFTY 50",0,,0,0,0,EQ,INDICES,NSE
264969,1035,INDIA VIX,"INDIA VIX",0,,0,0,0,EQ,INDICES,NSE
738561,2885,RELIANCE,"RELIANCE INDUSTRIES",0,,0,0.1,1,EQ,NSE,NSE
"""


class FakeKite:
    """Kite Connect v3 over HTTP, in memory: instruments, quotes, candles, margins,
    orders (with Kite's statuses and rules), trades, positions and funds."""

    def __init__(self):
        self.calls = []                         # (method, path, params, data, json)
        self.orders = {}                        # order_id -> history list
        self.n = 0
        self.funds = 300000.0
        self.net = {}                           # instrument_token -> net qty
        self.fills = []
        self.ltp = {}                           # token -> last price (for trigger-side rules)
        self.fail_network_on_place = False
        self.place_error = None                 # (status, error_type, message)

    def reply(self, data=None, status=200, err=None):
        if err:
            return KiteReply(status, False, None, err[0], err[1])
        return KiteReply(status, True, data)

    def call(self, method, path, *, family="other", params=None, data=None, json_body=None, timeout=None):
        self.calls.append((method, path, params, data, json_body))
        if method == "GET" and path == "/instruments/NFO":
            return self.reply(NFO_CSV)
        if method == "GET" and path == "/instruments/NSE":
            return self.reply(NSE_CSV)
        if method == "GET" and path == "/quote":
            keys = [v for k, v in params if k == "i"]
            out = {}
            for k in keys:
                if k == "NSE:NIFTY 50":
                    out[k] = {"instrument_token": 256265, "last_price": 25060.5, "ohlc": {"close": 25000}}
                elif k.endswith("CE") or k.endswith("PE"):
                    out[k] = {"instrument_token": 1, "last_price": 330.0 if "25000CE" in k else 120.0,
                              "oi": 1500000, "volume": 900000}
            return self.reply(out)
        if method == "GET" and path.startswith("/instruments/historical/"):
            return self.reply({"candles": [["2026-10-26T09:15:00+0530", 400.0, 415.0, 399.0, 414.0, 1200],
                                           ["2026-10-26T09:20:00+0530", 414.0, 416.0, 390.0, 392.0, 900]]})
        if method == "POST" and path == "/margins/basket":
            return self.reply({"initial": {"total": 145678.5}, "final": {"total": 145678.5},
                               "orders": [{"total": 145678.5}]})
        if method == "POST" and path == "/orders/regular":
            if self.fail_network_on_place:
                self.fail_network_on_place = False
                self._new(data)                 # it reached Kite, the answer was lost
                import requests
                raise requests.ConnectionError("read timed out")
            if self.place_error:
                return self.reply(status=self.place_error[0], err=self.place_error[1:])
            return self.reply({"order_id": self._new(data)})
        if method == "PUT" and path.startswith("/orders/regular/"):
            oid = path.rsplit("/", 1)[1]
            cur = self.orders[oid][-1]
            if cur["status"] in ("COMPLETE", "CANCELLED", "REJECTED"):
                return self.reply(status=400, err=("InputException",
                                                   "Order cannot be modified as it is being processed. Try later."))
            new = dict(cur, trigger_price=float(data.get("trigger_price", cur["trigger_price"])),
                       price=float(data.get("price", cur["price"])), modified=True)
            self.orders[oid].append(new)
            return self.reply({"order_id": oid})
        if method == "DELETE" and path.startswith("/orders/regular/"):
            oid = path.rsplit("/", 1)[1]
            cur = self.orders[oid][-1]
            if cur["status"] in ("COMPLETE", "CANCELLED", "REJECTED"):
                return self.reply(status=400, err=("InputException",
                                                   "Order cannot be cancelled as it is being processed. Try later."))
            self.orders[oid].append(dict(cur, status="CANCELLED", pending_quantity=0,
                                         cancelled_quantity=cur["quantity"] - cur["filled_quantity"]))
            return self.reply({"order_id": oid})
        if method == "GET" and path.startswith("/orders/") and not path.endswith("/trades"):
            oid = path.split("/")[2]
            return self.reply(list(self.orders[oid])) if oid in self.orders else self.reply(status=404, err=(
                "GeneralException", "Couldn't find that order"))
        if method == "GET" and path == "/orders":
            return self.reply([h[-1] for h in self.orders.values()])
        if method == "GET" and path == "/trades":
            return self.reply(list(self.fills))
        if method == "GET" and path == "/portfolio/positions":
            return self.reply({"net": [{"tradingsymbol": "X", "exchange": "NFO", "instrument_token": int(t),
                                        "product": "MIS", "quantity": q, "buy_price": 0, "sell_price": 0}
                                       for t, q in self.net.items()], "day": []})
        if method == "GET" and path == "/user/margins/equity":
            return self.reply({"enabled": True, "net": self.funds, "available": {"live_balance": self.funds}})
        if method == "GET" and path == "/user/profile":
            return self.reply({"user_id": "AB1234", "user_name": "Test"})
        return self.reply(status=404, err=("GeneralException", f"fake: no route {method} {path}"))

    def _new(self, d):
        self.n += 1
        oid = f"2510260000{self.n:05d}"
        token = {"NIFTY26OCT25000CE": 13168642, "NIFTY26OCT25000PE": 13168898}.get(d["tradingsymbol"], 1)
        qty = int(d["quantity"])
        o = {"order_id": oid, "status": "TRIGGER PENDING" if d["order_type"] == "SL" else "OPEN",
             "tradingsymbol": d["tradingsymbol"], "exchange": d["exchange"], "instrument_token": token,
             "transaction_type": d["transaction_type"], "order_type": d["order_type"], "product": d["product"],
             "validity": d["validity"], "quantity": qty, "filled_quantity": 0, "pending_quantity": qty,
             "cancelled_quantity": 0, "price": float(d.get("price", 0) or 0),
             "trigger_price": float(d.get("trigger_price", 0) or 0), "average_price": 0.0, "tag": d.get("tag"),
             "tags": [d.get("tag")], "status_message": None, "market_protection": 0}
        self.orders[oid] = [dict(o, status="PUT ORDER REQ RECEIVED"), o]
        if d["order_type"] == "MARKET":
            self.fill(oid, self.ltp.get(token, 400.0))
        return oid

    def fill(self, oid, price):
        cur = self.orders[oid][-1]
        done = dict(cur, status="COMPLETE", filled_quantity=cur["quantity"], pending_quantity=0, average_price=price)
        self.orders[oid].append(done)
        sign = -1 if cur["transaction_type"] == "SELL" else 1
        tok = str(cur["instrument_token"])
        self.net[tok] = self.net.get(tok, 0) + sign * cur["quantity"]
        self.fills.append({"trade_id": str(len(self.fills) + 1), "order_id": oid, "exchange": "NFO",
                           "tradingsymbol": cur["tradingsymbol"], "instrument_token": cur["instrument_token"],
                           "product": cur["product"], "average_price": price, "quantity": cur["quantity"],
                           "transaction_type": cur["transaction_type"],
                           "fill_timestamp": datetime.now(IST).strftime("%Y-%m-%d %H:%M:%S"),
                           "order_timestamp": datetime.now(IST).strftime("%H:%M:%S")})
        return done

    def sent(self, method, path_prefix):
        return [c for c in self.calls if c[0] == method and c[1].startswith(path_prefix)]


def fake_registry(fake) -> InstrumentRegistry:
    http = KiteHTTP("testkey", SecretStr("testtoken"))
    http.call = fake.call
    reg = InstrumentRegistry(http, cache_dir=str(TMP / f"inst{time.time_ns()}"))
    return reg


# ── 1. WebSocket binary parsing (Kite's byte layout) ─────────────────────────
def t_websocket():
    U32 = struct.Struct(">I")
    U16 = struct.Struct(">H")

    def frame(*packets):
        b = U16.pack(len(packets))
        for p in packets:
            b += U16.pack(len(p)) + p
        return b

    ltp = U32.pack(13168642) + U32.pack(33045)                                   # 330.45
    idx = b"".join(U32.pack(v) for v in (256265, 2506050, 2510000, 2495000, 2500000, 2498000, 0, 1793000000))
    ltt, exch = 1793000123, 1793000124
    full = (U32.pack(13168642) + U32.pack(33045) + U32.pack(65) + U32.pack(33100) + U32.pack(900000)
            + U32.pack(1000) + U32.pack(2000) + U32.pack(32000) + U32.pack(34000) + U32.pack(31000)
            + U32.pack(31500) + U32.pack(ltt) + U32.pack(1500000) + U32.pack(1600000) + U32.pack(1400000)
            + U32.pack(exch))
    for i in range(10):                                                          # 5 bids then 5 offers
        full += U32.pack(65 * (i + 1)) + U32.pack(33000 + i * 5) + U16.pack(i + 1) + b"\x00\x00"
    ticks = parse_binary(frame(ltp, idx, full))
    a, b, c = ticks
    check(f"ws: frame of 3 packets parsed ({len(ticks)}); LTP packet {a['last_price']} (paise /100)",
          len(ticks) == 3 and a["mode"] == "ltp" and abs(a["last_price"] - 330.45) < 1e-9 and a["tradable"])
    check(f"ws: index full packet — NIFTY 50 last {b['last_price']}, H/L/O/C order {b['high']}/{b['low']}/"
          f"{b['open']}/{b['close']}, not tradable",
          b["mode"] == "full" and not b["tradable"] and b["high"] == 25100 and b["low"] == 24950
          and b["open"] == 25000 and b["close"] == 24980 and b["exchange_ts"] == 1793000000)
    check(f"ws: tradable full packet — O/H/L/C {c['open']}/{c['high']}/{c['low']}/{c['close']}, OI {c['oi']}, "
          f"trade time {c['last_trade_time']}, depth bid0 {c['depth']['buy'][0]} offer4 {c['depth']['sell'][4]}",
          c["mode"] == "full" and c["open"] == 320 and c["high"] == 340 and c["low"] == 310 and c["close"] == 315
          and c["oi"] == 1500000 and c["last_trade_time"] == ltt and c["exchange_ts"] == exch
          and c["depth"]["buy"][0] == {"quantity": 65, "price": 330.0, "orders": 1}
          and c["depth"]["sell"][4] == {"quantity": 650, "price": 330.45, "orders": 10})
    check("ws: heartbeat (1 byte) and a truncated frame give nothing / only whole packets",
          parse_binary(b"\x00") == [] and len(split_packets(frame(ltp)[:-3])) == 0
          and len(split_packets(frame(ltp, ltp)[:-2])) == 1)
    now = datetime.fromtimestamp(ltt + 0.15, tz=timezone.utc)
    check("ws: tick time = the exchange's trade time when current, arrival time when stale",
          tick_time(ltt, now) == datetime.fromtimestamp(ltt, tz=timezone.utc)
          and tick_time(ltt - 60, now) == now and tick_time(None, now) == now)


# ── 2. instruments, quotes-as-chain, candles, margin ─────────────────────────
def t_client():
    fake = FakeKite()
    cl = OptionsKiteClient("testkey", SecretStr("testtoken"), product="MIS", cache_dir=str(TMP / "c1"))
    cl.http.call = fake.call
    cl.registry = fake_registry(fake)
    rows = parse_instruments(NFO_CSV)
    check(f"instruments: {len(rows)} rows parsed, name/strike/lot/tick from the real CSV shape",
          len(rows) == 8 and rows[1].name == "NIFTY" and rows[1].strike == 25000 and rows[1].lot_size == 65
          and rows[0].strike == 0 and rows[0].tick_size == 0.1)
    exps = cl.get_expiry_list()
    check(f"expiries from the list (NIFTY only, BANKNIFTY ignored): {exps}", exps == ["2026-10-27", "2026-11-03"])
    ci = cl.resolve_contract("2026-10-27", 25000, "CE")
    check(f"contract: 25000 CE 27-Oct -> token {ci and ci.security_id} lot {ci and ci.lot_size} {ci and ci.tradingsymbol}",
          ci and ci.security_id == "13168642" and ci.lot_size == 65 and ci.tradingsymbol == "NIFTY26OCT25000CE")
    ch = cl.get_chain("NIFTY 50", "NSE", "2026-10-27")
    q = fake.sent("GET", "/quote")[-1]
    keys = [v for k, v in q[2] if k == "i"]
    check(f"chain = ONE batched /quote ({len(keys)} keys incl. 'NSE:NIFTY 50'): spot {ch.spot}, 25000 CE "
          f"{ch.legs[25000.0]['ce'].last_price} OI {ch.legs[25000.0]['ce'].oi:.0f}",
          len(fake.sent("GET", "/quote")) == 1 and "NSE:NIFTY 50" in keys and "NFO:NIFTY26OCT25000CE" in keys
          and ch.spot == 25060.5 and ch.legs[25000.0]["ce"].last_price == 330.0 and ch.legs[25000.0]["ce"].oi == 1500000)
    check("chain: a second call inside the 2 s gap is skipped (Kite: 1 quote call a second)",
          cl.get_chain("NIFTY 50", "NSE", "2026-10-27") is None and len(fake.sent("GET", "/quote")) == 1)
    bars = cl.get_intraday_candles("13168642", "NFO", "OPTIDX", 5, "2026-10-26 09:00:00", "2026-10-26 09:30:00")
    h = fake.sent("GET", "/instruments/historical/")[-1]
    check(f"candles: /instruments/historical/13168642/5minute, {len(bars)} bars, 09:15 start = epoch "
          f"{bars[0]['timestamp']:.0f}",
          h[1] == "/instruments/historical/13168642/5minute" and h[2]["from"] == "2026-10-26 09:00:00"
          and len(bars) == 2 and bars[0]["timestamp"] == datetime(2026, 10, 26, 9, 15, tzinfo=IST).timestamp()
          and bars[1]["low"] == 390.0)
    m, live = cl.margin_per_lot("13168642", "NFO", 330.0, 65, 130000.0)
    mb = fake.sent("POST", "/margins/basket")[-1]
    check(f"margin: POST /margins/basket?consider_positions=false (JSON) -> Rs {m:,.2f} live={live}",
          live and m == 145678.5 and mb[2] == {"consider_positions": "false"}
          and mb[4][0]["tradingsymbol"] == "NIFTY26OCT25000CE" and mb[4][0]["transaction_type"] == "SELL"
          and mb[4][0]["quantity"] == 65 and mb[4][0]["product"] == "MIS")


# ── 3. broker: order fields, statuses, normalised books, errors ──────────────
def t_broker():
    fake = FakeKite()
    reg = fake_registry(fake)
    reg.ensure()
    b = KiteBroker("testkey", SecretStr("testtoken"), reg, exchange="NFO", product="MIS")
    b.http.call = fake.call
    oid, msg = b.place("SELL", "13168642", 65, "STOP_LOSS", price=320.15, trigger=329.5, tag="APabc")
    d = fake.sent("POST", "/orders/regular")[-1][3]
    check(f"place SELL stop-limit: {d}",
          oid and d["tradingsymbol"] == "NIFTY26OCT25000CE" and d["exchange"] == "NFO" and d["order_type"] == "SL"
          and d["transaction_type"] == "SELL" and d["quantity"] == 65 and d["product"] == "MIS" and d["validity"] == "DAY"
          and d["price"] == "320.15" and d["trigger_price"] == "329.50" and d["tag"] == "APabc"
          and "market_protection" not in d)
    st = b.parse(fake.orders[oid][-1], oid)
    check(f"TRIGGER PENDING -> engine status {st.status}", st.status == "PENDING")
    oid2, _ = b.place("BUY", "13168642", 65, "MARKET", tag="APmkt")
    d2 = fake.sent("POST", "/orders/regular")[-1][3]
    check(f"place MARKET: market_protection {d2.get('market_protection')}, no price/trigger",
          d2["order_type"] == "MARKET" and d2["market_protection"] == "-1" and "price" not in d2
          and "trigger_price" not in d2)
    ok, _ = b.modify(oid, "STOP_LOSS", 65, 310.0, 318.0)
    pm = fake.sent("PUT", f"/orders/regular/{oid}")[-1][3]
    check(f"modify: PUT /orders/regular/{{id}} {pm}", ok and pm["trigger_price"] == "318.00" and pm["order_type"] == "SL")
    ok_c, _ = b.cancel(oid)
    ok_c2, m2 = b.cancel(oid)
    check(f"cancel ok, then cancel again refused with Kite's text: '{m2}'",
          ok_c and not ok_c2 and "being processed" in m2 and b.parse(fake.orders[oid][-1], oid).status == "CANCELLED")
    cases = [({"status": "COMPLETE", "quantity": 65, "filled_quantity": 65}, "TRADED"),
             ({"status": "OPEN", "quantity": 65, "filled_quantity": 30, "order_type": "LIMIT"}, "PART_TRADED"),
             ({"status": "OPEN", "quantity": 65, "filled_quantity": 0, "order_type": "SL"}, "TRIGGERED"),
             ({"status": "OPEN", "quantity": 65, "filled_quantity": 0, "order_type": "LIMIT"}, "PENDING"),
             ({"status": "VALIDATION PENDING"}, "TRANSIT"), ({"status": "MODIFY PENDING"}, "TRANSIT"),
             ({"status": "REJECTED"}, "REJECTED"), ({"status": "CANCELLED"}, "CANCELLED")]
    check("status map: COMPLETE/OPEN part/OPEN SL/OPEN limit/in-between/REJECTED/CANCELLED",
          all(engine_status(o) == want for o, want in cases))
    n = normalize_order(fake.orders[oid][-1])
    check(f"order book normalised for the engine: {n['orderType']} {n['orderStatus']} tag {n['correlationId']}",
          n["orderType"] == "STOP_LOSS" and n["orderStatus"] == "CANCELLED" and n["correlationId"] == "APabc"
          and n["securityId"] == "13168642")
    pos = b.positions()
    tr = b.trades()
    check(f"positions: short shows netQty {pos[0]['netQty'] if pos else None} (MARKET BUY filled +65); trades "
          f"{len(tr)} fill(s) with exchangeTime", pos and pos[0]["netQty"] == 65 and tr[0]["tradedQuantity"] == 65
          and tr[0]["exchangeTime"])
    check(f"funds: /user/margins/equity net = Rs {b.available_funds():,.0f}", b.available_funds() == 300000.0)
    fake.place_error = (400, "InputException",
                        "Trigger price for stoploss sell orders should be lower than the last traded price (328.05).")
    none_id, why = b.place("SELL", "13168642", 65, "STOP_LOSS", price=320.0, trigger=329.5)
    check(f"rejection keeps Kite's reason (the engine sells at market on 'trigger'): '{why[:70]}...'",
          none_id is None and "trigger" in why.lower())
    fake.place_error = (400, "InputException", "IP 1.2.3.4 is not allowed to place orders for this app.")
    _, why_ip = b.place("SELL", "13168642", 65, "STOP_LOSS", price=320.0, trigger=329.5)
    check("static-IP rejection explains the IP whitelist", "IP Whitelist" in why_ip)
    fake.place_error = None
    fake.fail_network_on_place = True
    oid3, msg3 = b.place("SELL", "13168642", 65, "STOP_LOSS", price=320.0, trigger=329.5, tag="APnet")
    check(f"network error after Kite got it -> found by tag, NOT re-sent: {oid3} ({msg3})",
          oid3 and "tag" in msg3 and sum(1 for h in fake.orders.values() if h[-1]["tag"] == "APnet") == 1)
    s = OrderUpdateStream("k", SecretStr("t"))
    s._on_message(json.dumps({"type": "order", "data": dict(fake.orders[oid2][-1])}))
    s._on_message(json.dumps({"type": "order", "data": dict(fake.orders[oid2][-1], status="OPEN")}))
    check("order-update stream: a late non-final update never undoes a final one",
          engine_status(s.latest(oid2)) == "TRADED")


# ── 4. login / token ─────────────────────────────────────────────────────────
def t_auth():
    cs = kite_auth.checksum("key1", "req2", "sec3")
    check("checksum = sha256(api_key + request_token + api_secret)",
          cs == hashlib.sha256(b"key1req2sec3").hexdigest())
    a = kite_auth.expiry_ist(datetime(2026, 10, 26, 9, 41, tzinfo=IST))
    b = kite_auth.expiry_ist(datetime(2026, 10, 27, 2, 0, tzinfo=IST))
    check(f"token expiry: login 26-Oct 09:41 -> {a:%d-%b %H:%M}; login 27-Oct 02:00 -> {b:%d-%b %H:%M}",
          a == datetime(2026, 10, 27, 6, 0, tzinfo=IST) and b == datetime(2026, 10, 27, 6, 0, tzinfo=IST))
    check("request_token taken from a pasted redirect URL",
          kite_auth.request_token_from("http://127.0.0.1:5000/?action=login&type=login&status=success&request_token=Ab12Cd")
          == "Ab12Cd")
    env = TMP / "e.env"
    env.write_bytes(b"# keep me\r\nKITE_API_KEY=k\r\nKITE_ACCESS_TOKEN=old\r\nOTHER=1\r\n")
    kite_auth.write_env({"KITE_ACCESS_TOKEN": "new", "KITE_LOGIN_AT": "2026-10-26 09:41:00"}, env)
    txt = env.read_bytes().decode()
    check("write_env: replaces the token, adds the login time, keeps every other line and CRLF",
          "KITE_ACCESS_TOKEN=new\r\n" in txt and "KITE_LOGIN_AT=2026-10-26 09:41:00" in txt and "# keep me\r\n" in txt
          and "OTHER=1\r\n" in txt and "old" not in txt)


# ── 5. config ────────────────────────────────────────────────────────────────
def t_config():
    c = load_options_config()
    check(f"config: api_key / product {c.product} / NIFTY '{c.nifty_index_segment}:{c.nifty_security_id}' / "
          f"options on {c.option_exchange_segment} / chain every {c.chain_min_interval_secs:g}s",
          c.api_key == "testkey" and c.product in ("MIS", "NRML") and c.nifty_security_id == "NIFTY 50"
          and c.nifty_index_segment == "NSE" and c.option_exchange_segment == "NFO" and c.chain_min_interval_secs >= 1)
    import algopilot.options.config as C
    saved = os.environ.pop("KITE_API_KEY")
    C.load_dotenv = lambda *a, **k: None
    try:
        C.load_options_config()
        bad = False
    except C.OptionsConfigError:
        bad = True
    os.environ["KITE_API_KEY"] = saved
    check("config: no KITE_API_KEY -> refused with a clear message", bad)


# ── 6. the engine, live, on the Kite broker (fake Kite) ──────────────────────
class _Open:
    is_trading_allowed = True
    eod_squareoff_due = False


class _Margin:
    auth_failed = False
    async def margin_per_lot_async(self, *a, **k): return 150000.0, True
    def margin_cache_age(self, *a): return 0.0
    async def get_intraday_candles_async(self, *a, **k): return []
    def close(self): pass


def _cand(o, h, l, c, t0, i, side="CE"):
    s = (t0 + timedelta(minutes=5 * i)).astimezone(timezone.utc)
    return Candle(symbol=side, exchange_segment=2, security_id="x", start_ts=s, end_ts=s + timedelta(minutes=5),
                  open=o, high=h, low=l, close=c, volume=0)


async def t_engine():
    E.get_market_status = lambda *a, **k: _Open()
    E._ARM_POLL_SECS = 0.05
    E._FLAT_CONFIRM_SECS = 0.05
    fake = FakeKite()
    cfg = replace(load_options_config(), paper_trading=False, db_path=str(TMP / f"{time.time_ns()}.db"),
                  official_candles=False, entry_start="00:00", max_daily_loss=5000.0, entry_mode="break",
                  live_order_type="MARKET")
    e = E.OptionsEngine(cfg)
    reg = e.client.registry
    reg._http.call = fake.call
    reg.ensure()
    e.broker.http.call = fake.call
    e.client.close()
    e.client = _Margin()
    e.broker.stream = OrderUpdateStream(cfg.api_key, cfg.access_token)
    e._running = True
    e.expiry = "2026-10-27"
    for s, k in (("CE", 25000.0), ("PE", 25000.0)):
        ins = reg.option("2026-10-27", k, s)
        e.legs[s].security_id, e.legs[s].strike, e.legs[s].lot_size = str(ins.token), k, 65
    now = datetime.now(timezone.utc).replace(second=0, microsecond=0)
    t0 = now - timedelta(minutes=now.minute % 5) - timedelta(minutes=10)
    for i, bar in enumerate([(400, 401, 399, 400), (400, 415, 399, 414), (414, 416, 390, 392)]):
        await e._on_candle_close("CE", e.legs["CE"], _cand(*bar, t0, i), False)
    await asyncio.sleep(0.1)
    sells = [c for c in fake.sent("POST", "/orders/regular") if c[3]["transaction_type"] == "SELL"]
    d = sells[0][3] if sells else {}
    check(f"engine live: GREEN->RED parks a Kite SELL SL at HA Low - 0.5 -> {d.get('order_type')} trigger "
          f"{d.get('trigger_price')} limit {d.get('price')} x{d.get('quantity')} {d.get('tradingsymbol')} {d.get('product')}",
          d.get("order_type") == "SL" and d.get("trigger_price") == "389.50" and d.get("quantity") == 65
          and d.get("tradingsymbol") == "NIFTY26OCT25000CE" and d.get("product") == cfg.product
          and float(d.get("price")) <= 389.5)
    entry_id = e._armed.order_id if e._armed else None
    fake.ltp[13168642] = 389.0
    done = fake.fill(entry_id, 389.5)
    e.broker.stream.ingest(done)                           # Kite pushes the fill over the WebSocket
    for _ in range(40):
        await asyncio.sleep(0.05)
        if e.legs["CE"].position is not None and e.legs["CE"].position.stop_order_id:
            break
    pos = e.legs["CE"].position
    buys = [c for c in fake.sent("POST", "/orders/regular") if c[3]["transaction_type"] == "BUY"]
    b0 = buys[0][3] if buys else {}
    check(f"engine live: fill pushed -> short {pos and pos.qty} at {pos and pos.entry_price}; protective Kite BUY SL "
          f"trigger {b0.get('trigger_price')} limit {b0.get('price')} (+10%)",
          pos is not None and pos.entry_price == 389.5 and b0.get("order_type") == "SL"
          and b0.get("trigger_price") == "416.50" and abs(float(b0.get("price")) - 458.15) < 0.051)
    pos.cover_level = 410.0
    await e._sync_protective_stop("CE", e.legs["CE"])
    mod = fake.sent("PUT", f"/orders/regular/{pos.stop_order_id}")
    check(f"engine live: stop moved by PUT -> trigger {mod[-1][3].get('trigger_price') if mod else None}",
          mod and mod[-1][3]["trigger_price"] == "410.00")
    fake.ltp[13168642] = 380.0
    await e._live_close("CE", e.legs["CE"], "EOD_SQUAREOFF")
    e.ledger.flush()
    tr = e.ledger.get_all_trades(limit=5)
    last_buy = [c for c in fake.sent("POST", "/orders/regular") if c[3]["transaction_type"] == "BUY"][-1][3]
    stop_state = fake.orders[pos.stop_order_id or mod[-1][1].rsplit('/', 1)[1]][-1]["status"] if mod else "?"
    check(f"engine live: square-off cancels the stop ({stop_state}) then MARKET BUY with market_protection "
          f"{last_buy.get('market_protection')} -> trade booked {tr and tr[0]['exit_reason']} at {tr and tr[0]['exit_price']}",
          e.legs["CE"].position is None and tr and tr[0]["exit_reason"] == "EOD_SQUAREOFF"
          and tr[0]["exit_price"] == 380.0 and last_buy.get("order_type") == "MARKET"
          and last_buy.get("market_protection") == "-1" and stop_state == "CANCELLED")
    e.ledger.close()


def t_pipeline():
    import importlib.util
    spec = importlib.util.spec_from_file_location("pipe", ROOT / "scripts" / "daily_pipeline.py")
    pipe = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(pipe)
    tries = {"n": 0}

    def token():
        tries["n"] += 1
        if tries["n"] < 3:
            raise kite_auth.TokenError("not logged in yet")
        return "Kite session OK"
    slept = []
    ok = pipe.wait_for_login(token, now=lambda: datetime(2026, 10, 27, 8, 50, tzinfo=IST), sleep=slept.append)
    late = pipe.wait_for_login(lambda: (_ for _ in ()).throw(kite_auth.TokenError("no")),
                               now=lambda: datetime(2026, 10, 27, 15, 45, tzinfo=IST), sleep=slept.append)
    check(f"pipeline: waits for the morning login (checked {tries['n']}x, slept {len(slept)}x) and gives up after "
          f"the close", ok and tries["n"] == 3 and len(slept) == 2 and late is False)


def main() -> int:
    t_websocket()
    t_client()
    t_broker()
    t_auth()
    t_config()
    asyncio.run(t_engine())
    t_pipeline()
    for name, ok in RESULTS:
        print(("PASS  " if ok else "FAIL  ") + name)
    bad = [n for n, ok in RESULTS if not ok]
    print(f"\n{len(RESULTS) - len(bad)}/{len(RESULTS)} passed" + ("" if not bad else " — FAILURES ABOVE"))
    return 1 if bad else 0


if __name__ == "__main__":
    sys.exit(main())
