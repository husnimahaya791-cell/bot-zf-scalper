import os
import time
import hmac
import hashlib
import base64
import sqlite3
import logging
import threading
import signal
import sys
import concurrent.futures
import numpy as np
import pandas as pd
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from flask import Flask
from datetime import datetime, timezone, timedelta
from waitress import serve

# ==========================================
# LOGGING & KONFIGURASI LINGKUNGAN
# ==========================================
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[logging.StreamHandler()]
)

DB_FILE = os.environ.get("DB_PATH", "bot_database.db")
BITGET_API_KEY = os.environ.get("BITGET_API_KEY", "")
BITGET_SECRET_KEY = os.environ.get("BITGET_SECRET_KEY", "")
BITGET_PASSPHRASE = os.environ.get("BITGET_PASSPHRASE", "")
TELEGRAM_BOT_TOKEN = os.environ.get("TELEGRAM_BOT_TOKEN", "")
TELEGRAM_GROUP_ID = os.environ.get("TELEGRAM_GROUP_ID", "")

PARAMS_UNIFIED = {
    "length_period": 75,
    "batas_zf": 0.75,
    "min_drift": 0.35,
    "kepekaan_fractal": 15,
    "min_fvg_mult": 1.5,
    "sigma_sl_mult": 4.5,
    "rr1_ratio": 1.0,
    "rr2_ratio": 1.5,
    "rr3_ratio": 2.0,
    "use_trailing": False  # Trailing & Geser SL dinonaktifkan
}

DYNAMIC_ASSETS = {}
API_TO_DISPLAY = {}

state_lock = threading.RLock()
asset_states = {}

# HTTP Session dengan strategi Retry & Connection Pooling
http_session = requests.Session()
retry_strategy = Retry(
    total=3,
    backoff_factor=1,
    status_forcelist=[429, 500, 502, 503, 504]
)
adapter = HTTPAdapter(max_retries=retry_strategy, pool_connections=100, pool_maxsize=100)
http_session.mount("https://", adapter)
http_session.mount("http://", adapter)

# ==========================================
# UTILITAS & FORMATTING
# ==========================================
def fmt_duration(entry_time_str):
    if not entry_time_str:
        return ""
    try:
        entry_dt = pd.to_datetime(entry_time_str)
        if entry_dt.tzinfo is None:
            entry_dt = entry_dt.tz_localize('UTC')
        now_dt = datetime.now(timezone.utc)
        total_seconds = int((now_dt - entry_dt).total_seconds())
        if total_seconds < 0:
            total_seconds = 0
        hours = total_seconds // 3600
        minutes = (total_seconds % 3600) // 60
        if hours > 0:
            return f"Durasi: {hours} jam {minutes} menit"
        elif minutes > 0:
            return f"Durasi: {minutes} menit"
        else:
            return f"Durasi: {total_seconds} detik"
    except Exception:
        return ""

def get_tv_link(disp_name):
    clean = disp_name.replace("BITGET:", "").replace("/", "").replace("=F", "").replace("-", "").upper()
    if clean in ["EURUSD", "GBPUSD", "USDJPY", "USDCHF", "USDCAD", "AUDUSD", "NZDUSD", "EURGBP", "EURJPY", "GBPJPY"]:
        sym = f"FX:{clean}"
    elif clean in ["XAUUSD", "XAGUSD"]:
        sym = f"OANDA:{clean}"
    elif clean in ["USOUSD", "USOIL"]:
        sym = "TVC:USOIL"
    elif clean in ["UKOUSD", "UKOIL"]:
        sym = "TVC:UKOIL"
    elif clean in ["US500", "SP500"]:
        sym = "FOREXCOM:SPXUSD"
    elif clean in ["NAS100", "US100"]:
        sym = "FOREXCOM:NSXUSD"
    elif clean in ["US30"]:
        sym = "FOREXCOM:DJI"
    elif clean.endswith("USDT") or clean.endswith("PERP"):
        sym = f"BITGET:{clean}.P"
    else:
        sym = clean
    return f"https://www.tradingview.com/chart/?symbol={sym}"

def get_asset_hashtag(disp_name):
    clean = disp_name.replace("BITGET:", "").replace("/", "").replace("=F", "").replace("-", "").upper()
    return f"#{clean}"

def fmt_p(symbol, val):
    if val is None or val == "" or str(val) == "-":
        return "-"
    try:
        v = float(val)
    except (ValueError, TypeError):
        return str(val)
    if v == 0:
        return "0.00"
    abs_v = abs(v)
    sym_upper = str(symbol).upper()
    if "JPY" in sym_upper:
        return f"{v:,.3f}"
    if any(pair in sym_upper for pair in ["EUR", "GBP", "AUD", "NZD", "CAD", "CHF"]) and "USD" in sym_upper:
        return f"{v:,.5f}"
    if abs_v >= 1000:
        return f"{v:,.2f}"
    elif abs_v >= 1:
        formatted = f"{v:.4f}".rstrip('0').rstrip('.')
        if '.' in formatted:
            decimals = len(formatted.split('.')[1])
            if decimals < 2:
                formatted += '0' * (2 - decimals)
        else:
            formatted += '.00'
        return formatted
    elif abs_v >= 0.01:
        formatted = f"{v:.6f}".rstrip('0').rstrip('.')
        if '.' in formatted and len(formatted.split('.')[1]) < 2:
            formatted += '0'
        return formatted
    else:
        formatted = f"{v:.8f}".rstrip('0').rstrip('.')
        return formatted

# ==========================================
# DATABASE & PENGARSIPAN DINAMIS (BAB 8 & 9)
# ==========================================
def get_db_connection():
    conn = sqlite3.connect(DB_FILE, timeout=30)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL;")
    return conn

def init_db():
    with get_db_connection() as conn:
        cursor = conn.cursor()
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS positions (
                symbol TEXT PRIMARY KEY,
                pos_state INTEGER,
                entry_price REAL,
                active_sl REAL,
                active_tp1 REAL,
                active_tp2 REAL,
                active_tp3 REAL,
                hit_tp1 INTEGER,
                hit_tp2 INTEGER,
                last_signal_time TEXT,
                entry_time TEXT
            )
        ''')
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS trade_history (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                timestamp TEXT,
                symbol TEXT,
                result TEXT,
                entry REAL,
                exit REAL
            )
        ''')
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS recap_state (
                key TEXT PRIMARY KEY,
                val TEXT
            )
        ''')
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS archival_vault (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                symbol TEXT,
                timestamp TEXT,
                avg_d_res REAL,
                max_zf_score REAL,
                lambda_val REAL,
                status TEXT
            )
        ''')
        conn.commit()

def archive_session_data():
    try:
        now_iso = datetime.now(timezone.utc).isoformat()
        with state_lock:
            snapshot = [(sym, st) for sym, st in asset_states.items() if st["zf_score"] > 0]
        
        with get_db_connection() as conn:
            cursor = conn.cursor()
            for sym, st in snapshot:
                cursor.execute('''
                    INSERT INTO archival_vault (symbol, timestamp, avg_d_res, max_zf_score, lambda_val, status)
                    VALUES (?, ?, ?, ?, ?, ?)
                ''', (
                    sym, now_iso, st["d_res"], st["zf_score"],
                    st.get("lambda_elasticity", 1.0),
                    "CRITICAL" if st["zf_score"] > 0.8 else "NORMAL"
                ))
            conn.commit()
        purge_old_archives()
    except Exception as e:
        logging.error(f"[Archive Error] Gagal mengarsip sesi: {e}")

def purge_old_archives():
    try:
        cutoff = (datetime.now(timezone.utc) - timedelta(days=30)).isoformat()
        with get_db_connection() as conn:
            conn.execute('DELETE FROM archival_vault WHERE timestamp < ?', (cutoff,))
            conn.commit()
    except Exception as e:
        logging.error(f"[Purge Error] Gagal memangkas memori: {e}")

def create_empty_state():
    return {
        "live_price": 0.0,
        "d_res": 0.0,
        "zf_score": 0.0,
        "raw_drift": 0.0,
        "std_p": 0.0,
        "cvd": 0.0,
        "delta": 0.0,
        "h1_trend": 0,
        "pos_state": 0,
        "entry_price": None,
        "active_sl": None,
        "active_tp1": None,
        "active_tp2": None,
        "active_tp3": None,
        "hit_tp1": False,
        "hit_tp2": False,
        "last_signal_time": None,
        "entry_time": None,
        "candle_history": pd.DataFrame(),
        "ob_imbalance": 1.0,
        "spoofing_risk": False,
        "lambda_elasticity": 1.0
    }

def save_open_positions():
    try:
        with state_lock:
            snapshot = {sym: dict(st) for sym, st in asset_states.items() if st["pos_state"] != 0}
        with get_db_connection() as conn:
            cursor = conn.cursor()
            for sym, st in snapshot.items():
                cursor.execute('''
                    INSERT INTO positions
                    (symbol, pos_state, entry_price, active_sl, active_tp1, active_tp2, active_tp3, hit_tp1, hit_tp2, last_signal_time, entry_time)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    ON CONFLICT(symbol) DO UPDATE SET
                        pos_state=excluded.pos_state,
                        entry_price=excluded.entry_price,
                        active_sl=excluded.active_sl,
                        active_tp1=excluded.active_tp1,
                        active_tp2=excluded.active_tp2,
                        active_tp3=excluded.active_tp3,
                        hit_tp1=excluded.hit_tp1,
                        hit_tp2=excluded.hit_tp2,
                        last_signal_time=excluded.last_signal_time,
                        entry_time=excluded.entry_time
                ''', (
                    sym, st["pos_state"], st["entry_price"], st["active_sl"],
                    st["active_tp1"], st["active_tp2"], st["active_tp3"],
                    1 if st.get("hit_tp1") else 0,
                    1 if st.get("hit_tp2") else 0,
                    st.get("last_signal_time"),
                    st.get("entry_time")
                ))
            conn.commit()
    except Exception:
        pass

def load_open_positions():
    try:
        with get_db_connection() as conn:
            cursor = conn.cursor()
            cursor.execute('SELECT * FROM positions')
            rows = cursor.fetchall()
            with state_lock:
                for row in rows:
                    sym = row["symbol"]
                    if sym not in asset_states:
                        asset_states[sym] = create_empty_state()
                    keys = row.keys()
                    asset_states[sym].update({
                        "pos_state": row["pos_state"],
                        "entry_price": row["entry_price"],
                        "active_sl": row["active_sl"],
                        "active_tp1": row["active_tp1"],
                        "active_tp2": row["active_tp2"],
                        "active_tp3": row["active_tp3"],
                        "hit_tp1": bool(row["hit_tp1"]),
                        "hit_tp2": bool(row["hit_tp2"]),
                        "last_signal_time": row["last_signal_time"],
                        "entry_time": row["entry_time"] if "entry_time" in keys else None
                    })
    except Exception:
        pass

def load_recap_state():
    state = {"daily": "", "weekly": "", "monthly": ""}
    try:
        with get_db_connection() as conn:
            cursor = conn.cursor()
            cursor.execute('SELECT key, val FROM recap_state')
            for row in cursor.fetchall():
                if row["key"] in state:
                    state[row["key"]] = row["val"]
    except Exception:
        pass
    return state

def save_recap_state(state):
    try:
        with get_db_connection() as conn:
            cursor = conn.cursor()
            for k, v in state.items():
                cursor.execute('''
                    INSERT INTO recap_state (key, val) VALUES (?, ?)
                    ON CONFLICT(key) DO UPDATE SET val=excluded.val
                ''', (k, v))
            conn.commit()
    except Exception:
        pass

def log_trade_result(symbol, result_type, entry_p, exit_p):
    try:
        now_iso = datetime.now(timezone.utc).isoformat()
        with get_db_connection() as conn:
            conn.execute('''
                INSERT INTO trade_history (timestamp, symbol, result, entry, exit)
                VALUES (?, ?, ?, ?, ?)
            ''', (now_iso, symbol, result_type, entry_p, exit_p))
            conn.commit()
    except Exception:
        pass

def generate_recap(days, title):
    try:
        now = datetime.now(timezone.utc)
        cutoff = (now - timedelta(days=days)).isoformat()
        with get_db_connection() as conn:
            cursor = conn.cursor()
            cursor.execute('SELECT result FROM trade_history WHERE timestamp >= ?', (cutoff,))
            rows = cursor.fetchall()
        if not rows:
            return f"📊 <b>{title} ZF-CORE M91 PRO</b>\n\nTidak ada transaksi selesai dalam periode ini."
        total = len(rows)
        tp1_count = sum(1 for r in rows if r["result"] == "TP1")
        tp2_count = sum(1 for r in rows if r["result"] == "TP2")
        tp3_count = sum(1 for r in rows if r["result"] == "TP3")
        sl_count = sum(1 for r in rows if r["result"] == "SL")
        win_count = tp1_count + tp2_count + tp3_count
        win_rate = (win_count / total * 100) if total > 0 else 0.0
        return (
            f"📊 <b>{title} ZF-CORE M91 PRO</b>\n"
            f"━━━━━━━━━━━━━━━━━━━\n"
            f"📈 <b>Total Posisi Exit:</b> {total}\n"
            f"🎯 <b>Hit Take Profit 1:</b> {tp1_count}\n"
            f"🎯 <b>Hit Take Profit 2:</b> {tp2_count}\n"
            f"🎯 <b>Hit Take Profit 3:</b> {tp3_count}\n"
            f"🛑 <b>Hit Stop Loss:</b> {sl_count}\n"
            f"🔥 <b>Win Rate:</b> {win_rate:.1f}%\n"
            f"━━━━━━━━━━━━━━━━━━━"
        )
    except Exception as e:
        return f"Gagal membuat rekapan: {e}"

# ==========================================
# BITGET API AUTHENTICATION & MARKET DATA
# ==========================================
def bitget_signature(timestamp, method, request_path, body=""):
    message = timestamp + method.upper() + request_path + body
    mac = hmac.new(BITGET_SECRET_KEY.encode('utf-8'), message.encode('utf-8'), hashlib.sha256)
    return base64.b64encode(mac.digest()).decode('utf-8')

def bitget_headers(method, request_path, body=""):
    timestamp = str(int(time.time() * 1000))
    return {
        "ACCESS-KEY": BITGET_API_KEY,
        "ACCESS-SIGN": bitget_signature(timestamp, method, request_path, body),
        "ACCESS-TIMESTAMP": timestamp,
        "ACCESS-PASSPHRASE": BITGET_PASSPHRASE,
        "Content-Type": "application/json"
    }

def load_all_bitget_instruments():
    global DYNAMIC_ASSETS, API_TO_DISPLAY
    cfd_count = 0
    futures_count = 0

    try:
        path = "/api/v3/cfd/account/instruments"
        url = f"https://api.bitget.com{path}"
        headers = bitget_headers("GET", path) if BITGET_API_KEY else {}
        res = http_session.get(url, headers=headers, timeout=12).json()
        if res.get("code") == "00000" and "data" in res:
            for item in res.get("data", []):
                sym = item.get("symbol")
                if sym:
                    disp = sym
                    DYNAMIC_ASSETS[disp] = {
                        "api_symbol": sym,
                        "source": "bitget_cfd",
                        "params": PARAMS_UNIFIED
                    }
                    API_TO_DISPLAY[sym] = disp
                    cfd_count += 1
    except Exception as e:
        logging.error(f"[Init Error] Gagal memuat instrumen CFD: {e}")

    try:
        url = "https://api.bitget.com/api/v2/mix/market/tickers?productType=USDT-FUTURES"
        res = http_session.get(url, timeout=12).json()
        if res.get("code") == "00000" and "data" in res:
            for item in res.get("data", []):
                sym = item.get("symbol")
                if sym:
                    disp = f"BITGET:{sym}"
                    DYNAMIC_ASSETS[disp] = {
                        "api_symbol": sym,
                        "source": "bitget_futures",
                        "params": PARAMS_UNIFIED
                    }
                    API_TO_DISPLAY[sym] = disp
                    futures_count += 1
    except Exception as e:
        logging.error(f"[Init Error] Gagal memuat instrumen Futures: {e}")

    logging.info(f"✅ [Aset Dimuat] Total CFD: {cfd_count} | Futures: {futures_count}")

    with state_lock:
        for disp in DYNAMIC_ASSETS.keys():
            if disp not in asset_states:
                asset_states[disp] = create_empty_state()

# ==========================================
# BAB 3: ORDER BOOK DEPTH & SPOOFING ENGINE
# ==========================================
def get_bitget_orderbook(symbol, source):
    try:
        if source == "bitget_cfd":
            path = f"/api/v3/cfd/market/orderbook?symbol={symbol}&limit=15"
            url = f"https://api.bitget.com{path}"
            headers = bitget_headers("GET", path) if BITGET_API_KEY else {}
            res = http_session.get(url, headers=headers, timeout=12).json()
        else:
            url = f"https://api.bitget.com/api/v2/mix/market/orderbook?symbol={symbol}&productType=USDT-FUTURES&limit=15"
            res = http_session.get(url, timeout=12).json()

        if res.get("code") == "00000" and "data" in res:
            data = res["data"]
            bids = data.get("bids", [])
            asks = data.get("asks", [])
            if bids and asks:
                bid_vol = sum(float(b[1]) for b in bids)
                ask_vol = sum(float(a[1]) for a in asks)
                best_bid = float(bids[0][0])
                best_ask = float(asks[0][0])
                spread = max(best_ask - best_bid, 1e-6)

                imbalance = bid_vol / ask_vol if ask_vol > 0 else 1.0
                top_bid_vol = float(bids[0][1])
                top_ask_vol = float(asks[0][1])
                
                is_spoofing = (top_bid_vol / bid_vol > 0.8) if bid_vol > 0 else False
                is_spoofing = is_spoofing or ((top_ask_vol / ask_vol > 0.8) if ask_vol > 0 else False)

                mid_p = (best_bid + best_ask) / 2.0
                lambda_elasticity = (spread / mid_p * 100.0) if mid_p > 0 else 1.0

                return {
                    "imbalance": imbalance,
                    "spoofing": is_spoofing,
                    "lambda_elasticity": lambda_elasticity,
                    "bid_vol": bid_vol,
                    "ask_vol": ask_vol
                }
    except Exception:
        pass
    return None

def get_bitget_cfd_candles(symbol, granularity="5m"):
    gran_map = {"1m": "1m", "5m": "5m", "15m": "15m", "1h": "1h", "4h": "4h", "1D": "1D"}
    g = gran_map.get(granularity, "5m")
    path = f"/api/v3/cfd/market/candles?symbol={symbol}&granularity={g}&limit=1000"
    url = f"https://api.bitget.com{path}"
    try:
        headers = bitget_headers("GET", path) if BITGET_API_KEY else {}
        response = http_session.get(url, headers=headers, timeout=12)
        res = response.json()
        if res.get("code") == "00000" and "data" in res:
            raw_candles = res["data"]
            if raw_candles:
                data = [{
                    "datetime": pd.to_datetime(int(k[0]), unit='ms'),
                    "open": float(k[1]),
                    "high": float(k[2]),
                    "low": float(k[3]),
                    "close": float(k[4]),
                    "volume": float(k[5])
                } for k in raw_candles]
                df = pd.DataFrame(data)
                return df.sort_values('datetime').reset_index(drop=True)
    except Exception:
        pass
    return pd.DataFrame()

def get_bitget_futures_candles(symbol, granularity="5m"):
    gran_map = {"1m": "1m", "5m": "5m", "15m": "15m", "1h": "1h", "4h": "4h", "1D": "1D"}
    g = gran_map.get(granularity, "5m")
    url = f"https://api.bitget.com/api/v2/mix/market/candles?symbol={symbol}&productType=USDT-FUTURES&granularity={g}&limit=1000"
    try:
        res = http_session.get(url, timeout=12).json()
        if res.get("code") == "00000" and "data" in res:
            raw_candles = res["data"]
            if raw_candles:
                data = [{
                    "datetime": pd.to_datetime(int(k[0]), unit='ms'),
                    "open": float(k[1]),
                    "high": float(k[2]),
                    "low": float(k[3]),
                    "close": float(k[4]),
                    "volume": float(k[5])
                } for k in raw_candles]
                df = pd.DataFrame(data)
                return df.sort_values('datetime').reset_index(drop=True)
    except Exception:
        pass
    return pd.DataFrame()

# ==========================================
# BAB 4: FORMULASI MATEMATIS ZF-CORE ENGINE
# ==========================================
def calculate_zf_core(df, params, ob_data=None):
    if df.empty or len(df) < 200:
        return pd.DataFrame()
    p = params
    df = df.copy()

    df = df.replace([np.inf, -np.inf], np.nan).dropna(subset=['close', 'high', 'low', 'volume'])
    if len(df) < 200:
        return pd.DataFrame()

    try:
        df_temp = df.copy().set_index('datetime')
        df_h1 = df_temp.resample('1h').agg({
            'open': 'first', 'high': 'max', 'low': 'min', 'close': 'last', 'volume': 'sum'
        }).dropna().reset_index()
        df_h1['ema_h1'] = df_h1['close'].ewm(span=50, adjust=False).mean()
        df_h1['h1_trend'] = np.where(df_h1['close'] >= df_h1['ema_h1'], 1, -1)
        
        df = pd.merge_asof(
            df.sort_values('datetime'),
            df_h1[['datetime', 'h1_trend']].sort_values('datetime'),
            on='datetime',
            direction='backward'
        )
        df['h1_trend'] = df['h1_trend'].fillna(0)
    except Exception:
        df['h1_trend'] = 0

    vol_raw = df['volume']
    hl_range = (df['high'] - df['low']).replace(0, 1e-6)
    df['vol_eff'] = np.where(vol_raw > 0, vol_raw, hl_range)
    
    df['clv'] = np.clip(((df['close'] - df['low']) - (df['high'] - df['close'])) / hl_range, -1.0, 1.0)
    df['delta'] = df['vol_eff'] * df['clv']
    df['cvd'] = df['delta'].cumsum()

    lookback = p.get('kepekaan_fractal', 12)
    df['bull_div'] = (df['close'] < df['close'].shift(lookback)) & (df['cvd'] > df['cvd'].shift(lookback))
    df['bear_div'] = (df['close'] > df['close'].shift(lookback)) & (df['cvd'] < df['cvd'].shift(lookback))

    pv = df['close'] * df['vol_eff']
    pv_sum = pv.rolling(window=p['length_period']).sum()
    vol_sum = df['vol_eff'].rolling(window=p['length_period']).sum()
    ema_p = df['close'].ewm(span=p['length_period'], adjust=False).mean()
    
    df['p_pure'] = np.where(vol_sum > 0, pv_sum / vol_sum, ema_p)
    df['p_pure'] = df['p_pure'].replace(0, np.nan).fillna(df['close'])

    df['raw_drift'] = np.clip(((df['close'] - df['p_pure']) / df['p_pure']) * 100.0, -100.0, 100.0)
    df['d_res'] = df['raw_drift'].abs()

    v_avg = df['vol_eff'].rolling(window=p['length_period']).mean().replace(0, 1e-6)
    v_abs = (df['vol_eff'] - v_avg).abs()
    
    zf_ratio = np.clip(v_abs / df['vol_eff'], 0.0, 1.0)
    zf_x = np.clip(df['d_res'] * 10.0, 0.0, 20.0)
    df['zf_score'] = np.clip(zf_ratio * np.tanh(zf_x), 0.0, 1.0)

    dp_dt1 = df['close'] - df['close'].shift(1)
    dp_dt2 = df['close'].shift(1) - df['close'].shift(2)
    dp_dt3 = df['close'].shift(2) - df['close'].shift(3)
    d2p_dt2_curr = dp_dt1 - dp_dt2
    d2p_dt2_prev = dp_dt2 - dp_dt3
    df['is_inflection'] = ((d2p_dt2_curr * d2p_dt2_prev) <= 0) | (d2p_dt2_curr.abs() < 1e-5)

    high_max = df['high'].shift(1).rolling(window=p['kepekaan_fractal']).max()
    low_min = df['low'].shift(1).rolling(window=p['kepekaan_fractal']).min()
    df['is_fractal'] = (df['close'] >= high_max) | (df['close'] <= low_min)

    tr1 = df['high'] - df['low']
    tr2 = (df['high'] - df['close'].shift(1)).abs()
    tr3 = (df['low'] - df['close'].shift(1)).abs()
    tr = pd.concat([tr1, tr2, tr3], axis=1).max(axis=1)
    df['atr'] = tr.ewm(alpha=1/14, adjust=False).mean()
    min_fvg_dist = df['atr'] * p['min_fvg_mult']
    bull_fvg = df['low'] - df['high'].shift(2)
    bear_fvg = df['low'].shift(2) - df['high']
    price_jump = (df['close'] - df['close'].shift(2)).abs()
    max_fvg = pd.concat([bull_fvg, bear_fvg, price_jump], axis=1).max(axis=1)
    df['is_fvg'] = max_fvg >= min_fvg_dist
    df['std_p'] = df['close'].rolling(window=p['length_period']).std().fillna(df['close'] * 0.001)

    cvd_buy_confirm = ((df['delta'] > 0) & (df['cvd'] > df['cvd'].shift(1))) | df['bull_div']
    cvd_sell_confirm = ((df['delta'] < 0) & (df['cvd'] < df['cvd'].shift(1))) | df['bear_div']

    ob_spoof = ob_data.get("spoofing", False) if ob_data else False
    ob_imb = ob_data.get("imbalance", 1.0) if ob_data else 1.0

    df['raw_buy'] = (
        (df['raw_drift'] < 0) & (df['d_res'] >= p['min_drift']) &
        (df['zf_score'] >= p['batas_zf']) & df['is_inflection'] &
        df['is_fractal'] & df['is_fvg'] &
        (df['h1_trend'] == 1) & cvd_buy_confirm &
        (not ob_spoof) & (ob_imb >= 0.7)
    )
    df['raw_sell'] = (
        (df['raw_drift'] > 0) & (df['d_res'] >= p['min_drift']) &
        (df['zf_score'] >= p['batas_zf']) & df['is_inflection'] &
        df['is_fractal'] & df['is_fvg'] &
        (df['h1_trend'] == -1) & cvd_sell_confirm &
        (not ob_spoof) & (ob_imb <= 1.3)
    )
    return df

# ==========================================
# BAB 5 & 6: ANALISIS ASET & CIRCUIT BREAKER
# ==========================================
def analyze_single_asset(disp_name, cfg):
    api_sym = cfg["api_symbol"]
    source = cfg["source"]
    params = cfg["params"]

    ob_data = get_bitget_orderbook(api_sym, source)
    if ob_data is None:
        return

    if source == "bitget_cfd":
        df = get_bitget_cfd_candles(api_sym, granularity="5m")
    else:
        df = get_bitget_futures_candles(api_sym, granularity="5m")

    if df.empty or len(df) < 200:
        return

    df_calc = calculate_zf_core(df, params, ob_data)
    if df_calc.empty:
        return

    closed_row = df_calc.iloc[-2] if len(df_calc) >= 2 else df_calc.iloc[-1]
    curr_price = float(df_calc.iloc[-1]["close"])
    closed_row_time = str(closed_row.get("datetime", ""))

    raw_buy = bool(closed_row["raw_buy"])
    raw_sell = bool(closed_row["raw_sell"])
    std_p = float(closed_row["std_p"]) if not np.isnan(closed_row["std_p"]) else curr_price * 0.001
    clean_name = disp_name.replace("BITGET:", "")
    tv_url = get_tv_link(disp_name)
    hashtag = get_asset_hashtag(disp_name)

    state_changed = False
    signal_msg = None

    with state_lock:
        if disp_name not in asset_states:
            asset_states[disp_name] = create_empty_state()
        st = asset_states[disp_name]
        st["candle_history"] = df_calc
        st["live_price"] = curr_price
        st["d_res"] = float(closed_row["d_res"])
        st["zf_score"] = float(closed_row["zf_score"])
        st["raw_drift"] = float(closed_row["raw_drift"])
        st["cvd"] = float(closed_row.get("cvd", 0.0))
        st["delta"] = float(closed_row.get("delta", 0.0))
        st["h1_trend"] = int(closed_row.get("h1_trend", 0))
        st["std_p"] = std_p
        st["ob_imbalance"] = ob_data["imbalance"]
        st["spoofing_risk"] = ob_data["spoofing"]
        st["lambda_elasticity"] = ob_data["lambda_elasticity"]

        # Bab 6.1: PROTOKOL CIRCUIT BREAKER (ZF-Score > 0.99)
        if st["zf_score"] >= 0.99:
            if st["pos_state"] == 0:
                # Belum ada posisi aktif, blokir pencarian sinyal baru
                return
            else:
                # Sedang ada posisi aktif, biarkan posisi tetap terpantau TP/SL-nya
                pass

        buy_signal = (st["pos_state"] == 0) and raw_buy and (st.get("last_signal_time") != closed_row_time)
        sell_signal = (st["pos_state"] == 0) and raw_sell and (st.get("last_signal_time") != closed_row_time)

        h1_txt = "BULLISH 🟢" if st["h1_trend"] == 1 else "BEARISH 🔴" if st["h1_trend"] == -1 else "NEUTRAL ⚪"
        div_txt = " (Bullish Divergence 🚀)" if closed_row.get("bull_div") else " (Bearish Divergence 📉)" if closed_row.get("bear_div") else ""

        lot_rec = "🔥 MAX / LARGE LOT (Kondisi Kritis & Linier)" if st["zf_score"] >= 0.80 else "⚡ STANDARD LOT"

        if buy_signal:
            st["pos_state"] = 1
            st["entry_price"] = curr_price
            st["hit_tp1"] = False
            st["hit_tp2"] = False
            st["last_signal_time"] = closed_row_time
            st["entry_time"] = datetime.now(timezone.utc).isoformat()
            risk = std_p * params["sigma_sl_mult"]
            st["active_sl"] = curr_price - risk
            st["active_tp1"] = curr_price + (risk * params["rr1_ratio"])
            st["active_tp2"] = curr_price + (risk * params["rr2_ratio"])
            st["active_tp3"] = curr_price + (risk * params["rr3_ratio"])
            state_changed = True
            signal_msg = (
                f"🚨 <b>ZF-CORE M91 PRO BUY SIGNAL</b> 🚨\n\n"
                f"📊 <b>Pair:</b> {clean_name} {hashtag}\n"
                f"💵 <b>Entry:</b> {fmt_p(clean_name, curr_price)}\n"
                f"🎯 <b>Alokasi Posisi:</b> {lot_rec}\n"
                f"🌐 <b>H1 Trend:</b> {h1_txt}\n"
                f"🛑 <b>Stop Loss:</b> {fmt_p(clean_name, st['active_sl'])}\n"
                f"🎯 <b>TP 1:</b> {fmt_p(clean_name, st['active_tp1'])}\n"
                f"🎯 <b>TP 2:</b> {fmt_p(clean_name, st['active_tp2'])}\n"
                f"🎯 <b>TP 3:</b> {fmt_p(clean_name, st['active_tp3'])}\n"
                f"⚡ <b>ZF-Score:</b> {st['zf_score']:.2f} | <b>Drift:</b> {st['d_res']:.2f}%\n"
                f"📖 <b>Orderbook Ratio:</b> {st['ob_imbalance']:.2f} | <b>Spoofing:</b> SAFE\n"
                f"🌊 <b>Order Flow CVD:</b> {st['cvd']:,.0f} | <b>Delta:</b> {st['delta']:,.0f}{div_txt}\n\n"
                f"📈 <a href='{tv_url}'>Buka Grafik di TradingView</a>"
            )
        elif sell_signal:
            st["pos_state"] = -1
            st["entry_price"] = curr_price
            st["hit_tp1"] = False
            st["hit_tp2"] = False
            st["last_signal_time"] = closed_row_time
            st["entry_time"] = datetime.now(timezone.utc).isoformat()
            risk = std_p * params["sigma_sl_mult"]
            st["active_sl"] = curr_price + risk
            st["active_tp1"] = curr_price - (risk * params["rr1_ratio"])
            st["active_tp2"] = curr_price - (risk * params["rr2_ratio"])
            st["active_tp3"] = curr_price - (risk * params["rr3_ratio"])
            state_changed = True
            signal_msg = (
                f"🚨 <b>ZF-CORE M91 PRO SELL SIGNAL</b> 🚨\n\n"
                f"📊 <b>Pair:</b> {clean_name} {hashtag}\n"
                f"💵 <b>Entry:</b> {fmt_p(clean_name, curr_price)}\n"
                f"🎯 <b>Alokasi Posisi:</b> {lot_rec}\n"
                f"🌐 <b>H1 Trend:</b> {h1_txt}\n"
                f"🛑 <b>Stop Loss:</b> {fmt_p(clean_name, st['active_sl'])}\n"
                f"🎯 <b>TP 1:</b> {fmt_p(clean_name, st['active_tp1'])}\n"
                f"🎯 <b>TP 2:</b> {fmt_p(clean_name, st['active_tp2'])}\n"
                f"🎯 <b>TP 3:</b> {fmt_p(clean_name, st['active_tp3'])}\n"
                f"⚡ <b>ZF-Score:</b> {st['zf_score']:.2f} | <b>Drift:</b> {st['d_res']:.2f}%\n"
                f"📖 <b>Orderbook Ratio:</b> {st['ob_imbalance']:.2f} | <b>Spoofing:</b> SAFE\n"
                f"🌊 <b>Order Flow CVD:</b> {st['cvd']:,.0f} | <b>Delta:</b> {st['delta']:,.0f}{div_txt}\n\n"
                f"📈 <a href='{tv_url}'>Buka Grafik di TradingView</a>"
            )

    if state_changed:
        save_open_positions()
    if signal_msg:
        send_telegram_message(signal_msg)

# ==========================================
# TERMINAL SPINNER (BOLA/INDIKATOR BERPUTAR)
# ==========================================
def termux_running_spinner():
    """Menampilkan indikator bola berputar merah di terminal."""
    spinners = ['⠋', '⠙', '⠹', '⠸', '⠼', '⠴', '⠦', '⠧', '⠇', '⠏']
    i = 0
    while True:
        sys.stdout.write(f"\r\033[91m🔴 [ZF-CORE ACTIVE & RUNNING] {spinners[i]}\033[0m")
        sys.stdout.flush()
        i = (i + 1) % len(spinners)
        time.sleep(0.15)

# ==========================================
# SCHEDULER & REAL-TIME MONITORING (SL TETAP)
# ==========================================
def run_m91_scalper_scheduler():
    time.sleep(5)
    recap_state = load_recap_state()
    while True:
        try:
            now = datetime.now()
            seconds_to_next_5m = 300 - ((now.minute % 5) * 60 + now.second) + 8
            time.sleep(seconds_to_next_5m)

            wib_time = datetime.now(timezone.utc) + timedelta(hours=7)
            curr_daily_key = wib_time.strftime("%Y-%m-%d")
            curr_week_key = wib_time.strftime("%Y-W%U")
            curr_month_key = wib_time.strftime("%Y-%m")

            if recap_state.get("daily") != curr_daily_key:
                send_telegram_message(generate_recap(1, "REKAPAN HARIAN"))
                recap_state["daily"] = curr_daily_key
                save_recap_state(recap_state)
            if wib_time.weekday() == 0 and recap_state.get("weekly") != curr_week_key:
                send_telegram_message(generate_recap(7, "REKAPAN MINGGUAN"))
                recap_state["weekly"] = curr_week_key
                save_recap_state(recap_state)
            if wib_time.day == 1 and recap_state.get("monthly") != curr_month_key:
                send_telegram_message(generate_recap(30, "REKAPAN BULANAN"))
                recap_state["monthly"] = curr_month_key
                save_recap_state(recap_state)

            logging.info(f"🔄 [Scan Started] Menganalisis {len(DYNAMIC_ASSETS)} instrumen...")
            with concurrent.futures.ThreadPoolExecutor(max_workers=20) as executor:
                futures = [
                    executor.submit(analyze_single_asset, disp, cfg)
                    for disp, cfg in DYNAMIC_ASSETS.items()
                ]
                concurrent.futures.wait(futures)
            logging.info("✅ [Scan Complete] Pemindaian selesai.")

            archive_session_data()

        except Exception as e:
            logging.error(f"Scheduler error: {e}")

def realtime_sl_tp_monitor():
    while True:
        time.sleep(2)
        for p_type in ["USDT-FUTURES", "cfd"]:
            try:
                url = f"https://api.bitget.com/api/v2/mix/market/tickers?productType={p_type}"
                res = http_session.get(url, timeout=10).json()
                if res.get("code") == "00000" and "data" in res:
                    with state_lock:
                        for item in res["data"]:
                            sym = item.get("symbol") or item.get("instId")
                            last_p = item.get("lastPr")
                            if sym and last_p is not None:
                                price = float(last_p)
                                disp_name = API_TO_DISPLAY.get(sym) or f"BITGET:{sym}"
                                if disp_name in asset_states and price > 0:
                                    asset_states[disp_name]["live_price"] = price
            except Exception:
                pass

        pending_messages = []
        trade_logs = []
        state_changed = False

        with state_lock:
            for disp_name, st in list(asset_states.items()):
                if st["pos_state"] != 0 and st["live_price"] > 0:
                    curr = st["live_price"]
                    pos = st["pos_state"]
                    sl = st["active_sl"]
                    tp1 = st["active_tp1"]
                    tp2 = st["active_tp2"]
                    tp3 = st["active_tp3"]
                    entry = st["entry_price"]
                    clean_name = disp_name.replace("BITGET:", "")
                    hashtag = get_asset_hashtag(disp_name)

                    dur_info = fmt_duration(st.get("entry_time") or st.get("last_signal_time"))
                    dur_txt = f" | ⏱️ {dur_info}" if dur_info else ""

                    if pos == 1:
                        # SL TETAP (Tanpa Trailing / Tanpa Geser SL)
                        if sl is not None and curr <= sl:
                            st["pos_state"] = 0
                            st["entry_price"] = None
                            st["active_sl"] = None
                            st["active_tp1"] = None
                            st["active_tp2"] = None
                            st["active_tp3"] = None
                            st["hit_tp1"] = False
                            st["hit_tp2"] = False
                            st["entry_time"] = None
                            state_changed = True
                            pending_messages.append(f"🛑 <b>{clean_name} {hashtag} HIT STOP LOSS (EXIT)</b> @ {fmt_p(clean_name, curr)}{dur_txt}")
                            trade_logs.append((disp_name, "SL", entry, curr))
                        elif tp3 is not None and curr >= tp3:
                            st["pos_state"] = 0
                            st["entry_price"] = None
                            st["active_sl"] = None
                            st["active_tp1"] = None
                            st["active_tp2"] = None
                            st["active_tp3"] = None
                            st["hit_tp1"] = False
                            st["hit_tp2"] = False
                            st["entry_time"] = None
                            state_changed = True
                            pending_messages.append(f"🎯 <b>{clean_name} {hashtag} HIT TAKE PROFIT 3 (EXIT)</b> @ {fmt_p(clean_name, curr)}{dur_txt}")
                            trade_logs.append((disp_name, "TP3", entry, curr))
                        else:
                            if tp2 is not None and curr >= tp2 and not st.get("hit_tp2"):
                                st["hit_tp2"] = True
                                state_changed = True
                                pending_messages.append(f"🎯 <b>{clean_name} {hashtag} HIT TAKE PROFIT 2</b> @ {fmt_p(clean_name, curr)}{dur_txt}")
                                trade_logs.append((disp_name, "TP2", entry, curr))
                            elif tp1 is not None and curr >= tp1 and not st.get("hit_tp1"):
                                st["hit_tp1"] = True
                                state_changed = True
                                pending_messages.append(f"🎯 <b>{clean_name} {hashtag} HIT TAKE PROFIT 1</b> @ {fmt_p(clean_name, curr)}{dur_txt}")
                                trade_logs.append((disp_name, "TP1", entry, curr))

                    elif pos == -1:
                        # SL TETAP (Tanpa Trailing / Tanpa Geser SL)
                        if sl is not None and curr >= sl:
                            st["pos_state"] = 0
                            st["entry_price"] = None
                            st["active_sl"] = None
                            st["active_tp1"] = None
                            st["active_tp2"] = None
                            st["active_tp3"] = None
                            st["hit_tp1"] = False
                            st["hit_tp2"] = False
                            st["entry_time"] = None
                            state_changed = True
                            pending_messages.append(f"🛑 <b>{clean_name} {hashtag} HIT STOP LOSS (EXIT)</b> @ {fmt_p(clean_name, curr)}{dur_txt}")
                            trade_logs.append((disp_name, "SL", entry, curr))
                        elif tp3 is not None and curr <= tp3:
                            st["pos_state"] = 0
                            st["entry_price"] = None
                            st["active_sl"] = None
                            st["active_tp1"] = None
                            st["active_tp2"] = None
                            st["active_tp3"] = None
                            st["hit_tp1"] = False
                            st["hit_tp2"] = False
                            st["entry_time"] = None
                            state_changed = True
                            pending_messages.append(f"🎯 <b>{clean_name} {hashtag} HIT TAKE PROFIT 3 (EXIT)</b> @ {fmt_p(clean_name, curr)}{dur_txt}")
                            trade_logs.append((disp_name, "TP3", entry, curr))
                        else:
                            if tp2 is not None and curr <= tp2 and not st.get("hit_tp2"):
                                st["hit_tp2"] = True
                                state_changed = True
                                pending_messages.append(f"🎯 <b>{clean_name} {hashtag} HIT TAKE PROFIT 2</b> @ {fmt_p(clean_name, curr)}{dur_txt}")
                                trade_logs.append((disp_name, "TP2", entry, curr))
                            elif tp1 is not None and curr <= tp1 and not st.get("hit_tp1"):
                                st["hit_tp1"] = True
                                state_changed = True
                                pending_messages.append(f"🎯 <b>{clean_name} {hashtag} HIT TAKE PROFIT 1</b> @ {fmt_p(clean_name, curr)}{dur_txt}")
                                trade_logs.append((disp_name, "TP1", entry, curr))

        if state_changed:
            save_open_positions()
        for t in trade_logs:
            log_trade_result(*t)
        for msg in pending_messages:
            send_telegram_message(msg)

# ==========================================
# FLASK HEALTH SERVER & MAIN EXECUTION
# ==========================================
app = Flask(__name__)

@app.route('/')
def home():
    return "ZF-Core Scalper M91 Pro V16.3-OMNI (Fixed SL & Active Monitoring): Active", 200

def send_telegram_message(message):
    if not TELEGRAM_BOT_TOKEN or not TELEGRAM_GROUP_ID:
        return
    url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage"
    for i in range(0, len(message), 4000):
        payload = {
            "chat_id": TELEGRAM_GROUP_ID,
            "text": message[i:i+4000],
            "parse_mode": "HTML",
            "disable_web_page_preview": False
        }
        try:
            http_session.post(url, json=payload, timeout=12)
        except Exception:
            pass

def graceful_shutdown(signum, frame):
    save_open_positions()
    archive_session_data()
    sys.exit(0)

signal.signal(signal.SIGINT, graceful_shutdown)
signal.signal(signal.SIGTERM, graceful_shutdown)

if __name__ == "__main__":
    init_db()
    load_open_positions()
    load_all_bitget_instruments()

    spinner_thread = threading.Thread(target=termux_running_spinner, daemon=True)
    spinner_thread.start()

    realtime_thread = threading.Thread(target=realtime_sl_tp_monitor, daemon=True)
    realtime_thread.start()

    scheduler_thread = threading.Thread(target=run_m91_scalper_scheduler, daemon=True)
    scheduler_thread.start()

    port = int(os.environ.get("PORT", 5000))
    serve(app, host="0.0.0.0", port=port)
