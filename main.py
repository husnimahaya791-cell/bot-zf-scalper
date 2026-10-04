import os
import time
import json
import hmac
import hashlib
import base64
import sqlite3
import logging
import threading
import requests
import numpy as np
import pandas as pd
import websocket
import yfinance as yf
from flask import Flask
from datetime import datetime, timezone, timedelta

# Konfigurasi Logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[logging.StreamHandler()]
)

# File Penyimpanan SQLite
DB_FILE = os.environ.get("DB_PATH", "bot_database.db")

# Credentials Bitget
BITGET_API_KEY = os.environ.get("BITGET_API_KEY", "")
BITGET_SECRET_KEY = os.environ.get("BITGET_SECRET_KEY", "")
BITGET_PASSPHRASE = os.environ.get("BITGET_PASSPHRASE", "")

# Telegram Credentials
TELEGRAM_BOT_TOKEN = os.environ.get("TELEGRAM_BOT_TOKEN", "")
TELEGRAM_CHAT_ID = os.environ.get("TELEGRAM_CHAT_ID", "")
TELEGRAM_GROUP_ID = os.environ.get("TELEGRAM_GROUP_ID", "")

# Parameter Konfigurasi Strategi
PARAMS_FOREX = {
    "length_period": 50, "batas_zf": 0.12, "min_drift": 0.10, "use_ema_filter": True,
    "kepekaan_fractal": 8, "min_fvg_mult": 0.5, "sigma_sl_mult": 4.5,
    "rr1_ratio": 0.5, "rr2_ratio": 1.0, "rr3_ratio": 1.5, "use_trailing": True
}

PARAMS_BTC = {
    "length_period": 21, "batas_zf": 0.65, "min_drift": 0.17, "use_ema_filter": True,
    "kepekaan_fractal": 8, "min_fvg_mult": 0.5, "sigma_sl_mult": 4.5,
    "rr1_ratio": 0.5, "rr2_ratio": 1.0, "rr3_ratio": 1.5, "use_trailing": True
}

PARAMS_BITGET = {
    "length_period": 21, "batas_zf": 0.55, "min_drift": 0.15, "use_ema_filter": True,
    "kepekaan_fractal": 8, "min_fvg_mult": 0.5, "sigma_sl_mult": 4.5,
    "rr1_ratio": 0.5, "rr2_ratio": 1.0, "rr3_ratio": 1.5, "use_trailing": True
}

PARAMS_XAU_XAG = {
    "length_period": 21, "batas_zf": 0.51, "min_drift": 0.17, "use_ema_filter": True,
    "kepekaan_fractal": 8, "min_fvg_mult": 0.5, "sigma_sl_mult": 4.5,
    "rr1_ratio": 0.5, "rr2_ratio": 1.0, "rr3_ratio": 1.5, "use_trailing": True
}

PARAMS_UKOIL = {
    "length_period": 50, "batas_zf": 0.26, "min_drift": 0.15, "use_ema_filter": True,
    "kepekaan_fractal": 8, "min_fvg_mult": 0.5, "sigma_sl_mult": 4.5,
    "rr1_ratio": 0.5, "rr2_ratio": 1.0, "rr3_ratio": 1.5, "use_trailing": True
}

PARAMS_USOIL = {
    "length_period": 21, "batas_zf": 0.51, "min_drift": 0.27, "use_ema_filter": True,
    "kepekaan_fractal": 8, "min_fvg_mult": 0.5, "sigma_sl_mult": 4.5,
    "rr1_ratio": 0.5, "rr2_ratio": 1.0, "rr3_ratio": 1.5, "use_trailing": True
}

ASSET_CONFIG = {
    "Forex Majors": {
        "source": "twelvedata",
        "api_key": os.environ.get("TWELVEDATA_API_KEY_FOREX", ""),
        "assets": [
            {"api_symbol": "EUR/USD", "display_name": "EUR/USD", "params": PARAMS_FOREX},
            {"api_symbol": "GBP/USD", "display_name": "GBP/USD", "params": PARAMS_FOREX},
            {"api_symbol": "USD/JPY", "display_name": "USD/JPY", "params": PARAMS_FOREX},
            {"api_symbol": "USD/CHF", "display_name": "USD/CHF", "params": PARAMS_FOREX},
            {"api_symbol": "USD/CAD", "display_name": "USD/CAD", "params": PARAMS_FOREX},
            {"api_symbol": "AUD/USD", "display_name": "AUD/USD", "params": PARAMS_FOREX},
            {"api_symbol": "NZD/USD", "display_name": "NZD/USD", "params": PARAMS_FOREX},
        ],
        "interval": "5min"
    },
    "Gold TwelveData": {
        "source": "twelvedata",
        "api_key": os.environ.get("TWELVEDATA_API_KEY_GOLD", ""),
        "assets": [
            {"api_symbol": "XAU/USD", "display_name": "XAU/USD", "params": PARAMS_XAU_XAG}
        ],
        "interval": "5min"
    },
    "YFinance Commodities": {
        "source": "yfinance",
        "assets": [
            {"api_symbol": "SI=F", "display_name": "XAG/USD", "params": PARAMS_XAU_XAG},
            {"api_symbol": "CL=F", "display_name": "USOIL", "params": PARAMS_USOIL},
            {"api_symbol": "BZ=F", "display_name": "UKOIL", "params": PARAMS_UKOIL}
        ],
        "interval": "5m"
    },
    "Crypto": {
        "source": "yfinance",
        "assets": [
            {"api_symbol": "BTC-USD", "display_name": "BTC/USD", "params": PARAMS_BTC}
        ],
        "interval": "5m"
    }
}

API_TO_DISPLAY = {}
DISPLAY_TO_ASSET = {}

for group_name, group_cfg in ASSET_CONFIG.items():
    for item in group_cfg["assets"]:
        api_sym = item["api_symbol"]
        disp_name = item["display_name"]
        API_TO_DISPLAY[api_sym] = disp_name
        DISPLAY_TO_ASSET[disp_name] = {
            "group": group_name,
            "api_symbol": api_sym,
            "source": group_cfg["source"],
            "api_key": group_cfg.get("api_key", ""),
            "interval": group_cfg["interval"],
            "params": item["params"]
        }

state_lock = threading.Lock()
asset_states = {}

def get_db_connection():
    conn = sqlite3.connect(DB_FILE, timeout=10)
    conn.row_factory = sqlite3.Row
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
                last_signal_time TEXT
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
        conn.commit()

def create_empty_state():
    return {
        "live_price": 0.0,
        "d_res": 0.0,
        "zf_score": 0.0,
        "raw_drift": 0.0,
        "pos_state": 0,
        "entry_price": None,
        "active_sl": None,
        "active_tp1": None,
        "active_tp2": None,
        "active_tp3": None,
        "hit_tp1": False,
        "hit_tp2": False,
        "last_signal_time": None,
        "candle_history": pd.DataFrame()
    }

for disp_name in DISPLAY_TO_ASSET.keys():
    asset_states[disp_name] = create_empty_state()

http_session = requests.Session()

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

def get_bitget_top_gainers():
    path = "/api/v2/mix/market/tickers?productType=USDT-FUTURES"
    url = f"https://api.bitget.com{path}"
    headers = bitget_headers("GET", path) if BITGET_API_KEY else {}
    try:
        res = http_session.get(url, headers=headers, timeout=10).json()
        if res.get("code") == "00000" and "data" in res:
            tickers = res["data"]
            valid_tickers = [t for t in tickers if t.get("change24h") is not None]
            sorted_tickers = sorted(valid_tickers, key=lambda x: float(x.get("change24h", 0)), reverse=True)
            return sorted_tickers[:10]
    except Exception as e:
        logging.error(f"Gagal mengambil top gainers Bitget: {e}")
    return []

def get_bitget_futures_candles(symbol, granularity="5m"):
    gran_map = {"1m": "1m", "5m": "5m", "15m": "15m", "1h": "1h", "4h": "4h", "1D": "1D"}
    g = gran_map.get(granularity, "5m")
    path = f"/api/v2/mix/market/candles?symbol={symbol}&productType=USDT-FUTURES&granularity={g}&limit=200"
    url = f"https://api.bitget.com{path}"
    headers = bitget_headers("GET", path) if BITGET_API_KEY else {}
    try:
        res = http_session.get(url, headers=headers, timeout=10).json()
        if res.get("code") == "00000" and "data" in res:
            raw_candles = res["data"]
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
    except Exception as e:
        logging.error(f"Gagal mengambil candle Bitget {symbol}: {e}")
    return pd.DataFrame()

def save_open_positions():
    try:
        with get_db_connection() as conn:
            cursor = conn.cursor()
            with state_lock:
                for sym, st in asset_states.items():
                    if st["pos_state"] != 0:
                        cursor.execute('''
                            INSERT INTO positions 
                            (symbol, pos_state, entry_price, active_sl, active_tp1, active_tp2, active_tp3, hit_tp1, hit_tp2, last_signal_time)
                            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                            ON CONFLICT(symbol) DO UPDATE SET
                                pos_state=excluded.pos_state,
                                entry_price=excluded.entry_price,
                                active_sl=excluded.active_sl,
                                active_tp1=excluded.active_tp1,
                                active_tp2=excluded.active_tp2,
                                active_tp3=excluded.active_tp3,
                                hit_tp1=excluded.hit_tp1,
                                hit_tp2=excluded.hit_tp2,
                                last_signal_time=excluded.last_signal_time
                        ''', (
                            sym, st["pos_state"], st["entry_price"], st["active_sl"],
                            st["active_tp1"], st["active_tp2"], st["active_tp3"],
                            1 if st.get("hit_tp1") else 0,
                            1 if st.get("hit_tp2") else 0,
                            st.get("last_signal_time")
                        ))
                    else:
                        cursor.execute('DELETE FROM positions WHERE symbol = ?', (sym,))
            conn.commit()
    except Exception as e:
        logging.error(f"Gagal menyimpan posisi ke SQLite: {e}")

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
                    asset_states[sym].update({
                        "pos_state": row["pos_state"],
                        "entry_price": row["entry_price"],
                        "active_sl": row["active_sl"],
                        "active_tp1": row["active_tp1"],
                        "active_tp2": row["active_tp2"],
                        "active_tp3": row["active_tp3"],
                        "hit_tp1": bool(row["hit_tp1"]),
                        "hit_tp2": bool(row["hit_tp2"]),
                        "last_signal_time": row["last_signal_time"]
                    })
    except Exception as e:
        logging.error(f"Gagal memuat posisi dari SQLite: {e}")

def load_recap_state():
    state = {"daily": "", "weekly": "", "monthly": ""}
    try:
        with get_db_connection() as conn:
            cursor = conn.cursor()
            cursor.execute('SELECT key, val FROM recap_state')
            rows = cursor.fetchall()
            for row in rows:
                if row["key"] in state:
                    state[row["key"]] = row["val"]
    except Exception as e:
        logging.error(f"Gagal memuat recap state dari SQLite: {e}")
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
    except Exception as e:
        logging.error(f"Gagal menyimpan recap state ke SQLite: {e}")

def log_trade_result(symbol, result_type, entry_p, exit_p):
    try:
        now_iso = datetime.now(timezone.utc).isoformat()
        with get_db_connection() as conn:
            cursor = conn.cursor()
            cursor.execute('''
                INSERT INTO trade_history (timestamp, symbol, result, entry, exit)
                VALUES (?, ?, ?, ?, ?)
            ''', (now_iso, symbol, result_type, entry_p, exit_p))
            conn.commit()
    except Exception as e:
        logging.error(f"Gagal mencatat transaksi ke SQLite: {e}")

def generate_recap(days, title):
    try:
        now = datetime.now(timezone.utc)
        cutoff = (now - timedelta(days=days)).isoformat()
        
        with get_db_connection() as conn:
            cursor = conn.cursor()
            cursor.execute('''
                SELECT result FROM trade_history WHERE timestamp >= ?
            ''', (cutoff,))
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

def fmt_p(symbol, val):
    """
    Format angka dinamis presisi tinggi agar tidak memotong harga altcoins Bitget.
    """
    if val is None or np.isnan(val):
        return "-"
    try:
        val = float(val)
    except (ValueError, TypeError):
        return str(val)

    if val == 0:
        return "0.00"

    sym_upper = str(symbol).upper()

    # Forex
    if "JPY" in sym_upper:
        return f"{val:,.3f}"
    if any(pair in sym_upper for pair in ["EUR", "GBP", "AUD", "NZD", "CAD", "CHF"]) and "USD" in sym_upper:
        return f"{val:,.5f}"

    abs_val = abs(val)

    # Cryptocurrencies & Commodities
    if abs_val >= 1000:
        return f"{val:,.2f}"
    elif abs_val >= 10:
        return f"{val:,.2f}"
    elif abs_val >= 1.0:
        s = f"{val:,.4f}".rstrip('0')
        if s.endswith('.'):
            s += '00'
        elif len(s.split('.')[1]) < 2:
            s += '0'
        return s
    elif abs_val >= 0.001:
        s = f"{val:.6f}".rstrip('0')
        if len(s.split('.')[1]) < 4:
            s = f"{val:.4f}"
        return s
    elif abs_val >= 0.000001:
        s = f"{val:.8f}".rstrip('0')
        if len(s.split('.')[1]) < 6:
            s = f"{val:.6f}"
        return s
    else:
        return f"{val:.8f}"

app = Flask(__name__)

@app.route('/')
def home():
    return "ZF-Core Scalper M91 Pro: Active", 200

def send_telegram_message(message, target_chat_id=None):
    if target_chat_id is None:
        target_chat_id = TELEGRAM_CHAT_ID
        
    if not TELEGRAM_BOT_TOKEN or not target_chat_id:
        return
    url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage"
    payload = {"chat_id": target_chat_id, "text": message, "parse_mode": "HTML"}
    try:
        http_session.post(url, json=payload, timeout=10)
    except Exception as e:
        logging.error(f"Gagal mengirim pesan Telegram: {e}")

def calculate_zf_core(df, params):
    if df.empty or len(df) < 150:
        return df

    p = params
    df = df.copy()

    vol_raw = df['volume'] if 'volume' in df.columns else pd.Series(0, index=df.index)
    if (vol_raw == 0).all() or (vol_raw.std() == 0):
        df['vol_eff'] = (df['high'] - df['low']).replace(0, 1e-6)
    else:
        df['vol_eff'] = np.where(vol_raw > 0, vol_raw, (df['high'] - df['low']).replace(0, 1e-6))

    pv = df['close'] * df['vol_eff']
    pv_sum = pv.rolling(window=p['length_period']).sum()
    vol_sum = df['vol_eff'].rolling(window=p['length_period']).sum()
    ema_p = df['close'].ewm(span=p['length_period'], adjust=False).mean()
    df['p_pure'] = np.where(vol_sum > 0, pv_sum / vol_sum, ema_p)

    df['raw_drift'] = ((df['close'] - df['p_pure']) / df['p_pure']) * 100.0
    df['d_res'] = df['raw_drift'].abs()

    v_avg = df['vol_eff'].rolling(window=p['length_period']).mean()
    v_abs = (df['vol_eff'] - v_avg).abs()
    df['zf_ratio'] = np.where(df['vol_eff'] > 0, v_abs / df['vol_eff'], 0.0)

    zf_x = df['d_res'] * 10.0
    zf_e2x = np.exp(2.0 * zf_x)
    zf_tanh = (zf_e2x - 1.0) / (zf_e2x + 1.0)
    df['zf_score'] = df['zf_ratio'] * zf_tanh

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

    df['ema200'] = df['close'].ewm(span=200, adjust=False).mean()
    if p['use_ema_filter']:
        df['trend_buy'] = df['close'] > df['ema200']
        df['trend_sell'] = df['close'] < df['ema200']
    else:
        df['trend_buy'] = True
        df['trend_sell'] = True

    df['std_p'] = df['close'].rolling(window=p['length_period']).std()

    df['raw_buy'] = (
        (df['raw_drift'] < 0) &
        (df['d_res'] >= p['min_drift']) &
        (df['zf_score'] >= p['batas_zf']) &
        df['is_inflection'] &
        df['is_fractal'] &
        df['is_fvg'] &
        df['trend_buy']
    )
    
    df['raw_sell'] = (
        (df['raw_drift'] > 0) &
        (df['d_res'] >= p['min_drift']) &
        (df['zf_score'] >= p['batas_zf']) &
        df['is_inflection'] &
        df['is_fractal'] &
        df['is_fvg'] &
        df['trend_sell']
    )

    return df

def fetch_candles_for_symbol(api_symbol, source, api_key="", interval="5min"):
    try:
        if source == "yfinance":
            yf_interval = interval.replace("min", "m")
            ticker = yf.Ticker(api_symbol)
            df_yf = ticker.history(period="7d", interval=yf_interval)
            if not df_yf.empty:
                df_yf = df_yf.reset_index()
                date_col = 'Datetime' if 'Datetime' in df_yf.columns else 'Date'
                df_yf = df_yf.rename(columns={
                    date_col: 'datetime',
                    'Open': 'open',
                    'High': 'high',
                    'Low': 'low',
                    'Close': 'close',
                    'Volume': 'volume'
                })
                df_yf['datetime'] = pd.to_datetime(df_yf['datetime'])
                df_yf = df_yf.sort_values('datetime').reset_index(drop=True)
                return df_yf[['datetime', 'open', 'high', 'low', 'close', 'volume']]
                
        else:
            url = f"https://api.twelvedata.com/time_series?symbol={api_symbol}&interval={interval}&outputsize=200&apikey={api_key}"
            res = http_session.get(url, timeout=10).json()
            if "values" in res:
                data = res["values"]
                df = pd.DataFrame(data)
                df['datetime'] = pd.to_datetime(df['datetime'])
                df = df.sort_values('datetime').reset_index(drop=True)
                df['open'] = df['open'].astype(float)
                df['high'] = df['high'].astype(float)
                df['low'] = df['low'].astype(float)
                df['close'] = df['close'].astype(float)
                df['volume'] = df['volume'].astype(float) if 'volume' in df.columns else 0.0
                return df
    except Exception as e:
        logging.error(f"Gagal mengambil candle {api_symbol} ({source}): {e}")
    return pd.DataFrame()

def update_all_historical_data():
    for disp_name, info in DISPLAY_TO_ASSET.items():
        api_sym = info["api_symbol"]
        source = info["source"]
        key = info["api_key"]
        interval = info["interval"]
        params = info["params"]

        df = fetch_candles_for_symbol(api_sym, source, key, interval)
        if not df.empty:
            df_calc = calculate_zf_core(df.tail(200), params)
            closed_bar = df_calc.iloc[-2] if len(df_calc) >= 2 else df_calc.iloc[-1]
            with state_lock:
                if disp_name not in asset_states:
                    asset_states[disp_name] = create_empty_state()
                asset_states[disp_name]["candle_history"] = df_calc
                if asset_states[disp_name]["live_price"] == 0.0:
                    asset_states[disp_name]["live_price"] = float(df_calc.iloc[-1]["close"])
                asset_states[disp_name]["d_res"] = float(closed_bar["d_res"])
                asset_states[disp_name]["zf_score"] = float(closed_bar["zf_score"])
                asset_states[disp_name]["raw_drift"] = float(closed_bar["raw_drift"])
        time.sleep(0.2)

def process_single_asset_lifecycle(disp_name, params, current_price, closed_row):
    closed_row_time = str(closed_row.get("datetime", ""))
    raw_buy = bool(closed_row["raw_buy"])
    raw_sell = bool(closed_row["raw_sell"])
    std_p = float(closed_row["std_p"]) if not np.isnan(closed_row["std_p"]) else current_price * 0.001

    clean_name = disp_name.replace("BITGET:", "")

    with state_lock:
        if disp_name not in asset_states:
            asset_states[disp_name] = create_empty_state()

        st = asset_states[disp_name]
        pos_state = st["pos_state"]
        sl = st["active_sl"]
        tp1 = st["active_tp1"]
        tp2 = st["active_tp2"]
        tp3 = st["active_tp3"]
        entry = st["entry_price"]
        state_changed = False

        st["d_res"] = float(closed_row["d_res"])
        st["zf_score"] = float(closed_row["zf_score"])
        st["raw_drift"] = float(closed_row["raw_drift"])
        st["live_price"] = current_price

        # --- EVALUASI POSISI BUY ---
        if pos_state == 1:
            if params["use_trailing"]:
                trail_sl = current_price - (std_p * params["sigma_sl_mult"])
                if sl is not None and trail_sl > sl:
                    st["active_sl"] = trail_sl
                    sl = trail_sl
                    state_changed = True

            if sl is not None and current_price <= sl:
                st["pos_state"] = 0
                st["entry_price"] = None
                st["active_sl"] = None
                st["active_tp1"] = None
                st["active_tp2"] = None
                st["active_tp3"] = None
                st["hit_tp1"] = False
                st["hit_tp2"] = False
                state_changed = True
                send_telegram_message(f"🛑 <b>{clean_name} HIT STOP LOSS (EXIT)</b> @ {fmt_p(clean_name, current_price)}", target_chat_id=TELEGRAM_GROUP_ID)
                log_trade_result(disp_name, "SL", entry, current_price)

            elif tp3 is not None and current_price >= tp3:
                st["pos_state"] = 0
                st["entry_price"] = None
                st["active_sl"] = None
                st["active_tp1"] = None
                st["active_tp2"] = None
                st["active_tp3"] = None
                st["hit_tp1"] = False
                st["hit_tp2"] = False
                state_changed = True
                send_telegram_message(f"🎯 <b>{clean_name} HIT TAKE PROFIT 3 (EXIT)</b> @ {fmt_p(clean_name, current_price)}", target_chat_id=TELEGRAM_GROUP_ID)
                log_trade_result(disp_name, "TP3", entry, current_price)

            else:
                if tp2 is not None and current_price >= tp2 and not st.get("hit_tp2", False):
                    st["hit_tp2"] = True
                    if tp1 is not None:
                        st["active_sl"] = max(st["active_sl"] if st["active_sl"] is not None else tp1, tp1)
                    state_changed = True
                    send_telegram_message(f"🎯 <b>{clean_name} HIT TAKE PROFIT 2 (SL -> TP1)</b> @ {fmt_p(clean_name, current_price)}", target_chat_id=TELEGRAM_GROUP_ID)
                    log_trade_result(disp_name, "TP2", entry, current_price)

                elif tp1 is not None and current_price >= tp1 and not st.get("hit_tp1", False):
                    st["hit_tp1"] = True
                    if entry is not None:
                        st["active_sl"] = max(st["active_sl"] if st["active_sl"] is not None else entry, entry)
                    state_changed = True
                    send_telegram_message(f"🎯 <b>{clean_name} HIT TAKE PROFIT 1 (SL -> BE)</b> @ {fmt_p(clean_name, current_price)}", target_chat_id=TELEGRAM_GROUP_ID)
                    log_trade_result(disp_name, "TP1", entry, current_price)

        # --- EVALUASI POSISI SELL ---
        elif pos_state == -1:
            if params["use_trailing"]:
                trail_sl = current_price + (std_p * params["sigma_sl_mult"])
                if sl is not None and trail_sl < sl:
                    st["active_sl"] = trail_sl
                    sl = trail_sl
                    state_changed = True

            if sl is not None and current_price >= sl:
                st["pos_state"] = 0
                st["entry_price"] = None
                st["active_sl"] = None
                st["active_tp1"] = None
                st["active_tp2"] = None
                st["active_tp3"] = None
                st["hit_tp1"] = False
                st["hit_tp2"] = False
                state_changed = True
                send_telegram_message(f"🛑 <b>{clean_name} HIT STOP LOSS (EXIT)</b> @ {fmt_p(clean_name, current_price)}", target_chat_id=TELEGRAM_GROUP_ID)
                log_trade_result(disp_name, "SL", entry, current_price)

            elif tp3 is not None and current_price <= tp3:
                st["pos_state"] = 0
                st["entry_price"] = None
                st["active_sl"] = None
                st["active_tp1"] = None
                st["active_tp2"] = None
                st["active_tp3"] = None
                st["hit_tp1"] = False
                st["hit_tp2"] = False
                state_changed = True
                send_telegram_message(f"🎯 <b>{clean_name} HIT TAKE PROFIT 3 (EXIT)</b> @ {fmt_p(clean_name, current_price)}", target_chat_id=TELEGRAM_GROUP_ID)
                log_trade_result(disp_name, "TP3", entry, current_price)

            else:
                if tp2 is not None and current_price <= tp2 and not st.get("hit_tp2", False):
                    st["hit_tp2"] = True
                    if tp1 is not None:
                        st["active_sl"] = min(st["active_sl"] if st["active_sl"] is not None else tp1, tp1)
                    state_changed = True
                    send_telegram_message(f"🎯 <b>{clean_name} HIT TAKE PROFIT 2 (SL -> TP1)</b> @ {fmt_p(clean_name, current_price)}", target_chat_id=TELEGRAM_GROUP_ID)
                    log_trade_result(disp_name, "TP2", entry, current_price)

                elif tp1 is not None and current_price <= tp1 and not st.get("hit_tp1", False):
                    st["hit_tp1"] = True
                    if entry is not None:
                        st["active_sl"] = min(st["active_sl"] if st["active_sl"] is not None else entry, entry)
                    state_changed = True
                    send_telegram_message(f"🎯 <b>{clean_name} HIT TAKE PROFIT 1 (SL -> BE)</b> @ {fmt_p(clean_name, current_price)}", target_chat_id=TELEGRAM_GROUP_ID)
                    log_trade_result(disp_name, "TP1", entry, current_price)

        # --- PEMANTAUAN SINYAL BARU ---
        buy_signal = (st["pos_state"] == 0) and raw_buy and (st.get("last_signal_time") != closed_row_time)
        sell_signal = (st["pos_state"] == 0) and raw_sell and (st.get("last_signal_time") != closed_row_time)

        if buy_signal:
            st["pos_state"] = 1
            st["entry_price"] = current_price
            st["hit_tp1"] = False
            st["hit_tp2"] = False
            st["last_signal_time"] = closed_row_time
            risk = std_p * params["sigma_sl_mult"]
            st["active_sl"] = current_price - risk
            st["active_tp1"] = current_price + (risk * params["rr1_ratio"])
            st["active_tp2"] = current_price + (risk * params["rr2_ratio"])
            st["active_tp3"] = current_price + (risk * params["rr3_ratio"])
            state_changed = True

            msg_buy = (
                f"🚨 <b>ZF-CORE M91 PRO BUY SIGNAL</b> 🚨\n\n"
                f"📊 <b>Pair:</b> {clean_name}\n"
                f"💵 <b>Entry:</b> {fmt_p(clean_name, current_price)}\n"
                f"🛑 <b>Trailing SL:</b> {fmt_p(clean_name, st['active_sl'])}\n"
                f"🎯 <b>TP 1:</b> {fmt_p(clean_name, st['active_tp1'])}\n"
                f"🎯 <b>TP 2:</b> {fmt_p(clean_name, st['active_tp2'])}\n"
                f"🎯 <b>TP 3:</b> {fmt_p(clean_name, st['active_tp3'])}\n"
                f"⚡ <b>ZF-Score:</b> {st['zf_score']:.2f} | <b>Drift:</b> {st['d_res']:.2f}%"
            )
            send_telegram_message(msg_buy, target_chat_id=TELEGRAM_GROUP_ID)

        elif sell_signal:
            st["pos_state"] = -1
            st["entry_price"] = current_price
            st["hit_tp1"] = False
            st["hit_tp2"] = False
            st["last_signal_time"] = closed_row_time
            risk = std_p * params["sigma_sl_mult"]
            st["active_sl"] = current_price + risk
            st["active_tp1"] = current_price - (risk * params["rr1_ratio"])
            st["active_tp2"] = current_price - (risk * params["rr2_ratio"])
            st["active_tp3"] = current_price - (risk * params["rr3_ratio"])
            state_changed = True

            msg_sell = (
                f"🚨 <b>ZF-CORE M91 PRO SELL SIGNAL</b> 🚨\n\n"
                f"📊 <b>Pair:</b> {clean_name}\n"
                f"💵 <b>Entry:</b> {fmt_p(clean_name, current_price)}\n"
                f"🛑 <b>Trailing SL:</b> {fmt_p(clean_name, st['active_sl'])}\n"
                f"🎯 <b>TP 1:</b> {fmt_p(clean_name, st['active_tp1'])}\n"
                f"🎯 <b>TP 2:</b> {fmt_p(clean_name, st['active_tp2'])}\n"
                f"🎯 <b>TP 3:</b> {fmt_p(clean_name, st['active_tp3'])}\n"
                f"⚡ <b>ZF-Score:</b> {st['zf_score']:.2f} | <b>Drift:</b> {st['d_res']:.2f}%"
            )
            send_telegram_message(msg_sell, target_chat_id=TELEGRAM_GROUP_ID)

        if state_changed:
            save_open_positions()

        state_txt = " | [BUY 🟢]" if st["pos_state"] == 1 else " | [SELL 🔴]" if st["pos_state"] == -1 else ""
        return f"• <b>{clean_name}</b>: {fmt_p(clean_name, current_price)} | D_res: {st['d_res']:.2f}% | ZF: {st['zf_score']:.2f}{state_txt}"

def start_websocket_twelvedata(group_name, api_key, assets):
    api_symbols = [item["api_symbol"] for item in assets]
    ws_url = f"wss://ws.twelvedata.com/v1/quotes/price?apikey={api_key}"

    def on_open(ws):
        ws.send(json.dumps({"action": "subscribe", "params": {"symbols": ",".join(api_symbols)}}))

    def on_message(ws, message):
        try:
            data = json.loads(message)
            if data.get("event") == "price":
                raw_sym = data.get("symbol")
                price = float(data.get("price", 0))
                disp_name = API_TO_DISPLAY.get(raw_sym)
                if disp_name and price > 0:
                    with state_lock:
                        if disp_name not in asset_states:
                            asset_states[disp_name] = create_empty_state()
                        asset_states[disp_name]["live_price"] = price
        except Exception as e:
            logging.error(f"Error WebSocket message ({group_name}): {e}")

    while True:
        try:
            ws = websocket.WebSocketApp(ws_url, on_open=on_open, on_message=on_message)
            ws.run_forever(ping_interval=30, ping_timeout=10)
        except Exception as e:
            logging.error(f"Koneksi WebSocket terputus ({group_name}): {e}")
        time.sleep(5)

def run_m91_scalper_scheduler():
    time.sleep(5)
    recap_state = load_recap_state()

    while True:
        try:
            now = datetime.now()
            # Sinkronisasi ke awal interval 5 menit
            seconds_to_next_5m = 300 - ((now.minute % 5) * 60 + now.second)
            time.sleep(seconds_to_next_5m)

            wib_time = datetime.now(timezone.utc) + timedelta(hours=7)
            time_str = wib_time.strftime("%Y-%m-%d %H:%M:00 WIB")

            curr_daily_key = wib_time.strftime("%Y-%m-%d")
            curr_week_key = wib_time.strftime("%Y-W%U")
            curr_month_key = wib_time.strftime("%Y-%m")

            # Rekapan Periodik
            if recap_state.get("daily") != curr_daily_key:
                send_telegram_message(generate_recap(1, "REKAPAN HARIAN"), target_chat_id=TELEGRAM_GROUP_ID)
                recap_state["daily"] = curr_daily_key
                save_recap_state(recap_state)

            if wib_time.weekday() == 0 and recap_state.get("weekly") != curr_week_key:
                send_telegram_message(generate_recap(7, "REKAPAN MINGGUAN"), target_chat_id=TELEGRAM_GROUP_ID)
                recap_state["weekly"] = curr_week_key
                save_recap_state(recap_state)

            if wib_time.day == 1 and recap_state.get("monthly") != curr_month_key:
                send_telegram_message(generate_recap(30, "REKAPAN BULANAN"), target_chat_id=TELEGRAM_GROUP_ID)
                recap_state["monthly"] = curr_month_key
                save_recap_state(recap_state)

            update_all_historical_data()
            flat_status_logs = []

            # 1. ASET UTAMA
            for disp_name, info in DISPLAY_TO_ASSET.items():
                params = info["params"]
                with state_lock:
                    st = asset_states.get(disp_name, {})
                    df = st.get("candle_history", pd.DataFrame())
                    curr_price = st.get("live_price", 0.0)

                if df.empty or len(df) < 5:
                    flat_status_logs.append(f"• <b>{disp_name}</b>: Data belum siap")
                    continue

                closed_row = df.iloc[-2] if len(df) >= 2 else df.iloc[-1]
                log_line = process_single_asset_lifecycle(disp_name, params, curr_price, closed_row)
                flat_status_logs.append(log_line)

            # 2. TOP 10 BITGET FUTURES GAINERS (Real-time Ticker Price)
            top_gainers = get_bitget_top_gainers()
            active_bitget_symbols = [coin.get("symbol", "") for coin in top_gainers if coin.get("symbol")]
            ticker_price_map = {
                coin.get("symbol"): float(coin.get("lastPr", 0)) 
                for coin in top_gainers 
                if coin.get("symbol") and coin.get("lastPr")
            }

            with state_lock:
                for name, st in asset_states.items():
                    if name.startswith("BITGET:") and st.get("pos_state", 0) != 0:
                        raw_s = name.replace("BITGET:", "")
                        if raw_s not in active_bitget_symbols:
                            active_bitget_symbols.append(raw_s)

            if active_bitget_symbols:
                flat_status_logs.append("\n🚀 <b>BITGET TOP GAINERS & POSISI AKTIF:</b>")

            for sym in active_bitget_symbols:
                disp_name = f"BITGET:{sym}"
                df = get_bitget_futures_candles(sym, granularity="5m")
                
                if not df.empty and len(df) >= 150:
                    df_calc = calculate_zf_core(df, PARAMS_BITGET)
                    closed_row = df_calc.iloc[-2] if len(df_calc) >= 2 else df_calc.iloc[-1]
                    
                    # Ambil harga real-time langsung dari API Ticker Bitget
                    live_p = ticker_price_map.get(sym, 0.0)
                    if live_p > 0:
                        curr_price = live_p
                    else:
                        curr_price = float(df_calc.iloc[-1]["close"])
                    
                    with state_lock:
                        if disp_name not in asset_states:
                            asset_states[disp_name] = create_empty_state()
                        asset_states[disp_name]["candle_history"] = df_calc
                        asset_states[disp_name]["live_price"] = curr_price

                    log_line = process_single_asset_lifecycle(disp_name, PARAMS_BITGET, curr_price, closed_row)
                    flat_status_logs.append(log_line)
                else:
                    flat_status_logs.append(f"• <b>{sym}</b>: Data Belum Siap")
                
                time.sleep(0.1)

            # 3. KIRIM PESAN REKAP STATUS
            log_msg = (
                f"⚡ <b>ZF-Core Scalper M91 Pro Status (Sync 5m)</b>\n"
                f"Waktu : {time_str}\n\n" +
                "\n".join(flat_status_logs)
            )
            send_telegram_message(log_msg)

        except Exception as e:
            logging.error(f"Error pada loop scheduler utama: {e}")

# Inisialisasi awal
init_db()
load_open_positions()
update_all_historical_data()

# Menjalankan WebSocket TwelveData di Thread terpisah
for group_name, group_cfg in ASSET_CONFIG.items():
    if group_cfg["source"] == "twelvedata":
        ws_thread = threading.Thread(
            target=start_websocket_twelvedata,
            args=(group_name, group_cfg["api_key"], group_cfg["assets"]),
            daemon=True
        )
        ws_thread.start()

# Menjalankan Scheduler Utama
scheduler_thread = threading.Thread(target=run_m91_scalper_scheduler, daemon=True)
scheduler_thread.start()

if __name__ == "__main__":
    port = int(os.environ.get("PORT", 5000))
    app.run(host="0.0.0.0", port=port)
