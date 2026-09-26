import os
import time
import json
import threading
import requests
import numpy as np
import pandas as pd
import websocket
import yfinance as yf
from flask import Flask
from datetime import datetime, timezone, timedelta

STATE_FILE = "bot_state.json"
STATS_FILE = "trade_history.json"

GLOBAL_PARAMS = {
    "length_period": 20,
    "batas_zf": 0.55,
    "min_drift": 0.15,
    "use_ema_filter": True,
    "kepekaan_fractal": 8,
    "min_fvg_mult": 0.5,
    "sigma_sl_mult": 4.5,
    "rr1_ratio": 0.5,
    "rr2_ratio": 1.0,
    "rr3_ratio": 1.5,
    "use_trailing": True
}

ASSET_CONFIG = {
    "Forex Majors": {
        "source": "twelvedata",
        "api_key": os.environ.get("TWELVEDATA_API_KEY_FOREX", ""),
        "assets": [
            {"api_symbol": "EUR/USD", "display_name": "EUR/USD"},
            {"api_symbol": "GBP/USD", "display_name": "GBP/USD"},
            {"api_symbol": "USD/JPY", "display_name": "USD/JPY"},
            {"api_symbol": "USD/CHF", "display_name": "USD/CHF"},
            {"api_symbol": "USD/CAD", "display_name": "USD/CAD"},
            {"api_symbol": "AUD/USD", "display_name": "AUD/USD"},
            {"api_symbol": "NZD/USD", "display_name": "NZD/USD"},
        ],
        "interval": "30min",
        "params": GLOBAL_PARAMS
    },
    "Gold TwelveData": {
        "source": "twelvedata",
        "api_key": os.environ.get("TWELVEDATA_API_KEY_GOLD", ""),
        "assets": [
            {"api_symbol": "XAU/USD", "display_name": "XAU/USD"}
        ],
        "interval": "30min",
        "params": GLOBAL_PARAMS
    },
    "YFinance Commodities": {
        "source": "yfinance",
        "assets": [
            {"api_symbol": "SI=F", "display_name": "XAG/USD"},
            {"api_symbol": "CL=F",      "display_name": "USOIL"},
            {"api_symbol": "BZ=F",      "display_name": "UKOIL"}
        ],
        "interval": "30m",
        "params": GLOBAL_PARAMS
    },
    "Crypto": {
        "source": "binance",
        "assets": [
            {"api_symbol": "BTCUSDT", "display_name": "BTC/USD"}
        ],
        "interval": "30m",
        "params": GLOBAL_PARAMS
    }
}

TELEGRAM_BOT_TOKEN = os.environ.get("TELEGRAM_BOT_TOKEN", "")
TELEGRAM_CHAT_ID = os.environ.get("TELEGRAM_CHAT_ID", "")

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
            "params": group_cfg["params"]
        }

state_lock = threading.Lock()
asset_states = {}

for disp_name in DISPLAY_TO_ASSET.keys():
    asset_states[disp_name] = {
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
        "candle_history": pd.DataFrame()
    }

http_session = requests.Session()


def save_bot_state():
    try:
        data_to_save = {}
        with state_lock:
            for sym, st in asset_states.items():
                data_to_save[sym] = {
                    "pos_state": st["pos_state"],
                    "entry_price": st["entry_price"],
                    "active_sl": st["active_sl"],
                    "active_tp1": st["active_tp1"],
                    "active_tp2": st["active_tp2"],
                    "active_tp3": st["active_tp3"],
                    "hit_tp1": st.get("hit_tp1", False),
                    "hit_tp2": st.get("hit_tp2", False),
                }
        with open(STATE_FILE, "w") as f:
            json.dump(data_to_save, f, indent=2)
    except Exception as e:
        print(f"[-] Gagal menyimpan state: {e}")


def load_bot_state():
    if os.path.exists(STATE_FILE):
        try:
            with open(STATE_FILE, "r") as f:
                saved_data = json.load(f)
            with state_lock:
                for sym, st in saved_data.items():
                    if sym in asset_states:
                        asset_states[sym].update(st)
            print("[+] Berhasil memuat state posisi.")
        except Exception as e:
            print(f"[-] Gagal memuat state: {e}")


def log_trade_result(symbol, result_type, entry_p, exit_p):
    try:
        history = []
        if os.path.exists(STATS_FILE):
            with open(STATS_FILE, "r") as f:
                history = json.load(f)
        now_iso = datetime.now(timezone.utc).isoformat()
        history.append({
            "timestamp": now_iso,
            "symbol": symbol,
            "result": result_type,
            "entry": entry_p,
            "exit": exit_p
        })
        with open(STATS_FILE, "w") as f:
            json.dump(history, f, indent=2)
    except Exception as e:
        print(f"[-] Gagal mencatat riwayat: {e}")


def generate_recap(days, title):
    if not os.path.exists(STATS_FILE):
        return f"📊 <b>{title} ZF-CORE M91 PRO</b>\n\nBelum ada data transaksi."
    try:
        with open(STATS_FILE, "r") as f:
            history = json.load(f)
        now = datetime.now(timezone.utc)
        cutoff = now - timedelta(days=days)
        filtered = [h for h in history if datetime.fromisoformat(h["timestamp"]) >= cutoff]

        if not filtered:
            return f"📊 <b>{title} ZF-CORE M91 PRO</b>\n\nTidak ada transaksi selesai dalam periode ini."

        total = len(filtered)
        tp1_count = sum(1 for x in filtered if x["result"] == "TP1")
        tp2_count = sum(1 for x in filtered if x["result"] == "TP2")
        tp3_count = sum(1 for x in filtered if x["result"] == "TP3")
        sl_count = sum(1 for x in filtered if x["result"] == "SL")
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
        return f"[-] Gagal membuat rekapan: {e}"


def fmt_p(symbol, val):
    if val is None or np.isnan(val):
        return "-"
    sym_upper = symbol.upper()
    if "JPY" in sym_upper or "XAG" in sym_upper:
        return f"{val:,.3f}"
    elif any(pair in sym_upper for pair in ["EUR", "GBP", "AUD", "NZD", "CAD", "CHF"]) and "USD" in sym_upper:
        return f"{val:,.5f}"
    else:
        return f"{val:,.2f}"


app = Flask(__name__)


@app.route('/')
def home():
    return "ZF-Core Scalper M91 Pro: Active", 200


def send_telegram_message(message):
    if not TELEGRAM_BOT_TOKEN or not TELEGRAM_CHAT_ID:
        return
    url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage"
    payload = {"chat_id": TELEGRAM_CHAT_ID, "text": message, "parse_mode": "HTML"}
    try:
        http_session.post(url, json=payload, timeout=10)
    except Exception as e:
        print(f"[-] Telegram Exception: {e}")


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
    df['atr'] = tr.rolling(window=14).mean()

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


def fetch_candles_for_symbol(api_symbol, source, api_key="", interval="30min"):
    try:
        if source == "binance":
            url = f"https://api.binance.com/api/v3/klines?symbol={api_symbol}&interval={interval}&limit=200"
            res = http_session.get(url, timeout=10).json()
            if isinstance(res, list) and len(res) > 0:
                data = []
                for k in res:
                    data.append({
                        "datetime": pd.to_datetime(k[0], unit='ms'),
                        "open": float(k[1]),
                        "high": float(k[2]),
                        "low": float(k[3]),
                        "close": float(k[4]),
                        "volume": float(k[5])
                    })
                return pd.DataFrame(data)

        elif source == "yfinance":
            yf_interval = "30m" if "30" in interval else "1h"
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
            elif "message" in res:
                print(f"[-] TwelveData Error [{api_symbol}]: {res['message']}")
    except Exception as e:
        print(f"[-] Fetch error [{api_symbol}]: {e}")
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
            last_row = df_calc.iloc[-1]
            with state_lock:
                asset_states[disp_name]["candle_history"] = df_calc
                asset_states[disp_name]["live_price"] = float(last_row["close"])
                asset_states[disp_name]["d_res"] = float(last_row["d_res"])
                asset_states[disp_name]["zf_score"] = float(last_row["zf_score"])
                asset_states[disp_name]["raw_drift"] = float(last_row["raw_drift"])
        time.sleep(0.5)


def start_websocket_binance():
    ws_url = "wss://stream.binance.com:9443/ws/btcusdt@trade"

    def on_message(ws, message):
        try:
            data = json.loads(message)
            price = float(data.get("p", 0))
            if price > 0 and "BTC/USD" in asset_states:
                with state_lock:
                    asset_states["BTC/USD"]["live_price"] = price
        except Exception:
            pass

    def on_error(ws, error):
        pass

    def on_close(ws, status, msg):
        pass

    while True:
        try:
            ws = websocket.WebSocketApp(ws_url, on_message=on_message, on_error=on_error, on_close=on_close)
            ws.run_forever(ping_interval=30, ping_timeout=10)
        except Exception:
            pass
        time.sleep(5)


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
                if disp_name and disp_name in asset_states and price > 0:
                    with state_lock:
                        asset_states[disp_name]["live_price"] = price
        except Exception:
            pass

    def on_error(ws, error):
        pass

    def on_close(ws, status, msg):
        pass

    while True:
        try:
            ws = websocket.WebSocketApp(ws_url, on_open=on_open, on_message=on_message, on_error=on_error, on_close=on_close)
            ws.run_forever(ping_interval=30, ping_timeout=10)
        except Exception:
            pass
        time.sleep(5)


def run_m91_scalper_scheduler():
    time.sleep(5)
    last_daily_key = ""
    last_weekly_key = ""
    last_monthly_key = ""

    while True:
        try:
            now = datetime.now()
            seconds_to_next_5m = 300 - ((now.minute % 5) * 60 + now.second)
            time.sleep(seconds_to_next_5m)

            wib_time = datetime.now(timezone.utc) + timedelta(hours=7)
            time_str = wib_time.strftime("%Y-%m-%d %H:%M:00 WIB")

            curr_daily_key = wib_time.strftime("%Y-%m-%d")
            curr_week_key = wib_time.strftime("%Y-W%U")
            curr_month_key = wib_time.strftime("%Y-%m")

            if wib_time.hour == 0 and wib_time.minute < 5:
                if last_daily_key != curr_daily_key:
                    send_telegram_message(generate_recap(1, "REKAPAN HARIAN"))
                    last_daily_key = curr_daily_key

            if wib_time.weekday() == 0 and wib_time.hour == 0 and wib_time.minute < 5:
                if last_weekly_key != curr_week_key:
                    send_telegram_message(generate_recap(7, "REKAPAN MINGGUAN"))
                    last_weekly_key = curr_week_key

            if wib_time.day == 1 and wib_time.hour == 0 and wib_time.minute < 5:
                if last_monthly_key != curr_month_key:
                    send_telegram_message(generate_recap(30, "REKAPAN BULANAN"))
                    last_monthly_key = curr_month_key

            update_all_historical_data()
            flat_status_logs = []

            for disp_name, info in DISPLAY_TO_ASSET.items():
                params = info["params"]

                with state_lock:
                    st = asset_states[disp_name]
                    df = st["candle_history"]
                    curr_price = st["live_price"]

                if df.empty or len(df) < 5:
                    flat_status_logs.append(f"• <b>{disp_name}</b>: Data belum siap")
                    continue

                last_row = df.iloc[-1]
                raw_buy = bool(last_row["raw_buy"])
                raw_sell = bool(last_row["raw_sell"])
                std_p = float(last_row["std_p"]) if not np.isnan(last_row["std_p"]) else curr_price * 0.001

                with state_lock:
                    pos_state = st["pos_state"]
                    sl = st["active_sl"]
                    tp1 = st["active_tp1"]
                    tp2 = st["active_tp2"]
                    tp3 = st["active_tp3"]
                    entry = st["entry_price"]
                    state_changed = False

                    if pos_state == 1:
                        if params["use_trailing"]:
                            trail_sl = curr_price - (std_p * params["sigma_sl_mult"])
                            if sl is not None and trail_sl > sl:
                                st["active_sl"] = trail_sl
                                sl = trail_sl
                                state_changed = True

                        if sl is not None and curr_price <= sl:
                            st["pos_state"] = 0
                            st["entry_price"] = None
                            state_changed = True
                            send_telegram_message(f"🛑 <b>{disp_name} HIT STOP LOSS (EXIT)</b> @ {fmt_p(disp_name, curr_price)}")
                            log_trade_result(disp_name, "SL", entry, curr_price)
                        elif tp3 is not None and curr_price >= tp3:
                            st["pos_state"] = 0
                            st["entry_price"] = None
                            state_changed = True
                            send_telegram_message(f"🎯 <b>{disp_name} HIT TAKE PROFIT 3 (EXIT)</b> @ {fmt_p(disp_name, curr_price)}")
                            log_trade_result(disp_name, "TP3", entry, curr_price)
                        elif tp2 is not None and curr_price >= tp2 and st.get("hit_tp2") is not True:
                            st["hit_tp2"] = True
                            send_telegram_message(f"🎯 <b>{disp_name} HIT TAKE PROFIT 2</b> @ {fmt_p(disp_name, curr_price)}")
                            log_trade_result(disp_name, "TP2", entry, curr_price)
                        elif tp1 is not None and curr_price >= tp1 and st.get("hit_tp1") is not True:
                            st["hit_tp1"] = True
                            send_telegram_message(f"🎯 <b>{disp_name} HIT TAKE PROFIT 1</b> @ {fmt_p(disp_name, curr_price)}")
                            log_trade_result(disp_name, "TP1", entry, curr_price)

                    elif pos_state == -1:
                        if params["use_trailing"]:
                            trail_sl = curr_price + (std_p * params["sigma_sl_mult"])
                            if sl is not None and trail_sl < sl:
                                st["active_sl"] = trail_sl
                                sl = trail_sl
                                state_changed = True

                        if sl is not None and curr_price >= sl:
                            st["pos_state"] = 0
                            st["entry_price"] = None
                            state_changed = True
                            send_telegram_message(f"🛑 <b>{disp_name} HIT STOP LOSS (EXIT)</b> @ {fmt_p(disp_name, curr_price)}")
                            log_trade_result(disp_name, "SL", entry, curr_price)
                        elif tp3 is not None and curr_price <= tp3:
                            st["pos_state"] = 0
                            st["entry_price"] = None
                            state_changed = True
                            send_telegram_message(f"🎯 <b>{disp_name} HIT TAKE PROFIT 3 (EXIT)</b> @ {fmt_p(disp_name, curr_price)}")
                            log_trade_result(disp_name, "TP3", entry, curr_price)
                        elif tp2 is not None and curr_price <= tp2 and st.get("hit_tp2") is not True:
                            st["hit_tp2"] = True
                            send_telegram_message(f"🎯 <b>{disp_name} HIT TAKE PROFIT 2</b> @ {fmt_p(disp_name, curr_price)}")
                            log_trade_result(disp_name, "TP2", entry, curr_price)
                        elif tp1 is not None and curr_price <= tp1 and st.get("hit_tp1") is not True:
                            st["hit_tp1"] = True
                            send_telegram_message(f"🎯 <b>{disp_name} HIT TAKE PROFIT 1</b> @ {fmt_p(disp_name, curr_price)}")
                            log_trade_result(disp_name, "TP1", entry, curr_price)

                    buy_signal = (st["pos_state"] == 0) and raw_buy
                    sell_signal = (st["pos_state"] == 0) and raw_sell

                    if buy_signal:
                        st["pos_state"] = 1
                        st["entry_price"] = curr_price
                        st["hit_tp1"] = False
                        st["hit_tp2"] = False
                        risk = std_p * params["sigma_sl_mult"]
                        st["active_sl"] = curr_price - risk
                        st["active_tp1"] = curr_price + (risk * params["rr1_ratio"])
                        st["active_tp2"] = curr_price + (risk * params["rr2_ratio"])
                        st["active_tp3"] = curr_price + (risk * params["rr3_ratio"])
                        state_changed = True

                        msg_buy = (
                            f"🚨 <b>ZF-CORE M91 PRO BUY SIGNAL</b> 🚨\n\n"
                            f"📊 <b>Pair:</b> {disp_name}\n"
                            f"💵 <b>Entry:</b> {fmt_p(disp_name, curr_price)}\n"
                            f"🛑 <b>Trailing SL:</b> {fmt_p(disp_name, st['active_sl'])}\n"
                            f"🎯 <b>TP 1:</b> {fmt_p(disp_name, st['active_tp1'])}\n"
                            f"🎯 <b>TP 2:</b> {fmt_p(disp_name, st['active_tp2'])}\n"
                            f"🎯 <b>TP 3:</b> {fmt_p(disp_name, st['active_tp3'])}\n"
                            f"⚡ <b>ZF-Score:</b> {st['zf_score']:.2f} | <b>Drift:</b> {st['d_res']:.2f}%"
                        )
                        send_telegram_message(msg_buy)

                    elif sell_signal:
                        st["pos_state"] = -1
                        st["entry_price"] = curr_price
                        st["hit_tp1"] = False
                        st["hit_tp2"] = False
                        risk = std_p * params["sigma_sl_mult"]
                        st["active_sl"] = curr_price + risk
                        st["active_tp1"] = curr_price - (risk * params["rr1_ratio"])
                        st["active_tp2"] = curr_price - (risk * params["rr2_ratio"])
                        st["active_tp3"] = curr_price - (risk * params["rr3_ratio"])
                        state_changed = True

                        msg_sell = (
                            f"🚨 <b>ZF-CORE M91 PRO SELL SIGNAL</b> 🚨\n\n"
                            f"📊 <b>Pair:</b> {disp_name}\n"
                            f"💵 <b>Entry:</b> {fmt_p(disp_name, curr_price)}\n"
                            f"🛑 <b>Trailing SL:</b> {fmt_p(disp_name, st['active_sl'])}\n"
                            f"🎯 <b>TP 1:</b> {fmt_p(disp_name, st['active_tp1'])}\n"
                            f"🎯 <b>TP 2:</b> {fmt_p(disp_name, st['active_tp2'])}\n"
                            f"🎯 <b>TP 3:</b> {fmt_p(disp_name, st['active_tp3'])}\n"
                            f"⚡ <b>ZF-Score:</b> {st['zf_score']:.2f} | <b>Drift:</b> {st['d_res']:.2f}%"
                        )
                        send_telegram_message(msg_sell)

                    if state_changed:
                        save_bot_state()

                    state_txt = "BUY" if st["pos_state"] == 1 else "SELL" if st["pos_state"] == -1 else "NEUTRAL"
                    flat_status_logs.append(
                        f"• <b>{disp_name}</b>: {fmt_p(disp_name, curr_price)} | D_res: {st['d_res']:.2f}% | ZF: {st['zf_score']:.2f} | [{state_txt}]"
                    )

            log_msg = (
                f"⚡ <b>ZF-Core Scalper M91 Pro Status (Sync 5m)</b>\n"
                f"Waktu : {time_str}\n\n" +
                "\n".join(flat_status_logs)
            )
            send_telegram_message(log_msg)

        except Exception as e:
            print(f"[-] Error Scheduler: {e}")


load_bot_state()
update_all_historical_data()

for group_name, group_cfg in ASSET_CONFIG.items():
    source = group_cfg["source"]
    if source == "binance":
        ws_thread = threading.Thread(
            target=start_websocket_binance,
            daemon=True
        )
        ws_thread.start()
    elif source == "twelvedata":
        ws_thread = threading.Thread(
            target=start_websocket_twelvedata,
            args=(group_name, group_cfg["api_key"], group_cfg["assets"]),
            daemon=True
        )
        ws_thread.start()

scheduler_thread = threading.Thread(target=run_m91_scalper_scheduler, daemon=True)
scheduler_thread.start()

if __name__ == "__main__":
    port = int(os.environ.get("PORT", 5000))
    app.run(host="0.0.0.0", port=port)
