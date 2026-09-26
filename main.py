import os
import time
import json
import threading
import requests
import numpy as np
import pandas as pd
import websocket
from flask import Flask
from datetime import datetime, timezone, timedelta

STATE_FILE = "bot_state.json"
STATS_FILE = "trade_history.json"

GLOBAL_PARAMS = {
    "length_period": 21,
    "batas_zf": 0.51,
    "min_drift": 0.17,
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
        "api_key": os.environ.get("TWELVEDATA_API_KEY_FOREX", "YOUR_API_KEY_FOREX"),
        "symbols": ["EUR/USD", "GBP/USD", "USD/JPY", "USD/CHF", "USD/CAD", "AUD/USD", "NZD/USD"],
        "interval": "1h",
        "params": GLOBAL_PARAMS
    },
    "Gold": {
        "api_key": os.environ.get("TWELVEDATA_API_KEY_XAU", "YOUR_API_KEY_XAU"),
        "symbols": ["XAU/USD"],
        "interval": "5min",
        "params": GLOBAL_PARAMS
    },
    "Silver": {
        "api_key": os.environ.get("TWELVEDATA_API_KEY_XAG", "YOUR_API_KEY_XAG"),
        "symbols": ["XAG/USD"],
        "interval": "5min",
        "params": GLOBAL_PARAMS
    },
    "US Oil": {
        "api_key": os.environ.get("TWELVEDATA_API_KEY_WTI", "YOUR_API_KEY_WTI"),
        "symbols": ["WTI/USD"],
        "interval": "5min",
        "params": GLOBAL_PARAMS
    },
    "UK Oil": {
        "api_key": os.environ.get("TWELVEDATA_API_KEY_XBR", "YOUR_API_KEY_XBR"),
        "symbols": ["XBR/USD"],
        "interval": "5min",
        "params": GLOBAL_PARAMS
    },
    "Crypto": {
        "api_key": os.environ.get("TWELVEDATA_API_KEY_CRYPTO", "YOUR_API_KEY_CRYPTO"),
        "symbols": ["BTC/USD"],
        "interval": "5min",
        "params": GLOBAL_PARAMS
    }
}

TELEGRAM_BOT_TOKEN = os.environ.get("TELEGRAM_BOT_TOKEN", "YOUR_TELEGRAM_BOT_TOKEN")
TELEGRAM_CHAT_ID = os.environ.get("TELEGRAM_CHAT_ID", "YOUR_TELEGRAM_CHAT_ID")

SYMBOLS = [sym for group in ASSET_CONFIG.values() for sym in group["symbols"]]

state_lock = threading.Lock()
asset_states = {}

for sym in SYMBOLS:
    asset_states[sym] = {
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
        "candle_history": pd.DataFrame()
    }


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
            print("[+] Berhasil memuat state posisi dari penyimpanan lokal.")
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
        print(f"[-] Gagal mencatat riwayat transaksi: {e}")


def generate_recap(days, title):
    if not os.path.exists(STATS_FILE):
        return f"📊 <b>{title} ZF-CORE M91 PRO</b>\n\nBelum ada data transaksi yang tercatat."
    try:
        with open(STATS_FILE, "r") as f:
            history = json.load(f)
        now = datetime.now(timezone.utc)
        cutoff = now - timedelta(days=days)
        filtered = []
        for h in history:
            t = datetime.fromisoformat(h["timestamp"])
            if t >= cutoff:
                filtered.append(h)

        if not filtered:
            return f"📊 <b>{title} ZF-CORE M91 PRO</b>\n\nTidak ada transaksi selesai dalam periode ini."

        total = len(filtered)
        tp1_count = sum(1 for x in filtered if x["result"] == "TP1")
        tp2_count = sum(1 for x in filtered if x["result"] == "TP2")
        tp3_count = sum(1 for x in filtered if x["result"] == "TP3")
        sl_count = sum(1 for x in filtered if x["result"] == "SL")
        win_count = tp1_count + tp2_count + tp3_count
        win_rate = (win_count / total * 100) if total > 0 else 0.0

        msg = (
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
        return msg
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
        requests.post(url, json=payload, timeout=10)
    except Exception as e:
        print(f"[-] Exception Telegram: {e}")


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

    df['raw_buy'] = (df['raw_drift'] < 0) & (df['d_res'] >= p['min_drift']) & (df['zf_score'] >= p['batas_zf']) & df['is_inflection'] & df['is_fractal'] & df['is_fvg'] & df['trend_buy']
    df['raw_sell'] = (df['raw_drift'] > 0) & (df['d_res'] >= p['min_drift']) & (df['zf_score'] >= p['batas_zf']) & df['is_inflection'] & df['is_fractal'] & df['is_fvg'] & df['trend_sell']

    return df


def fetch_candles_for_symbol(symbol, api_key, interval="5min"):
    try:
        url = f"https://api.twelvedata.com/time_series?symbol={symbol}&interval={interval}&outputsize=200&apikey={api_key}"
        res = requests.get(url, timeout=10).json()
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
            print(f"[-] TwelveData Error [{symbol}]: {res['message']}")
    except Exception as e:
        print(f"[-] Gagal fetch candle {symbol}: {e}")
    return pd.DataFrame()


def update_all_historical_data():
    for group_name, group_cfg in ASSET_CONFIG.items():
        key = group_cfg["api_key"]
        interval = group_cfg["interval"]
        params = group_cfg["params"]
        for sym in group_cfg["symbols"]:
            df = fetch_candles_for_symbol(sym, key, interval)
            if not df.empty:
                df_calc = calculate_zf_core(df.tail(200), params)
                last_row = df_calc.iloc[-1]
                with state_lock:
                    asset_states[sym]["candle_history"] = df_calc
                    if asset_states[sym]["live_price"] == 0.0:
                        asset_states[sym]["live_price"] = float(last_row["close"])
                    asset_states[sym]["d_res"] = float(last_row["d_res"])
                    asset_states[sym]["zf_score"] = float(last_row["zf_score"])
                    asset_states[sym]["raw_drift"] = float(last_row["raw_drift"])
            time.sleep(1.2)


def start_websocket_for_group(group_name, api_key, symbols):
    ws_url = f"wss://ws.twelvedata.com/v1/quotes/price?apikey={api_key}"

    def on_open(ws):
        print(f"[+] WS [{group_name}] Connected. Subscribing {symbols}")
        ws.send(json.dumps({"action": "subscribe", "params": {"symbols": ",".join(symbols)}}))

    def on_message(ws, message):
        try:
            data = json.loads(message)
            if data.get("event") == "price":
                sym = data.get("symbol")
                price = float(data.get("price", 0))
                if sym in asset_states and price > 0:
                    with state_lock:
                        asset_states[sym]["live_price"] = price
        except Exception:
            pass

    def on_error(ws, error):
        print(f"[-] WS [{group_name}] Error: {error}")

    def on_close(ws, status, msg):
        print(f"[-] WS [{group_name}] Reconnecting...")

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
            seconds_to_next_5m = 300 - ((now.minute % 5) * 60 + now.second) + 8
            time.sleep(seconds_to_next_5m)

            wib_time = datetime.now(timezone.utc) + timedelta(hours=7)
            time_str = wib_time.strftime("%Y-%m-%d %H:%M:00 WIB")

            curr_daily_key = wib_time.strftime("%Y-%m-%d")
            curr_week_key = wib_time.strftime("%Y-W%U")
            curr_month_key = wib_time.strftime("%Y-%m")

            if wib_time.hour == 0 and wib_time.minute < 10:
                if last_daily_key != curr_daily_key:
                    recap_msg = generate_recap(1, "REKAPAN HARIAN")
                    send_telegram_message(recap_msg)
                    last_daily_key = curr_daily_key

            if wib_time.weekday() == 0 and wib_time.hour == 0 and wib_time.minute < 10:
                if last_weekly_key != curr_week_key:
                    recap_msg = generate_recap(7, "REKAPAN MINGGUAN")
                    send_telegram_message(recap_msg)
                    last_weekly_key = curr_week_key

            if wib_time.day == 1 and wib_time.hour == 0 and wib_time.minute < 10:
                if last_monthly_key != curr_month_key:
                    recap_msg = generate_recap(30, "REKAPAN BULANAN")
                    send_telegram_message(recap_msg)
                    last_monthly_key = curr_month_key

            update_all_historical_data()
            category_logs = []

            for group_name, group_cfg in ASSET_CONFIG.items():
                group_lines = [f"\n<b>[{group_name.upper()} - {group_cfg['interval']}]</b>"]
                symbols_list = group_cfg["symbols"]
                params = group_cfg["params"]

                for sym in symbols_list:
                    with state_lock:
                        st = asset_states[sym]
                        df = st["candle_history"]
                        curr_price = st["live_price"]

                    if df.empty or len(df) < 5:
                        group_lines.append(f"• <b>{sym}</b>: Data belum siap")
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
                                send_telegram_message(f"🛑 <b>{sym} HIT STOP LOSS (EXIT)</b> @ {fmt_p(sym, curr_price)}")
                                log_trade_result(sym, "SL", entry, curr_price)
                            elif tp3 is not None and curr_price >= tp3:
                                st["pos_state"] = 0
                                st["entry_price"] = None
                                state_changed = True
                                send_telegram_message(f"🎯 <b>{sym} HIT TAKE PROFIT 3 (EXIT)</b> @ {fmt_p(sym, curr_price)}")
                                log_trade_result(sym, "TP3", entry, curr_price)
                            elif tp2 is not None and curr_price >= tp2 and st.get("hit_tp2") is not True:
                                st["hit_tp2"] = True
                                send_telegram_message(f"🎯 <b>{sym} HIT TAKE PROFIT 2</b> @ {fmt_p(sym, curr_price)}")
                                log_trade_result(sym, "TP2", entry, curr_price)
                            elif tp1 is not None and curr_price >= tp1 and st.get("hit_tp1") is not True:
                                st["hit_tp1"] = True
                                send_telegram_message(f"🎯 <b>{sym} HIT TAKE PROFIT 1</b> @ {fmt_p(sym, curr_price)}")
                                log_trade_result(sym, "TP1", entry, curr_price)

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
                                send_telegram_message(f"🛑 <b>{sym} HIT STOP LOSS (EXIT)</b> @ {fmt_p(sym, curr_price)}")
                                log_trade_result(sym, "SL", entry, curr_price)
                            elif tp3 is not None and curr_price <= tp3:
                                st["pos_state"] = 0
                                st["entry_price"] = None
                                state_changed = True
                                send_telegram_message(f"🎯 <b>{sym} HIT TAKE PROFIT 3 (EXIT)</b> @ {fmt_p(sym, curr_price)}")
                                log_trade_result(sym, "TP3", entry, curr_price)
                            elif tp2 is not None and curr_price <= tp2 and st.get("hit_tp2") is not True:
                                st["hit_tp2"] = True
                                send_telegram_message(f"🎯 <b>{sym} HIT TAKE PROFIT 2</b> @ {fmt_p(sym, curr_price)}")
                                log_trade_result(sym, "TP2", entry, curr_price)
                            elif tp1 is not None and curr_price <= tp1 and st.get("hit_tp1") is not True:
                                st["hit_tp1"] = True
                                send_telegram_message(f"🎯 <b>{sym} HIT TAKE PROFIT 1</b> @ {fmt_p(sym, curr_price)}")
                                log_trade_result(sym, "TP1", entry, curr_price)

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
                                f"📂 <b>Kategori:</b> {group_name} ({group_cfg['interval']})\n"
                                f"📊 <b>Pair:</b> {sym}\n"
                                f"💵 <b>Entry:</b> {fmt_p(sym, curr_price)}\n"
                                f"🛑 <b>Trailing SL:</b> {fmt_p(sym, st['active_sl'])}\n"
                                f"🎯 <b>TP 1:</b> {fmt_p(sym, st['active_tp1'])}\n"
                                f"🎯 <b>TP 2:</b> {fmt_p(sym, st['active_tp2'])}\n"
                                f"🎯 <b>TP 3:</b> {fmt_p(sym, st['active_tp3'])}\n"
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
                                f"📂 <b>Kategori:</b> {group_name} ({group_cfg['interval']})\n"
                                f"📊 <b>Pair:</b> {sym}\n"
                                f"💵 <b>Entry:</b> {fmt_p(sym, curr_price)}\n"
                                f"🛑 <b>Trailing SL:</b> {fmt_p(sym, st['active_sl'])}\n"
                                f"🎯 <b>TP 1:</b> {fmt_p(sym, st['active_tp1'])}\n"
                                f"🎯 <b>TP 2:</b> {fmt_p(sym, st['active_tp2'])}\n"
                                f"🎯 <b>TP 3:</b> {fmt_p(sym, st['active_tp3'])}\n"
                                f"⚡ <b>ZF-Score:</b> {st['zf_score']:.2f} | <b>Drift:</b> {st['d_res']:.2f}%"
                            )
                            send_telegram_message(msg_sell)

                        if state_changed:
                            save_bot_state()

                        state_txt = "BUY" if st["pos_state"] == 1 else "SELL" if st["pos_state"] == -1 else "NEUTRAL"
                        group_lines.append(
                            f"• <b>{sym}</b>: {fmt_p(sym, curr_price)} | D_res: {st['d_res']:.2f}% | ZF: {st['zf_score']:.2f} | [{state_txt}]"
                        )

                category_logs.append("\n".join(group_lines))

            log_msg = (
                f"⚡ <b>ZF-Core Scalper M91 Pro Status</b>\n"
                f"Waktu : {time_str}\n" +
                "\n".join(category_logs)
            )
            send_telegram_message(log_msg)

        except Exception as e:
            print(f"[-] Error Scheduler: {e}")


load_bot_state()
update_all_historical_data()

for group_name, group_cfg in ASSET_CONFIG.items():
    ws_thread = threading.Thread(
        target=start_websocket_for_group,
        args=(group_name, group_cfg["api_key"], group_cfg["symbols"]),
        daemon=True
    )
    ws_thread.start()

scheduler_thread = threading.Thread(target=run_m91_scalper_scheduler, daemon=True)
scheduler_thread.start()

if __name__ == "__main__":
    port = int(os.environ.get("PORT", 5000))
    app.run(host="0.0.0.0", port=port)
