import json
import math
import os
import threading
import time
import warnings
from datetime import datetime, timedelta
from concurrent.futures import ThreadPoolExecutor

import requests
import websocket
from dotenv import load_dotenv
from flask import Flask  # <-- Ditambahkan untuk Render

# Matikan warning agar konsol tetap bersih
warnings.filterwarnings("ignore")

load_dotenv()
os.system("cls" if os.name == "nt" else "clear")

# ==========================================
# INISIALISASI SERVER WEB FOR RENDER
# ==========================================
app = Flask(__name__)

@app.route('/')
def home():
    return f"Bot {BOT_NAME} Berhasil Aktif & Running 24/7!", 200

# ==========================================
# KONFIGURASI BOT & PARAMETER ASET
# ==========================================
BOT_NAME = "ZF-Core Scalper M91 (BTC & XAU)"
TELEGRAM_TOKEN = os.getenv("TELEGRAM_TOKEN")
CHAT_ID_LOG = os.getenv("CHAT_ID_LOG")
CHAT_ID_SIGNAL = os.getenv("CHAT_ID_SIGNAL")
TWELVEDATA_API_KEY = os.getenv("TWELVEDATA_API_KEY")

TIMEFRAME = "5m"
TRADES_FILE = "active_trades_m5.json"
STATS_FILE = "daily_stats_m5.json"

TOTAL_CANDLE = 200
LENGTH_PERIOD = 21
BATAS_KRITIS_ZF = 0.51
MIN_TOPOLOGICAL_DRIFT = 0.17
MIN_FVG_MULT = 0.5  # Multiplier FVG berbasis ATR
USE_EMA_FILTER = True
SIGMA_SL_MULT = 4.5  # Base Risk Multiplier

TARGET_ASSETS = ["BTC/USD", "XAU/USD"]

ASSET_CONFIG = {
    "BTC/USD": {"spread": 5.0, "min_d_res": MIN_TOPOLOGICAL_DRIFT},
    "XAU/USD": {"spread": 0.30, "min_d_res": MIN_TOPOLOGICAL_DRIFT}
}

TV_SYMBOL_MAP = {
    "BTC/USD": "BINANCE:BTCUSDT",
    "XAU/USD": "OANDA:XAUUSD"
}

# Storage Memori Global
data_lock = threading.RLock()
kline_buffer = {asset: [] for asset in TARGET_ASSETS}
live_prices = {asset: 0.0 for asset in TARGET_ASSETS}
active_trades = {asset: None for asset in TARGET_ASSETS}
LAST_ALERT_WAKTU_ZF = {asset: None for asset in TARGET_ASSETS}

telegram_executor = ThreadPoolExecutor(max_workers=3)
daily_stats = {"DATE": datetime.now().strftime("%Y-%m-%d"), "TOTAL": 0, "TP1": 0, "TP2": 0, "TP3": 0, "SL": 0, "BE": 0}


def get_tv_link(asset):
    tv_symbol = TV_SYMBOL_MAP.get(asset, asset.replace('/', ''))
    return f"https://www.tradingview.com/chart/?symbol={tv_symbol}"

def get_decimal_places(asset):
    return 2 if asset in ["BTC/USD", "XAU/USD"] else 4


# ==========================================
# MANAJEMEN DATABASE LOKAL (JSON)
# ==========================================
def load_active_trades():
    if os.path.exists(TRADES_FILE):
        try:
            with open(TRADES_FILE, "r") as f:
                with data_lock:
                    active_trades.update(json.load(f))
            print("[+] DB Trades: Transaksi aktif dimuat.")
        except Exception as e:
            print(f"[-] Gagal membaca {TRADES_FILE}: {e}")

def save_active_trades():
    with data_lock:
        try:
            temp_file = TRADES_FILE + ".tmp"
            with open(temp_file, "w") as f:
                json.dump(active_trades, f, indent=4)
            os.replace(temp_file, TRADES_FILE)
        except Exception:
            pass

def load_daily_stats():
    global daily_stats
    current_date = datetime.now().strftime("%Y-%m-%d")
    default_stats = {"DATE": current_date, "TOTAL": 0, "TP1": 0, "TP2": 0, "TP3": 0, "SL": 0, "BE": 0}
    if os.path.exists(STATS_FILE):
        try:
            with open(STATS_FILE, "r") as f:
                with data_lock:
                    daily_stats.update(json.load(f))
        except Exception:
            with data_lock:
                daily_stats.update(default_stats)
    else:
        with data_lock:
            daily_stats.update(default_stats)
        save_daily_stats()

def save_daily_stats():
    with data_lock:
        try:
            temp_file = STATS_FILE + ".tmp"
            with open(temp_file, "w") as f:
                json.dump(daily_stats, f, indent=4)
            os.replace(temp_file, STATS_FILE)
        except Exception:
            pass


# ==========================================
# MODUL TELEGRAM
# ==========================================
def _post_to_telegram(url, payload):
    try:
        requests.post(url, json=payload, timeout=5)
    except Exception:
        pass

def send_telegram_msg(chat_id, text):
    if TELEGRAM_TOKEN and chat_id:
        url = f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage"
        payload = {"chat_id": chat_id, "text": text, "parse_mode": "HTML", "disable_web_page_preview": True}
        telegram_executor.submit(_post_to_telegram, url, payload)


# ==========================================
# INTEGRASI DATA REAL-TIME & THREAD-SAFETY
# ==========================================
def get_safe_kline_snapshot(asset):
    with data_lock:
        return [row[:] for row in kline_buffer.get(asset, [])]

def update_live_data(asset, open_p, high_p, low_p, close_p, volume=None, is_closed=False):
    with data_lock:
        live_prices[asset] = close_p
        buf = kline_buffer[asset]

        if is_closed or not buf:
            init_vol = volume if (volume is not None and volume > 0) else 1.0
            buf.append([open_p, high_p, low_p, close_p, init_vol])
            if len(buf) > TOTAL_CANDLE:
                buf.pop(0)
        else:
            prev_high = max(buf[-1][1], high_p)
            prev_low = min(buf[-1][2], low_p)
            
            if volume is not None and volume > 0:
                current_vol = volume
            else:
                current_vol = buf[-1][4] + 1.0

            buf[-1] = [buf[-1][0], prev_high, prev_low, close_p, current_vol]

    monitor_active_trades_realtime(asset, close_p)

def seed_klines():
    if not TWELVEDATA_API_KEY:
        print("[-] Error: TWELVEDATA_API_KEY belum dikonfigurasi pada file .env / Environment Variables")
        return

    for asset in TARGET_ASSETS:
        try:
            url = f"https://api.twelvedata.com/time_series?symbol={asset}&interval=5min&outputsize={TOTAL_CANDLE}&apikey={TWELVEDATA_API_KEY}"
            res = requests.get(url, timeout=15)
            if res.status_code == 200 and "values" in res.json():
                raw = list(reversed(res.json()["values"]))
                with data_lock:
                    kline_buffer[asset] = [
                        [float(i["open"]), float(i["high"]), float(i["low"]), float(i["close"]), float(i.get("volume", 100))]
                        for i in raw
                    ]
                    if raw:
                        live_prices[asset] = float(raw[-1]["close"])
                print(f"[+] Seed data TwelveData {asset} berhasil.")
            else:
                print(f"[-] Gagal seed TwelveData {asset}: {res.json().get('message', 'API Error')}")
            time.sleep(1)
        except Exception as e:
            print(f"[-] Error seed TwelveData {asset}: {e}")

def start_twelvedata_websocket():
    if not TWELVEDATA_API_KEY:
        return

    symbols_str = ",".join(TARGET_ASSETS)
    ws_url = f"wss://ws.twelvedata.com/v1/quotes/price?apikey={TWELVEDATA_API_KEY}"
    last_candle_time = {asset: 0 for asset in TARGET_ASSETS}

    def on_message(ws, message):
        try:
            data = json.loads(message)
            if data.get("event") == "price" and data.get("symbol") in TARGET_ASSETS:
                asset = data.get("symbol")
                price = float(data.get("price"))
                tick_time = int(data.get("timestamp"))
                candle_time = (tick_time // 300) * 300

                is_closed = False
                if last_candle_time[asset] == 0:
                    last_candle_time[asset] = candle_time
                elif candle_time > last_candle_time[asset]:
                    is_closed = True
                    last_candle_time[asset] = candle_time

                update_live_data(asset, price, price, price, price, volume=None, is_closed=is_closed)
        except Exception:
            pass

    def run_ws():
        while True:
            try:
                ws = websocket.WebSocketApp(
                    ws_url,
                    on_message=on_message,
                    on_open=lambda ws: ws.send(json.dumps({"action": "subscribe", "params": {"symbols": symbols_str}}))
                )
                print(f"[+] WebSocket Twelve Data aktif. Subscribed: {symbols_str}")
                ws.run_forever(ping_interval=30)
            except Exception:
                pass
            time.sleep(5)

    threading.Thread(target=run_ws, daemon=True).start()


# ==========================================
# MATEMATIKA KUANTITATIF & TEKNIKAL
# ==========================================
def calculate_atr(klines, period=14):
    if len(klines) < 2:
        return 0.0
    tr_list = []
    for i in range(1, len(klines)):
        h = klines[i][1]
        l = klines[i][2]
        prev_c = klines[i - 1][3]
        tr = max(h - l, abs(h - prev_c), abs(l - prev_c))
        tr_list.append(tr)
    
    if not tr_list:
        return 0.0
    window = min(len(tr_list), period)
    return sum(tr_list[-window:]) / window

def calculate_ema(data, period=200):
    if not data:
        return 0.0
    actual_period = min(len(data), period)
    multiplier = 2 / (actual_period + 1)
    ema = sum(data[:actual_period]) / actual_period
    for p in data[actual_period:]:
        ema = (p - ema) * multiplier + ema
    return ema

def get_effective_volume(prices, volumes):
    if not volumes or len(volumes) < 2 or sum(volumes[-LENGTH_PERIOD:]) == 0 or len(set(volumes[-LENGTH_PERIOD:])) <= 1:
        if len(prices) >= 2:
            return [abs(prices[i] - prices[i - 1]) + 1e-5 for i in range(1, len(prices))]
        return [1.0] * len(prices)
    return volumes

def calculate_p_pure(prices, volumes, window=LENGTH_PERIOD):
    if len(prices) < window:
        return calculate_ema(prices, window)
    eff_vols = get_effective_volume(prices, volumes)
    p_sub = prices[-window:]
    v_sub = eff_vols[-window:]
    pv_sum = sum(p * v for p, v in zip(p_sub, v_sub))
    vol_sum = sum(v_sub)
    return (pv_sum / vol_sum) if vol_sum > 0 else calculate_ema(prices, window)

def calculate_std(data):
    if len(data) < 2:
        return 0.0
    mean = sum(data) / len(data)
    return math.sqrt(sum((x - mean) ** 2 for x in data) / (len(data) - 1))

def calculate_topological_drift(p_market, p_pure):
    if p_pure == 0:
        return 0.0, 0.0
    raw_drift = ((p_market - p_pure) / p_pure) * 100
    return abs(raw_drift), raw_drift

def calculate_zf_score(v_abs, v_total, d_res):
    if v_total == 0:
        return 0.0
    zf_ratio = v_abs / v_total
    zf_x = d_res * 10
    zf_e2x = math.exp(2 * zf_x) if zf_x < 100 else 1e10
    zf_tanh = (zf_e2x - 1) / (zf_e2x + 1)
    return zf_ratio * zf_tanh

def get_simplified_zf(prices, raw_volumes, d_res, window=LENGTH_PERIOD):
    volumes = get_effective_volume(prices, raw_volumes)
    if len(volumes) >= window:
        v_avg = sum(volumes[-window:]) / window
        v_abs = abs(volumes[-1] - v_avg)
        v_total = volumes[-1] if volumes[-1] > 0 else 1.0
    else:
        v_abs = abs(prices[-1] - prices[-2]) if len(prices) > 1 else 0.001
        v_total = 1.0
    return calculate_zf_score(v_abs, v_total, d_res)

def check_second_derivative_inflection(prices):
    if len(prices) < 4:
        return False, 0.0
    dp_dt1 = prices[-1] - prices[-2]
    dp_dt2 = prices[-2] - prices[-3]
    d2p_dt2_curr = dp_dt1 - dp_dt2
    d2p_dt2_prev = dp_dt2 - (prices[-3] - prices[-4])
    is_inflection = (d2p_dt2_curr * d2p_dt2_prev <= 0) or (abs(d2p_dt2_curr) < 1e-5)
    return is_inflection, d2p_dt2_curr

def check_fractal_structure(klines, kepekaan=8):
    if len(klines) < (kepekaan + 1):
        return True
    highs = [k[1] for k in klines[-(kepekaan + 1):-1]]
    lows = [k[2] for k in klines[-(kepekaan + 1):-1]]
    close = klines[-1][3]
    return (close >= max(highs)) or (close <= min(lows))

def check_imbalance_gap_dynamic(klines, asset, min_fvg_mult=MIN_FVG_MULT):
    if len(klines) < 3:
        return False
    atr_val = calculate_atr(klines, 14)
    min_fvg_dist = atr_val * min_fvg_mult
    bull_fvg = klines[-1][2] - klines[-3][1]
    bear_fvg = klines[-3][2] - klines[-1][1]
    price_jump = abs(klines[-1][3] - klines[-3][3])
    return max(bull_fvg, bear_fvg, price_jump) >= min_fvg_dist


# ==========================================
# EKSEKUSI TRADING & PEMINDAIAN REAL-TIME
# ==========================================
def monitor_active_trades_realtime(asset, p):
    with data_lock:
        trade = active_trades.get(asset)
    if not trade:
        return

    act = trade["action"]
    ent = trade["entry"]
    sl = trade["sl"]
    tp1, tp2, tp3 = trade["tp1"], trade["tp2"], trade["tp3"]
    tp1_hit = trade.get("tp1_hit", False)
    tp2_hit = trade.get("tp2_hit", False)

    is_buy = (act == "BUY")

    sl_hit = (p <= sl) if is_buy else (p >= sl)
    tp3_hit = (p >= tp3) if is_buy else (p <= tp3)
    tp2_triggered = ((p >= tp2) if is_buy else (p <= tp2)) and not tp2_hit
    tp1_triggered = ((p >= tp1) if is_buy else (p <= tp1)) and not tp1_hit

    msg, is_closed = "", False
    waktu, dec, clean_asset = datetime.now().strftime("%H:%M:%S"), get_decimal_places(asset), asset.replace('/', '')

    if sl_hit:
        if tp2_hit:
            record_trade_result("TP1")
            msg = f"🔒 <b>PROFIT LOCKED (HIT SL AT TP1)</b>\n\n#{clean_asset}\n<code>Aksi: {act}\nEntry: {ent:.{dec}f}\nExit (TP1): {p:.{dec}f}\nWaktu: {waktu} WIB</code>"
        elif tp1_hit:
            record_trade_result("BE")
            msg = f"🛡 <b>BREAK-EVEN HIT</b>\n\n#{clean_asset}\n<code>Aksi: {act}\nEntry: {ent:.{dec}f}\nExit (Entry): {p:.{dec}f}\nWaktu: {waktu} WIB</code>"
        else:
            record_trade_result("SL")
            msg = f"🔴 <b>STOP LOSS HIT</b>\n\n#{clean_asset}\n<code>Aksi: {act}\nEntry: {ent:.{dec}f}\nExit: {p:.{dec}f}\nWaktu: {waktu} WIB</code>"
        is_closed = True

    elif tp3_hit:
        record_trade_result("TP3")
        msg = f"🚀 <b>TAKE PROFIT 3 HIT (FULL PROFIT)</b>\n\n#{clean_asset} #PROFIT\n<code>Aksi: {act}\nEntry: {ent:.{dec}f}\nTP3: {p:.{dec}f}\nWaktu: {waktu} WIB</code>"
        is_closed = True

    elif tp2_triggered:
        record_trade_result("TP2")
        msg = f"🎯 <b>TAKE PROFIT 2 HIT</b>\n\n#{clean_asset}\n<code>Aksi: {act}\nEntry: {ent:.{dec}f}\nTP2: {p:.{dec}f}\nStatus: SL dipindah ke TP 1 ({tp1:.{dec}f})</code>"
        with data_lock:
            active_trades[asset].update({"tp2_hit": True, "sl": tp1})
        save_active_trades()

    elif tp1_triggered:
        record_trade_result("TP1")
        msg = f"🎯 <b>TAKE PROFIT 1 HIT</b>\n\n#{clean_asset}\n<code>Aksi: {act}\nEntry: {ent:.{dec}f}\nTP1: {p:.{dec}f}\nStatus: SL dipindah ke Entry / BE ({ent:.{dec}f})</code>"
        with data_lock:
            active_trades[asset].update({"tp1_hit": True, "sl": ent})
        save_active_trades()

    if msg:
        send_telegram_msg(CHAT_ID_SIGNAL, msg)
    if is_closed:
        with data_lock:
            active_trades[asset] = None
        save_active_trades()

def check_daily_recap():
    with data_lock:
        current_date = datetime.now().strftime("%Y-%m-%d")
        if current_date != daily_stats["DATE"]:
            msg = (f"📊 <b>REKAP HARIAN {BOT_NAME}</b> 📊\nTanggal: {daily_stats['DATE']}\n\n"
                   f"Total Sinyal : {daily_stats['TOTAL']}\n🚀 Full TP3 : {daily_stats['TP3']}\n"
                   f"🎯 TP2 Hit : {daily_stats['TP2']}\n🎯 TP1 Hit : {daily_stats['TP1']}\n"
                   f"🛡 Break-Even : {daily_stats['BE']}\n🔴 Stop Loss : {daily_stats['SL']}\n")
            send_telegram_msg(CHAT_ID_SIGNAL, msg)
            daily_stats.update({"DATE": current_date, "TOTAL": 0, "TP1": 0, "TP2": 0, "TP3": 0, "SL": 0, "BE": 0})
            save_daily_stats()

def record_trade_result(result_type):
    with data_lock:
        if result_type in daily_stats:
            daily_stats[result_type] += 1
    save_daily_stats()

def send_price_log():
    waktu = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    log = [f"⚡ <b>{BOT_NAME}</b>\nWaktu : {waktu} WIB\n\n📊 Real-Time Price"]
    for asset in TARGET_ASSETS:
        p = live_prices[asset]
        dec = get_decimal_places(asset)
        klines = get_safe_kline_snapshot(asset)
        if not klines:
            continue
        c = [k[3] for k in klines]
        v = [k[4] for k in klines]
        p_pure = calculate_p_pure(c, v, LENGTH_PERIOD)
        d_res, _ = calculate_topological_drift(p, p_pure)
        log.append(f"{asset:<8}: $ {p:,.{dec}f} | D_res: {d_res:.2f}% | ZF: {get_simplified_zf(c, v, d_res, LENGTH_PERIOD):.2f}")
    send_telegram_msg(CHAT_ID_LOG, "\n".join(log))

def run_signal_scan():
    check_daily_recap()
    for asset, cfg in ASSET_CONFIG.items():
        with data_lock:
            if active_trades.get(asset):
                continue

        klines = get_safe_kline_snapshot(asset)
        p_market = live_prices.get(asset, 0.0)

        if p_market == 0.0 or len(klines) < 20:
            continue

        last_sig = LAST_ALERT_WAKTU_ZF.get(asset)
        if last_sig and (datetime.now() - last_sig).total_seconds() < 900:
            continue

        prices = [k[3] for k in klines]
        vols = [k[4] for k in klines]
        
        p_pure = calculate_p_pure(prices, vols, LENGTH_PERIOD)
        d_res, r_drift = calculate_topological_drift(p_market, p_pure)

        if d_res < cfg.get("min_d_res", MIN_TOPOLOGICAL_DRIFT):
            continue
        
        if not check_imbalance_gap_dynamic(klines, asset, min_fvg_mult=MIN_FVG_MULT):
            continue
            
        if not check_fractal_structure(klines, 8):
            continue

        is_inflection, _ = check_second_derivative_inflection(prices)
        if not is_inflection:
            continue

        zf = get_simplified_zf(prices, vols, d_res, LENGTH_PERIOD)
        if zf < BATAS_KRITIS_ZF:
            continue

        ema200 = calculate_ema(prices, 200)
        trend_buy = not USE_EMA_FILTER or (prices[-1] > ema200)
        trend_sell = not USE_EMA_FILTER or (prices[-1] < ema200)

        is_buy = (r_drift < 0) and trend_buy
        is_sell = (r_drift > 0) and trend_sell

        if not (is_buy or is_sell):
            continue

        act = "BUY" if is_buy else "SELL"
        d = 1 if is_buy else -1
        std_p = calculate_std(prices[-LENGTH_PERIOD:])

        sl_distance = std_p * SIGMA_SL_MULT

        sl_p = p_market - d * sl_distance - d * cfg.get("spread", 0.0)
        tp1_p = p_market + d * (sl_distance * 0.5) - d * cfg.get("spread", 0.0)
        tp2_p = p_market + d * (sl_distance * 1.0) - d * cfg.get("spread", 0.0)
        tp3_p = p_market + d * (sl_distance * 1.5) - d * cfg.get("spread", 0.0)

        with data_lock:
            active_trades[asset] = {
                "action": act, "entry": p_market, "sl": sl_p,
                "tp1": tp1_p, "tp2": tp2_p, "tp3": tp3_p,
                "tp1_hit": False, "tp2_hit": False
            }
            daily_stats["TOTAL"] += 1
            LAST_ALERT_WAKTU_ZF[asset] = datetime.now()

        save_active_trades()
        save_daily_stats()

        dec = get_decimal_places(asset)
        msg = (f"⚡ <b>SINYAL {BOT_NAME}</b> ⚡\n\n#{asset.replace('/', '')} #M5 #{act}\n\n"
               f"<code>Aksi: {'🟢' if is_buy else '🔴'} {act}\nWaktu: {datetime.now().strftime('%H:%M:%S')} WIB\n\n"
               f"ENTRY: {p_market:.{dec}f}\nSL : {sl_p:.{dec}f}\nTP 1 : {tp1_p:.{dec}f} (RR 0.5)\nTP 2 : {tp2_p:.{dec}f} (RR 1.0)\nTP 3 : {tp3_p:.{dec}f} (RR 1.5)\n\n"
               f"D_res: {d_res:.2f}%\nZF : {zf:.2f}</code>\n\n<a href='{get_tv_link(asset)}'>📊 Chart</a>")
        send_telegram_msg(CHAT_ID_SIGNAL, msg)
        print(f"[+] SINYAL M5: {asset} - {act} @ {p_market}")

def sleep_until_next_candle():
    now = datetime.now()
    next_minute = ((now.minute // 5) + 1) * 5
    if next_minute >= 60:
        target = (now + timedelta(hours=1)).replace(minute=0, second=0, microsecond=0)
    else:
        target = now.replace(minute=next_minute, second=0, microsecond=0)
    sleep_sec = (target - now).total_seconds()
    if sleep_sec > 0:
        time.sleep(sleep_sec)


# ==========================================
# MAIN LOOP WORKER THREAD
# ==========================================
def start_bot_worker():
    print(f"==================================================\n BOT {BOT_NAME} SIAP \n==================================================")
    load_active_trades()
    load_daily_stats()
    seed_klines()

    # Jalankan WebSocket TwelveData untuk streaming data real-time BTC & XAU
    start_twelvedata_websocket()

    while True:
        try:
            sleep_until_next_candle()
            send_price_log()
            time.sleep(3.0)
            run_signal_scan()
        except Exception as e:
            print(f"[-] Error utama: {e}")
            time.sleep(5)


if __name__ == "__main__":
    # 1. Jalankan Loop Utama Bot Trading di Thread Terpisah
    bot_thread = threading.Thread(target=start_bot_worker, daemon=True)
    bot_thread.start()

    # 2. Jalankan Flask Server di Main Thread untuk Memenuhi Port Render
    port = int(os.environ.get("PORT", 10000))
    app.run(host="0.0.0.0", port=port)
