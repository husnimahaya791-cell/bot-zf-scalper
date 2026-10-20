import re
import os
import time
import json
import hmac
import hashlib
import base64
import logging
import asyncio
import requests
from pyrogram import Client, filters

# ==========================================
# LOGGING CONFIGURATION
# ==========================================
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s"
)

# ==========================================
# ENVIRONMENT VARIABLES
# ==========================================
API_ID = int(os.environ.get("TELEGRAM_API_ID", "0"))
API_HASH = os.environ.get("TELEGRAM_API_HASH", "")
SESSION_STRING = os.environ.get("TELEGRAM_SESSION_STRING", "")
TARGET_CHAT_ID = int(os.environ.get("TELEGRAM_GROUP_ID", "0"))

BITGET_API_KEY = os.environ.get("BITGET_TRADE_API_KEY", "")
BITGET_SECRET_KEY = os.environ.get("BITGET_TRADE_SECRET_KEY", "")
BITGET_PASSPHRASE = os.environ.get("BITGET_TRADE_PASSPHRASE", "")

RISK_PERCENT = float(os.environ.get("RISK_PERCENT", "1.0"))  # Default 1% risiko per trade
LEVERAGE = int(os.environ.get("LEVERAGE", "10"))

# Inisialisasi Pyrogram dengan String Session (Non-Interactive Cloud Mode)
app = Client(
    "zf_autotrader_cloud",
    api_id=API_ID,
    api_hash=API_HASH,
    session_string=SESSION_STRING
)

# ==========================================
# BITGET FUTURES API EXECUTION
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

def get_account_balance():
    """Mengambil saldo USDT yang tersedia di Bitget Futures."""
    path = "/api/v2/mix/account/accounts?productType=USDT-FUTURES"
    url = f"https://api.bitget.com{path}"
    headers = bitget_headers("GET", path)
    try:
        res = requests.get(url, headers=headers, timeout=10).json()
        if res.get("code") == "00000" and "data" in res:
            for acc in res["data"]:
                if acc.get("marginCoin") == "USDT":
                    return float(acc.get("available", 0.0))
    except Exception as e:
        logging.error(f"[API Balance Error] {e}")
    return 0.0

def place_futures_order(symbol, side, size, sl_price, tp1_price):
    """Mengeksekusi Market Order di Bitget Futures beserta TP1 dan SL bawaan."""
    path = "/api/v2/mix/order/place-order"
    url = f"https://api.bitget.com{path}"
    
    clean_symbol = symbol.replace("BITGET:", "").replace(".P", "").strip()
    
    payload = {
        "productType": "USDT-FUTURES",
        "symbol": clean_symbol,
        "marginCoin": "USDT",
        "size": str(size),
        "side": side.lower(),  # 'buy' untuk Long, 'sell' untuk Short
        "orderType": "market",
        "presetStopLossPrice": str(sl_price),
        "presetTakeProfitPrice": str(tp1_price)
    }
    
    body_json = json.dumps(payload)
    headers = bitget_headers("POST", path, body_json)
    
    try:
        res = requests.post(url, data=body_json, headers=headers, timeout=10).json()
        if res.get("code") == "00000":
            logging.info(f"✅ [AUTO-TRADE SUCCESS] Order {clean_symbol} {side.upper()} berhasil dieksekusi!")
            return res.get("data")
        else:
            logging.error(f"❌ [AUTO-TRADE FAILED] {clean_symbol}: {res.get('msg')}")
    except Exception as e:
        logging.error(f"❌ [API Exec Error] {e}")
    return None

# ==========================================
# SIGNAL PARSER ENGINE
# ==========================================
def parse_zf_signal(text):
    """Menganalisis dan membedah format teks sinyal dari ZF-Core."""
    try:
        if "ZF-CORE M91 PRO" not in text:
            return None

        side = None
        if "BUY SIGNAL" in text:
            side = "buy"
        elif "SELL SIGNAL" in text:
            side = "sell"
            
        if not side:
            return None

        pair_match = re.search(r"Pair:\s*([A-Za-z0-9.]+)", text)
        entry_match = re.search(r"Entry:\s*([0-9.]+)", text)
        sl_match = re.search(r"Stop Loss:\s*([0-9.]+)", text)
        tp1_match = re.search(r"TP 1:\s*([0-9.]+)", text)

        if pair_match and entry_match and sl_match and tp1_match:
            return {
                "symbol": pair_match.group(1).strip(),
                "side": side,
                "entry": float(entry_match.group(1)),
                "sl": float(sl_match.group(1)),
                "tp1": float(tp1_match.group(1))
            }
    except Exception as e:
        logging.error(f"[Parsing Error] Gagal membedah teks sinyal: {e}")
    return None

# ==========================================
# TELEGRAM SIGNAL LISTENER (PYROGRAM)
# ==========================================
@app.on_message(filters.chat(TARGET_CHAT_ID) & filters.text)
async def signal_listener(client, message):
    text = message.text
    signal_data = parse_zf_signal(text)
    
    if signal_data:
        sym = signal_data["symbol"]
        side = signal_data["side"]
        entry = signal_data["entry"]
        sl = signal_data["sl"]
        tp1 = signal_data["tp1"]
        
        logging.info(f"🚀 [Sinyal Diterima] {sym} | Action: {side.upper()} | Entry: {entry} | SL: {sl}")
        
        balance = get_account_balance()
        if balance <= 0:
            logging.warning("⚠️ Saldo USDT tidak mencukupi atau API Error.")
            return

        risk_amount = balance * (RISK_PERCENT / 100.0)
        price_diff = abs(entry - sl)
        if price_diff <= 0:
            return

        raw_size = risk_amount / price_diff
        size = round(raw_size, 2)

        place_futures_order(
            symbol=sym,
            side=side,
            size=size,
            sl_price=sl,
            tp1_price=tp1
        )

# ==========================================
# MAIN EXECUTION
# ==========================================
if __name__ == "__main__":
    logging.info("⚡ Auto-Trader Listener Aktif di Cloud Railway...")
    app.run()
