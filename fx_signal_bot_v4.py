"""
FX Signal Discord通知Bot v3
=============================
v2からの改善点:
  1. チェック頻度最適化
     - ポジションなし: 4H足確定タイミングのみ(1日6回)
     - ポジションあり: 毎分チェック
  2. データソース二段構成
     - エントリー判定(4H足MA): yfinance (4時間に1回、遅延許容)
     - 現在値(SL/トレイリング): Twelve Data無料枠 (リアルタイム)
     - フォールバック: Twelve Data失敗時 → yfinanceで代替
  3. DB読み込みフェイルセーフ
     - 読み込み失敗 → スキップ(ポジション情報を消さない)
     - 書き込み失敗 → リトライ(最大3回)
  4. yfinance/TwelveDataリトライ(指数バックオフ)
  5. Discord Webhookレート制限対策
     - 送信間隔制御 + 429レスポンス時のRetry-After対応
  6. Webhook URL/APIキー環境変数化
  7. GBPJPY/EURJPY: 1H足MAクロス + 4Hトレンドフィルターエントリー

環境変数:
  DISCORD_WEBHOOK_URL  — Discord Webhook URL (必須)
  DYNAMO_TABLE         — DynamoDBテーブル名 (デフォルト: fx-signal-bot)
  TWELVEDATA_API_KEY   — Twelve Data APIキー (無料枠でOK)
"""

import json
import os
import urllib.request
import urllib.error
from datetime import datetime, timezone, timedelta
from decimal import Decimal, ROUND_HALF_UP
import time as _time

import boto3
import yfinance as yf
import pandas as pd
import numpy as np

# ============================================================
# 設定
# ============================================================

DISCORD_WEBHOOK_URL = os.environ.get("DISCORD_WEBHOOK_URL", "")
DYNAMO_TABLE = os.environ.get("DYNAMO_TABLE", "fx-signal-bot")
TWELVEDATA_API_KEY = os.environ.get("TWELVEDATA_API_KEY", "")

MA_SHORT = 10
MA_MID = 25
SL_NOTIFY_THRESHOLD = 5

H4_CONFIRM_HOURS = {0, 4, 8, 12, 16, 20}
H4_CONFIRM_WINDOW = 3
H1_CONFIRM_WINDOW = 3  # 1H足確定後3分以内

MAX_RETRIES = 3
RETRY_BASE_DELAY = 1.5
DISCORD_MIN_INTERVAL = 1.2

PAIRS = {
    "EURUSD": {"symbol": "EURUSD=X", "td_symbol": "EUR/USD", "pip": 0.0001, "flag": "🇪🇺🇺🇸", "name": "ユーロドル", "digits": 5, "countries": ["EU", "US"], "sl_pips": 40, "trigger_pips": 25, "dist_pips": 20, "entry_tf": "4h"},
    "GBPUSD": {"symbol": "GBPUSD=X", "td_symbol": "GBP/USD", "pip": 0.0001, "flag": "🇬🇧🇺🇸", "name": "ポンドドル", "digits": 5, "countries": ["GB", "US"], "sl_pips": 25, "trigger_pips": 25, "dist_pips": 20, "entry_tf": "4h"},
    "USDJPY": {"symbol": "USDJPY=X", "td_symbol": "USD/JPY", "pip": 0.01, "flag": "🇺🇸🇯🇵", "name": "ドル円", "digits": 3, "countries": ["US", "JP"], "sl_pips": 30, "trigger_pips": 25, "dist_pips": 20, "entry_tf": "4h"},
    "GBPJPY": {"symbol": "GBPJPY=X", "td_symbol": "GBP/JPY", "pip": 0.01, "flag": "🇬🇧🇯🇵", "name": "ポンド円", "digits": 3, "countries": ["GB", "JP"], "sl_pips": 50, "trigger_pips": 20, "dist_pips": 15, "entry_tf": "1h"},
    "AUDUSD": {"symbol": "AUDUSD=X", "td_symbol": "AUD/USD", "pip": 0.0001, "flag": "🇦🇺🇺🇸", "name": "豪ドル", "digits": 5, "countries": ["AU", "US"], "sl_pips": 50, "trigger_pips": 30, "dist_pips": 20, "entry_tf": "4h"},
    "USDCHF": {"symbol": "USDCHF=X", "td_symbol": "USD/CHF", "pip": 0.0001, "flag": "🇺🇸🇨🇭", "name": "ドルフラン", "digits": 5, "countries": ["US", "CH"], "sl_pips": 30, "trigger_pips": 25, "dist_pips": 15, "entry_tf": "4h"},
    "USDCAD": {"symbol": "USDCAD=X", "td_symbol": "USD/CAD", "pip": 0.0001, "flag": "🇺🇸🇨🇦", "name": "カナドル", "digits": 5, "countries": ["US", "CA"], "sl_pips": 30, "trigger_pips": 20, "dist_pips": 10, "entry_tf": "4h"},
    "EURJPY": {"symbol": "EURJPY=X", "td_symbol": "EUR/JPY", "pip": 0.01, "flag": "🇪🇺🇯🇵", "name": "ユロ円", "digits": 3, "countries": ["EU", "JP"], "sl_pips": 30, "trigger_pips": 20, "dist_pips": 10, "entry_tf": "1h"},
    "AUDJPY": {"symbol": "AUDJPY=X", "td_symbol": "AUD/JPY", "pip": 0.01, "flag": "🇦🇺🇯🇵", "name": "豪円", "digits": 3, "countries": ["AU", "JP"], "sl_pips": 40, "trigger_pips": 30, "dist_pips": 25, "entry_tf": "4h"},
}

dynamodb = boto3.resource("dynamodb")
table = dynamodb.Table(DYNAMO_TABLE)
_last_discord_send = 0.0

# ============================================================
# 経済指標カレンダー (2026年)
# ============================================================

ECONOMIC_EVENTS_2026 = {
    "2026-01-28": [{"name": "FOMC", "country": "US", "time_jst": "04:00"}],
    "2026-03-18": [{"name": "FOMC", "country": "US", "time_jst": "04:00"}],
    "2026-04-29": [{"name": "FOMC", "country": "US", "time_jst": "04:00"}],
    "2026-06-17": [{"name": "FOMC", "country": "US", "time_jst": "04:00"}],
    "2026-07-29": [{"name": "FOMC", "country": "US", "time_jst": "04:00"}],
    "2026-09-16": [{"name": "FOMC", "country": "US", "time_jst": "04:00"}],
    "2026-10-28": [{"name": "FOMC", "country": "US", "time_jst": "04:00"}],
    "2026-12-09": [{"name": "FOMC", "country": "US", "time_jst": "04:00"}],
    "2026-01-09": [{"name": "米雇用統計", "country": "US", "time_jst": "22:30"}],
    "2026-02-06": [{"name": "米雇用統計", "country": "US", "time_jst": "22:30"}],
    "2026-03-06": [{"name": "米雇用統計", "country": "US", "time_jst": "22:30"}],
    "2026-04-03": [{"name": "米雇用統計", "country": "US", "time_jst": "21:30"}],
    "2026-05-08": [{"name": "米雇用統計", "country": "US", "time_jst": "21:30"}],
    "2026-06-05": [{"name": "米雇用統計", "country": "US", "time_jst": "21:30"}],
    "2026-07-02": [{"name": "米雇用統計", "country": "US", "time_jst": "21:30"}],
    "2026-08-07": [{"name": "米雇用統計", "country": "US", "time_jst": "21:30"}],
    "2026-09-04": [{"name": "米雇用統計", "country": "US", "time_jst": "21:30"}],
    "2026-10-02": [{"name": "米雇用統計", "country": "US", "time_jst": "21:30"}],
    "2026-11-06": [{"name": "米雇用統計", "country": "US", "time_jst": "22:30"}],
    "2026-12-04": [{"name": "米雇用統計", "country": "US", "time_jst": "22:30"}],
    "2026-02-11": [{"name": "米CPI", "country": "US", "time_jst": "22:30"}],
    "2026-03-11": [{"name": "米CPI", "country": "US", "time_jst": "22:30"}],
    "2026-04-10": [{"name": "米CPI", "country": "US", "time_jst": "21:30"}],
    "2026-05-12": [{"name": "米CPI", "country": "US", "time_jst": "21:30"}],
    "2026-06-10": [{"name": "米CPI", "country": "US", "time_jst": "21:30"}],
    "2026-07-14": [{"name": "米CPI", "country": "US", "time_jst": "21:30"}],
    "2026-08-12": [{"name": "米CPI", "country": "US", "time_jst": "21:30"}],
    "2026-09-11": [{"name": "米CPI", "country": "US", "time_jst": "21:30"}],
    "2026-10-13": [{"name": "米CPI", "country": "US", "time_jst": "21:30"}],
    "2026-11-12": [{"name": "米CPI", "country": "US", "time_jst": "22:30"}],
    "2026-12-10": [{"name": "米CPI", "country": "US", "time_jst": "22:30"}],
    "2026-01-23": [{"name": "日銀会合", "country": "JP", "time_jst": "12:00"}],
    "2026-03-13": [{"name": "日銀会合", "country": "JP", "time_jst": "12:00"}],
    "2026-04-28": [{"name": "日銀会合", "country": "JP", "time_jst": "12:00"}],
    "2026-06-16": [{"name": "日銀会合", "country": "JP", "time_jst": "12:00"}],
    "2026-07-17": [{"name": "日銀会合", "country": "JP", "time_jst": "12:00"}],
    "2026-09-17": [{"name": "日銀会合", "country": "JP", "time_jst": "12:00"}],
    "2026-10-29": [{"name": "日銀会合", "country": "JP", "time_jst": "12:00"}],
    "2026-12-18": [{"name": "日銀会合", "country": "JP", "time_jst": "12:00"}],
    "2026-01-22": [{"name": "ECB政策金利", "country": "EU", "time_jst": "22:15"}],
    "2026-03-05": [{"name": "ECB政策金利", "country": "EU", "time_jst": "22:15"}],
    "2026-04-16": [{"name": "ECB政策金利", "country": "EU", "time_jst": "21:15"}],
    "2026-06-04": [{"name": "ECB政策金利", "country": "EU", "time_jst": "21:15"}],
    "2026-07-16": [{"name": "ECB政策金利", "country": "EU", "time_jst": "21:15"}],
    "2026-09-10": [{"name": "ECB政策金利", "country": "EU", "time_jst": "21:15"}],
    "2026-10-22": [{"name": "ECB政策金利", "country": "EU", "time_jst": "22:15"}],
    "2026-12-10": [{"name": "ECB政策金利", "country": "EU", "time_jst": "22:15"}],
    "2026-02-05": [{"name": "BOE政策金利", "country": "GB", "time_jst": "21:00"}],
    "2026-03-19": [{"name": "BOE政策金利", "country": "GB", "time_jst": "21:00"}],
    "2026-05-07": [{"name": "BOE政策金利", "country": "GB", "time_jst": "20:00"}],
    "2026-06-18": [{"name": "BOE政策金利", "country": "GB", "time_jst": "20:00"}],
    "2026-08-06": [{"name": "BOE政策金利", "country": "GB", "time_jst": "20:00"}],
    "2026-09-17": [{"name": "BOE政策金利", "country": "GB", "time_jst": "20:00"}],
    "2026-11-05": [{"name": "BOE政策金利", "country": "GB", "time_jst": "21:00"}],
    "2026-12-17": [{"name": "BOE政策金利", "country": "GB", "time_jst": "21:00"}],
    "2026-02-17": [{"name": "RBA政策金利", "country": "AU", "time_jst": "12:30"}],
    "2026-04-07": [{"name": "RBA政策金利", "country": "AU", "time_jst": "12:30"}],
    "2026-05-19": [{"name": "RBA政策金利", "country": "AU", "time_jst": "12:30"}],
    "2026-07-07": [{"name": "RBA政策金利", "country": "AU", "time_jst": "12:30"}],
    "2026-08-04": [{"name": "RBA政策金利", "country": "AU", "time_jst": "12:30"}],
    "2026-09-01": [{"name": "RBA政策金利", "country": "AU", "time_jst": "12:30"}],
    "2026-11-03": [{"name": "RBA政策金利", "country": "AU", "time_jst": "12:30"}],
    "2026-12-01": [{"name": "RBA政策金利", "country": "AU", "time_jst": "12:30"}],
}


def get_todays_events(countries):
    now_utc = datetime.now(timezone.utc)
    today = now_utc.strftime("%Y-%m-%d")
    tomorrow = (now_utc + timedelta(days=1)).strftime("%Y-%m-%d")
    relevant = []
    for date_str in [today, tomorrow]:
        for e in ECONOMIC_EVENTS_2026.get(date_str, []):
            if e["country"] in countries or e["country"] == "US":
                relevant.append({**e, "date": date_str})
    return relevant


def format_event_warning(events):
    if not events:
        return ""
    lines = [f"  {e['date'][5:]} {e['time_jst']} JST  {e['name']}" for e in events]
    return "\n⚠️ **本日〜明日の重要指標**\n```\n" + "\n".join(lines) + "\n```\n_指標前後はエントリー見送り推奨_"


# ============================================================
# タイミング判定
# ============================================================

def is_h4_confirm_time(now_utc):
    """4H確定時 (0,4,8,12,16,20時の0〜2分)"""
    return now_utc.hour in H4_CONFIRM_HOURS and now_utc.minute < H4_CONFIRM_WINDOW


def is_h1_confirm_time(now_utc):
    """毎時0〜2分（1H足確定直後）"""
    return now_utc.minute < H1_CONFIRM_WINDOW


# ============================================================
# DynamoDB（フェイルセーフ）
# ============================================================

def default_state():
    return {"in_position": False, "direction": None, "entry_price": None, "entry_time": None, "initial_sl": None, "trailing_active": False, "current_sl": None, "last_notified_sl": None, "peak_price": None, "last_signal_time": None, "last_signal_dir": None}

_DB_READ_FAILED = "__DB_READ_FAILED__"

def load_state(pair):
    try:
        resp = table.get_item(Key={"id": f"state_{pair}"})
        if "Item" in resp:
            return {k: float(v) if isinstance(v, Decimal) else v for k, v in resp["Item"].get("state", {}).items()}
        return default_state()
    except Exception as e:
        print(f"  [{pair}] ❌ DB読み込みエラー: {e}")
        return _DB_READ_FAILED


def save_state(pair, state):
    for attempt in range(MAX_RETRIES):
        try:
            clean = {k: Decimal(str(v)).quantize(Decimal('0.00001')) if isinstance(v, float) else v for k, v in state.items()}
            table.put_item(Item={"id": f"state_{pair}", "state": clean, "updated_at": datetime.now(timezone.utc).isoformat()})
            return True
        except Exception as e:
            print(f"  [{pair}] ⚠️ DB書き込みエラー(試行{attempt+1}): {e}")
            if attempt < MAX_RETRIES - 1:
                _time.sleep(RETRY_BASE_DELAY * (2 ** attempt))
    print(f"  [{pair}] ❌ DB書き込み最終失敗")
    return False


# ============================================================
# データ取得（リトライ付き）
# ============================================================

def _retry(func, label, max_retries=MAX_RETRIES):
    for attempt in range(max_retries):
        try:
            result = func()
            if result is not None:
                return result
        except Exception as e:
            print(f"    {label} エラー(試行{attempt+1}): {e}")
        if attempt < max_retries - 1:
            _time.sleep(RETRY_BASE_DELAY * (2 ** attempt))
    return None


def fetch_4h_data(symbol):
    def _f():
        df = yf.download(symbol, period="30d", interval="1h", progress=False)
        if df.empty: return None
        if isinstance(df.columns, pd.MultiIndex): df.columns = df.columns.get_level_values(0)
        df_4h = df.resample("4h").agg({"Open": "first", "High": "max", "Low": "min", "Close": "last", "Volume": "sum"}).dropna()
        df_4h["ma_short"] = df_4h["Close"].rolling(window=MA_SHORT).mean()
        df_4h["ma_mid"] = df_4h["Close"].rolling(window=MA_MID).mean()
        return df_4h.dropna()
    return _retry(_f, f"yfinance 4H ({symbol})")


def fetch_1h_data(symbol):
    """1H足データ取得（MA計算用）"""
    def _f():
        df = yf.download(symbol, period="7d", interval="1h", progress=False)
        if df.empty: return None
        if isinstance(df.columns, pd.MultiIndex):
            df.columns = df.columns.get_level_values(0)
        df["ma_short"] = df["Close"].rolling(window=MA_SHORT).mean()
        df["ma_mid"] = df["Close"].rolling(window=MA_MID).mean()
        return df.dropna()
    return _retry(_f, f"yfinance 1H ({symbol})")


def fetch_current_price_twelvedata(td_symbol):
    if not TWELVEDATA_API_KEY: return None
    def _f():
        url = f"https://api.twelvedata.com/price?symbol={td_symbol}&apikey={TWELVEDATA_API_KEY}"
        req = urllib.request.Request(url, headers={"User-Agent": "FXSignalBot/3.0"})
        with urllib.request.urlopen(req, timeout=5) as resp:
            data = json.loads(resp.read().decode())
        if "price" in data: return float(data["price"])
        print(f"    TwelveData: {data.get('message', data.get('code', 'unknown'))}")
        return None
    return _retry(_f, f"TwelveData ({td_symbol})", max_retries=1)


def fetch_current_price_yfinance(symbol):
    def _f():
        df = yf.download(symbol, period="1d", interval="1m", progress=False)
        if df.empty: return None
        if isinstance(df.columns, pd.MultiIndex): df.columns = df.columns.get_level_values(0)
        return float(df["Close"].iloc[-1])
    return _retry(_f, f"yfinance price ({symbol})", max_retries=2)


def fetch_current_price(info):
    price = fetch_current_price_twelvedata(info["td_symbol"])
    if price is not None:
        return price, "twelvedata"
    print(f"    → yfinanceにフォールバック")
    price = fetch_current_price_yfinance(info["symbol"])
    if price is not None:
        return price, "yfinance"
    return None, None


# ============================================================
# Discord通知（レート制限対策）
# ============================================================

def send_discord(content, color=0x3498DB):
    global _last_discord_send
    if not DISCORD_WEBHOOK_URL:
        print(f"  ⚠️ DISCORD_WEBHOOK_URL未設定")
        return
    elapsed = _time.time() - _last_discord_send
    if elapsed < DISCORD_MIN_INTERVAL:
        _time.sleep(DISCORD_MIN_INTERVAL - elapsed)
    payload = {"content": "@everyone", "allowed_mentions": {"parse": ["everyone"]}, "embeds": [{"description": content, "color": color, "timestamp": datetime.now(timezone.utc).isoformat(), "footer": {"text": "FX Signal Bot v3"}}]}
    data = json.dumps(payload).encode("utf-8")
    req = urllib.request.Request(DISCORD_WEBHOOK_URL, data=data, headers={"Content-Type": "application/json", "User-Agent": "FXSignalBot/3.0"}, method="POST")
    for attempt in range(MAX_RETRIES):
        try:
            with urllib.request.urlopen(req, timeout=10):
                _last_discord_send = _time.time()
                return
        except urllib.error.HTTPError as e:
            if e.code == 429:
                retry_after = float(e.headers.get("Retry-After", 2))
                print(f"    Discord 429, {retry_after}s待機")
                _time.sleep(retry_after)
                continue
            print(f"    Discord HTTPエラー: {e.code}")
            break
        except Exception as e:
            print(f"    Discord送信エラー(試行{attempt+1}): {e}")
            if attempt < MAX_RETRIES - 1: _time.sleep(1)
    _last_discord_send = _time.time()


# ============================================================
# 通知フォーマット
# ============================================================

def fmt(price, digits): return f"{price:.{digits}f}"

def notify_entry(pair, info, direction, price, sl, event_warning="", entry_label=""):
    d, sl_pips, trig, dist = info["digits"], info["sl_pips"], info["trigger_pips"], info["dist_pips"]
    color = 0x2ECC71 if direction == "long" else 0xE74C3C
    arrow = "🟢 ロング (買い)" if direction == "long" else "🔴 ショート (売り)"
    cross = "上抜け" if direction == "long" else "下抜け"
    # エントリーの根拠を表示（1H+4Hフィルター or 4H MAクロス）
    tf_label = entry_label if entry_label else f"MA{MA_SHORT}がMA{MA_MID}を{cross}"
    content = (f"## {info['flag']} {pair} エントリー\n**{arrow}**\n\n```\n  通貨ペア : {info['name']} ({pair})\n  価格     : {fmt(price, d)}\n  損切(SL) : {fmt(sl, d)}  (-{sl_pips}pips)\n  設定     : SL{sl_pips} T{trig}D{dist}\n```\n\n📌 **やること**\n1. {info['name']}を{'買い' if direction == 'long' else '売り'}エントリー\n2. SLを **{fmt(sl, d)}** に設定\n3. トレイリング通知を待つ\n\n_{tf_label}_")
    if event_warning: content += event_warning
    send_discord(content, color)

def notify_trailing_start(pair, info, direction, entry_price, price, new_sl):
    d, pip = info["digits"], info["pip"]
    pnl = (price - entry_price) / pip if direction == "long" else (entry_price - price) / pip
    sl_pnl = (new_sl - entry_price) / pip if direction == "long" else (entry_price - new_sl) / pip
    content = (f"## {info['flag']} {pair} トレイリング発動 🔄\n{'⬆️ ロング' if direction == 'long' else '⬇️ ショート'}\n\n```\n  現在値 : {fmt(price, d)}  (含み益 +{pnl:.0f}pips)\n  新SL   : {fmt(new_sl, d)}  (SL時 {sl_pnl:+.0f}pips)\n```\n\n📌 → SLを **{fmt(new_sl, d)}** に移動\n_今後も伸びればSL更新を通知します_")
    send_discord(content, 0xF39C12)

def notify_sl_update(pair, info, direction, price, new_sl, entry_price):
    d, pip = info["digits"], info["pip"]
    pnl = (price - entry_price) / pip if direction == "long" else (entry_price - price) / pip
    sl_pnl = (new_sl - entry_price) / pip if direction == "long" else (entry_price - new_sl) / pip
    content = (f"## {info['flag']} {pair} SL更新 📐\n{'⬆️ ロング' if direction == 'long' else '⬇️ ショート'}\n\n```\n  現在値 : {fmt(price, d)}  (含み益 +{pnl:.0f}pips)\n  新SL   : {fmt(new_sl, d)}  (SL時 {sl_pnl:+.0f}pips)\n```\n\n📌 → SLを **{fmt(new_sl, d)}** に移動")
    send_discord(content, 0x9B59B6)

def notify_sl_hit(pair, info, direction, entry_price, exit_sl, price):
    d, pip = info["digits"], info["pip"]
    pnl = (exit_sl - entry_price) / pip if direction == "long" else (entry_price - exit_sl) / pip
    emoji = "💰" if pnl > 0 else "💔"
    result = "利確(トレイリング)" if pnl > 0 else "損切り"
    color = 0x2ECC71 if pnl > 0 else 0xE74C3C
    content = (f"## {info['flag']} {pair} {result} {emoji}\n{'⬆️ ロング' if direction == 'long' else '⬇️ ショート'}\n\n```\n  エントリー : {fmt(entry_price, d)}\n  決済SL     : {fmt(exit_sl, d)}\n  現在値     : {fmt(price, d)}\n  損益       : {pnl:+.1f}pips\n```")
    send_discord(content, color)


# ============================================================
# シグナル判定
# ============================================================

def check_entry(pair, info, df_4h, state, event_warning=""):
    """従来の4H足MAクロスエントリー判定"""
    if df_4h is None or len(df_4h) < 2: return state
    current, prev = df_4h.iloc[-1], df_4h.iloc[-2]
    price = float(current["Close"])
    candle_time = str(df_4h.index[-1])
    pip, sl_pips = info["pip"], info["sl_pips"]
    signal = None
    if prev["ma_short"] <= prev["ma_mid"] and current["ma_short"] > current["ma_mid"]: signal = "long"
    elif prev["ma_short"] >= prev["ma_mid"] and current["ma_short"] < current["ma_mid"]: signal = "short"
    if signal:
        if state.get("last_signal_time") == candle_time and state.get("last_signal_dir") == signal:
            print(f"  [{pair}] 重複スキップ"); return state
        sl = price - sl_pips * pip if signal == "long" else price + sl_pips * pip
        print(f"  [{pair}] {'🟢' if signal == 'long' else '🔴'} {signal.upper()} @ {price}")
        cross = "上抜け" if signal == "long" else "下抜け"
        notify_entry(pair, info, signal, price, sl, event_warning, f"4H MA{MA_SHORT}がMA{MA_MID}を{cross}")
        state.update({"in_position": True, "direction": signal, "entry_price": price, "entry_time": candle_time, "initial_sl": sl, "trailing_active": False, "current_sl": sl, "last_notified_sl": sl, "peak_price": price, "last_signal_time": candle_time, "last_signal_dir": signal})
    else:
        print(f"  [{pair}] 待機中 (MA{MA_SHORT}={current['ma_short']:.5f} MA{MA_MID}={current['ma_mid']:.5f})")
    return state


def get_4h_trend(df_4h):
    """4H足のMA方向を返す: 'long', 'short', or None"""
    if df_4h is None or len(df_4h) < 1:
        return None
    latest = df_4h.iloc[-1]
    if latest["ma_short"] > latest["ma_mid"]:
        return "long"
    elif latest["ma_short"] < latest["ma_mid"]:
        return "short"
    return None


def check_entry_1h_filtered(pair, info, df_1h, df_4h, state, event_warning=""):
    """1H足MAクロス + 4Hトレンドフィルターでエントリー判定"""
    if df_1h is None or len(df_1h) < 2:
        return state

    # 4Hフィルター
    trend = get_4h_trend(df_4h)
    if trend is None:
        print(f"  [{pair}] 4Hトレンド不明、スキップ")
        return state

    # 1H足MAクロス判定
    current, prev = df_1h.iloc[-1], df_1h.iloc[-2]
    price = float(current["Close"])
    candle_time = str(df_1h.index[-1])
    pip, sl_pips = info["pip"], info["sl_pips"]

    signal = None
    if prev["ma_short"] <= prev["ma_mid"] and current["ma_short"] > current["ma_mid"]:
        signal = "long"
    elif prev["ma_short"] >= prev["ma_mid"] and current["ma_short"] < current["ma_mid"]:
        signal = "short"

    # 4Hトレンドと不一致ならスキップ
    if signal and signal != trend:
        print(f"  [{pair}] 1H {signal} シグナルだが 4H {trend} と不一致、スキップ")
        return state

    if signal:
        if state.get("last_signal_time") == candle_time and state.get("last_signal_dir") == signal:
            print(f"  [{pair}] 重複スキップ")
            return state
        sl = price - sl_pips * pip if signal == "long" else price + sl_pips * pip
        print(f"  [{pair}] {'🟢' if signal == 'long' else '🔴'} {signal.upper()} @ {price} (1H+4Hfilter)")
        cross = "上抜け" if signal == "long" else "下抜け"
        notify_entry(pair, info, signal, price, sl, event_warning, f"1H MA{MA_SHORT}がMA{MA_MID}を{cross} (4Hフィルター: {trend})")
        state.update({
            "in_position": True, "direction": signal,
            "entry_price": price, "entry_time": candle_time,
            "initial_sl": sl, "trailing_active": False,
            "current_sl": sl, "last_notified_sl": sl,
            "peak_price": price, "last_signal_time": candle_time,
            "last_signal_dir": signal
        })
    else:
        print(f"  [{pair}] 1H待機中 (trend={trend})")

    return state


def check_trailing(pair, info, price, state):
    try:
        direction = state.get("direction")
        entry_price = state.get("entry_price")
        current_sl = state.get("current_sl")
        peak = state.get("peak_price", entry_price)
        last_notified_sl = state.get("last_notified_sl", current_sl)
        pip = info["pip"]
        trail_trigger = float(Decimal(str(info["trigger_pips"])) * Decimal(str(pip)))
        trail_dist = float(Decimal(str(info["dist_pips"])) * Decimal(str(pip)))

        # 型を強制的にfloatに
        price = float(price)
        current_sl = float(current_sl)
        entry_price = float(entry_price)
        peak = float(peak)
        last_notified_sl = float(last_notified_sl)

        # ========== SL到達判定（従来通り） ==========
        sl_hit = (direction == "long" and price <= current_sl) or (direction == "short" and price >= current_sl)

        # ========== SL通過検知（ブローカー決済後の反発対策） ==========
        sl_crossed = False
        if state.get("trailing_active", False) and not sl_hit:
            if direction == "long":
                sl_margin = info.get("dist_pips", 10) * pip
                if price <= current_sl + sl_margin and peak > current_sl + sl_margin:
                    sl_crossed = True
            else:
                sl_margin = info.get("dist_pips", 10) * pip
                if price >= current_sl - sl_margin and peak < current_sl - sl_margin:
                    sl_crossed = True

        print(f"  [{pair}] SL判定: dir={direction} price={price} sl={current_sl} hit={sl_hit} crossed={sl_crossed}")

        if sl_hit:
            print(f"  [{pair}] ⚠️ SL到達確定 (price={price}, SL={current_sl})")
            notify_sl_hit(pair, info, direction, entry_price, current_sl, price)
            state["in_position"] = False
            save_state(pair, state)
            return state

        if sl_crossed:
            pnl = (current_sl - entry_price) / pip if direction == "long" else (entry_price - current_sl) / pip
            emoji = "💰" if pnl > 0 else "💔"
            result = "利確(トレイリング)" if pnl > 0 else "損切り"
            color = 0x2ECC71 if pnl > 0 else 0xE74C3C
            d = info["digits"]
            content = (
                f"## {info['flag']} {pair} {result}の可能性 {emoji}\n"
                f"{'⬆️ ロング' if direction == 'long' else '⬇️ ショート'}\n\n"
                f"```\n"
                f"  エントリー : {fmt(entry_price, d)}\n"
                f"  トレイルSL : {fmt(current_sl, d)}\n"
                f"  現在値     : {fmt(price, d)}\n"
                f"  推定損益   : {pnl:+.1f}pips\n"
                f"```\n\n"
                f"📌 **ブローカーでSL決済済みか確認してください**\n"
                f"_価格がSL付近まで戻ったため、ブローカー側で約定済みの可能性があります_"
            )
            send_discord(content, color)
            state["in_position"] = False
            save_state(pair, state)
            return state

        # ========== ピーク更新 & トレイリング ==========
        if direction == "long":
            if price > peak: peak = price
            state["peak_price"] = peak
            if peak - entry_price >= trail_trigger:
                new_sl = float(Decimal(str(peak)) - Decimal(str(trail_dist)))
                if new_sl > current_sl:
                    state["current_sl"] = new_sl
                    if not state.get("trailing_active", False):
                        state["trailing_active"] = True
                        notify_trailing_start(pair, info, direction, entry_price, price, new_sl)
                        state["last_notified_sl"] = new_sl
                    elif (new_sl - last_notified_sl) / pip >= SL_NOTIFY_THRESHOLD:
                        notify_sl_update(pair, info, direction, price, new_sl, entry_price)
                        state["last_notified_sl"] = new_sl
        else:
            if price < peak: peak = price
            state["peak_price"] = peak
            if entry_price - peak >= trail_trigger:
                new_sl = float(Decimal(str(peak)) + Decimal(str(trail_dist)))
                if new_sl < current_sl:
                    state["current_sl"] = new_sl
                    if not state.get("trailing_active", False):
                        state["trailing_active"] = True
                        notify_trailing_start(pair, info, direction, entry_price, price, new_sl)
                        state["last_notified_sl"] = new_sl
                    elif (last_notified_sl - new_sl) / pip >= SL_NOTIFY_THRESHOLD:
                        notify_sl_update(pair, info, direction, price, new_sl, entry_price)
                        state["last_notified_sl"] = new_sl

        pnl = (price - entry_price) / pip if direction == "long" else (entry_price - price) / pip
        print(f"  [{pair}] {direction.upper()} P&L={pnl:+.0f}pips SL={state['current_sl']} Trail={'ON' if state.get('trailing_active') else 'OFF'}")
        return state

    except Exception as e:
        print(f"  [{pair}] ❌ check_trailing例外: {type(e).__name__}: {e}")
        try:
            _price = float(price)
            _sl = float(state.get("current_sl", 0))
            _dir = state.get("direction")
            if (_dir == "long" and _price <= _sl) or (_dir == "short" and _price >= _sl):
                print(f"  [{pair}] ⚠️ フォールバックSL判定: 到達")
                notify_sl_hit(pair, info, _dir, float(state.get("entry_price", 0)), _sl, _price)
                state["in_position"] = False
                save_state(pair, state)
        except Exception as e2:
            print(f"  [{pair}] ❌ フォールバックも失敗: {e2}")
        return state


# ============================================================
# Lambda ハンドラー
# ============================================================

def lambda_handler(event, context):
    now_utc = datetime.now(timezone.utc)
    h4_time = is_h4_confirm_time(now_utc)
    h1_time = is_h1_confirm_time(now_utc)
    print(f"=== FX Signal Bot v3 {now_utc.isoformat()} | 4H確定: {'YES' if h4_time else 'NO'} | 1H確定: {'YES' if h1_time else 'NO'} ===")

    processed = skipped = 0

    for pair, info in PAIRS.items():
        state = load_state(pair)
        if state == _DB_READ_FAILED:
            print(f"  [{pair}] ⏭️ DB読み込み失敗、スキップ")
            skipped += 1
            continue

        has_position = state.get("in_position", False)
        entry_tf = info.get("entry_tf", "4h")

        if not has_position:
            # エントリー判定: タイミングに応じて分岐
            if entry_tf == "1h" and h1_time:
                # 1H+4Hフィルターエントリー（GBPJPY, EURJPY）
                print(f"\n--- {pair} (1H+4Hフィルター) ---")
                processed += 1
                event_warning = format_event_warning(get_todays_events(info["countries"]))
                df_1h = fetch_1h_data(info["symbol"])
                df_4h = fetch_4h_data(info["symbol"])
                if df_1h is not None:
                    state = check_entry_1h_filtered(pair, info, df_1h, df_4h, state, event_warning)
                else:
                    print(f"  [{pair}] 1H足取得失敗")
            elif entry_tf == "4h" and h4_time:
                # 従来の4Hエントリー
                print(f"\n--- {pair} (4H) ---")
                processed += 1
                event_warning = format_event_warning(get_todays_events(info["countries"]))
                df_4h = fetch_4h_data(info["symbol"])
                if df_4h is not None:
                    state = check_entry(pair, info, df_4h, state, event_warning)
                else:
                    print(f"  [{pair}] 4H足取得失敗")
            else:
                continue  # タイミングでなければスキップ
        else:
            # ポジション保有中: 従来通り毎分チェック
            print(f"\n--- {pair} ---")
            processed += 1
            try:
                print(f"  ポジション: {state.get('direction')} @ {state.get('entry_price')}")
                price, source = fetch_current_price(info)
                if price is not None:
                    print(f"  現在値: {price} ({source})")
                    state["price_fail_count"] = 0
                    state = check_trailing(pair, info, price, state)
                else:
                    print(f"  [{pair}] 現在値取得失敗、次回リトライ")
            except Exception as e:
                print(f"  [{pair}] ❌ ポジション処理で例外: {type(e).__name__}: {e}")

        save_state(pair, state)

    print(f"\n=== 完了 (処理:{processed} スキップ:{skipped}) ===")
    return {"statusCode": 200, "body": "OK"}