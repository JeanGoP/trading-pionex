import requests
import pandas as pd
import numpy as np
import time
import hmac
import hashlib
import asyncio
import os
import threading
import queue
import random
from datetime import datetime, timedelta, timezone
from email.utils import parsedate_to_datetime
import xml.etree.ElementTree as ET
from ta.momentum import RSIIndicator
from ta.trend import MACD, EMAIndicator, ADXIndicator
from ta.volatility import BollingerBands, AverageTrueRange
from database import (
    SessionLocal, guardar_bot, cerrar_bot, Bot,
    guardar_ciclo, get_configuracion, get_estadisticas,
    guardar_seguimiento, get_seguimientos_pendientes,
    actualizar_seguimiento, get_reporte_seguimiento
)

PIONEX_BASE_URL = "https://api.pionex.com"
TELEGRAM_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN", "")
TELEGRAM_CHAT = os.getenv("TELEGRAM_CHAT_ID", "")
CRYPTOPANIC_TOKEN = os.getenv("CRYPTOPANIC_TOKEN", "")

_telegram_queue: "queue.Queue[tuple[str, str, str]]" = queue.Queue(maxsize=1000)
_telegram_worker_started = False
_telegram_worker_lock = threading.Lock()
_telegram_not_configured_logged = False
_telegram_state_lock = threading.Lock()
_telegram_state = {
    "worker_started": False,
    "queue_size": 0,
    "sent_ok": 0,
    "sent_fail": 0,
    "last_enqueued_utc": None,
    "last_attempt_utc": None,
    "last_ok_utc": None,
    "last_fail_utc": None,
    "last_error": None,
}
_news_cache: dict = {}
_news_not_configured_logged = False

# Pares con futuros perpetuos disponibles en Pionex
PARES_FUTURES_VALIDOS = {
    "BTC_USDT", "ETH_USDT", "BNB_USDT", "SOL_USDT", "XRP_USDT",
    "ADA_USDT", "DOGE_USDT", "MATIC_USDT", "DOT_USDT", "AVAX_USDT",
    "LINK_USDT", "UNI_USDT", "ATOM_USDT", "LTC_USDT", "ETC_USDT",
    "BCH_USDT", "APT_USDT", "OP_USDT", "ARB_USDT", "FIL_USDT",
    "NEAR_USDT", "INJ_USDT", "SUI_USDT", "TRX_USDT", "TON_USDT",
    "WLD_USDT", "IMX_USDT", "SAND_USDT", "MANA_USDT", "AAVE_USDT",
    "MKR_USDT", "SNX_USDT", "CRV_USDT", "1INCH_USDT", "ENS_USDT",
    "RUNE_USDT", "THETA_USDT", "FTM_USDT", "ALGO_USDT", "VET_USDT",
    "EGLD_USDT", "XTZ_USDT", "ZEC_USDT", "DASH_USDT", "CHZ_USDT",
    "HOT_USDT", "ZIL_USDT", "ENJ_USDT", "BAT_USDT", "STORJ_USDT",
    "AGLD_USDT", "AEVO_USDT", "AI_USDT", "API3_USDT", "APE_USDT",
    "ALICE_USDT", "AXL_USDT", "ACE_USDT", "ACH_USDT", "AUCTION_USDT",
    "BLUR_USDT", "COMP_USDT", "CYBER_USDT", "DYDX_USDT", "FET_USDT",
    "GALA_USDT", "GMX_USDT", "GRT_USDT", "ICP_USDT", "JTO_USDT",
    "JUP_USDT", "KAVA_USDT", "LDO_USDT", "LINA_USDT", "LUNA_USDT",
    "MAGIC_USDT", "MASK_USDT", "OCEAN_USDT", "PEOPLE_USDT", "PERP_USDT",
    "PYTH_USDT", "RDNT_USDT", "RNDR_USDT", "SEI_USDT", "SHIB_USDT",
    "SKL_USDT", "SSV_USDT", "STMX_USDT", "STX_USDT", "SUSHI_USDT",
    "TIA_USDT", "WAXP_USDT", "YFI_USDT", "ZRX_USDT"
}

# Estado global del sistema
sistema_estado = {
    "activo": False,
    "modo_prueba": True,
    "ciclo_actual": 0,
    "bots_activos": {},
    "pares_activos": set(),
    "ultimo_analisis": None,
    "ultimo_reporte": None,
    "ultimo_heartbeat": None,
    "ultimo_seguimiento": None,
    "logs": [],
    "websocket_clients": set()
}


# ============================================================
# LOGS EN TIEMPO REAL
# ============================================================
def add_log(mensaje: str, tipo: str = "INFO"):
    log = {
        "timestamp": datetime.now().strftime("%H:%M:%S"),
        "tipo": tipo,
        "mensaje": mensaje
    }
    sistema_estado["logs"].append(log)
    if len(sistema_estado["logs"]) > 100:
        sistema_estado["logs"].pop(0)
    print(f"[{log['timestamp']}] [{tipo}] {mensaje}")
    return log


async def broadcast_update(data: dict):
    if sistema_estado["websocket_clients"]:
        import json
        message = json.dumps(data)
        disconnected = set()
        for client in sistema_estado["websocket_clients"]:
            try:
                await client.send_text(message)
            except Exception:
                disconnected.add(client)
        sistema_estado["websocket_clients"] -= disconnected


# ============================================================
# TELEGRAM
# ============================================================
def notify(message: str):
    send_telegram(TELEGRAM_TOKEN, TELEGRAM_CHAT, message)


def send_telegram(token: str, chat_id: str, message: str):
    global _telegram_not_configured_logged
    resolved_token = token or TELEGRAM_TOKEN
    resolved_chat = chat_id or TELEGRAM_CHAT
    if not resolved_token or not resolved_chat:
        if not _telegram_not_configured_logged:
            add_log("Telegram no configurado", "WARNING")
            _telegram_not_configured_logged = True
        return

    if len(message) > 3900:
        message = message[:3900] + "..."

    _ensure_telegram_worker()
    try:
        _telegram_queue.put_nowait((resolved_token, resolved_chat, message))
        with _telegram_state_lock:
            _telegram_state["queue_size"] = _telegram_queue.qsize()
            _telegram_state["last_enqueued_utc"] = datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")
    except queue.Full:
        add_log("Cola de Telegram llena: mensaje descartado", "WARNING")
        with _telegram_state_lock:
            _telegram_state["sent_fail"] += 1
            _telegram_state["last_fail_utc"] = datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")
            _telegram_state["last_error"] = "queue_full"


def get_telegram_status():
    with _telegram_state_lock:
        data = dict(_telegram_state)
    data["queue_size"] = _telegram_queue.qsize()
    return data


def _ensure_telegram_worker():
    global _telegram_worker_started
    if _telegram_worker_started:
        return
    with _telegram_worker_lock:
        if _telegram_worker_started:
            return
        t = threading.Thread(target=_telegram_worker, name="telegram-worker", daemon=True)
        t.start()
        _telegram_worker_started = True
        with _telegram_state_lock:
            _telegram_state["worker_started"] = True


def _telegram_worker():
    while True:
        try:
            token, chat_id, message = _telegram_queue.get()
            try:
                _send_telegram_with_retries(token, chat_id, message)
            finally:
                _telegram_queue.task_done()
                with _telegram_state_lock:
                    _telegram_state["queue_size"] = _telegram_queue.qsize()
        except Exception as e:
            add_log(f"Telegram worker error: {e}", "ERROR")
            time.sleep(1)


def _send_telegram_with_retries(token: str, chat_id: str, message: str):
    url = f"https://api.telegram.org/bot{token}/sendMessage"
    payload = {"chat_id": chat_id, "text": message, "parse_mode": "HTML"}

    last_error = None
    for attempt in range(1, 6):
        try:
            with _telegram_state_lock:
                _telegram_state["last_attempt_utc"] = datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")
            r = requests.post(url, json=payload, timeout=15)
            if r.status_code == 200:
                try:
                    data = r.json()
                except Exception:
                    data = None
                if isinstance(data, dict) and data.get("ok") is False:
                    last_error = f"Telegram ok=false: {str(data)[:200]}"
                else:
                    add_log("Telegram enviado OK", "SUCCESS")
                    with _telegram_state_lock:
                        _telegram_state["sent_ok"] += 1
                        _telegram_state["last_ok_utc"] = datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")
                        _telegram_state["last_error"] = None
                    return

            if r.status_code == 429:
                retry_after = 1.0
                try:
                    data = r.json()
                    retry_after = float(data.get("parameters", {}).get("retry_after", retry_after))
                except Exception:
                    pass
                time.sleep(min(max(retry_after, 1.0), 30.0))
                continue

            last_error = f"HTTP {r.status_code}: {r.text[:200]}"

        except Exception as e:
            last_error = str(e)

        backoff = min(2 ** (attempt - 1), 30)
        jitter = random.uniform(0, 0.5)
        time.sleep(backoff + jitter)

    add_log(f"Telegram fallo tras reintentos: {last_error}", "ERROR")
    with _telegram_state_lock:
        _telegram_state["sent_fail"] += 1
        _telegram_state["last_fail_utc"] = datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")
        _telegram_state["last_error"] = last_error


def _cfg_bool(config: dict, key: str, default: bool) -> bool:
    raw = str(config.get(key, str(default))).strip().lower()
    if raw in {"1", "true", "yes", "y", "on"}:
        return True
    if raw in {"0", "false", "no", "n", "off"}:
        return False
    return default


def _cfg_int(config: dict, key: str, default: int) -> int:
    try:
        return int(float(config.get(key, default)))
    except Exception:
        return default


def _cfg_float(config: dict, key: str, default: float) -> float:
    try:
        return float(config.get(key, default))
    except Exception:
        return default


def _cfg_csv_set(config: dict, key: str, default_csv: str) -> set:
    raw = str(config.get(key, default_csv))
    parts = [p.strip().lower() for p in raw.split(",") if p.strip()]
    return set(parts)


def _cfg_split_list(config: dict, key: str, default_value: str, sep: str = "|") -> list:
    raw = str(config.get(key, default_value))
    return [p.strip() for p in raw.split(sep) if p.strip()]


def _coin_from_symbol(symbol: str) -> str:
    if not symbol:
        return ""
    return symbol.split("_", 1)[0].upper()


def _parse_iso_datetime(value: str):
    if not value:
        return None
    try:
        if value.endswith("Z"):
            value = value[:-1] + "+00:00"
        return datetime.fromisoformat(value)
    except Exception:
        return None


def _parse_rss_datetime(value: str):
    if not value:
        return None
    try:
        dt = parsedate_to_datetime(value)
        if dt.tzinfo is None:
            return dt
        return dt.astimezone(timezone.utc).replace(tzinfo=None)
    except Exception:
        return None


def _coin_aliases(coin: str) -> list:
    aliases = {
        "BTC": ["bitcoin", "btc"],
        "ETH": ["ethereum", "eth"],
        "BNB": ["bnb", "binance coin"],
        "XRP": ["xrp", "ripple"],
        "SOL": ["solana", "sol"],
        "DOGE": ["dogecoin", "doge"],
        "ADA": ["cardano", "ada"],
        "DOT": ["polkadot", "dot"],
        "AVAX": ["avalanche", "avax"],
        "LINK": ["chainlink", "link"],
    }
    base = coin.lower()
    return sorted(set([base] + aliases.get(coin.upper(), [])))


def _fetch_rss_news(coin: str, config: dict):
    lookback_hours = _cfg_int(config, "news_lookback_hours", 12)
    max_items = _cfg_int(config, "news_max_items", 10)
    env_urls = os.getenv("NEWS_RSS_URLS", "").strip()
    default_urls = (
        "https://www.coindesk.com/arc/outboundfeeds/rss/|"
        "https://cointelegraph.com/rss"
    )
    configured_urls = _cfg_split_list(config, "news_rss_urls", env_urls or default_urls, sep="|")
    feed_urls = configured_urls if configured_urls else [u for u in default_urls.split("|") if u]
    aliases = _coin_aliases(coin)
    now = datetime.utcnow()
    headlines = []
    seen = set()

    for feed_url in feed_urls:
        try:
            r = requests.get(feed_url, timeout=12, headers={"User-Agent": "trading-pionex-bot/1.0"})
            if r.status_code != 200:
                continue
            root = ET.fromstring(r.content)
        except Exception:
            continue

        items = root.findall(".//item")
        for item in items:
            title = (item.findtext("title") or "").strip()
            if not title:
                continue

            title_l = title.lower()
            if coin and aliases and not any(alias in title_l for alias in aliases):
                continue

            pub = _parse_rss_datetime((item.findtext("pubDate") or "").strip())
            if pub is not None:
                hours_ago = (now - pub).total_seconds() / 3600
                if hours_ago > lookback_hours:
                    continue

            if title in seen:
                continue
            seen.add(title)
            headlines.append(title)
            if len(headlines) >= max_items:
                return {"ok": True, "headlines": headlines, "source": "rss"}

    return {"ok": True, "headlines": headlines, "source": "rss"}


def _fetch_cryptopanic(coin: str, config: dict):
    global _news_not_configured_logged
    token = CRYPTOPANIC_TOKEN.strip()
    if not token:
        if not _news_not_configured_logged:
            add_log("Noticias habilitadas pero falta CRYPTOPANIC_TOKEN (se omite filtro de noticias)", "WARNING")
            _news_not_configured_logged = True
        return {"ok": False, "headlines": [], "source": "cryptopanic", "error": "missing_token"}

    lookback_hours = _cfg_int(config, "news_lookback_hours", 12)
    max_items = _cfg_int(config, "news_max_items", 10)
    if not coin:
        return {"ok": True, "headlines": [], "source": "cryptopanic"}

    url = "https://cryptopanic.com/api/v1/posts/"
    params = {
        "auth_token": token,
        "currencies": coin,
        "kind": "news",
        "public": "true",
    }

    try:
        r = requests.get(url, params=params, timeout=12)
        if r.status_code != 200:
            return {"ok": False, "headlines": [], "source": "cryptopanic", "error": f"http_{r.status_code}"}
        data = r.json()
        results = data.get("results", []) if isinstance(data, dict) else []
    except Exception as e:
        return {"ok": False, "headlines": [], "source": "cryptopanic", "error": str(e)}

    now = datetime.utcnow()
    headlines = []
    for item in results[: max_items * 2]:
        title = ""
        created_at = None
        try:
            title = str(item.get("title", "")).strip()
            created_at = _parse_iso_datetime(str(item.get("created_at", "")).strip())
        except Exception:
            title = ""
            created_at = None

        if not title:
            continue

        if created_at is not None:
            if created_at.tzinfo is None:
                created_utc = created_at
            else:
                created_utc = created_at.astimezone(timezone.utc).replace(tzinfo=None)
            hours_ago = (now - created_utc).total_seconds() / 3600
            if hours_ago > lookback_hours:
                continue

        headlines.append(title)
        if len(headlines) >= max_items:
            break

    return {"ok": True, "headlines": headlines, "source": "cryptopanic"}


def _news_assess(symbol: str, config: dict):
    enabled = _cfg_bool(config, "news_enabled", True)
    if not enabled:
        return {"enabled": False, "ok": True, "blocked": False, "score": 0, "summary": ""}

    cache_minutes = _cfg_int(config, "news_cache_minutes", 15)
    coin = _coin_from_symbol(symbol)
    cache_key = (coin, _cfg_int(config, "news_lookback_hours", 12), _cfg_int(config, "news_max_items", 10))
    now_ts = time.time()
    cached = _news_cache.get(cache_key)
    if cached and (now_ts - cached["ts"]) < cache_minutes * 60:
        payload = cached["payload"]
    else:
        provider = str(config.get("news_provider", "rss")).strip().lower()
        if provider == "cryptopanic":
            payload = _fetch_cryptopanic(coin, config)
        elif provider in {"rss", "free_rss"}:
            payload = _fetch_rss_news(coin, config)
        else:
            payload = {"ok": False, "headlines": [], "source": provider, "error": "unsupported_provider"}
        _news_cache[cache_key] = {"ts": now_ts, "payload": payload}

    headlines = payload.get("headlines", []) if isinstance(payload, dict) else []
    if not headlines:
        return {"enabled": True, "ok": bool(payload.get("ok")), "blocked": False, "score": 0, "summary": ""}

    block_words = _cfg_csv_set(config, "news_block_keywords", "")
    positive_words = _cfg_csv_set(config, "news_positive_keywords", "")

    blocked = False
    score = 0
    matched_block = set()
    matched_positive = set()

    for h in headlines:
        text = str(h).lower()
        for w in block_words:
            if w and w in text:
                matched_block.add(w)
        for w in positive_words:
            if w and w in text:
                matched_positive.add(w)

    if matched_block:
        blocked = True
        score -= 25
    if matched_positive and not blocked:
        score += 5

    top = headlines[0][:160]
    tags = []
    if matched_block:
        tags.append("riesgo:" + "/".join(sorted(list(matched_block))[:3]))
    if matched_positive:
        tags.append("positivo:" + "/".join(sorted(list(matched_positive))[:3]))

    summary = top
    if tags:
        summary = f"{top} ({' | '.join(tags)})"

    return {"enabled": True, "ok": bool(payload.get("ok")), "blocked": blocked, "score": score, "summary": summary}


def _apply_news_layer(analyzed: list, config: dict) -> list:
    enabled = _cfg_bool(config, "news_enabled", True)
    if not enabled:
        return analyzed

    min_score = _cfg_int(config, "min_score", 75)
    out = []
    for item in analyzed:
        symbol = item.get("symbol")
        news = _news_assess(symbol, config)
        item["news_score"] = news.get("score", 0)
        item["news_summary"] = news.get("summary", "")
        if news.get("summary"):
            razones = item.get("razones", "")
            item["razones"] = (razones + " | " if razones else "") + f"Noticias: {news.get('summary')}"

        new_score = int(item.get("score", 0)) + int(news.get("score", 0))
        item["score"] = new_score

        if news.get("blocked"):
            add_log(f"{symbol}: bloqueado por noticias recientes ({news.get('summary','')})", "WARNING")
            continue
        if new_score < min_score:
            continue

        out.append(item)

    return out


# PIONEX API
def get_signature(params: dict, secret: str) -> str:
    sorted_params = "&".join(f"{k}={v}" for k, v in sorted(params.items()))
    return hmac.new(
        secret.encode("utf-8"),
        sorted_params.encode("utf-8"),
        hashlib.sha256
    ).hexdigest()


def pionex_request(api_key: str, secret: str, method: str, endpoint: str, params=None, body=None):
    if params is None:
        params = {}
    params["timestamp"] = int(time.time() * 1000)
    params["KEY"] = api_key
    params["SIGN"] = get_signature(params, secret)

    url = PIONEX_BASE_URL + endpoint
    headers = {"Content-Type": "application/json"}

    try:
        if method == "GET":
            response = requests.get(url, params=params, headers=headers, timeout=10)
        else:
            response = requests.post(url, params=params, json=body, headers=headers, timeout=10)
        return response.json()
    except Exception as e:
        add_log(f"Error API Pionex: {e}", "ERROR")
        return None


def get_precio_actual(api_key: str, secret: str, symbol: str) -> float:
    response = pionex_request(api_key, secret, "GET", "/api/v1/market/tickers")
    if response and response.get("result"):
        tickers = response.get("data", {}).get("tickers", [])
        for t in tickers:
            if t.get("symbol") == symbol:
                return float(t.get("close", 0))
    return 0.0


def get_precios_actuales(api_key: str, secret: str) -> dict:
    response = pionex_request(api_key, secret, "GET", "/api/v1/market/tickers")
    if not response or not response.get("result"):
        return {}
    tickers = response.get("data", {}).get("tickers", [])
    out = {}
    for t in tickers:
        sym = t.get("symbol")
        if not sym:
            continue
        try:
            out[sym] = float(t.get("close", 0))
        except Exception:
            continue
    return out


def detener_bot(api_key: str, secret: str, bot_id: str) -> dict:
    bot_id = str(bot_id or "").strip()
    if not bot_id:
        return {"ok": False, "error": "missing_bot_id"}

    db = SessionLocal()
    bot = db.query(Bot).filter(Bot.bot_id == bot_id).first()
    if not bot:
        db.close()
        return {"ok": False, "error": "not_found"}

    if bot.estado != "ACTIVO":
        estado = bot.estado
        db.close()
        return {"ok": False, "error": "not_active", "estado": estado}

    symbol = bot.symbol
    modo_prueba = bool(bot.modo_prueba)

    if modo_prueba:
        bot.estado = "CERRADO"
        bot.fecha_cierre = datetime.utcnow()
        db.commit()
        db.close()

        sistema_estado["bots_activos"].pop(bot_id, None)
        try:
            sistema_estado["pares_activos"].discard(symbol)
        except Exception:
            pass

        add_log(f"[PRUEBA] Bot cerrado manualmente: {symbol} | ID: {bot_id}", "WARNING")
        notify(f"🛑 <b>[PRUEBA] Bot cerrado manualmente</b>\n\n📊 Par: <b>{symbol}</b>\n🆔 ID: <b>{bot_id}</b>")
        return {"ok": True, "modo_prueba": True, "bot_id": bot_id, "symbol": symbol}

    db.close()

    endpoints = [
        ("/api/v1/bot/terminate", {"botId": bot_id}),
        ("/api/v1/bot/stop", {"botId": bot_id}),
        ("/api/v1/bot/close", {"botId": bot_id}),
        ("/api/v1/bot/terminate", {"botIdList": [bot_id]}),
        ("/api/v1/bot/stop", {"botIdList": [bot_id]}),
        ("/api/v1/bot/close", {"botIdList": [bot_id]}),
    ]

    last_error = None
    last_resp = None
    ok = False
    used_endpoint = None

    for endpoint, body in endpoints:
        try:
            resp = pionex_request(api_key, secret, "POST", endpoint, body=body)
            last_resp = resp
            if resp and resp.get("result"):
                ok = True
                used_endpoint = endpoint
                break
            last_error = str(resp)
        except Exception as e:
            last_error = str(e)

    if not ok:
        add_log(f"No se pudo cerrar bot {bot_id} en Pionex: {last_error}", "ERROR")
        return {"ok": False, "error": "pionex_stop_failed", "details": last_error, "response": last_resp}

    db = SessionLocal()
    cerrar_bot(db, bot_id, ganancia=0.0, ganancia_pct=0.0, estado="CERRADO")
    db.close()

    sistema_estado["bots_activos"].pop(bot_id, None)
    try:
        sistema_estado["pares_activos"].discard(symbol)
    except Exception:
        pass

    add_log(f"Bot cerrado manualmente: {symbol} | ID: {bot_id}", "WARNING")
    notify(f"🛑 <b>Bot cerrado manualmente</b>\n\n📊 Par: <b>{symbol}</b>\n🆔 ID: <b>{bot_id}</b>")
    return {"ok": True, "modo_prueba": False, "bot_id": bot_id, "symbol": symbol, "endpoint": used_endpoint}


# ============================================================
# CARGAR PARES ACTIVOS DESDE DB AL INICIO
# ============================================================
def cargar_pares_activos_desde_db():
    """Al arrancar el servidor, recupera los pares activos desde la DB
    para evitar que se dupliquen bots al reiniciar Render."""
    try:
        from database import Bot
        db = SessionLocal()
        bots_activos = db.query(Bot).filter(
            Bot.estado == "ACTIVO",
            Bot.modo_prueba == True
        ).all()
        db.close()

        for bot in bots_activos:
            sistema_estado["pares_activos"].add(bot.symbol)
            sistema_estado["bots_activos"][bot.bot_id] = {
                "symbol": bot.symbol,
                "precio_entrada": bot.precio_entrada,
                "score": bot.score,
                "rsi": bot.rsi,
            }

        if bots_activos:
            add_log(f"Recuperados {len(bots_activos)} bots activos desde DB", "INFO")
    except Exception as e:
        add_log(f"Error cargando pares activos: {e}", "ERROR")


# ============================================================
# MODULO 1 - SCANNER
# ============================================================
def scan_pairs(api_key: str, secret: str, config: dict):
    add_log("Escaneando pares del mercado...", "INFO")
    try:
        min_volume = float(config.get("min_volume_24h", 5000000))
    except Exception:
        min_volume = 5000000
    try:
        min_vol = float(config.get("min_volatility", 0.1))
    except Exception:
        min_vol = 0.1
    try:
        max_vol = float(config.get("max_volatility", 20.0))
    except Exception:
        max_vol = 20.0

    response = pionex_request(api_key, secret, "GET", "/api/v1/market/tickers")
    if not response or not response.get("result"):
        add_log("Error obteniendo pares del mercado", "ERROR")
        return []

    tickers = response.get("data", {}).get("tickers", [])

    # Solo pares con futuros perpetuos válidos en Pionex
    usdt_pairs = [t for t in tickers if t.get("symbol", "") in PARES_FUTURES_VALIDOS]
    add_log(f"Total pares futuros válidos encontrados: {len(usdt_pairs)}", "INFO")

    candidates = []
    for ticker in usdt_pairs:
        try:
            symbol = ticker.get("symbol", "")
            if symbol in sistema_estado["pares_activos"]:
                continue

            volume = float(ticker.get("amount", 0))
            close = float(ticker.get("close", 0))
            open_price = float(ticker.get("open", 0))

            if volume < min_volume:
                continue
            if close <= 0 or open_price <= 0:
                continue

            change_pct = abs((close - open_price) / open_price * 100)
            if change_pct < min_vol or change_pct > max_vol:
                continue

            candidates.append({
                "symbol": symbol,
                "volume": volume,
                "price": close,
                "change_pct": round(change_pct, 2)
            })
        except Exception:
            continue

    add_log(f"Candidatos tras filtro inicial: {len(candidates)}", "INFO")
    return candidates


# ============================================================
# MODULO 2 - ANALIZADOR CON ESTRATEGIA PROFESIONAL
# ============================================================
def get_klines(api_key: str, secret: str, symbol: str, interval="60M", limit=100):
    params = {"symbol": symbol, "interval": interval, "limit": limit}
    response = pionex_request(api_key, secret, "GET", "/api/v1/market/klines", params=params)

    if not response or not response.get("result"):
        return None

    klines = response.get("data", {}).get("klines", [])
    if not klines or len(klines) < 20:
        return None

    try:
        df = pd.DataFrame(klines)
        df = df.rename(columns={
            "time": "timestamp", "open": "open", "close": "close",
            "high": "high", "low": "low", "volume": "volume"
        })
        df[["open", "high", "low", "close", "volume"]] = df[
            ["open", "high", "low", "close", "volume"]
        ].astype(float)
        return df
    except Exception as e:
        add_log(f"Error parseando klines de {symbol}: {e}", "ERROR")
        return None


def analyze_pair(api_key: str, secret: str, pair_info: dict, config: dict):
    symbol = pair_info["symbol"]
    df = get_klines(api_key, secret, symbol)
    if df is None or len(df) < 50:
        return None

    try:
        def _cfg_float(key: str, default: float) -> float:
            try:
                return float(config.get(key, default))
            except Exception:
                return default

        def _cfg_int(key: str, default: int) -> int:
            try:
                return int(float(config.get(key, default)))
            except Exception:
                return default

        rsi_min = _cfg_float("rsi_min", 35.0)
        rsi_max = _cfg_float("rsi_max", 65.0)
        min_score = _cfg_int("min_score", 75)
        max_atr_pct = _cfg_float("max_atr_pct", 7.0)
        adx_min = _cfg_float("adx_min", 15.0)
        bb_width_min = _cfg_float("bb_width_min", 2.0)
        bb_width_max = _cfg_float("bb_width_max", 6.0)
        distancia_media_max = _cfg_float("distancia_media_max", 1.5)
        vol_ratio_min = _cfg_float("vol_ratio_min", 1.0)

        rsi_series = RSIIndicator(close=df["close"], window=14).rsi()
        rsi = rsi_series.iloc[-1]
        rsi_prev = rsi_series.iloc[-2]
        rsi_subiendo = rsi > rsi_prev

        macd_obj = MACD(close=df["close"])
        macd_line = macd_obj.macd().iloc[-1]
        signal_line = macd_obj.macd_signal().iloc[-1]
        macd_hist = macd_obj.macd_diff().iloc[-1]
        macd_hist_prev = macd_obj.macd_diff().iloc[-2]
        macd_positivo = macd_hist > 0
        macd_creciendo = macd_hist > macd_hist_prev

        bb = BollingerBands(close=df["close"], window=20)
        bb_high = bb.bollinger_hband().iloc[-1]
        bb_low = bb.bollinger_lband().iloc[-1]
        bb_mid = bb.bollinger_mavg().iloc[-1]
        bb_width = ((bb_high - bb_low) / bb_mid * 100)
        precio_actual = df["close"].iloc[-1]
        distancia_media = abs(precio_actual - bb_mid) / bb_mid * 100

        ema50 = EMAIndicator(close=df["close"], window=50).ema_indicator().iloc[-1]
        ema20 = EMAIndicator(close=df["close"], window=20).ema_indicator().iloc[-1]
        precio_sobre_ema20 = precio_actual > ema20
        precio_sobre_ema50 = precio_actual > ema50
        ema_tendencia = ema20 > ema50

        atr = AverageTrueRange(
            high=df["high"], low=df["low"], close=df["close"], window=14
        ).average_true_range().iloc[-1]
        atr_pct = (atr / precio_actual) * 100

        adx = ADXIndicator(high=df["high"], low=df["low"], close=df["close"], window=14).adx().iloc[-1]

        vol_avg = df["volume"].tail(20).mean()
        vol_current = df["volume"].iloc[-1]
        vol_ratio = vol_current / vol_avg if vol_avg > 0 else 0
        vol_ultimas3 = df["volume"].tail(3).mean()
        vol_creciente = vol_ultimas3 > vol_avg

        score = 0
        razones = []

        if 40 <= rsi <= 55:
            score += 25
            razones.append(f"RSI óptimo ({round(rsi,1)})")
        elif rsi_min <= rsi <= rsi_max:
            score += 15
            razones.append(f"RSI aceptable ({round(rsi,1)})")
        elif (rsi_min - 5) <= rsi <= (rsi_max + 5):
            score += 5

        if rsi_subiendo and rsi < 60:
            score += 15
            razones.append("RSI en alza")

        if macd_positivo and macd_creciendo:
            score += 20
            razones.append("MACD positivo y creciendo")
        elif macd_positivo or macd_creciendo:
            score += 10

        if bb_width_min <= bb_width <= bb_width_max and distancia_media < distancia_media_max:
            score += 20
            razones.append(f"BB ideal ({round(bb_width,1)}%)")
        elif (bb_width_min - 0.5) <= bb_width <= (bb_width_max + 2.0):
            score += 10

        if vol_creciente and vol_ratio >= vol_ratio_min:
            score += 10
            razones.append("Volumen creciente")

        if ema_tendencia and precio_sobre_ema20:
            score += 10
            razones.append("Tendencia EMA (20>50)")

        if precio_sobre_ema20 and precio_sobre_ema50:
            score += 10
            razones.append("Precio sobre EMAs")
        elif precio_sobre_ema20:
            score += 5

        if adx_min > 0 and adx >= adx_min:
            score += 5
            razones.append(f"ADX fuerte ({round(adx,1)})")

        if atr_pct > max_atr_pct:
            add_log(f"{symbol}: ATR muy alto ({round(atr_pct,1)}%) - descartado", "WARNING")
            return None

        if rsi > 75 or rsi < 25:
            add_log(f"{symbol}: RSI extremo ({round(rsi,1)}) - descartado", "WARNING")
            return None

        if bb_width < 1.0:
            add_log(f"{symbol}: BB muy estrecho ({round(bb_width,1)}%) - descartado", "WARNING")
            return None

        add_log(
            f"{symbol}: Score={score} | RSI={round(rsi,1)} | "
            f"MACD={'✓' if macd_positivo else '✗'} | BB={round(bb_width,1)}%",
            "INFO"
        )

        if score < min_score:
            return None

        return {
            **pair_info,
            "rsi": round(rsi, 2),
            "rsi_subiendo": rsi_subiendo,
            "macd": round(macd_line, 6),
            "signal": round(signal_line, 6),
            "macd_hist": round(macd_hist, 6),
            "bb_width": round(bb_width, 2),
            "atr_pct": round(atr_pct, 2),
            "adx": round(float(adx), 2),
            "vol_ratio": round(vol_ratio, 2),
            "score": score,
            "razones": ", ".join(razones)
        }

    except Exception as e:
        add_log(f"Error analizando {symbol}: {e}", "ERROR")
        return None


# ============================================================
# MODULO 3 - SELECTOR INTELIGENTE
# ============================================================
def select_best_pairs(api_key: str, secret: str, candidates: list, config: dict):
    max_bots = int(config.get("max_active_bots", 2))
    add_log(f"Analizando {min(len(candidates), 20)} candidatos con estrategia profesional...", "INFO")

    analyzed = []
    for pair in candidates[:20]:
        result = analyze_pair(api_key, secret, pair, config)
        if result:
            analyzed.append(result)
        time.sleep(0.3)

    if not analyzed:
        add_log("Ningún par pasó el análisis de triple confluencia", "WARNING")
        return []

    analyzed = _apply_news_layer(analyzed, config)
    if not analyzed:
        add_log("Ningún par pasó el filtro de noticias", "WARNING")
        return []

    analyzed.sort(key=lambda x: x["score"], reverse=True)
    best = analyzed[:max_bots]

    for p in best:
        add_log(
            f"✅ Par seleccionado: {p['symbol']} | Score: {p['score']} | "
            f"RSI: {p['rsi']} | {p.get('razones', '')}",
            "SUCCESS"
        )

    return best


# ============================================================
# MODULO 4 - EJECUTOR / SIMULADOR
# ============================================================
def abrir_bot(api_key: str, secret: str, pair_info: dict, config: dict):
    symbol = pair_info["symbol"]
    price = pair_info["price"]
    range_pct = float(config.get("price_range_pct", 5.0))
    investment = float(config.get("investment_usdt", 20))
    leverage = int(config.get("leverage", 2))
    grid_count = int(config.get("grid_count", 60))
    take_profit = float(config.get("take_profit_pct", 1.0))
    stop_loss = float(config.get("stop_loss_pct", 5.0))
    modo_prueba = config.get("modo_prueba", "true") == "true"
    try:
        score_value = int(pair_info.get("score", 0))
    except Exception:
        score_value = 0
    score_display = max(0, min(score_value, 100))

    if symbol in sistema_estado["pares_activos"]:
        add_log(f"Ya existe un bot activo para {symbol}, saltando...", "WARNING")
        return None

    lower = round(price * (1 - range_pct / 100), 6)
    upper = round(price * (1 + range_pct / 100), 6)

    bot_data = {
        "symbol": symbol,
        "precio_entrada": price,
        "lower_price": lower,
        "upper_price": upper,
        "inversion": investment,
        "grillas": grid_count,
        "apalancamiento": leverage,
        "take_profit": take_profit,
        "stop_loss": stop_loss,
        "score": pair_info.get("score", 0),
        "rsi": pair_info.get("rsi", 0),
        "bb_width": pair_info.get("bb_width", 0),
        "modo_prueba": modo_prueba
    }

    if modo_prueba:
        bot_id = f"SIM_{symbol}_{int(time.time())}"
        bot_data["bot_id"] = bot_id
        bot_data["estado"] = "ACTIVO"

        db = SessionLocal()
        guardar_bot(db, bot_data)

        guardar_seguimiento(db, {
            "bot_id": bot_id,
            "symbol": symbol,
            "precio_entrada": price,
            "lower_price": lower,
            "upper_price": upper,
            "score": pair_info.get("score", 0),
            "rsi": pair_info.get("rsi", 0),
            "bb_width": pair_info.get("bb_width", 0),
            "precio_actual": price,
            "max_precio": price,
            "min_precio": price,
            "resultado": "PENDIENTE"
        })
        db.close()

        sistema_estado["bots_activos"][bot_id] = bot_data
        sistema_estado["pares_activos"].add(symbol)

        add_log(f"[PRUEBA] Bot simulado: {symbol} | ${price} | Score: {score_value}", "SUCCESS")

        notify(
            f"🧪 <b>[PRUEBA] Bot simulado — Estrategia Pro</b>\n\n"
            f"📊 Par: <b>{symbol}</b>\n"
            f"💵 Precio entrada: <b>${price}</b>\n"
            f"📈 Rango: <b>${lower} — ${upper}</b>\n"
            f"🔢 Grillas: <b>{grid_count}</b>\n"
            f"⚡ Apalancamiento: <b>{leverage}x</b>\n"
            f"✅ Take Profit: <b>{take_profit}%</b>\n"
            f"🛑 Stop Loss: <b>{stop_loss}%</b>\n"
            f"📊 Score: <b>{score_display}/100</b>\n"
            f"📉 RSI: <b>{pair_info.get('rsi', 0)}</b>\n"
            f"📏 BB Width: <b>{pair_info.get('bb_width', 0)}%</b>\n"
            f"⚡ ATR: <b>{pair_info.get('atr_pct', 0)}%</b>\n"
            f"🗞 Noticias: <b>{pair_info.get('news_summary', '—')}</b>\n"
            f"🎯 Razones: <b>{pair_info.get('razones', '')}</b>\n\n"
            f"👁 Seguimiento automático activado\n"
            f"⚠️ <i>Simulación - no es dinero real</i>"
        )
        return bot_id

    else:
        body = {
            "symbol": symbol,
            "type": "NEUTRAL_FUTURES_GRID",
            "leverageLevel": leverage,
            "lowerPrice": str(lower),
            "upperPrice": str(upper),
            "gridCount": grid_count,
            "amount": str(investment),
            "takeProfitRatio": str(take_profit / 100),
            "stopLossRatio": str(stop_loss / 100),
            "stopLossDelay": 5
        }

        response = pionex_request(api_key, secret, "POST", "/api/v1/bot/create", body=body)

        if response and response.get("result"):
            bot_id = response.get("data", {}).get("botId", f"BOT_{int(time.time())}")
            bot_data["bot_id"] = bot_id
            bot_data["estado"] = "ACTIVO"

            db = SessionLocal()
            guardar_bot(db, bot_data)
            db.close()

            sistema_estado["bots_activos"][bot_id] = bot_data
            sistema_estado["pares_activos"].add(symbol)

            add_log(f"Bot abierto: {symbol} | ID: {bot_id}", "SUCCESS")

            notify(
                f"🤖 <b>Bot abierto — Estrategia Pro</b>\n\n"
                f"📊 Par: <b>{symbol}</b>\n"
                f"💵 Precio: <b>${price}</b>\n"
                f"📈 Rango: <b>${lower} — ${upper}</b>\n"
                f"📊 Score: <b>{score_display}/100</b>\n"
                f"📉 RSI: <b>{pair_info.get('rsi', 0)}</b>\n"
                f"🗞 Noticias: <b>{pair_info.get('news_summary', '—')}</b>\n"
                f"🎯 Razones: <b>{pair_info.get('razones', '')}</b>"
            )
            return bot_id
        else:
            add_log(f"Error abriendo bot en {symbol}", "ERROR")
            return None


# ============================================================
# MODULO 5 - MONITOR Y REINVERSION
# ============================================================
def monitor_bots(api_key: str, secret: str, config: dict):
    add_log("Monitoreando bots activos...", "INFO")
    max_bots = int(config.get("max_active_bots", 2))
    modo_prueba = config.get("modo_prueba", "true") == "true"

    if modo_prueba:
        bots_activos = len(sistema_estado["bots_activos"])
        add_log(f"[PRUEBA] Bots activos: {bots_activos}/{max_bots}", "INFO")

        if bots_activos < max_bots:
            add_log("Iniciando nuevo ciclo de análisis...", "INFO")
            ciclo = run_cycle(api_key, secret, config)
            return {"tipo": "monitor_prueba", "bots_activos": bots_activos, "max_bots": max_bots, "accion": "run_cycle", "ciclo": ciclo}
        return {"tipo": "monitor_prueba", "bots_activos": bots_activos, "max_bots": max_bots, "accion": "sin_accion"}

    bots_pionex_response = pionex_request(api_key, secret, "GET", "/api/v1/bot/list",
                                          params={"status": "RUNNING"})
    if not bots_pionex_response or not bots_pionex_response.get("result"):
        add_log("Error obteniendo bots de Pionex", "ERROR")
        return {"tipo": "monitor_real", "accion": "error_listado"}

    bots_corriendo = bots_pionex_response.get("data", {}).get("bots", [])
    pares_corriendo = {b.get("symbol") for b in bots_corriendo}
    sistema_estado["pares_activos"] = pares_corriendo
    add_log(f"Bots activos en Pionex: {len(bots_corriendo)}/{max_bots}", "INFO")

    if len(bots_corriendo) < max_bots:
        balance_response = pionex_request(api_key, secret, "GET", "/api/v1/account/balances")
        balance = 0
        if balance_response and balance_response.get("result"):
            balances = balance_response.get("data", {}).get("balances", [])
            for b in balances:
                if b.get("coin") == "USDT":
                    balance = float(b.get("free", 0))

        investment = float(config.get("investment_usdt", 20))
        add_log(f"Balance disponible: ${balance:.2f}", "INFO")

        if balance >= investment:
            add_log("Reinvirtiendo automáticamente...", "SUCCESS")
            ciclo = run_cycle(api_key, secret, config)
            return {"tipo": "monitor_real", "bots_activos": len(bots_corriendo), "max_bots": max_bots, "accion": "reinvertir", "ciclo": ciclo}
        else:
            add_log(f"Balance insuficiente: ${balance:.2f} < ${investment:.2f}", "WARNING")
            return {"tipo": "monitor_real", "bots_activos": len(bots_corriendo), "max_bots": max_bots, "accion": "balance_insuficiente", "balance": balance, "investment": investment}
    return {"tipo": "monitor_real", "bots_activos": len(bots_corriendo), "max_bots": max_bots, "accion": "sin_accion"}


# ============================================================
# MODULO 6 - SEGUIMIENTO AUTOMATICO
# ============================================================
def seguimiento_automatico(api_key: str, secret: str):
    add_log("Ejecutando seguimiento automático...", "INFO")

    db = SessionLocal()
    config = get_configuracion(db)
    pendientes = get_seguimientos_pendientes(db)
    db.close()

    if not pendientes:
        add_log("Sin seguimientos pendientes", "INFO")
        mensaje = "👁 <b>Seguimiento</b>\n\nSin seguimientos pendientes."
        notify(mensaje)
        sistema_estado["ultimo_seguimiento"] = {
            "utc": datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S"),
            "pendientes": 0,
            "mensaje": mensaje
        }
        return {"pendientes": 0}

    add_log(f"Seguimientos pendientes: {len(pendientes)}", "INFO")
    prices = get_precios_actuales(api_key, secret)
    top_n = _cfg_int(config, "telegram_followup_top_n", 5)
    summary_lines = []
    resultados = {"GANADO": 0, "STOP_LOSS": 0, "EXPIRADO": 0, "PENDIENTE": 0, "SIN_PRECIO": 0}

    for seg in pendientes:
        try:
            precio_actual = float(prices.get(seg.symbol, 0))
            if precio_actual <= 0:
                resultados["SIN_PRECIO"] += 1
                continue

            horas = int((datetime.utcnow() - seg.fecha_apertura).total_seconds() / 3600)
            max_precio = max(seg.max_precio, precio_actual)
            min_precio = min(seg.min_precio, precio_actual)
            dentro_rango = seg.lower_price <= precio_actual <= seg.upper_price

            ganancia_potencial = ((max_precio - seg.precio_entrada) / seg.precio_entrada * 100)
            hubiera_ganado = ganancia_potencial >= 1.0 and dentro_rango

            caida = ((seg.precio_entrada - min_precio) / seg.precio_entrada * 100)
            hubiera_stop_loss = caida >= 5.0 or precio_actual < seg.lower_price * 0.95

            resultado = "PENDIENTE"
            if hubiera_ganado:
                resultado = "GANADO"
            elif hubiera_stop_loss:
                resultado = "STOP_LOSS"
            elif horas >= 72:
                resultado = "EXPIRADO"
            resultados[resultado] = resultados.get(resultado, 0) + 1

            db = SessionLocal()
            actualizar_seguimiento(db, seg.id, {
                "precio_actual": precio_actual,
                "max_precio": max_precio,
                "min_precio": min_precio,
                "dentro_rango": dentro_rango,
                "hubiera_ganado": hubiera_ganado,
                "hubiera_stop_loss": hubiera_stop_loss,
                "horas_seguimiento": horas,
                "resultado": resultado,
                "fecha_resultado": datetime.utcnow() if resultado != "PENDIENTE" else None
            })
            db.close()

            if resultado != "PENDIENTE":
                emoji = "✅" if resultado == "GANADO" else "❌" if resultado == "STOP_LOSS" else "⏰"
                add_log(
                    f"Seguimiento {seg.symbol}: {resultado} | "
                    f"Entrada: ${seg.precio_entrada} | Actual: ${precio_actual}",
                    "SUCCESS" if resultado == "GANADO" else "WARNING"
                )
                notify(
                    f"{emoji} <b>Resultado seguimiento</b>\n\n"
                    f"📊 Par: <b>{seg.symbol}</b>\n"
                    f"💵 Precio entrada: <b>${seg.precio_entrada}</b>\n"
                    f"💵 Precio actual: <b>${precio_actual}</b>\n"
                    f"📈 Máximo alcanzado: <b>${max_precio}</b>\n"
                    f"📉 Mínimo alcanzado: <b>${min_precio}</b>\n"
                    f"⏱ Horas seguimiento: <b>{horas}h</b>\n"
                    f"🏆 Resultado: <b>{resultado}</b>\n"
                    f"📊 Score original: <b>{seg.score}/100</b>"
                )
            else:
                if len(summary_lines) < top_n:
                    try:
                        delta_pct = ((precio_actual - seg.precio_entrada) / seg.precio_entrada) * 100
                    except Exception:
                        delta_pct = 0.0
                    mark = "✓" if dentro_rango else "✗"
                    summary_lines.append(f"• {seg.symbol}: ${precio_actual:.6g} ({delta_pct:+.2f}%) rango {mark} | {horas}h")

            time.sleep(0.5)

        except Exception as e:
            add_log(f"Error en seguimiento de {seg.symbol}: {e}", "ERROR")

    enabled_followup = _cfg_bool(config, "telegram_followup_enabled", True)

    if enabled_followup:
        lines = "\n".join(summary_lines) if summary_lines else "Sin detalle disponible."
        mensaje = (
            "👁 <b>Seguimiento (estado)</b>\n\n"
            f"Pendientes: <b>{len(pendientes)}</b>\n"
            f"✅ Ganado: <b>{resultados.get('GANADO', 0)}</b> | ❌ SL: <b>{resultados.get('STOP_LOSS', 0)}</b> | ⏰ Exp: <b>{resultados.get('EXPIRADO', 0)}</b>\n\n"
            f"{lines}"
        )
        notify(mensaje)
        sistema_estado["ultimo_seguimiento"] = {
            "utc": datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S"),
            "pendientes": len(pendientes),
            "resultados": resultados,
            "mensaje": mensaje
        }

    return {"pendientes": len(pendientes), "resultados": resultados}

# ============================================================
# REPORTE DIARIO
# ============================================================
def reporte_diario():
    add_log("Generando reporte diario...", "INFO")

    db = SessionLocal()
    reporte = get_reporte_seguimiento(db)
    stats = get_estadisticas(db)
    db.close()

    total = reporte["total_simulados"]
    ganados = reporte["hubieran_ganado"]
    stop_loss = reporte["hubieran_stop_loss"]
    tasa = reporte["tasa_exito"]
    pendientes = reporte["pendientes"]

    if total == 0:
        notify(
            f"📊 <b>Reporte diario</b>\n\n"
            f"Sin operaciones completadas aún.\n"
            f"Pendientes de seguimiento: <b>{pendientes}</b>"
        )
        return

    evaluacion = "🟢 Rentable" if tasa >= 60 else "🟡 Neutral" if tasa >= 40 else "🔴 Necesita ajustes"

    notify(
        f"📊 <b>Reporte Diario — Sistema de Trading</b>\n\n"
        f"━━━━━━━━━━━━━━━━━━━━\n"
        f"🤖 Total simulados: <b>{total}</b>\n"
        f"✅ Hubieran ganado: <b>{ganados}</b>\n"
        f"❌ Hubieran Stop Loss: <b>{stop_loss}</b>\n"
        f"⏳ Pendientes: <b>{pendientes}</b>\n"
        f"━━━━━━━━━━━━━━━━━━━━\n"
        f"🎯 Tasa de éxito: <b>{tasa}%</b>\n"
        f"📈 Evaluación: <b>{evaluacion}</b>\n"
        f"━━━━━━━━━━━━━━━━━━━━\n"
        f"💰 Ganancia real acumulada: <b>${stats['ganancia_real']}</b>\n"
        f"🔄 Ciclos completados: <b>{stats['total_ciclos']}</b>\n\n"
        f"{'✅ El sistema está listo para modo real.' if tasa >= 60 else '⚠️ Continuar en modo prueba.'}"
    )

    sistema_estado["ultimo_reporte"] = datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")


# ============================================================
# CICLO PRINCIPAL
# ============================================================
def run_cycle(api_key: str, secret: str, config: dict):
    sistema_estado["ciclo_actual"] += 1
    ciclo_num = sistema_estado["ciclo_actual"]
    modo_prueba = config.get("modo_prueba", "true") == "true"

    add_log(f"=== CICLO #{ciclo_num} {'[PRUEBA]' if modo_prueba else '[REAL]'} ===", "INFO")
    sistema_estado["ultimo_analisis"] = datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")

    candidates = scan_pairs(api_key, secret, config)
    if not candidates:
        add_log("No se encontraron candidatos", "WARNING")
        return {"ciclo": ciclo_num, "modo_prueba": modo_prueba, "candidatos": 0, "seleccionados": 0, "bots_abiertos": 0, "resultado": "sin_candidatos"}

    best_pairs = select_best_pairs(api_key, secret, candidates, config)
    if not best_pairs:
        add_log("Ningún par pasó el análisis de triple confluencia", "WARNING")
        notify("⚠️ <b>Ningún par pasó la estrategia profesional</b>\nEl sistema seguirá monitoreando.")
        return {"ciclo": ciclo_num, "modo_prueba": modo_prueba, "candidatos": len(candidates), "seleccionados": 0, "bots_abiertos": 0, "resultado": "sin_seleccion"}

    bots_abiertos = 0
    for pair in best_pairs:
        bot_id = abrir_bot(api_key, secret, pair, config)
        if bot_id:
            bots_abiertos += 1
        time.sleep(1)

    db = SessionLocal()
    guardar_ciclo(db, {
        "numero": ciclo_num,
        "pares_escaneados": len(candidates),
        "pares_seleccionados": len(best_pairs),
        "bots_abiertos": bots_abiertos,
        "modo_prueba": modo_prueba
    })
    db.close()

    add_log(f"Ciclo #{ciclo_num} completado. Bots abiertos: {bots_abiertos}", "SUCCESS")
    return {"ciclo": ciclo_num, "modo_prueba": modo_prueba, "candidatos": len(candidates), "seleccionados": len(best_pairs), "bots_abiertos": bots_abiertos, "resultado": "ok"}


# ============================================================
# LOOP PRINCIPAL ASINCRONO
# ============================================================
async def trading_loop(api_key: str, secret: str, telegram_token: str, telegram_chat: str):
    add_log("Sistema de trading iniciado con Estrategia Profesional", "SUCCESS")

    # Cargar pares activos desde DB para evitar duplicados al reiniciar
    cargar_pares_activos_desde_db()

    notify(
        f"🚀 <b>Sistema iniciado — Estrategia Profesional</b>\n\n"
        f"🎯 <b>Triple Confluencia:</b> RSI + MACD + Bollinger Bands\n"
        f"📊 Score mínimo: <b>70/100</b>\n"
        f"👁 Seguimiento automático: <b>cada 4 horas</b>\n"
        f"📋 Reporte diario: <b>cada 24 horas</b>\n"
        f"⚙️ Modo: <b>{'PRUEBA' if sistema_estado['modo_prueba'] else 'REAL'}</b>\n"
        f"⏰ Análisis: <b>cada 30 minutos</b>\n"
        f"🔒 Pares con futuros válidos: <b>{len(PARES_FUTURES_VALIDOS)}</b>"
    )

    ciclo_counter = 0
    seguimiento_counter = 0
    reporte_counter = 0
    last_heartbeat_ts = 0.0
    last_error_notify_ts = 0.0

    while sistema_estado["activo"]:
        try:
            db = SessionLocal()
            config = get_configuracion(db)
            db.close()

            step_result = None
            if ciclo_counter % 2 == 0:
                step_result = await asyncio.to_thread(run_cycle, api_key, secret, config)
            else:
                step_result = await asyncio.to_thread(monitor_bots, api_key, secret, config)

            seguimiento_counter += 1
            if seguimiento_counter >= 8:
                await asyncio.to_thread(seguimiento_automatico, api_key, secret)
                seguimiento_counter = 0

            reporte_counter += 1
            if reporte_counter >= 48:
                await asyncio.to_thread(reporte_diario)
                reporte_counter = 0

            db = SessionLocal()
            stats = get_estadisticas(db)
            pendientes = 0
            try:
                pendientes = len(get_seguimientos_pendientes(db))
            except Exception:
                pendientes = 0
            db.close()

            heartbeat_enabled = _cfg_bool(config, "telegram_heartbeat_enabled", True)
            heartbeat_minutes = _cfg_int(config, "telegram_heartbeat_minutes", 30)
            interval_s = max(60, heartbeat_minutes * 60)
            now_ts = time.time()
            if heartbeat_enabled and (now_ts - last_heartbeat_ts) >= interval_s - 2:
                modo = "PRUEBA" if config.get("modo_prueba", "true") == "true" else "REAL"
                extra = ""
                if isinstance(step_result, dict):
                    if "resultado" in step_result:
                        extra = f"\nResultado: <b>{step_result.get('resultado')}</b>"
                    elif "accion" in step_result:
                        extra = f"\nAcción: <b>{step_result.get('accion')}</b>"
                mensaje = (
                    f"⏱ <b>Update {heartbeat_minutes}m</b>\n\n"
                    f"Modo: <b>{modo}</b>\n"
                    f"Ciclo: <b>#{sistema_estado['ciclo_actual']}</b>\n"
                    f"Bots activos: <b>{len(sistema_estado['bots_activos'])}</b>\n"
                    f"Pendientes seguimiento: <b>{pendientes}</b>\n"
                    f"Ganancia total: <b>${stats.get('ganancia_total', 0)}</b>\n"
                    f"Último análisis: <b>{sistema_estado.get('ultimo_analisis') or '--'}</b>"
                    f"{extra}"
                )
                notify(mensaje)
                sistema_estado["ultimo_heartbeat"] = {
                    "utc": datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S"),
                    "minutos": heartbeat_minutes,
                    "ciclo": sistema_estado.get("ciclo_actual", 0),
                    "bots_activos": len(sistema_estado.get("bots_activos", {})),
                    "pendientes_seguimiento": pendientes,
                    "ganancia_total": stats.get("ganancia_total", 0),
                    "mensaje": mensaje
                }
                last_heartbeat_ts = now_ts

            await broadcast_update({
                "type": "stats_update",
                "data": stats,
                "logs": sistema_estado["logs"][-10:],
                "ultimo_analisis": sistema_estado["ultimo_analisis"],
                "bots_activos": len(sistema_estado["bots_activos"]),
                "telegram": get_telegram_status(),
                "ultimo_heartbeat": sistema_estado.get("ultimo_heartbeat"),
                "ultimo_seguimiento": sistema_estado.get("ultimo_seguimiento")
            })

            ciclo_counter += 1
            await asyncio.sleep(1800)

        except Exception as e:
            add_log(f"Error en trading loop: {e}", "ERROR")
            now_ts = time.time()
            if now_ts - last_error_notify_ts > 1800:
                notify(f"⚠️ <b>Error en trading loop</b>\n\n{str(e)[:300]}")
                last_error_notify_ts = now_ts
            await asyncio.sleep(60)

    add_log("Sistema de trading detenido", "WARNING")
    notify("🛑 <b>Sistema de Trading detenido</b>")
