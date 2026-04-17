import requests
import pandas as pd
import time
import hmac
import hashlib
import asyncio
import os
from datetime import datetime
from ta.momentum import RSIIndicator
from ta.trend import MACD
from ta.volatility import BollingerBands
from database import (
    SessionLocal, guardar_bot, cerrar_bot,
    guardar_ciclo, get_configuracion, get_estadisticas
)

PIONEX_BASE_URL = "https://api.pionex.com"
TELEGRAM_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN", "")
TELEGRAM_CHAT = os.getenv("TELEGRAM_CHAT_ID", "")

# Estado global del sistema
sistema_estado = {
    "activo": False,
    "modo_prueba": True,
    "ciclo_actual": 0,
    "bots_activos": {},
    "pares_activos": set(),  # FIX: evitar duplicados
    "ultimo_analisis": None,
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
    if not TELEGRAM_TOKEN or not TELEGRAM_CHAT:
        add_log("Telegram no configurado", "WARNING")
        return
    url = f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage"
    try:
        r = requests.post(url, json={
            "chat_id": TELEGRAM_CHAT,
            "text": message,
            "parse_mode": "HTML"
        }, timeout=10)
        if r.status_code == 200:
            add_log("Telegram enviado OK", "SUCCESS")
        else:
            add_log(f"Telegram error: {r.text[:100]}", "ERROR")
    except Exception as e:
        add_log(f"Error Telegram: {e}", "ERROR")


def send_telegram(token: str, chat_id: str, message: str):
    notify(message)


# ============================================================
# PIONEX API
# ============================================================
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


# ============================================================
# MODULO 1 - SCANNER
# ============================================================
def scan_pairs(api_key: str, secret: str, config: dict):
    add_log("Escaneando pares del mercado...", "INFO")
    min_volume = 500000
    min_vol = 0.1
    max_vol = 20.0

    response = pionex_request(api_key, secret, "GET", "/api/v1/market/tickers")
    if not response or not response.get("result"):
        add_log("Error obteniendo pares del mercado", "ERROR")
        return []

    tickers = response.get("data", {}).get("tickers", [])
    usdt_pairs = [t for t in tickers if t.get("symbol", "").endswith("_USDT")]
    add_log(f"Total pares USDT encontrados: {len(usdt_pairs)}", "INFO")

    candidates = []
    for ticker in usdt_pairs:
        try:
            symbol = ticker.get("symbol", "")

            # FIX: saltar pares que ya tienen bot activo
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
# MODULO 2 - ANALIZADOR
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
            "time": "timestamp",
            "open": "open",
            "close": "close",
            "high": "high",
            "low": "low",
            "volume": "volume"
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
        rsi = RSIIndicator(close=df["close"], window=14).rsi().iloc[-1]
        macd_obj = MACD(close=df["close"])
        macd_line = macd_obj.macd().iloc[-1]
        signal_line = macd_obj.macd_signal().iloc[-1]
        bb = BollingerBands(close=df["close"], window=20)
        bb_width = ((bb.bollinger_hband().iloc[-1] - bb.bollinger_lband().iloc[-1])
                    / bb.bollinger_mavg().iloc[-1] * 100)

        vol_avg = df["volume"].tail(20).mean()
        vol_current = df["volume"].iloc[-1]
        vol_ratio = vol_current / vol_avg if vol_avg > 0 else 0

        # FIX: Score real basado en indicadores
        score = 0

        # RSI neutral es mejor para grilla neutral (entre 40-60 es ideal)
        if 40 <= rsi <= 60:
            score += 40
        elif 30 <= rsi <= 70:
            score += 20

        # Bollinger Bands ancho medio = buena volatilidad para grilla
        if 2.0 <= bb_width <= 8.0:
            score += 30
        elif 1.0 <= bb_width <= 12.0:
            score += 15

        # Volumen estable
        if 0.7 <= vol_ratio <= 1.5:
            score += 20

        # Volumen alto = más liquidez
        if pair_info["volume"] > 1_000_000:
            score += 10

        add_log(f"{symbol}: RSI={round(rsi,1)} BB={round(bb_width,1)}% Score={score}", "INFO")

        # Solo retorna si tiene score mínimo de 30
        if score < 30:
            return None

        return {
            **pair_info,
            "rsi": round(rsi, 2),
            "macd": round(macd_line, 6),
            "signal": round(signal_line, 6),
            "bb_width": round(bb_width, 2),
            "vol_ratio": round(vol_ratio, 2),
            "score": score
        }
    except Exception as e:
        add_log(f"Error analizando {symbol}: {e}", "ERROR")
        return None


# ============================================================
# MODULO 3 - SELECTOR INTELIGENTE
# ============================================================
def select_best_pairs(api_key: str, secret: str, candidates: list, config: dict):
    max_bots = int(config.get("max_active_bots", 2))
    add_log(f"Analizando {min(len(candidates), 20)} candidatos con indicadores técnicos...", "INFO")

    analyzed = []
    for pair in candidates[:20]:
        result = analyze_pair(api_key, secret, pair, config)
        if result:
            analyzed.append(result)
        time.sleep(0.3)

    if not analyzed:
        add_log("Ningún par pasó el análisis técnico", "WARNING")
        return []

    analyzed.sort(key=lambda x: x["score"], reverse=True)
    best = analyzed[:max_bots]

    for p in best:
        add_log(f"Par seleccionado: {p['symbol']} | Score: {p['score']} | RSI: {p['rsi']}", "SUCCESS")

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

    # FIX: verificar duplicados
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
        db.close()

        sistema_estado["bots_activos"][bot_id] = bot_data
        sistema_estado["pares_activos"].add(symbol)  # FIX: registrar par activo

        add_log(f"[PRUEBA] Bot simulado: {symbol} | ${price} | Score: {pair_info.get('score', 0)}", "SUCCESS")

        notify(
            f"🧪 <b>[PRUEBA] Bot simulado</b>\n\n"
            f"📊 Par: <b>{symbol}</b>\n"
            f"💵 Precio: <b>${price}</b>\n"
            f"📈 Rango: <b>${lower} — ${upper}</b>\n"
            f"🔢 Grillas: <b>{grid_count}</b>\n"
            f"⚡ Apalancamiento: <b>{leverage}x</b>\n"
            f"✅ Take Profit: <b>{take_profit}%</b>\n"
            f"🛑 Stop Loss: <b>{stop_loss}%</b>\n"
            f"📊 Score: <b>{pair_info.get('score', 0)}/100</b>\n"
            f"📉 RSI: <b>{pair_info.get('rsi', 0)}</b>\n"
            f"📏 BB Width: <b>{pair_info.get('bb_width', 0)}%</b>\n\n"
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
            sistema_estado["pares_activos"].add(symbol)  # FIX: registrar par activo

            add_log(f"Bot abierto: {symbol} | ID: {bot_id}", "SUCCESS")

            notify(
                f"🤖 <b>Bot abierto</b>\n\n"
                f"📊 Par: <b>{symbol}</b>\n"
                f"💵 Precio: <b>${price}</b>\n"
                f"📈 Rango: <b>${lower} — ${upper}</b>\n"
                f"📊 Score: <b>{pair_info.get('score', 0)}/100</b>\n"
                f"📉 RSI: <b>{pair_info.get('rsi', 0)}</b>"
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
            run_cycle(api_key, secret, config)
        return

    bots_pionex_response = pionex_request(api_key, secret, "GET", "/api/v1/bot/list",
                                          params={"status": "RUNNING"})
    if not bots_pionex_response or not bots_pionex_response.get("result"):
        add_log("Error obteniendo bots de Pionex", "ERROR")
        return

    bots_corriendo = bots_pionex_response.get("data", {}).get("bots", [])
    pares_corriendo = {b.get("symbol") for b in bots_corriendo}
    sistema_estado["pares_activos"] = pares_corriendo  # FIX: sincronizar pares activos
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
            run_cycle(api_key, secret, config)
        else:
            add_log(f"Balance insuficiente: ${balance:.2f} < ${investment:.2f}", "WARNING")


# ============================================================
# CICLO PRINCIPAL
# ============================================================
def run_cycle(api_key: str, secret: str, config: dict):
    sistema_estado["ciclo_actual"] += 1
    ciclo_num = sistema_estado["ciclo_actual"]
    modo_prueba = config.get("modo_prueba", "true") == "true"

    add_log(f"=== CICLO #{ciclo_num} {'[PRUEBA]' if modo_prueba else '[REAL]'} ===", "INFO")
    sistema_estado["ultimo_analisis"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    candidates = scan_pairs(api_key, secret, config)
    if not candidates:
        add_log("No se encontraron candidatos", "WARNING")
        return

    best_pairs = select_best_pairs(api_key, secret, candidates, config)
    if not best_pairs:
        add_log("Ningún par pasó el análisis técnico", "WARNING")
        notify("⚠️ <b>Ningún par pasó el análisis técnico</b>\nEl sistema seguirá monitoreando.")
        return

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


# ============================================================
# LOOP PRINCIPAL ASINCRONO
# ============================================================
async def trading_loop(api_key: str, secret: str, telegram_token: str, telegram_chat: str):
    add_log("Sistema de trading iniciado", "SUCCESS")
    notify(
        f"🚀 <b>Sistema de Trading iniciado</b>\n\n"
        f"⚙️ Modo: <b>{'PRUEBA' if sistema_estado['modo_prueba'] else 'REAL'}</b>\n"
        f"⏰ Análisis cada 30 minutos"
    )
    ciclo_counter = 0

    while sistema_estado["activo"]:
        try:
            db = SessionLocal()
            config = get_configuracion(db)
            db.close()

            if ciclo_counter % 2 == 0:
                run_cycle(api_key, secret, config)
            else:
                monitor_bots(api_key, secret, config)

            db = SessionLocal()
            stats = get_estadisticas(db)
            db.close()

            await broadcast_update({
                "type": "stats_update",
                "data": stats,
                "logs": sistema_estado["logs"][-10:],
                "ultimo_analisis": sistema_estado["ultimo_analisis"],
                "bots_activos": len(sistema_estado["bots_activos"])
            })

            ciclo_counter += 1
            await asyncio.sleep(1800)

        except Exception as e:
            add_log(f"Error en trading loop: {e}", "ERROR")
            await asyncio.sleep(60)

    add_log("Sistema de trading detenido", "WARNING")
    notify("🛑 <b>Sistema de Trading detenido</b>")
