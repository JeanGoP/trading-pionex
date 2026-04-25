import asyncio
import json
import os
from datetime import datetime
from typing import Optional
from fastapi import FastAPI, WebSocket, WebSocketDisconnect, Depends, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
from sqlalchemy.orm import Session
from dotenv import load_dotenv

from database import (
    init_db, get_db, get_configuracion, actualizar_configuracion,
    get_estadisticas, SessionLocal, Bot, Ciclo, Ganancia
)
from bot import sistema_estado, trading_loop, add_log, run_cycle, monitor_bots, send_telegram, get_telegram_status, detener_bot

load_dotenv()

# ============================================================
# CONFIGURACION DE LA APP
# ============================================================
app = FastAPI(
    title="Trading Pionex API",
    description="Sistema de trading automatizado para Pionex",
    version="2.0.0"
)

cors_origins_raw = os.getenv("CORS_ALLOW_ORIGINS", "*").strip()
cors_allow_all = cors_origins_raw == "*"
cors_origins = (
    ["*"]
    if cors_allow_all
    else [o.strip() for o in cors_origins_raw.split(",") if o.strip()]
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=cors_origins,
    allow_credentials=not cors_allow_all,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Variables de entorno
PIONEX_API_KEY = os.getenv("PIONEX_API_KEY", "")
PIONEX_SECRET = os.getenv("PIONEX_SECRET", "")
TELEGRAM_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN", "")
TELEGRAM_CHAT = os.getenv("TELEGRAM_CHAT_ID", "")

trading_task = None


# ============================================================
# MODELOS PYDANTIC
# ============================================================
class ConfigUpdate(BaseModel):
    clave: str
    valor: str


class SistemaAction(BaseModel):
    accion: str


class TelegramTest(BaseModel):
    mensaje: str = "Prueba: mensaje desde Trading Pionex API"


class BotStopRequest(BaseModel):
    bot_id: str


# ============================================================
# EVENTOS DE INICIO
# ============================================================
@app.on_event("startup")
async def startup_event():
    global trading_task
    init_db()
    add_log("Servidor iniciado correctamente", "SUCCESS")

    if PIONEX_API_KEY and PIONEX_SECRET:
        sistema_estado["activo"] = True
        trading_task = asyncio.create_task(
            trading_loop(PIONEX_API_KEY, PIONEX_SECRET, TELEGRAM_TOKEN, TELEGRAM_CHAT)
        )
        add_log("Sistema iniciado automáticamente", "SUCCESS")
    else:
        add_log("API Key no configurada - inicia manualmente", "WARNING")


# ============================================================
# WEBSOCKET - TIEMPO REAL
# ============================================================
@app.websocket("/ws")
async def websocket_endpoint(websocket: WebSocket):
    await websocket.accept()
    sistema_estado["websocket_clients"].add(websocket)
    add_log("Cliente WebSocket conectado", "INFO")

    try:
        db = SessionLocal()
        stats = get_estadisticas(db)
        config = get_configuracion(db)
        db.close()

        await websocket.send_text(json.dumps({
            "type": "initial_data",
            "data": {
                "stats": stats,
                "config": config,
                "logs": sistema_estado["logs"][-20:],
                "sistema_activo": sistema_estado["activo"],
                "modo_prueba": sistema_estado["modo_prueba"],
                "ultimo_analisis": sistema_estado["ultimo_analisis"],
                "bots_activos": len(sistema_estado["bots_activos"]),
                "telegram": get_telegram_status(),
                "ultimo_heartbeat": sistema_estado.get("ultimo_heartbeat"),
                "ultimo_seguimiento": sistema_estado.get("ultimo_seguimiento")
            }
        }))

        while True:
            data = await websocket.receive_text()
            msg = json.loads(data)
            if msg.get("type") == "ping":
                await websocket.send_text(json.dumps({"type": "pong"}))

    except WebSocketDisconnect:
        sistema_estado["websocket_clients"].discard(websocket)
        add_log("Cliente WebSocket desconectado", "INFO")
    except Exception as e:
        sistema_estado["websocket_clients"].discard(websocket)
        add_log(f"Error WebSocket: {e}", "ERROR")


# ============================================================
# ENDPOINTS - SISTEMA
# ============================================================
@app.get("/")
async def root():
    return {
        "nombre": "Trading Pionex API",
        "version": "2.0.0",
        "estado": "activo" if sistema_estado["activo"] else "inactivo",
        "modo": "prueba" if sistema_estado["modo_prueba"] else "real"
    }


@app.get("/api/estado")
async def get_estado():
    db = SessionLocal()
    stats = get_estadisticas(db)
    config = get_configuracion(db)
    db.close()

    return {
        "sistema_activo": sistema_estado["activo"],
        "modo_prueba": sistema_estado["modo_prueba"],
        "ciclo_actual": sistema_estado["ciclo_actual"],
        "ultimo_analisis": sistema_estado["ultimo_analisis"],
        "bots_activos": len(sistema_estado["bots_activos"]),
        "telegram": get_telegram_status(),
        "ultimo_heartbeat": sistema_estado.get("ultimo_heartbeat"),
        "ultimo_seguimiento": sistema_estado.get("ultimo_seguimiento"),
        "stats": stats,
        "config": config
    }


@app.get("/api/telegram/status")
async def telegram_status():
    return {"telegram": get_telegram_status()}


@app.post("/api/telegram/test")
async def telegram_test(body: TelegramTest):
    send_telegram(TELEGRAM_TOKEN, TELEGRAM_CHAT, body.mensaje)
    return {"ok": True, "telegram": get_telegram_status()}


@app.post("/api/sistema")
async def controlar_sistema(action: SistemaAction):
    global trading_task

    if action.accion == "iniciar":
        if sistema_estado["activo"]:
            return {"mensaje": "El sistema ya está activo", "estado": "activo"}

        if not PIONEX_API_KEY or not PIONEX_SECRET:
            raise HTTPException(status_code=400, detail="API Key de Pionex no configurada")

        sistema_estado["activo"] = True
        add_log("Sistema iniciado por el usuario", "SUCCESS")

        trading_task = asyncio.create_task(
            trading_loop(PIONEX_API_KEY, PIONEX_SECRET, TELEGRAM_TOKEN, TELEGRAM_CHAT)
        )

        return {"mensaje": "Sistema iniciado correctamente", "estado": "activo"}

    elif action.accion == "detener":
        sistema_estado["activo"] = False
        if trading_task:
            trading_task.cancel()
            trading_task = None
        add_log("Sistema detenido por el usuario", "WARNING")
        return {"mensaje": "Sistema detenido", "estado": "inactivo"}

    elif action.accion == "ciclo_manual":
        if not PIONEX_API_KEY or not PIONEX_SECRET:
            raise HTTPException(status_code=400, detail="API Key no configurada")

        db = SessionLocal()
        config = get_configuracion(db)
        db.close()

        asyncio.create_task(
            asyncio.to_thread(run_cycle, PIONEX_API_KEY, PIONEX_SECRET, config)
        )
        return {"mensaje": "Ciclo manual iniciado", "estado": "corriendo"}

    else:
        raise HTTPException(status_code=400, detail="Acción no válida")


# ============================================================
# ENDPOINTS - CONFIGURACION
# ============================================================
@app.get("/api/configuracion")
async def get_config():
    db = SessionLocal()
    config = get_configuracion(db)
    db.close()
    return config


@app.put("/api/configuracion")
async def update_config(update: ConfigUpdate):
    db = SessionLocal()
    config = actualizar_configuracion(db, update.clave, update.valor)
    db.close()

    if update.clave == "modo_prueba":
        sistema_estado["modo_prueba"] = update.valor == "true"
    if update.clave == "loop_interval_minutes":
        sistema_estado["wake_sleep"] = True

    add_log(f"Configuración actualizada: {update.clave} = {update.valor}", "INFO")
    return {"mensaje": "Configuración actualizada", "clave": update.clave, "valor": update.valor}


@app.put("/api/configuracion/batch")
async def update_config_batch(updates: dict):
    db = SessionLocal()
    for clave, valor in updates.items():
        actualizar_configuracion(db, clave, str(valor))
    db.close()
    if "loop_interval_minutes" in updates:
        sistema_estado["wake_sleep"] = True
    add_log("Configuración actualizada en lote", "INFO")
    return {"mensaje": "Configuración actualizada correctamente"}


# ============================================================
# ENDPOINTS - BOTS
# ============================================================
@app.get("/api/bots")
async def get_bots(estado: Optional[str] = None, limit: int = 50):
    db = SessionLocal()
    query = db.query(Bot)
    if estado:
        query = query.filter(Bot.estado == estado)
    bots = query.order_by(Bot.fecha_apertura.desc()).limit(limit).all()
    db.close()

    return [
        {
            "id": b.id,
            "bot_id": b.bot_id,
            "symbol": b.symbol,
            "precio_entrada": b.precio_entrada,
            "lower_price": b.lower_price,
            "upper_price": b.upper_price,
            "inversion": b.inversion,
            "grillas": b.grillas,
            "apalancamiento": b.apalancamiento,
            "take_profit": b.take_profit,
            "stop_loss": b.stop_loss,
            "score": b.score,
            "rsi": b.rsi,
            "bb_width": b.bb_width,
            "bot_type": b.bot_type or "NEUTRAL_FUTURES_GRID",
            "estado": b.estado,
            "ganancia": b.ganancia,
            "ganancia_pct": b.ganancia_pct,
            "modo_prueba": b.modo_prueba,
            "fecha_apertura": b.fecha_apertura.strftime("%Y-%m-%d %H:%M:%S") if b.fecha_apertura else None,
            "fecha_cierre": b.fecha_cierre.strftime("%Y-%m-%d %H:%M:%S") if b.fecha_cierre else None
        }
        for b in bots
    ]


@app.get("/api/bots/activos")
async def get_bots_activos():
    db = SessionLocal()
    bots = db.query(Bot).filter(Bot.estado == "ACTIVO").all()
    db.close()
    return {"total": len(bots), "bots": [b.symbol for b in bots]}


@app.post("/api/bots/stop")
async def stop_bot(body: BotStopRequest):
    if not body.bot_id:
        raise HTTPException(status_code=400, detail="bot_id requerido")

    result = await asyncio.to_thread(detener_bot, PIONEX_API_KEY, PIONEX_SECRET, body.bot_id)
    if not result.get("ok"):
        err = result.get("error") or "error"
        code = 404 if err == "not_found" else 400
        raise HTTPException(status_code=code, detail=result)
    return result


# ============================================================
# ENDPOINTS - GANANCIAS
# ============================================================
@app.get("/api/ganancias")
async def get_ganancias(limit: int = 50):
    db = SessionLocal()
    ganancias = db.query(Ganancia).order_by(Ganancia.fecha.desc()).limit(limit).all()
    db.close()

    return [
        {
            "id": g.id,
            "bot_id": g.bot_id,
            "symbol": g.symbol,
            "ganancia_usdt": g.ganancia_usdt,
            "ganancia_pct": g.ganancia_pct,
            "modo_prueba": g.modo_prueba,
            "fecha": g.fecha.strftime("%Y-%m-%d %H:%M:%S") if g.fecha else None
        }
        for g in ganancias
    ]


@app.get("/api/ganancias/resumen")
async def get_resumen_ganancias():
    db = SessionLocal()
    ganancias = db.query(Ganancia).all()
    db.close()

    total = sum(g.ganancia_usdt for g in ganancias)
    real = sum(g.ganancia_usdt for g in ganancias if not g.modo_prueba)
    prueba = sum(g.ganancia_usdt for g in ganancias if g.modo_prueba)

    por_symbol = {}
    for g in ganancias:
        if g.symbol not in por_symbol:
            por_symbol[g.symbol] = 0
        por_symbol[g.symbol] += g.ganancia_usdt

    top_symbols = sorted(por_symbol.items(), key=lambda x: x[1], reverse=True)[:5]

    return {
        "ganancia_total": round(total, 4),
        "ganancia_real": round(real, 4),
        "ganancia_prueba": round(prueba, 4),
        "total_operaciones": len(ganancias),
        "top_symbols": [{"symbol": s, "ganancia": round(g, 4)} for s, g in top_symbols]
    }


# ============================================================
# ENDPOINTS - CICLOS
# ============================================================
@app.get("/api/ciclos")
async def get_ciclos(limit: int = 20):
    db = SessionLocal()
    ciclos = db.query(Ciclo).order_by(Ciclo.fecha.desc()).limit(limit).all()
    db.close()

    return [
        {
            "id": c.id,
            "numero": c.numero,
            "pares_escaneados": c.pares_escaneados,
            "pares_seleccionados": c.pares_seleccionados,
            "bots_abiertos": c.bots_abiertos,
            "modo_prueba": c.modo_prueba,
            "fecha": c.fecha.strftime("%Y-%m-%d %H:%M:%S") if c.fecha else None
        }
        for c in ciclos
    ]


# ============================================================
# ENDPOINTS - LOGS
# ============================================================
@app.get("/api/logs")
async def get_logs(limit: int = 50):
    logs = sistema_estado["logs"][-limit:]
    return {"logs": logs[::-1]}


# ============================================================
# ENDPOINTS - ESTADISTICAS
# ============================================================
@app.get("/api/estadisticas")
async def get_stats():
    db = SessionLocal()
    stats = get_estadisticas(db)
    db.close()
    return {
        **stats,
        "sistema_activo": sistema_estado["activo"],
        "modo_prueba": sistema_estado["modo_prueba"],
        "ciclo_actual": sistema_estado["ciclo_actual"],
        "ultimo_analisis": sistema_estado["ultimo_analisis"]
    }


# ============================================================
# INICIO DEL SERVIDOR
# ============================================================
if __name__ == "__main__":
    import uvicorn
    uvicorn.run("main:app", host="0.0.0.0", port=8000, reload=True)


# ============================================================
# CERRAR TODOS LOS BOTS ACTIVOS SIMULADOS
# ============================================================

@app.post("/api/bots/limpiar")
async def limpiar_bots_simulados():
    db = SessionLocal()
    bots = db.query(Bot).filter(Bot.estado == "ACTIVO", Bot.modo_prueba == True).all()
    for bot in bots:
        bot.estado = "CERRADO"
        bot.fecha_cierre = datetime.utcnow()
    db.commit()
    db.close()
    sistema_estado["bots_activos"] = {}
    sistema_estado["pares_activos"] = set()
    add_log("Bots simulados limpiados", "SUCCESS")
    return {"mensaje": f"Se cerraron {len(bots)} bots simulados"}
