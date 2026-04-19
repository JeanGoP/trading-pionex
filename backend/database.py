from sqlalchemy import create_engine, Column, Integer, Float, String, Boolean, DateTime, text
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker
from datetime import datetime
import os

DATABASE_URL = "sqlite:///./trading.db"

engine = create_engine(DATABASE_URL, connect_args={"check_same_thread": False})
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)
Base = declarative_base()


# ============================================================
# MODELOS DE BASE DE DATOS
# ============================================================

class Bot(Base):
    __tablename__ = "bots"

    id = Column(Integer, primary_key=True, index=True)
    bot_id = Column(String, unique=True, index=True)
    symbol = Column(String)
    precio_entrada = Column(Float)
    lower_price = Column(Float)
    upper_price = Column(Float)
    inversion = Column(Float)
    grillas = Column(Integer)
    apalancamiento = Column(Integer)
    take_profit = Column(Float)
    stop_loss = Column(Float)
    score = Column(Integer)
    rsi = Column(Float)
    bb_width = Column(Float)
    bot_type = Column(String, default="NEUTRAL_FUTURES_GRID")
    estado = Column(String, default="ACTIVO")  # ACTIVO, CERRADO, STOP_LOSS
    ganancia = Column(Float, default=0.0)
    ganancia_pct = Column(Float, default=0.0)
    modo_prueba = Column(Boolean, default=True)
    fecha_apertura = Column(DateTime, default=datetime.utcnow)
    fecha_cierre = Column(DateTime, nullable=True)


class Ciclo(Base):
    __tablename__ = "ciclos"

    id = Column(Integer, primary_key=True, index=True)
    numero = Column(Integer)
    pares_escaneados = Column(Integer)
    pares_seleccionados = Column(Integer)
    bots_abiertos = Column(Integer)
    modo_prueba = Column(Boolean, default=True)
    fecha = Column(DateTime, default=datetime.utcnow)


class Ganancia(Base):
    __tablename__ = "ganancias"

    id = Column(Integer, primary_key=True, index=True)
    bot_id = Column(String)
    symbol = Column(String)
    ganancia_usdt = Column(Float)
    ganancia_pct = Column(Float)
    modo_prueba = Column(Boolean, default=True)
    fecha = Column(DateTime, default=datetime.utcnow)


class Configuracion(Base):
    __tablename__ = "configuracion"

    id = Column(Integer, primary_key=True, index=True)
    clave = Column(String, unique=True)
    valor = Column(String)
    fecha_actualizacion = Column(DateTime, default=datetime.utcnow)


# ============================================================
# FUNCIONES DE BASE DE DATOS
# ============================================================

def get_db():
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()

class Seguimiento(Base):
    __tablename__ = "seguimiento"

    id = Column(Integer, primary_key=True, index=True)
    bot_id = Column(String, index=True)
    symbol = Column(String)
    precio_entrada = Column(Float)
    lower_price = Column(Float)
    upper_price = Column(Float)
    score = Column(Integer)
    rsi = Column(Float)
    bb_width = Column(Float)
    precio_actual = Column(Float, default=0.0)
    max_precio = Column(Float, default=0.0)
    min_precio = Column(Float, default=0.0)
    dentro_rango = Column(Boolean, default=True)
    hubiera_ganado = Column(Boolean, default=False)
    hubiera_stop_loss = Column(Boolean, default=False)
    horas_seguimiento = Column(Integer, default=0)
    resultado = Column(String, default="PENDIENTE")
    fecha_apertura = Column(DateTime, default=datetime.utcnow)
    fecha_resultado = Column(DateTime, nullable=True)

def init_db():
    Base.metadata.create_all(bind=engine)
    db = SessionLocal()
    try:
        cols = db.execute(text("PRAGMA table_info(bots)")).fetchall()
        col_names = {row[1] for row in cols}
        if "bot_type" not in col_names:
            db.execute(text("ALTER TABLE bots ADD COLUMN bot_type VARCHAR"))
            db.execute(text("UPDATE bots SET bot_type = 'NEUTRAL_FUTURES_GRID' WHERE bot_type IS NULL"))
            db.commit()
    except Exception:
        db.rollback()

    configs_iniciales = [
        ("modo_prueba", "true"),
        ("sistema_activo", "false"),
        ("investment_usdt", "20"),
        ("leverage", "2"),
        ("take_profit_pct", "1.0"),
        ("stop_loss_pct", "5.0"),
        ("grid_count", "60"),
        ("max_active_bots", "2"),
        ("price_range_pct", "5.0"),
        ("min_volume_24h", "5000000"),
        ("min_volatility", "2.0"),
        ("max_volatility", "8.0"),
        ("rsi_min", "35"),
        ("rsi_max", "65"),
        ("min_score", "75"),
        ("max_atr_pct", "7.0"),
        ("adx_min", "15.0"),
        ("bb_width_min", "2.0"),
        ("bb_width_max", "6.0"),
        ("distancia_media_max", "1.5"),
        ("vol_ratio_min", "1.0"),
        ("news_enabled", "true"),
        ("news_provider", "rss"),
        ("news_rss_urls", "https://www.coindesk.com/arc/outboundfeeds/rss/|https://cointelegraph.com/rss"),
        ("news_lookback_hours", "12"),
        ("news_max_items", "10"),
        ("news_cache_minutes", "15"),
        ("news_block_keywords", "hack,exploit,breach,bankruptcy,insolvency,investigation,sec,lawsuit,delist,delisting,suspended,halt,scam,rug,attack,vulnerability"),
        ("news_positive_keywords", "partnership,listing,listed,integrates,adoption,upgrade,launch,mainnet,ETF,approval,record,breakout"),
        ("telegram_heartbeat_enabled", "true"),
        ("telegram_heartbeat_minutes", "30"),
        ("telegram_followup_enabled", "true"),
        ("telegram_followup_top_n", "5"),
    ]

    for clave, valor in configs_iniciales:
        existing = db.query(Configuracion).filter(Configuracion.clave == clave).first()
        if not existing:
            db.add(Configuracion(clave=clave, valor=valor))

    # Migracion segura: si venia usando proveedor de pago sin token, pasa a RSS gratis.
    provider = db.query(Configuracion).filter(Configuracion.clave == "news_provider").first()
    if provider and str(provider.valor).strip().lower() == "cryptopanic" and not os.getenv("CRYPTOPANIC_TOKEN", "").strip():
        provider.valor = "rss"
        provider.fecha_actualizacion = datetime.utcnow()

    db.commit()
    db.close()
    print("Base de datos inicializada correctamente.")


def guardar_bot(db, bot_data: dict):
    bot = Bot(**bot_data)
    db.add(bot)
    db.commit()
    db.refresh(bot)
    return bot


def cerrar_bot(db, bot_id: str, ganancia: float, ganancia_pct: float, estado: str = "CERRADO"):
    bot = db.query(Bot).filter(Bot.bot_id == bot_id).first()
    if bot:
        bot.estado = estado
        bot.ganancia = ganancia
        bot.ganancia_pct = ganancia_pct
        bot.fecha_cierre = datetime.utcnow()
        db.commit()

        ganancia_reg = Ganancia(
            bot_id=bot_id,
            symbol=bot.symbol,
            ganancia_usdt=ganancia,
            ganancia_pct=ganancia_pct,
            modo_prueba=bot.modo_prueba
        )
        db.add(ganancia_reg)
        db.commit()
    return bot


def guardar_ciclo(db, ciclo_data: dict):
    ciclo = Ciclo(**ciclo_data)
    db.add(ciclo)
    db.commit()
    db.refresh(ciclo)
    return ciclo


def get_configuracion(db):
    configs = db.query(Configuracion).all()
    return {c.clave: c.valor for c in configs}


def actualizar_configuracion(db, clave: str, valor: str):
    config = db.query(Configuracion).filter(Configuracion.clave == clave).first()
    if config:
        config.valor = valor
        config.fecha_actualizacion = datetime.utcnow()
        db.commit()
    return config


def get_estadisticas(db):
    total_bots = db.query(Bot).count()
    bots_activos = db.query(Bot).filter(Bot.estado == "ACTIVO").count()
    bots_cerrados = db.query(Bot).filter(Bot.estado == "CERRADO").count()
    bots_stop_loss = db.query(Bot).filter(Bot.estado == "STOP_LOSS").count()

    ganancias = db.query(Ganancia).all()
    ganancia_total = sum(g.ganancia_usdt for g in ganancias)
    ganancia_real = sum(g.ganancia_usdt for g in ganancias if not g.modo_prueba)
    ganancia_prueba = sum(g.ganancia_usdt for g in ganancias if g.modo_prueba)

    ciclos = db.query(Ciclo).count()

    return {
        "total_bots": total_bots,
        "bots_activos": bots_activos,
        "bots_cerrados": bots_cerrados,
        "bots_stop_loss": bots_stop_loss,
        "ganancia_total": round(ganancia_total, 4),
        "ganancia_real": round(ganancia_real, 4),
        "ganancia_prueba": round(ganancia_prueba, 4),
        "total_ciclos": ciclos
    }

def guardar_seguimiento(db, data: dict):
    seg = Seguimiento(**data)
    db.add(seg)
    db.commit()
    db.refresh(seg)
    return seg


def get_seguimientos_pendientes(db):
    return db.query(Seguimiento).filter(
        Seguimiento.resultado == "PENDIENTE"
    ).all()


def actualizar_seguimiento(db, seg_id: int, data: dict):
    seg = db.query(Seguimiento).filter(Seguimiento.id == seg_id).first()
    if seg:
        for k, v in data.items():
            setattr(seg, k, v)
        db.commit()
    return seg


def get_reporte_seguimiento(db):
    segs = db.query(Seguimiento).filter(
        Seguimiento.resultado != "PENDIENTE"
    ).all()

    total = len(segs)
    ganados = sum(1 for s in segs if s.hubiera_ganado)
    stop_loss = sum(1 for s in segs if s.hubiera_stop_loss)
    pendientes = db.query(Seguimiento).filter(
        Seguimiento.resultado == "PENDIENTE"
    ).count()

    tasa_exito = (ganados / total * 100) if total > 0 else 0

    return {
        "total_simulados": total,
        "hubieran_ganado": ganados,
        "hubieran_stop_loss": stop_loss,
        "pendientes": pendientes,
        "tasa_exito": round(tasa_exito, 1)
    }
