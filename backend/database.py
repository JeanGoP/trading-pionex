from sqlalchemy import create_engine, Column, Integer, Float, String, Boolean, DateTime
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker
from datetime import datetime

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


def init_db():
    Base.metadata.create_all(bind=engine)
    db = SessionLocal()

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
    ]

    for clave, valor in configs_iniciales:
        existing = db.query(Configuracion).filter(Configuracion.clave == clave).first()
        if not existing:
            db.add(Configuracion(clave=clave, valor=valor))

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