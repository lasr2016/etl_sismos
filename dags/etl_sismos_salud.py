"""
PIPELINE ETL SISMOS (USGS) & SALUD (DEIS) -> DATA WAREHOUSE & DATAMART POSTGRESQL

Estructura modular adaptada para evaluación:
    1. INICIALIZAR: Crea esquemas (raw, staging, dw, mart) y tablas si no existen.
    2. EXTRACT: Consume APIs públicas (USGS y DEIS) y respalda en raw.
    3. TRANSFORM: Limpieza, desanidación y normalización (staging).
    4. MODELAR: Construye dimensiones (tiempo, geografía, establecimientos) y hechos.
    5. CALIDAD: Asserts de consistencia e integridad referencial.
    6. LOAD: Inserción idempotente (ON CONFLICT) en el esquema dw.
    7. MART & KPIS: Cálculo de métricas analíticas en el esquema mart.
    8. AUDITORÍA: Registro de métricas de ejecución en etl_audit.
"""
import json
import logging
import os
import time
import unicodedata
from datetime import datetime
from pathlib import Path

import pandas as pd
import requests
from sqlalchemy import create_engine, text
from sqlalchemy.engine import URL

BASE = Path(__file__).parent
SALIDA = BASE / "salida"
SALIDA.mkdir(exist_ok=True)

# --- Conexión a PostgreSQL (Lectura desde variables de entorno) ---
PG_HOST = os.getenv("PGHOST", "localhost")
PG_PORT = int(os.getenv("PGPORT", "5432"))
PG_DB = os.getenv("PGDATABASE", "gestion2")
PG_USER = os.getenv("PGUSER", "postgres")
PG_PASSWORD = os.getenv("PGPASSWORD", "54321")

def get_engine():
    url = URL.create(
        "postgresql+psycopg2",
        username=PG_USER,
        password=PG_PASSWORD,
        host=PG_HOST,
        port=PG_PORT,
        database=PG_DB,
    )
    return create_engine(url)

logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(levelname)s | %(message)s", datefmt="%H:%M:%S")
log = logging.getLogger("etl_sismos_salud")

# --------------------------------------------------------------------------
# DDL: CREACIÓN AUTOMÁTICA DE ESQUEMAS Y TABLAS
# --------------------------------------------------------------------------
DDL_INIT = """
CREATE SCHEMA IF NOT EXISTS raw;
CREATE SCHEMA IF NOT EXISTS staging;
CREATE SCHEMA IF NOT EXISTS dw;
CREATE SCHEMA IF NOT EXISTS mart;

-- Tabla de Auditoría
CREATE TABLE IF NOT EXISTS etl_audit (
    id SERIAL PRIMARY KEY,
    dag_id VARCHAR(100) NOT NULL,
    run_id VARCHAR(100),
    filas_leidas INTEGER,
    filas_cargadas INTEGER,
    estado VARCHAR(20) NOT NULL,
    duracion_seg NUMERIC(10, 2),
    fecha_ejecucion TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Capa RAW: almacenamiento de carga cruda
CREATE TABLE IF NOT EXISTS raw.usgs_sismos (
    id SERIAL PRIMARY KEY,
    payload JSONB NOT NULL,
    fecha_extraccion TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS raw.deis_establecimientos (
    id SERIAL PRIMARY KEY,
    payload JSONB NOT NULL,
    fecha_extraccion TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- =========================================================================
-- Capa STAGING: tablas temporales/intermedias estructuradas
-- =========================================================================
CREATE TABLE IF NOT EXISTS staging.stg_dim_tiempo (
    tiempo_id INTEGER,
    fecha DATE,
    anio SMALLINT,
    mes SMALLINT,
    dia SMALLINT,
    trimestre SMALLINT,
    es_fin_de_semana SMALLINT
);

CREATE TABLE IF NOT EXISTS staging.stg_dim_geo (
    zona_id INTEGER,
    macro_zona VARCHAR(50),
    descripcion TEXT
);

CREATE TABLE IF NOT EXISTS staging.stg_salud (
    codigo_deis VARCHAR(30),
    nombre VARCHAR(200),
    tipo VARCHAR(100),
    comuna VARCHAR(100),
    region VARCHAR(100)
);

CREATE TABLE IF NOT EXISTS staging.stg_fact_sismos (
    sismo_id VARCHAR(50),
    tiempo_id INTEGER,
    zona_id INTEGER,
    fecha_utc TIMESTAMP,
    magnitud NUMERIC(4, 2),
    profundidad_km NUMERIC(8, 2),
    latitud NUMERIC(9, 6),
    longitud NUMERIC(9, 6),
    lugar TEXT
);

-- Capa DW: Dimensiones
CREATE TABLE IF NOT EXISTS dw.dim_tiempo (
    tiempo_id INTEGER PRIMARY KEY,
    fecha DATE NOT NULL,
    anio SMALLINT NOT NULL,
    mes SMALLINT NOT NULL,
    dia SMALLINT NOT NULL,
    trimestre SMALLINT NOT NULL,
    es_fin_de_semana SMALLINT NOT NULL
);

CREATE TABLE IF NOT EXISTS dw.dim_geografia (
    zona_id SERIAL PRIMARY KEY,
    macro_zona VARCHAR(50) NOT NULL UNIQUE,
    descripcion TEXT
);

CREATE TABLE IF NOT EXISTS dw.dim_establecimiento (
    establecimiento_key SERIAL PRIMARY KEY,
    codigo_deis VARCHAR(30) UNIQUE NOT NULL,
    nombre VARCHAR(200) NOT NULL,
    tipo VARCHAR(100),
    comuna VARCHAR(100),
    region VARCHAR(100)
);

-- Capa DW: Hechos
CREATE TABLE IF NOT EXISTS dw.fact_sismos (
    sismo_id VARCHAR(50) PRIMARY KEY,
    tiempo_id INTEGER NOT NULL REFERENCES dw.dim_tiempo(tiempo_id),
    zona_id INTEGER REFERENCES dw.dim_geografia(zona_id),
    fecha_utc TIMESTAMP NOT NULL,
    magnitud NUMERIC(4, 2) NOT NULL,
    profundidad_km NUMERIC(8, 2) NOT NULL,
    latitud NUMERIC(9, 6) NOT NULL,
    longitud NUMERIC(9, 6) NOT NULL,
    lugar TEXT
);

-- Capa MART: Vistas o tablas de KPIs
CREATE TABLE IF NOT EXISTS mart.kpi_sismicidad_zona_mes (
    anio INTEGER,
    mes INTEGER,
    macro_zona VARCHAR(50),
    total_sismos INTEGER,
    magnitud_promedio NUMERIC(4,2),
    sismos_severos INTEGER,
    PRIMARY KEY (anio, mes, macro_zona)
);
"""

def init_database() -> None:
    engine = get_engine()
    with engine.begin() as con:
        con.exec_driver_sql(DDL_INIT)
    log.info("Esquemas (raw, staging, dw, mart) y tablas listos.")

# --------------------------------------------------------------------------
# EXTRACT
# --------------------------------------------------------------------------
def extract_usgs_sismos(start_date: str = "2024-01-01", end_date: str = "2024-12-31") -> list:
    """Consume API de USGS para el polígono de Chile y almacena crudo en raw."""
    url = "https://earthquake.usgs.gov/fdsnws/event/1/query"
    params = {
        "format": "geojson",
        "starttime": start_date,
        "endtime": end_date,
        "minmagnitude": 3.0,
        "minlatitude": -56.0,
        "maxlatitude": -17.5,
        "minlongitude": -76.0,
        "maxlongitude": -66.0,
    }
    log.info("Consultando API USGS (%s a %s)...", start_date, end_date)
    res = requests.get(url, params=params, timeout=35)
    res.raise_for_status()
    data = res.json()
    features = data.get("features", [])
    log.info("USGS: %d sismos encontrados", len(features))

    # Guardar en crudo en raw (requisito de arquitectura)
    engine = get_engine()
    with engine.begin() as con:
        con.execute(
            text("INSERT INTO raw.usgs_sismos (payload) VALUES (:payload)"),
            {"payload": json.dumps(data)}
        )
    return features


def extract_deis_establecimientos() -> list:
    """Consume catálogo abierto de establecimientos de salud de Chile."""
    # Catálogo API CKAN datos.gob.cl / DEIS MINSAL
    url = "https://datos.gob.cl/api/3/action/datastore_search"
    params = {"resource_id": "060a0a55-13e6-4275-8ec6-36d9346ff080", "limit": 1000}
    try:
        res = requests.get(url, params=params, timeout=20)
        res.raise_for_status()
        records = res.json().get("result", {}).get("records", [])
    except Exception as e:
        log.warning("Fallo al consumir API DEIS en línea (%s). Usando fallback de resiliencia.", e)
        # Fallback sintético mínimo de resiliencia si la API externa del ministerio cae
        records = [
            {"Codigo": "HOSP-01", "Nombre": "Hospital Regional de Antofagasta", "Tipo": "Hospital", "Comuna": "Antofagasta", "Region": "Antofagasta"},
            {"Codigo": "HOSP-02", "Nombre": "Hospital Regional de Talca", "Tipo": "Hospital", "Comuna": "Talca", "Region": "Maule"},
            {"Codigo": "HOSP-03", "Nombre": "Hospital Dr. Sotero del Rio", "Tipo": "Hospital", "Comuna": "Puente Alto", "Region": "Metropolitana"},
            {"Codigo": "HOSP-04", "Nombre": "Hospital Regional de Concepcion", "Tipo": "Hospital", "Comuna": "Concepción", "Region": "Biobío"},
        ]

    engine = get_engine()
    with engine.begin() as con:
        con.execute(
            text("INSERT INTO raw.deis_establecimientos (payload) VALUES (:payload)"),
            {"payload": json.dumps(records)}
        )
    log.info("DEIS: %d establecimientos registrados", len(records))
    return records

# --------------------------------------------------------------------------
# TRANSFORM & MODELADO
# --------------------------------------------------------------------------
def build_dim_tiempo(anio: int = 2024) -> pd.DataFrame:
    """Idéntico a build_dim_fecha del script de clase."""
    f = pd.DataFrame({"fecha": pd.date_range(f"{anio}-01-01", f"{anio}-12-31")})
    f["tiempo_id"] = f["fecha"].dt.strftime("%Y%m%d").astype(int)
    f["anio"] = f["fecha"].dt.year
    f["mes"] = f["fecha"].dt.month
    f["dia"] = f["fecha"].dt.day
    f["trimestre"] = f["fecha"].dt.quarter
    f["es_fin_de_semana"] = (f["fecha"].dt.dayofweek >= 5).astype(int)
    return f

def build_dim_geografia() -> pd.DataFrame:
    zonas = [
        {"zona_id": 1, "macro_zona": "Zona Norte", "descripcion": "Regiones XV, I, II y III"},
        {"zona_id": 2, "macro_zona": "Zona Centro-Norte", "descripcion": "Regiones IV y V"},
        {"zona_id": 3, "macro_zona": "Zona Central", "descripcion": "Regiones RM, VI y VII"},
        {"zona_id": 4, "macro_zona": "Zona Sur", "descripcion": "Regiones XVI, VIII, IX y XIV"},
        {"zona_id": 5, "macro_zona": "Zona Austral", "descripcion": "Regiones X, XI y XII"},
    ]
    return pd.DataFrame(zonas)

def transform_sismos(features: list) -> pd.DataFrame:
    filas = []
    for s in features:
        prop = s.get("properties", {})
        geom = s.get("geometry", {}).get("coordinates", [0, 0, 0])
        filas.append({
            "sismo_id": str(s.get("id")),
            "fecha_utc": pd.to_datetime(prop.get("time"), unit="ms"),
            "magnitud": prop.get("mag"),
            "lugar": prop.get("place", "Desconocido"),
            "longitud": geom[0],
            "latitud": geom[1],
            "profundidad_km": geom[2],
        })
    df = pd.DataFrame(filas)
    df = df.dropna(subset=["sismo_id", "magnitud", "fecha_utc"]).drop_duplicates(subset="sismo_id")
    df["magnitud"] = pd.to_numeric(df["magnitud"], errors="coerce")
    df["profundidad_km"] = pd.to_numeric(df["profundidad_km"], errors="coerce")
    df["tiempo_id"] = df["fecha_utc"].dt.strftime("%Y%m%d").astype(int)

    def resolver_zona(lat):
        if lat >= -26.0: return 1       # Norte
        elif lat >= -32.0: return 2     # Centro-Norte
        elif lat >= -36.0: return 3     # Central
        elif lat >= -42.0: return 4     # Sur
        return 5                        # Austral

    df["zona_id"] = df["latitud"].apply(resolver_zona)
    return df

def transform_establecimientos(records: list) -> pd.DataFrame:
    df = pd.DataFrame(records)
    cols = {"Codigo": "codigo_deis", "Nombre": "nombre", "Tipo": "tipo", "Comuna": "comuna", "Region": "region"}
    # Normalizar columnas según vengan en mayúscula o minúscula
    df = df.rename(columns={c: cols.get(c, c.lower()) for c in df.columns})
    disponibles = [c for c in ["codigo_deis", "nombre", "tipo", "comuna", "region"] if c in df.columns]
    df = df[disponibles].dropna(subset=["codigo_deis"]).drop_duplicates(subset="codigo_deis")
    return df

# --------------------------------------------------------------------------
# CALIDAD
# --------------------------------------------------------------------------
def check_calidad(fact_sismos: pd.DataFrame, dim_tiempo: pd.DataFrame) -> None:
    assert fact_sismos["sismo_id"].is_unique, "Existen sismo_id duplicados en el DataFrame"
    assert fact_sismos[["sismo_id", "tiempo_id", "magnitud"]].notna().all().all(), "Valores nulos en claves o métricas"
    assert (fact_sismos["magnitud"] > 0).all(), "Magnitudes menores o iguales a cero detectadas"
    assert fact_sismos["tiempo_id"].isin(dim_tiempo["tiempo_id"]).all(), "Fechas huérfanas fuera de dim_tiempo"
    log.info("Control de calidad superado: 100%% consistencia.")

# --------------------------------------------------------------------------
# LOAD (IDEMPOTENTE EN DW)
# --------------------------------------------------------------------------
def load_warehouse_sismos(dim_tiempo: pd.DataFrame, dim_geo: pd.DataFrame, fact: pd.DataFrame) -> int:
    """Carga dimensiones (tiempo, geografia) y tabla de hechos de sismos en el DW."""
    engine = get_engine()
    with engine.begin() as con:
        if not dim_tiempo.empty:
            dim_tiempo.to_sql("stg_dim_tiempo", con, schema="staging", if_exists="replace", index=False)
            con.execute(text("""
                INSERT INTO dw.dim_tiempo (tiempo_id,fecha,anio,mes,dia,trimestre,es_fin_de_semana)
                SELECT tiempo_id,fecha,anio,mes,dia,trimestre,es_fin_de_semana FROM staging.stg_dim_tiempo
                ON CONFLICT (tiempo_id) DO NOTHING;
            """))

        if not dim_geo.empty:
            dim_geo.to_sql("stg_dim_geo", con, schema="staging", if_exists="replace", index=False)
            con.execute(text("""
                INSERT INTO dw.dim_geografia SELECT * FROM staging.stg_dim_geo
                ON CONFLICT (zona_id) DO UPDATE SET macro_zona = EXCLUDED.macro_zona;
            """))

        filas_cargadas = 0
        if not fact.empty:
            fact.to_sql("stg_fact_sismos", con, schema="staging", if_exists="replace", index=False)
            resultado = con.execute(text("""
                INSERT INTO dw.fact_sismos (sismo_id, tiempo_id, zona_id, fecha_utc, magnitud, profundidad_km, latitud, longitud, lugar)
                SELECT sismo_id, tiempo_id, zona_id, fecha_utc, magnitud, profundidad_km, latitud, longitud, lugar
                FROM staging.stg_fact_sismos
                ON CONFLICT (sismo_id) DO NOTHING;
            """))
            filas_cargadas = resultado.rowcount
    log.info("Carga en dw (sismos) finalizada. Filas insertadas: %d", filas_cargadas)
    return filas_cargadas


def load_warehouse_salud(dim_salud: pd.DataFrame) -> int:
    """Carga la dimension de establecimientos de salud en el DW."""
    if dim_salud.empty:
        return 0

    engine = get_engine()
    with engine.begin() as con:
        dim_salud.to_sql("stg_salud", con, schema="staging", if_exists="replace", index=False)
        resultado = con.execute(text("""
            INSERT INTO dw.dim_establecimiento (codigo_deis, nombre, tipo, comuna, region)
            SELECT codigo_deis, nombre, tipo, comuna, region FROM staging.stg_salud
            ON CONFLICT (codigo_deis) DO NOTHING;
        """))
        filas_cargadas = resultado.rowcount
    log.info("Carga en dw (salud) finalizada. Filas insertadas: %d", filas_cargadas)
    return filas_cargadas


def load_warehouse(dim_tiempo: pd.DataFrame, dim_geo: pd.DataFrame, dim_salud: pd.DataFrame, fact: pd.DataFrame) -> int:
    """Funcion unificada para ejecucion por CLI o pipeline conjunto."""
    cargadas_sismos = load_warehouse_sismos(dim_tiempo, dim_geo, fact)
    cargadas_salud = load_warehouse_salud(dim_salud)
    return cargadas_sismos + cargadas_salud

# --------------------------------------------------------------------------
# MART & INDICADORES (KPIS)
# --------------------------------------------------------------------------
def build_mart_kpis() -> pd.DataFrame:
    """Calcula y almacena los KPIs de negocio en el esquema mart."""
    engine = get_engine()
    sql_kpi = """
    INSERT INTO mart.kpi_sismicidad_zona_mes (anio, mes, macro_zona, total_sismos, magnitud_promedio, sismos_severos)
    SELECT 
        t.anio,
        t.mes,
        g.macro_zona,
        COUNT(f.sismo_id) AS total_sismos,
        ROUND(AVG(f.magnitud), 2) AS magnitud_promedio,
        COUNT(CASE WHEN f.magnitud >= 5.0 THEN 1 END) AS sismos_severos
    FROM dw.fact_sismos f
    JOIN dw.dim_tiempo t ON f.tiempo_id = t.tiempo_id
    JOIN dw.dim_geografia g ON f.zona_id = g.zona_id
    GROUP BY t.anio, t.mes, g.macro_zona
    ON CONFLICT (anio, mes, macro_zona) DO UPDATE SET
        total_sismos = EXCLUDED.total_sismos,
        magnitud_promedio = EXCLUDED.magnitud_promedio,
        sismos_severos = EXCLUDED.sismos_severos;
    """
    with engine.begin() as con:
        con.execute(text(sql_kpi))
        resumen = pd.read_sql(text("SELECT * FROM mart.kpi_sismicidad_zona_mes ORDER BY anio, mes, total_sismos DESC;"), con)
    resumen.to_csv(SALIDA / "indicadores_sismicidad.csv", index=False)
    log.info("Datamart de KPIs actualizado y exportado a salida/indicadores_sismicidad.csv")
    return resumen

# --------------------------------------------------------------------------
# AUDITORÍA
# --------------------------------------------------------------------------
def registrar_auditoria(dag_id: str, run_id: str, leidas: int, cargadas: int, estado: str, duracion: float):
    engine = get_engine()
    with engine.begin() as con:
        con.execute(text("""
            INSERT INTO etl_audit (dag_id, run_id, filas_leidas, filas_cargadas, estado, duracion_seg)
            VALUES (:d, :r, :fl, :fc, :e, :dur)
        """), {"d": dag_id, "r": run_id, "fl": leidas, "fc": cargadas, "e": estado, "dur": round(duracion, 2)})
    log.info("Auditoría guardada en etl_audit: estado=%s, duracion=%.2fs", estado, duracion)

# --------------------------------------------------------------------------
# MAIN (PRUEBA DIRECTA SIN AIRFLOW)
# --------------------------------------------------------------------------
if __name__ == "__main__":
    t0 = time.time()
    try:
        init_database()
        sismos_raw = extract_usgs_sismos("2024-01-01", "2024-03-31") # Rango de prueba
        salud_raw = extract_deis_establecimientos()

        dim_tiempo = build_dim_tiempo(2024)
        dim_geo = build_dim_geografia()
        dim_salud = transform_establecimientos(salud_raw)
        fact_sismos = transform_sismos(sismos_raw)

        check_calidad(fact_sismos, dim_tiempo)
        cargadas = load_warehouse(dim_tiempo, dim_geo, dim_salud, fact_sismos)
        build_mart_kpis()

        duracion = time.time() - t0
        registrar_auditoria("manual_run", "cli_test", len(sismos_raw), cargadas, "SUCCESS", duracion)
    except Exception as err:
        duracion = time.time() - t0
        registrar_auditoria("manual_run", "cli_test", 0, 0, "FAILED", duracion)
        log.error("Fallo durante el pipeline: %s", err)
        raise