import time
from datetime import datetime
from pathlib import Path
from airflow.sdk import dag, task

BASE = Path(__file__).parent
SALIDA = BASE / "salida"
SALIDA.mkdir(exist_ok=True)

# --------------------------------------------------------------------------
# PARÁMETROS CONFIGURABLES DEL ANÁLISIS DE EXPOSICIÓN (CRITERIOS B y E)
# --------------------------------------------------------------------------
MAGNITUD_MINIMA = 4.5       # Umbral de magnitud sísmica relevante (default: 4.5 - 5.0)
RADIO_KM_BASE = 50.0        # Radio de exposición circular en kilómetros
PROFUNDIDAD_MAX_KM = 300.0  # Profundidad máxima en km

# Proyecciones poblacionales oficiales (Censo / INE) para normalización
POBLACION_MACRO_ZONA = {
    "Zona Norte": 1_680_000,        # Arica, Tarapacá, Antofagasta, Atacama
    "Zona Centro-Norte": 2_870_000, # Coquimbo, Valparaíso
    "Zona Central": 10_400_000,     # Metropolitana, O'Higgins, Maule
    "Zona Sur": 3_630_000,          # Ñuble, Biobío, Araucanía, Los Ríos
    "Zona Austral": 1_190_000,      # Los Lagos, Aysén, Magallanes
}

def haversine_vectorizado(lat1, lon1, lat2_arr, lon2_arr):
    """Cálculo vectorizado de distancia geodésica en km (Fórmula de Haversine)."""
    import numpy as np
    R = 6371.0
    phi1 = np.radians(lat1)
    phi2 = np.radians(lat2_arr)
    delta_phi = np.radians(lat2_arr - lat1)
    delta_lambda = np.radians(lon2_arr - lon1)
    a = np.sin(delta_phi / 2.0)**2 + np.cos(phi1) * np.cos(phi2) * np.sin(delta_lambda / 2.0)**2
    c = 2 * np.arctan2(np.sqrt(a), np.sqrt(1 - a))
    return R * c

@dag(
    dag_id="dag_03_indicadores",
    start_date=datetime(2026, 1, 1),
    schedule=None,
    catchup=False,
    tags=["mart", "exposicion_sismica", "reportes"],
)
def dag_03_indicadores():

    @task
    def calcular_mart_y_exportar(**context):
        import matplotlib
        matplotlib.use("Agg")
        import matplotlib.pyplot as plt
        import numpy as np
        import pandas as pd
        from sqlalchemy import text
        import etl_sismos_salud as p

        t0 = time.time()
        engine = p.get_engine()

        # ------------------------------------------------------------------
        # 1. ACTUALIZAR DATAMART GENERAL EN GESTION_SISMO
        # ------------------------------------------------------------------
        sql_mart = """
        CREATE TABLE IF NOT EXISTS mart.kpi_sismicidad_salud (
            anio INT,
            mes INT,
            macro_zona VARCHAR(50),
            total_sismos INT,
            magnitud_promedio NUMERIC(4,2),
            sismos_severos INT,
            total_hospitales INT,
            sismos_por_hospital NUMERIC(5,2),
            PRIMARY KEY (anio, mes, macro_zona)
        );

        INSERT INTO mart.kpi_sismicidad_salud (
            anio, mes, macro_zona, total_sismos, magnitud_promedio, 
            sismos_severos, total_hospitales, sismos_por_hospital
        )
        SELECT 
            t.anio,
            t.mes,
            g.macro_zona,
            COUNT(f.sismo_id) AS total_sismos,
            ROUND(AVG(f.magnitud), 2) AS magnitud_promedio,
            COUNT(CASE WHEN f.magnitud >= 5.0 THEN 1 END) AS sismos_severos,
            COALESCE(h.total_hosp, 0) AS total_hospitales,
            ROUND(
                COUNT(f.sismo_id)::numeric / NULLIF(COALESCE(h.total_hosp, 1), 0), 2
            ) AS sismos_por_hospital
        FROM dw.dim_tiempo t
        CROSS JOIN dw.dim_geografia g
        LEFT JOIN dw.fact_sismos f 
            ON f.tiempo_id = t.tiempo_id AND f.zona_id = g.zona_id
        LEFT JOIN (
            SELECT 
                CASE 
                    WHEN region IN ('Arica Parinacota', 'Arica y Parinacota', 'Tarapacá', 'Antofagasta', 'Atacama') THEN 'Zona Norte'
                    WHEN region IN ('Coquimbo', 'Valparaíso') THEN 'Zona Centro-Norte'
                    WHEN region IN ('Metropolitana de Santiago', 'Libertador Gral. B. O''Higgins', 'Libertador General Bernardo O''Higgins', 'Maule') THEN 'Zona Central'
                    WHEN region IN ('Ñuble', 'Biobío', 'La Araucanía', 'Los Ríos') THEN 'Zona Sur'
                    WHEN region IN ('Los Lagos', 'Aysén del General Carlos Ibañez del Campo', 'Magallanes y de la Antártica Chilena') THEN 'Zona Austral'
                    ELSE 'Zona Austral'
                END AS macro_zona,
                COUNT(DISTINCT codigo_deis) AS total_hosp
            FROM dw.dim_establecimiento
            WHERE estado_funcionamiento ILIKE 'vigente%%'
            GROUP BY 1
        ) h ON g.macro_zona = h.macro_zona
        GROUP BY t.anio, t.mes, g.macro_zona, h.total_hosp
        ON CONFLICT (anio, mes, macro_zona) DO UPDATE SET
            total_sismos = EXCLUDED.total_sismos,
            magnitud_promedio = EXCLUDED.magnitud_promedio,
            sismos_severos = EXCLUDED.sismos_severos,
            total_hospitales = EXCLUDED.total_hospitales,
            sismos_por_hospital = EXCLUDED.sismos_por_hospital;
        """

        with engine.begin() as con:
            con.execute(text(sql_mart))
            
            # Exportar Tablón Dimensional
            tablon = pd.read_sql(text("""
                SELECT f.sismo_id, f.fecha_utc, f.magnitud, f.profundidad_km, f.lugar,
                       g.macro_zona, t.anio, t.mes, t.dia
                FROM dw.fact_sismos f
                JOIN dw.dim_tiempo t ON f.tiempo_id = t.tiempo_id
                JOIN dw.dim_geografia g ON f.zona_id = g.zona_id;
            """), con)
            tablon.to_csv(SALIDA / "tablon_sismos_salud.csv", index=False)

            # Exportar Resumen de Indicadores
            kpis = pd.read_sql(text("""
                SELECT * FROM mart.kpi_sismicidad_salud 
                WHERE total_sismos > 0 
                ORDER BY anio, mes, total_sismos DESC;
            """), con)
            kpis.to_csv(SALIDA / "indicadores_sismicidad_salud.csv", index=False)

            # --------------------------------------------------------------
            # 2. ANÁLISIS ESPACIAL DE EXPOSICIÓN (DESDE DW)
            # --------------------------------------------------------------
            df_estab = pd.read_sql(text("""
                SELECT codigo_deis, nombre, tipo, comuna, region, latitud, longitud, 
                       estado_funcionamiento, nivel_complejidad
                FROM dw.dim_establecimiento
                WHERE estado_funcionamiento ILIKE 'vigente%%'
                  AND latitud IS NOT NULL AND longitud IS NOT NULL;
            """), con)

            df_sismos = pd.read_sql(text("""
                SELECT sismo_id, latitud, longitud, magnitud, profundidad_km, fecha_utc, lugar
                FROM dw.fact_sismos
                WHERE magnitud >= :min_mag AND profundidad_km <= :max_prof;
            """), con, params={"min_mag": MAGNITUD_MINIMA, "max_prof": PROFUNDIDAD_MAX_KM})

        # Mapeo oficial de macrozona para establecimientos
        def get_macro_zona(r):
            reg = (r or "").strip()
            if reg in ['Arica Parinacota', 'Arica y Parinacota', 'Tarapacá', 'Antofagasta', 'Atacama']:
                return 'Zona Norte'
            elif reg in ['Coquimbo', 'Valparaíso']:
                return 'Zona Centro-Norte'
            elif reg in ['Metropolitana de Santiago', "Libertador Gral. B. O'Higgins", "Libertador General Bernardo O'Higgins", 'Maule']:
                return 'Zona Central'
            elif reg in ['Ñuble', 'Biobío', 'La Araucanía', 'Los Ríos']:
                return 'Zona Sur'
            elif reg in ['Los Lagos', 'Aysén del General Carlos Ibañez del Campo', 'Magallanes y de la Antártica Chilena']:
                return 'Zona Austral'
            return 'Desconocida'

        df_estab["macro_zona"] = df_estab["region"].apply(get_macro_zona)

        # Cálculo Haversine de Exposición Espacial
        sismos_lat = df_sismos["latitud"].values
        sismos_lon = df_sismos["longitud"].values
        sismos_mag = df_sismos["magnitud"].values

        n_sismos_exp = []
        max_mag_exp = []

        for _, row in df_estab.iterrows():
            if len(sismos_lat) > 0:
                dist = haversine_vectorizado(row["latitud"], row["longitud"], sismos_lat, sismos_lon)
                cercanos = dist <= RADIO_KM_BASE
                cant = int(np.sum(cercanos))
                n_sismos_exp.append(cant)
                max_mag_exp.append(float(np.max(sismos_mag[cercanos])) if cant > 0 else 0.0)
            else:
                n_sismos_exp.append(0)
                max_mag_exp.append(0.0)

        df_estab["sismos_cercanos"] = n_sismos_exp
        df_estab["max_mag_cercana"] = max_mag_exp
        df_estab["esta_expuesto"] = df_estab["sismos_cercanos"] > 0

        # Resumen de Exposición por Macrozona
        res_exp = df_estab.groupby("macro_zona").agg(
            total_establecimientos=("codigo_deis", "count"),
            establecimientos_expuestos=("esta_expuesto", "sum"),
            max_magnitud_cercana=("max_mag_cercana", "max")
        ).reset_index()

        res_exp["pct_expuestos"] = ((res_exp["establecimientos_expuestos"] / res_exp["total_establecimientos"]) * 100).round(1)
        res_exp["poblacion"] = res_exp["macro_zona"].map(POBLACION_MACRO_ZONA).fillna(1)
        res_exp["estab_por_100k_hab"] = ((res_exp["total_establecimientos"] / res_exp["poblacion"]) * 100_000).round(1)
        res_exp["radio_km"] = RADIO_KM_BASE
        res_exp["magnitud_criterio"] = MAGNITUD_MINIMA

        orden_zonas = ["Zona Norte", "Zona Centro-Norte", "Zona Central", "Zona Sur", "Zona Austral"]
        res_exp = res_exp.set_index("macro_zona").reindex(orden_zonas).reset_index().fillna(0)

        # Guardar tabla de KPI de exposición en mart
        with engine.begin() as con:
            con.execute(text("""
                CREATE TABLE IF NOT EXISTS mart.kpi_exposicion_sismos (
                    macro_zona VARCHAR(50) PRIMARY KEY,
                    total_establecimientos INT,
                    establecimientos_expuestos INT,
                    pct_expuestos NUMERIC(5, 2),
                    max_magnitud_cercana NUMERIC(4, 2),
                    poblacion INT,
                    estab_por_100k_hab NUMERIC(6, 2),
                    radio_km NUMERIC(5, 1),
                    magnitud_criterio NUMERIC(3, 1),
                    fecha_calculo TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                );
            """))
            res_exp.to_sql("stg_kpi_exposicion", con, schema="staging", if_exists="replace", index=False)
            con.execute(text("""
                INSERT INTO mart.kpi_exposicion_sismos (
                    macro_zona, total_establecimientos, establecimientos_expuestos, pct_expuestos,
                    max_magnitud_cercana, poblacion, estab_por_100k_hab, radio_km, magnitud_criterio
                )
                SELECT macro_zona, total_establecimientos, establecimientos_expuestos, pct_expuestos,
                       max_magnitud_cercana, poblacion, estab_por_100k_hab, radio_km, magnitud_criterio
                FROM staging.stg_kpi_exposicion
                ON CONFLICT (macro_zona) DO UPDATE SET
                    total_establecimientos = EXCLUDED.total_establecimientos,
                    establecimientos_expuestos = EXCLUDED.establecimientos_expuestos,
                    pct_expuestos = EXCLUDED.pct_expuestos,
                    max_magnitud_cercana = EXCLUDED.max_magnitud_cercana,
                    poblacion = EXCLUDED.poblacion,
                    estab_por_100k_hab = EXCLUDED.estab_por_100k_hab,
                    radio_km = EXCLUDED.radio_km,
                    magnitud_criterio = EXCLUDED.magnitud_criterio,
                    fecha_calculo = CURRENT_TIMESTAMP;
            """))

        # Exportar CSV de resumen analítico
        res_exp.to_csv(SALIDA / "resumen_exposicion_sismos_salud.csv", index=False)

        # ------------------------------------------------------------------
        # 3. GENERACIÓN DE VISUALIZACIÓN MULTIPANEL (MAPA + BARRAS INDEPENDIENTES)
        # ------------------------------------------------------------------
        fig = plt.figure(figsize=(15, 8), constrained_layout=True)
        gs = fig.add_gridspec(2, 2, width_ratios=[1.2, 1.8])

        # Panel A: Mapa Geográfico de Exposición
        ax_mapa = fig.add_subplot(gs[:, 0])
        no_exp = df_estab[~df_estab["esta_expuesto"]]
        si_exp = df_estab[df_estab["esta_expuesto"]]

        ax_mapa.scatter(no_exp["longitud"], no_exp["latitud"], s=6, c="#4caf50", alpha=0.4, label="No expuesto (d > 50km)")
        ax_mapa.scatter(si_exp["longitud"], si_exp["latitud"], s=10, c="#d32f2f", alpha=0.7, label=f"Expuesto (d ≤ {int(RADIO_KM_BASE)}km)")
        
        if not df_sismos.empty:
            tam_sismos = (df_sismos["magnitud"] ** 2.3) * 1.5
            ax_mapa.scatter(df_sismos["longitud"], df_sismos["latitud"], s=tam_sismos, c="#ff9800", edgecolors="black", linewidth=0.5, alpha=0.6, label=f"Epicentros (M ≥ {MAGNITUD_MINIMA})")

        ax_mapa.set_title("A. Distribución Espacial y Exposición", fontsize=11, fontweight="bold")
        ax_mapa.set_xlabel("Longitud")
        ax_mapa.set_ylabel("Latitud")
        ax_mapa.set_xlim(-76, -66)
        ax_mapa.set_ylim(-56, -17)
        ax_mapa.legend(loc="lower left", fontsize=8)
        ax_mapa.grid(True, linestyle=":", alpha=0.5)

        # Panel B: % de Establecimientos Expuestos por Macrozona
        ax_pct = fig.add_subplot(gs[0, 1])
        y_pos = np.arange(len(res_exp))
        barras_pct = ax_pct.barh(y_pos, res_exp["pct_expuestos"], color="#d32f2f", height=0.55, alpha=0.85)
        ax_pct.set_yticks(y_pos)
        ax_pct.set_yticklabels(res_exp["macro_zona"], fontweight="bold")
        ax_pct.set_xlabel("% de Centros de Salud en Radio de Amenaza", fontsize=10)
        ax_pct.set_title("B. Porcentaje de Red de Salud Expuesta a Sismos", fontsize=11, fontweight="bold")
        ax_pct.set_xlim(0, max(res_exp["pct_expuestos"].max() * 1.25, 10))
        ax_pct.bar_label(barras_pct, fmt="%.1f%%", padding=4, fontsize=9, fontweight="bold")
        ax_pct.grid(axis="x", linestyle="--", alpha=0.5)

        # Panel C: Métrica Normalizada: Establecimientos por 100.000 Habitantes
        ax_norm = fig.add_subplot(gs[1, 1])
        barras_norm = ax_norm.barh(y_pos, res_exp["estab_por_100k_hab"], color="#1976d2", height=0.55, alpha=0.85)
        ax_norm.set_yticks(y_pos)
        ax_norm.set_yticklabels(res_exp["macro_zona"], fontweight="bold")
        ax_norm.set_xlabel("Establecimientos por cada 100.000 Habitantes (Censo / INE)", fontsize=10)
        ax_norm.set_title("C. Cobertura Sanitaria Normalizada por Población", fontsize=11, fontweight="bold")
        ax_norm.set_xlim(0, max(res_exp["estab_por_100k_hab"].max() * 1.25, 20))
        ax_norm.bar_label(barras_norm, fmt="%.1f", padding=4, fontsize=9, fontweight="bold")
        ax_norm.grid(axis="x", linestyle="--", alpha=0.5)

        # Título General y Nota de Transparencia Metodológica (Criterio D)
        fig.suptitle(f"Exposición Sísmica de Establecimientos de Salud por Macrozona (Chile)", fontsize=14, fontweight="bold", y=0.98)
        nota_metodologica = (
            f"Nota Metodológica: Exposición definida como proximidad (distancia Haversine ≤ {RADIO_KM_BASE} km) a eventos USGS con Magnitud ≥ {MAGNITUD_MINIMA}.\n"
            "Limitaciones: La exposición espacial NO equivale a vulnerabilidad estructural ni a pérdida de operatividad clínica hospitalaria.\n"
            "El catálogo USGS cubre un período muestral acotado y no reemplaza la zonificación sísmica de amenaza de largo plazo (Norma NCh433)."
        )
        fig.text(0.5, 0.01, nota_metodologica, ha="center", fontsize=8, style="italic", bbox=dict(boxstyle="round,pad=0.5", facecolor="#f5f5f5", edgecolor="#cccccc"))

        plt.savefig(SALIDA / "grafico_indicadores_sismos.png", dpi=300)
        plt.close()

        duracion = time.time() - t0
        run_id = context.get("run_id", "manual")
        p.registrar_auditoria("dag_03_indicadores", run_id, len(df_estab), len(res_exp), "SUCCESS", duracion)

    calcular_mart_y_exportar()

dag_03_indicadores()