import time
from datetime import datetime
from pathlib import Path
from airflow.sdk import dag, task

BASE = Path(__file__).parent
SALIDA = BASE / "salida"
SALIDA.mkdir(exist_ok=True)

@dag(
    dag_id="dag_03_indicadores",
    start_date=datetime(2026, 1, 1),
    schedule=None,
    catchup=False,
    tags=["mart", "indicadores", "reportes"],
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

        # 1. Poblar el Datamart en PostgreSQL (4 a 6 indicadores en SQL)
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

        # 2. Generar el gráfico explicativo comparativo (Sismos vs Centros de Salud)
        if not kpis.empty:
            resumen = kpis.groupby("macro_zona").agg({
                "total_sismos": "sum",
                "total_hospitales": "max"
            })
            
            orden_zonas = ["Zona Austral", "Zona Sur", "Zona Central", "Zona Centro-Norte", "Zona Norte"]
            resumen = resumen.reindex([z for z in orden_zonas if z in resumen.index]).fillna(0)

            zonas = resumen.index
            y = np.arange(len(zonas))
            alto = 0.35

            fig, ax = plt.subplots(figsize=(10, 6))

            barras_sismos = ax.barh(y - alto/2, resumen["total_sismos"], alto, label="Total Sismos", color="#e65100")
            barras_salud = ax.barh(y + alto/2, resumen["total_hospitales"], alto, label="Establecimientos Salud (DEIS)", color="#1976d2")

            ax.bar_label(barras_sismos, padding=3, fontsize=9, fontweight="bold")
            ax.bar_label(barras_salud, padding=3, fontsize=9, fontweight="bold")

            ax.set_title("Vulnerabilidad Sísmica vs. Cobertura de Establecimientos de Salud", fontsize=12, pad=15)
            ax.set_xlabel("Cantidad de Registros")
            ax.set_ylabel("Macro Zona")
            ax.set_yticks(y)
            ax.set_yticklabels(zonas)
            ax.legend(loc="lower right")
            ax.grid(axis="x", linestyle="--", alpha=0.5)

            plt.tight_layout()
            plt.savefig(SALIDA / "grafico_indicadores_sismos.png", dpi=300)
            plt.close()

        duracion = time.time() - t0
        run_id = context.get("run_id", "manual")
        p.registrar_auditoria("dag_03_indicadores", run_id, len(tablon), len(kpis), "SUCCESS", duracion)

    calcular_mart_y_exportar()

dag_03_indicadores()