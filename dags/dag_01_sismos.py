import time
from datetime import datetime, timedelta
from pathlib import Path
from airflow.sdk import dag, task

TMP = Path(__file__).parent / "salida" / "tmp"

def _guardar(obj, nombre):
    import pandas as pd
    TMP.mkdir(parents=True, exist_ok=True)
    pd.to_pickle(obj, TMP / f"{nombre}.pkl")

def _leer(nombre):
    import pandas as pd
    return pd.read_pickle(TMP / f"{nombre}.pkl")

default_args = {
    "owner": "airflow",
    "retries": 2,
    "retry_delay": timedelta(seconds=15),
}

@dag(
    dag_id="dag_01_sismos",
    default_args=default_args,
    start_date=datetime(2026, 1, 1),
    schedule=None,
    catchup=False,
    tags=["fuente_usgs", "sismos", "dw"],
)
def dag_01_sismos():

    @task
    def inicializar_bd():
        import etl_sismos_salud as p
        p.init_database()

    @task
    def extract_sismos():
        import etl_sismos_salud as p
        sismos_raw = p.extract_usgs_sismos(start_date="2024-01-01", end_date="2024-03-31")
        _guardar(sismos_raw, "sismos_raw")

    @task
    def transform_sismos():
        import etl_sismos_salud as p
        sismos_raw = _leer("sismos_raw")
        dim_tiempo = p.build_dim_tiempo(2024)
        dim_geo = p.build_dim_geografia()
        fact_sismos = p.transform_sismos(sismos_raw)

        _guardar(dim_tiempo, "dim_tiempo")
        _guardar(dim_geo, "dim_geo")
        _guardar(fact_sismos, "fact_sismos")

    @task
    def validar_calidad():
        import etl_sismos_salud as p
        p.check_calidad(_leer("fact_sismos"), _leer("dim_tiempo"))

    @task
    def load_dw(**context):
        import etl_sismos_salud as p
        import pandas as pd
        t0 = time.time()
        
        dim_tiempo = _leer("dim_tiempo")
        dim_geo = _leer("dim_geo")
        fact_sismos = _leer("fact_sismos")
        
        # Carga idempotente en dw
        cargadas = p.load_warehouse(dim_tiempo, dim_geo, pd.DataFrame(), fact_sismos)
        
        duracion = time.time() - t0
        run_id = context.get("run_id", "manual")
        p.registrar_auditoria("dag_01_sismos", run_id, len(fact_sismos), cargadas, "SUCCESS", duracion)

    inicializar_bd() >> extract_sismos() >> transform_sismos() >> validar_calidad() >> load_dw()

dag_01_sismos()