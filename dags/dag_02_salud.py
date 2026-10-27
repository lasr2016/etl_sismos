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
    dag_id="dag_02_salud",
    default_args=default_args,
    start_date=datetime(2026, 1, 1),
    schedule=None,
    catchup=False,
    tags=["fuente_deis", "salud", "dw"],
)
def dag_02_salud():

    @task
    def inicializar_bd():
        import etl_sismos_salud as p
        p.init_database()

    @task
    def extract_salud():
        import etl_sismos_salud as p
        salud_raw = p.extract_deis_establecimientos()
        _guardar(salud_raw, "salud_raw")

    @task
    def transform_salud():
        import etl_sismos_salud as p
        salud_raw = _leer("salud_raw")
        dim_salud = p.transform_establecimientos(salud_raw)
        _guardar(dim_salud, "dim_salud")

    @task
    def load_salud_dw(**context):
        import etl_sismos_salud as p
        import pandas as pd
        t0 = time.time()
        
        dim_salud = _leer("dim_salud")
        # Inserta solo la dimensión salud
        cargadas = p.load_warehouse_salud(dim_salud)
        
        duracion = time.time() - t0
        run_id = context.get("run_id", "manual")
        p.registrar_auditoria("dag_02_salud", run_id, len(dim_salud), cargadas, "SUCCESS", duracion)

    inicializar_bd() >> extract_salud() >> transform_salud() >> load_salud_dw()

dag_02_salud()