import time
from datetime import datetime, timedelta
from airflow.sdk import dag, task

default_args = {
    "owner": "airflow",
    "retries": 1,
    "retry_delay": timedelta(seconds=10),
}

@dag(
    dag_id="dag_03_mart_kpis",
    default_args=default_args,
    start_date=datetime(2026, 1, 1),
    schedule=None,
    catchup=False,
    tags=["mart", "analitica", "kpis"],
)
def dag_03_mart_kpis():

    @task
    def generar_indicadores_mart(**context):
        import etl_sismos_salud as p
        t0 = time.time()
        
        kpis = p.build_mart_kpis()
        
        duracion = time.time() - t0
        run_id = context.get("run_id", "manual")
        p.registrar_auditoria("dag_03_mart_kpis", run_id, len(kpis), len(kpis), "SUCCESS", duracion)

    generar_indicadores_mart()

dag_03_mart_kpis()