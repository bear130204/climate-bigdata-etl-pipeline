from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime

with DAG(
    dag_id="weather_bigdata_etl",
    start_date=datetime(2025,1,1),
    schedule="*/1 * * * *",
    catchup=False,
    tags=["bigdata","weather"],
) as dag:

    extract = BashOperator(
        task_id="extract",
        bash_command='echo "Extract NOAA data"'
    )

    transform = BashOperator(
        task_id="transform",
        bash_command='echo "Transform with Spark"'
    )

    load = BashOperator(
        task_id="load_sql",
        bash_command='echo "Load SQL Server"'
    )

    visualization = BashOperator(
        task_id="visualization",
        bash_command='echo "Ready for dashboard"'
    )

    extract >> transform >> load >> visualization