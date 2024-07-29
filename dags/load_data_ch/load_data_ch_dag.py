from airflow.decorators import dag, task
from load_data_ch.scripts.delete_file import delete_file
from load_data_ch.scripts.load_data_ch import load_data_ch
from load_data_ch.scripts.download_file import download_file
from operators.clickhouse_operator import ClickhouseOperator


URL = 'https://datasets.clickhouse.com/cell_towers.csv.xz'
SOURCE_PATH = 'cell_towers.csv'


@dag(dag_id="load_data_ch",
    schedule=None,
    catchup=False,
    tags=["clickhouse"],
    template_searchpath = ['/opt/airflow/dags/load_data_ch'])
def etl():
    @task(task_id="load_data_file")
    def load_data_file(url, file_path) -> str:
        download_file(url, file_path)
    run_load_data_file = load_data_file(url=URL, file_path = SOURCE_PATH)

    ch_create_table = ClickhouseOperator(
        task_id="ch_create_table", 
        sql="scripts/create_table.sql", 
        conn_id="clickhouse"
        )

    @task(task_id="load_data_ch")
    def task_load_data_ch(source_path, **kwargs):
        load_data_ch(source_path)
    run_task_load_data_ch = task_load_data_ch(SOURCE_PATH)

    @task(task_id="delete_local_file")
    def delete_local_file(source_path, **kwargs):
        delete_file(source_path)
    run_delete_local_file = delete_local_file(SOURCE_PATH)

    run_load_data_file >> ch_create_table >> run_task_load_data_ch >> run_delete_local_file

etl()