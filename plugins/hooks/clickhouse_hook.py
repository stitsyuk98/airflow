import logging
from clickhouse_driver import Client
from airflow.exceptions import AirflowException
from airflow.hooks.base import BaseHook

class ClickhouseHook(BaseHook):

    def __init__(self, conn_id: str):
        super().__init__()
        self.conn_id = conn_id

    def get_client(self):
        conn = self.get_connection(self.conn_id)
        client = Client(host=conn.host,
                    user=conn.login,
                    password=conn.password,
                    port=conn.port,
                    secure=True,
                    verify=True,
                    ca_certs='/opt/airflow/plugins/certificates/RootCA.crt')
        logging.info(f"created client to {conn.conn_type} on host {conn.host} for user {conn.login}")
        return client

    def run(self, sql):
        client = self.get_client()
        result = client.execute(sql)
        logging.info(f"executed sql:\n{sql}")
        client.disconnect_connection()
        return result

    def insert_dataframe(self, df, sql, settings=None):
        client = self.get_client()
        inserted_rows = client.insert_dataframe(
            sql,
            df,
            settings=settings,
        )
        logging.info(f"inserted_rows: {inserted_rows}\nexecuted sql:\n{sql}")
        client.disconnect_connection()
        return inserted_rows
    