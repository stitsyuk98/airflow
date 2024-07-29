import logging
from airflow.models.baseoperator import BaseOperator
from airflow.utils.decorators import apply_defaults
from hooks.clickhouse_hook import ClickhouseHook


class ClickhouseOperator(BaseOperator):
    template_fields = ("sql",)
    template_fields_renderers = {"sql": "sql"}
    template_ext = (".sql",)

    def __init__(self, conn_id: str, sql: str, **kwargs) -> None:
        super().__init__(**kwargs)
        self.conn_id = conn_id
        self.sql = sql
        logging.info(self.sql)

    def execute(self, context):
        hook = ClickhouseHook(conn_id=self.conn_id)
        result = hook.run(self.sql)
