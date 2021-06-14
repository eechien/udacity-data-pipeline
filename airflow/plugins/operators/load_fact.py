from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class LoadFactOperator(BaseOperator):

    ui_color = '#F98866'

    @apply_defaults
    def __init__(
        self,
        redshift_conn_id="",
        load_fact_sql="",
        fact_table="",
        id_column="",
        *args,
        **kwargs
    ):

        super(LoadFactOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.load_fact_sql = load_fact_sql
        self.fact_table = fact_table
        self.id_column = id_column

    def execute(self, context):
        redshift_hook = PostgresHook(self.redshift_conn_id)
        redshift_hook.run(
            f"""
            INSERT INTO {self.fact_table}
            {self.load_fact_sql}
            WHERE {self.id_column} NOT IN (SELECT {self.id_column} FROM {self.fact_table})
            """
        )
        self.log.info(f"Loaded rows into fact table {self.fact_table}")
