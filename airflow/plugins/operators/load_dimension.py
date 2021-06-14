from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class LoadDimensionOperator(BaseOperator):

    ui_color = '#80BD9E'

    @apply_defaults
    def __init__(
        self,
        redshift_conn_id="",
        load_dimension_sql="",
        dimension_table="",
        append_only=False,
        *args,
        **kwargs
    ):
        super(LoadDimensionOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.load_dimension_sql = load_dimension_sql
        self.dimension_table = dimension_table
        self.append_only = append_only

    def execute(self, context):
        redshift_hook = PostgresHook(self.redshift_conn_id)
        if not self.append_only:
            redshift_hook.run(
                f"TRUNCATE {self.dimension_table}"
            )
        redshift_hook.run(
            f"""
            INSERT INTO {self.dimension_table}
            {self.load_dimension_sql}
            """
        )
        self.log.info(f"Loaded data into dimension table {self.dimension_table}")
