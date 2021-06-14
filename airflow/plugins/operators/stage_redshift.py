from airflow.contrib.hooks.aws_hook import AwsHook
from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class StageToRedshiftOperator(BaseOperator):
    ui_color = '#358140'

    @apply_defaults
    def __init__(
        self,
        redshift_conn_id="",
        aws_conn_id="",
        staging_table="",
        resource="",
        format_sql="",
        append_only=False,
        *args,
        **kwargs
    ):

        super(StageToRedshiftOperator, self).__init__(*args, **kwargs)
        self.aws_conn_id = aws_conn_id
        self.redshift_conn_id = redshift_conn_id
        self.staging_table = staging_table
        self.resource = resource
        self.format_sql = format_sql
        self.append_only = append_only

    def execute(self, context):
        redshift_hook = PostgresHook(self.redshift_conn_id)
        aws_hook = AwsHook(self.aws_conn_id)
        credentials = aws_hook.get_credentials()
        if not self.append_only:
            redshift_hook.run(f"TRUNCATE {self.staging_table}")
        redshift_hook.run(
            f"""
            COPY {self.staging_table} FROM '{self.resource}'
            CREDENTIALS 'aws_access_key_id={credentials.access_key};aws_secret_access_key={credentials.secret_key}'
            {self.format_sql}
            COMPUPDATE off
            REGION 'us-west-2';
            """
        )
        self.log.info(f"Copied {self.staging_table} to Redshift")
