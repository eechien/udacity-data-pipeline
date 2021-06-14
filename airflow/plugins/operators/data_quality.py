from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from enum import Enum

class DataQualityOperator(BaseOperator):
    class EqOperation(Enum):
        LT = "lt"
        LTE = "lte"
        GT = "gt"
        GTE = "gte"
        EQ = "eq"
        NE = "ne"

    
    ui_color = '#89DA59'

    @apply_defaults
    def __init__(
        self,
        redshift_conn_id="",
        query_results=[],
        *args,
        **kwargs
    ):

        super(DataQualityOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.query_results = query_results

    def execute(self, context):
        redshift_hook = PostgresHook(self.redshift_conn_id)
        for query, result, operator in self.query_results:
            self.log.info(f"Data quality check: {query}")
            records = redshift_hook.get_records(query)
            if len(records) < 1 or len(records[0]) < 1:
                raise ValueError("Data quality check failed. No results.")
            try:
                if operator == DataQualityOperator.EqOperation.LT:
                    assert records[0][0] < result
                elif operator == DataQualityOperator.EqOperation.LTE:
                    assert records[0][0] <= result
                elif operator == DataQualityOperator.EqOperation.GT:
                    assert records[0][0] > result
                elif operator == DataQualityOperator.EqOperation.GTE:
                    assert records[0][0] >= result
                elif operator == DataQualityOperator.EqOperation.EQ:
                    assert records[0][0] == result
                else:
                    assert records[0][0] != result
            except Exception as e:
                raise ValueError(f"Data quality check failed. {e}")
                
        self.log.info("Data quality check passed.")
