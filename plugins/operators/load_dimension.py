from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class LoadDimensionOperator(BaseOperator):

    ui_color = '#80BD9E'

    sql_template = "INSERT INTO {} {}"

    @apply_defaults
    def __init__(self,
                 postgres_conn_id="redshift",
                 table="",
                 sql="",
                 *args, **kwargs):

        super(LoadDimensionOperator, self).__init__(*args, **kwargs)
        self.postgres_conn_id=postgres_conn_id
        self.table=table
        self.sql=sql

    def execute(self, context):
        redshift = PostgresHook(postgres_conn_id=self.postgres_conn_id)
        # TODO: clear table before running sql? Load partitioned?
        redshift.run(LoadDimensionOperator.sql_template.format(self.table, self.sql))
