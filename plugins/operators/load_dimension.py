from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class LoadDimensionOperator(BaseOperator):
    """
    Loads data into dimension tables
    """
    ui_color = '#80BD9E'

    sql_insert_template = "INSERT INTO {} {}"
    sql_truncate_template = "TRUNCATE TABLE {}"

    @apply_defaults
    def __init__(self,
                 postgres_conn_id="redshift",
                 table="",
                 sql="",
                 append=True,
                 *args, **kwargs):
        """Initializes the operator

        Args:
            postgres_conn_id (str): name of the connection created in Airflow.
            table (str): destination table name
            sql (str): sql string for selecting data from source table
            append (bool, optional): if false the table will be truncated before insert new rows. Defaults to True.
        """
        super(LoadDimensionOperator, self).__init__(*args, **kwargs)
        self.postgres_conn_id=postgres_conn_id
        self.table=table
        self.sql=sql
        self.append=append

    def execute(self, context):
        redshift = PostgresHook(postgres_conn_id=self.postgres_conn_id)
        if(self.append):
            self.log.info("Truncating table {}".format(self.table))
            redshift.run(LoadDimensionOperator.sql_truncate_template.format(self.table))
      
        self.log.info("Inserting data into table {}".format(self.table))
        redshift.run(LoadDimensionOperator.sql_insert_template.format(self.table, self.sql))
