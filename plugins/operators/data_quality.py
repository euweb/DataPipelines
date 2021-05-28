from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class DataQualityOperator(BaseOperator):

    ui_color = '#89DA59'

    @apply_defaults
    def __init__(self,
                 # Define your operators params (with defaults) here
                 # Example:
                 # conn_id = your-connection-name
                 *args, **kwargs):

        super(DataQualityOperator, self).__init__(*args, **kwargs)
        # Map params here
        # Example:
        # self.conn_id = conn_id

    def execute(self, context):
        ''' TODO 
            A task using the data quality operator is in the DAG and at least one data quality check is done
                Data quality check is done with correct operator
            The operator raises an error if the check fails pass
                The DAG either fails or retries n times
            The operator is parametrized
                Operator uses params to get the tests and the results, tests are not hard coded to the operator
	    '''
        self.log.info('DataQualityOperator not implemented yet')