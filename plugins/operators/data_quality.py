from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
import logging


class DataQualityOperator(BaseOperator):

    CHECK_DBL_VALUES = 'check_double_values'
    CHECK_COUNT = 'check_count'

    ui_color = '#89DA59'

    PK_TEMPLATE = '''
    select tco.constraint_schema, kcu.column_name as key_column
    from information_schema.table_constraints tco
    join information_schema.key_column_usage kcu 
        on kcu.constraint_name = tco.constraint_name
        and kcu.constraint_schema = tco.constraint_schema
        and kcu.constraint_name = tco.constraint_name
    where tco.constraint_type = 'PRIMARY KEY' AND kcu.table_name='{}'
    order by tco.constraint_schema,
        tco.constraint_name,
        kcu.ordinal_position;
    '''

    DBL_VALUES_TEMPLATE = '''
    SELECT {pk_cols}, count_total
        FROM (
            SELECT {pk_cols}, SUM(1) AS count_total
            FROM {table}
            GROUP BY {pk_cols}
        )
        WHERE count_total > 1
        LIMIT 1
    '''

    @apply_defaults
    def __init__(self,
                 redshift_conn_id,
                 tables,
                 checks,
                 *args, **kwargs):

        super(DataQualityOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.tables = tables
        self.checks = checks

    def get_pks(self, table):
        redshift_hook = PostgresHook(self.redshift_conn_id)
        records = redshift_hook.get_records(
            DataQualityOperator.PK_TEMPLATE.format(table))
        if len(records) < 1 or len(records[0]) < 1:
            logging.info(
                f"No primary key for table {table} - nothing to check")
        else:
            columns = []
            for row in records:
                columns.append(row[1])
            return columns

    def execute(self, context):
        ''' TODO 
            A task using the data quality operator is in the DAG and at least one data quality check is done
                Data quality check is done with correct operator
            The operator raises an error if the check fails pass
                The DAG either fails or retries n times
            The operator is parametrized
                Operator uses params to get the tests and the results, tests are not hard coded to the operator
            '''
        for check in self.checks:
            if(check == DataQualityOperator.CHECK_COUNT):
                redshift_hook = PostgresHook(self.redshift_conn_id)
                for table in self.tables:
                    records = redshift_hook.get_records(
                        f"SELECT COUNT(*) FROM {table}")
                    if len(records) < 1 or len(records[0]) < 1:
                        raise ValueError(
                            f"Data quality check failed. {table} returned no results")
                    num_records = records[0][0]
                    if num_records < 1:
                        raise ValueError(
                            f"Data quality check failed. {table} contained 0 rows")
                    logging.info(
                        f"Data quality on table {table} check passed with {records[0][0]} records")
            elif(check == DataQualityOperator.CHECK_DBL_VALUES):
                redshift_hook = PostgresHook(self.redshift_conn_id)
                for table in self.tables:
                    pks = self.get_pks(table)
                    logging.debug("primary keys {pks} in table {table}")
                    if pks:
                        pk_cols = ','.join(pks)
                        records = redshift_hook.get_records(DataQualityOperator.DBL_VALUES_TEMPLATE.format(
                            table=table,
                            pk_cols=pk_cols
                        ))
                        if len(records) > 0:
                            raise ValueError(
                                f"Data quality check failed. There are rows with duplicate primary keys of table {table}")
                        logging.info(
                            f"Data quality on table {table} check passed without duplicate primary keys")
            else:
                logging.warn("Data quality check. Unknown check {check}")
