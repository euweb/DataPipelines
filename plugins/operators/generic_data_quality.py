from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
import logging
import operator


def get_truth(a, op, b):
    """compares tow values without using eval()

    Args:
        a (Any): first value to compare
        op (str): compare operator
        b (Any): second value to compare

    Returns:
        [type]: [description]
    """
    ops = {'>': operator.gt,
           '<': operator.lt,
           '>=': operator.ge,
           '<=': operator.le,
           '=': operator.eq,
           '!=': operator.ne}
    return ops[op](a, b)


class GenericDataQualityOperator(BaseOperator):
    """Implements data quality checks for given tables
    Possible checks are:
        - check for not empty tables
        - check duplicate values for primary keys

    Raises:
        ValueError: if quality check fails

    Returns: none
    """

    ui_color = '#89DA59'

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

    def __init__(self,
                 redshift_conn_id,
                 checks,
                 *args, **kwargs):
        """Initializes the operator

        Args:
            redshift_conn_id (str): name of the connection created in Airflow->Admin->Connections
            checks (array(dict)): array of checks like 
                {'test_sql': "SELECT COUNT(*) FROM ...", 'expected_result': 0, "comparison": '>'}
        """
        super(GenericDataQualityOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.checks = checks

    def execute(self, context):
        """executs checks for tables

        Args:
            context: airflow execution context

        Raises:
            ValueError: raised if any check fails
        """
        redshift_hook = PostgresHook(self.redshift_conn_id)
        for check in self.checks:
            test_sql = check['test_sql']
            expected_result = check['expected_result']
            comparison = check['comparison']
            records = redshift_hook.get_records(test_sql)
            if comparison == 'none':
                # no result expected
                if len(records) < 1 or len(records[0]) < 1:
                    logging.info(
                        f"Data quality check {test_sql} passed. No rows returned.")
                else:
                    raise ValueError(
                        f"Data quality check failed. Unexpected return {records[0][0]} for {test_sql}")
            else:
                # compare expected and found values
                if len(records) < 1 or len(records[0]) < 1:
                    raise ValueError(
                        f"Data quality check failed. {test_sql} returned no results")
                if get_truth(records[0][0], comparison, expected_result):
                    logging.info(
                        f"Data quality check {test_sql} passed")
                else:
                    raise ValueError(
                        f"Data quality check failed. Expected {expected_result} for {test_sql} but got {records[0][0]}")
