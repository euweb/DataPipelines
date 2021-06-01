import datetime

from airflow import DAG
from airflow.operators.postgres_operator import PostgresOperator
from operators import LoadDimensionOperator
from airflow.utils.dates import days_ago

def load_dimensions_subdag(
        parent_dag_name,
        task_id,
        redshift_conn_id,
        dimension_tables_config,
        args,
        append=True,
        **kwargs):
    """executes LoadDimensionOperator for every table defined in dimension_tables_config

    Args:
        parent_dag_name (str): name of the parent DAG
        task_id (str): task id of the parent DAG
        redshift_conn_id (str): name of the connection created in Airflow
        dimension_tables_config (dict): structure containing tables and their insert sql statements
        args: default_args
        append (bool, optional): if false the tables will be truncated before insert new rows. Defaults to True.

    Returns:
        DAG: DAG with LoadDimensionOperator for each table
    """
    dag = DAG(
        dag_id=f"{parent_dag_name}.{task_id}",
        default_args=args,
        start_date=days_ago(2),
        schedule_interval="@daily",
        max_active_runs=1
    )

    for table in dimension_tables_config:
        sql = dimension_tables_config[table]
        LoadDimensionOperator(
            task_id=f'Load_{table}_dim_table',
            dag=dag,
            postgres_conn_id=redshift_conn_id,
            table=table,
            append=append,
            sql=sql
        )

    return dag
