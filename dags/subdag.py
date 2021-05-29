import datetime

from airflow import DAG
from airflow.operators.postgres_operator import PostgresOperator
from operators import LoadDimensionOperator
from airflow.utils.dates import days_ago


# Returns a DAG which creates a table if it does not exist, and then proceeds
# to load data into that table from S3. When the load is complete, a data
# quality  check is performed to assert that at least one row of data is
# present.
def load_dimensions_subdag(
        parent_dag_name,
        task_id,
        redshift_conn_id,
        dimension_tables_config,
        args, 
        append=True, 
        **kwargs):

    dag = DAG(
        dag_id=f"{parent_dag_name}.{task_id}",
        default_args=args,
        start_date=days_ago(2),
        schedule_interval="@daily",
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
