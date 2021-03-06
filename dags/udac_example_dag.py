from datetime import datetime, timedelta

from airflow import DAG
from airflow.models import Variable
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.postgres_operator import PostgresOperator
from airflow.operators.subdag_operator import SubDagOperator
from helpers import SqlQueries
from operators import ( LoadFactOperator, GenericDataQualityOperator,
                       StageToRedshiftOperator)

from subdag import load_dimensions_subdag

DAG_ID = 'udac_example_dag'
START_DATE = datetime(2020, 1, 12)
APPEND = Variable.get("udac_example_dag.append", False)

default_args = {
    'owner': 'udacity',
    'depends_on_past': False,
    'start_date': START_DATE,
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
    'catchup': False
}

dag = DAG(DAG_ID,
          default_args=default_args,
          description='Load and transform data in Redshift with Airflow',
          schedule_interval='@hourly',
          max_active_runs=1
          )


start_operator = DummyOperator(task_id='Begin_execution',  dag=dag)
# added as described here: https://knowledge.udacity.com/questions/163614
create_tables_task = PostgresOperator(
    task_id="create_tables",
    dag=dag,
    sql='create_tables.sql',
    postgres_conn_id="redshift"
)

# stage_events_to_redshift = DummyOperator(task_id='stage_events_to_redshift',  dag=dag)
stage_events_to_redshift = StageToRedshiftOperator(
    task_id='Stage_events',
    dag=dag,
    table="staging_events",
    redshift_conn_id="redshift",
    aws_credentials_id="aws_credentials",
    s3_bucket="mm-udacity-dend",
    s3_key="log_data",
    region="eu-west-1",
    json_path="s3://mm-udacity-dend/log_json_path.json"
)

# stage_songs_to_redshift = DummyOperator(task_id='stage_songs_to_redshift',  dag=dag)
stage_songs_to_redshift = StageToRedshiftOperator(
    task_id='Stage_songs',
    dag=dag,
    table="staging_songs",
    redshift_conn_id="redshift",
    aws_credentials_id="aws_credentials",
    s3_bucket="mm-udacity-dend",
    s3_key="song_data",
    region="eu-west-1",
)

load_songplays_table = LoadFactOperator(
    task_id='Load_songplays_fact_table',
    dag=dag,
    postgres_conn_id="redshift",
    table="songplays",
    sql=SqlQueries.songplay_table_insert
)

dimension_tables_config = {
    "users": SqlQueries.user_table_insert,
    "songs": SqlQueries.song_table_insert,
    "artists": SqlQueries.artist_table_insert,
    "time": SqlQueries.time_table_insert
}

load_dimension_tables = SubDagOperator(
    subdag=load_dimensions_subdag(
        f'{DAG_ID}',
        'load_dimension_tables',
        "redshift",
        dimension_tables_config,
        start_date=START_DATE,
        schedule_interval="@hourly",
        append=APPEND,
        args=default_args
    ),
    task_id='load_dimension_tables',
    default_args=default_args,
    dag=dag
)

data_quality_checks = [
    # check table contents
    {'test_sql': "SELECT COUNT(*) FROM users", 'expected_result': 0, "comparison": '>'},
    {'test_sql': "SELECT COUNT(*) FROM songs", 'expected_result': 0, "comparison": '>'},
    {'test_sql': "SELECT COUNT(*) FROM artists", 'expected_result': 0, "comparison": '>'},
    {'test_sql': "SELECT COUNT(*) FROM time", 'expected_result': 0, "comparison": '>'},
    # check doubles in primary keys
    {'test_sql': GenericDataQualityOperator.DBL_VALUES_TEMPLATE.format(table='users',pk_cols='userid, "level"'), 'expected_result': 0, "comparison": 'none'},
    # check nulls in primary keys
    {'test_sql': "SELECT COUNT(*) FROM users WHERE userid is NULL or level is NULL", 'expected_result': 0, "comparison": '='},
]

run_quality_checks = GenericDataQualityOperator(
    task_id='Run_data_quality_checks',
    redshift_conn_id="redshift",
    checks=data_quality_checks,
    dag=dag
)

end_operator = DummyOperator(task_id='Stop_execution',  dag=dag)

start_operator >> create_tables_task

create_tables_task >> stage_events_to_redshift >> load_songplays_table
create_tables_task >> stage_songs_to_redshift >> load_songplays_table

load_songplays_table >> load_dimension_tables >> run_quality_checks
run_quality_checks >> end_operator
