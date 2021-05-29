from datetime import datetime, timedelta
import os
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.postgres_operator import PostgresOperator
from airflow.operators.subdag_operator import SubDagOperator

from operators import (StageToRedshiftOperator, LoadFactOperator,
                       DataQualityOperator)
from helpers import SqlQueries

from subdag import load_dimensions_subdag

# AWS_KEY = os.environ.get('AWS_KEY')
# AWS_SECRET = os.environ.get('AWS_SECRET')
DAG_ID = 'udac_example_dag5'
START_DATE = datetime(2020, 1, 12)
APPEND = False

'''
    TODO
    - Simple and dynamic operators, as little hard coding as possible
    - Effective use of parameters in tasks
    - Clean formatting of values in SQL strings
    + Load dimensions with a subdag
'''

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
          schedule_interval='@once'  # TODO run once an hour
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
        schedule_interval="@daily",
        append=APPEND,
        args=default_args
    ),
    task_id='load_dimension_tables',
    default_args=default_args,
    dag=dag
)

start_operator = DummyOperator(task_id='Begin_execution',  dag=dag)
#create_tables_task = DummyOperator(task_id='create_tables',  dag=dag)
# added as described here: https://knowledge.udacity.com/questions/163614
create_tables_task = PostgresOperator(
    task_id="create_tables",
    dag=dag,
    sql='create_tables.sql',
    postgres_conn_id="redshift"
)

#stage_events_to_redshift = DummyOperator(task_id='stage_events_to_redshift',  dag=dag)
stage_events_to_redshift = StageToRedshiftOperator(
    task_id='Stage_events',
    dag=dag,
    table="staging_events",
    redshift_conn_id="redshift",
    aws_credentials_id="aws_credentials",
    s3_bucket="mm-udacity-dend",
    s3_key="log_data",
    json_path="s3://mm-udacity-dend/log_json_path.json"
)

#stage_songs_to_redshift = DummyOperator(task_id='stage_songs_to_redshift',  dag=dag)
stage_songs_to_redshift = StageToRedshiftOperator(
    task_id='Stage_songs',
    dag=dag,
    table="staging_songs",
    redshift_conn_id="redshift",
    aws_credentials_id="aws_credentials",
    s3_bucket="mm-udacity-dend",
    s3_key="song_data"
)

load_songplays_table = LoadFactOperator(
    task_id='Load_songplays_fact_table',
    dag=dag,
    postgres_conn_id="redshift",
    table="songplays",
    sql=SqlQueries.songplay_table_insert
)

# TODO - Subdag for loading dimensions

# load_user_dimension_table = LoadDimensionOperator(
#     task_id='Load_user_dim_table',
#     dag=dag,
#     postgres_conn_id="redshift",
#     table="users",
#     sql=SqlQueries.user_table_insert
# )

# load_song_dimension_table = LoadDimensionOperator(
#     task_id='Load_song_dim_table',
#     dag=dag,
#     postgres_conn_id="redshift",
#     table="songs",
#     sql=SqlQueries.song_table_insert
# )

# load_artist_dimension_table = LoadDimensionOperator(
#     task_id='Load_artist_dim_table',
#     dag=dag,
#     postgres_conn_id="redshift",
#     table="artists",
#     sql=SqlQueries.artist_table_insert
# )

# load_time_dimension_table = LoadDimensionOperator(
#     task_id='Load_time_dim_table',
#     dag=dag,
#     postgres_conn_id="redshift",
#     table="time",
#     sql=SqlQueries.time_table_insert
# )

run_quality_checks = DataQualityOperator(
    task_id='Run_data_quality_checks',
    dag=dag
)

end_operator = DummyOperator(task_id='Stop_execution',  dag=dag)

start_operator >> create_tables_task

create_tables_task >> stage_events_to_redshift >> load_songplays_table
create_tables_task >> stage_songs_to_redshift >> load_songplays_table

# load_songplays_table >> load_song_dimension_table >> run_quality_checks
# load_songplays_table >> load_user_dimension_table >> run_quality_checks
# load_songplays_table >> load_artist_dimension_table >> run_quality_checks
# load_songplays_table >> load_time_dimension_table >> run_quality_checks
load_songplays_table >> load_dimension_tables >> run_quality_checks
run_quality_checks >> end_operator
