from datetime import datetime, timedelta
import os
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from operators import (
    DataQualityOperator,
    LoadDimensionOperator,
    LoadFactOperator,
    StageToRedshiftOperator,
)
from helpers import SqlQueries

AWS_KEY = os.environ.get('AWS_KEY')
AWS_SECRET = os.environ.get('AWS_SECRET')

default_args = {
    "owner": "udacity",
    "start_date": datetime.now(),
    "end_date": datetime.now() + timedelta(days=1),
    "depends_on_past": False,
    "retries": 3,
    "retry_delay": timedelta(minutes=5),
    "catchup": False,
    "email_on_retry": False,
}

dag = DAG(
    'sparkify_dag',
    default_args=default_args,
    description='Load and transform data in Redshift with Airflow',
    schedule_interval="@hourly",
)

start_operator = DummyOperator(task_id='Begin_execution', dag=dag)

stage_events_to_redshift = StageToRedshiftOperator(
    task_id='Stage_events',
    dag=dag,
    redshift_conn_id="redshift",
    aws_conn_id="aws_credentials",
    staging_table="staging_events",
    resource="s3://udacity-dend/log_data",
    format_sql="json 's3://udacity-dend/log_json_path.json'"
)

stage_songs_to_redshift = StageToRedshiftOperator(
    task_id='Stage_songs',
    dag=dag,
    redshift_conn_id="redshift",
    aws_conn_id="aws_credentials",
    staging_table="staging_songs",
    resource="s3://udacity-dend/song_data",
    format_sql="format as json 'auto'"
)

load_songplays_table = LoadFactOperator(
    task_id="Load_songplays_fact_table",
    dag=dag,
    redshift_conn_id="redshift",
    load_fact_sql=SqlQueries.songplay_table_insert,
    fact_table="songplays",
    id_column="playid"
)

load_user_dimension_table = LoadDimensionOperator(
    task_id='Load_user_dim_table',
    dag=dag,
    redshift_conn_id="redshift",
    load_dimension_sql=SqlQueries.user_table_insert,
    dimension_table="users"
)

load_song_dimension_table = LoadDimensionOperator(
    task_id='Load_song_dim_table',
    dag=dag,
    redshift_conn_id="redshift",
    load_dimension_sql=SqlQueries.song_table_insert,
    dimension_table="songs"
)

load_artist_dimension_table = LoadDimensionOperator(
    task_id='Load_artist_dim_table',
    dag=dag,
    redshift_conn_id="redshift",
    load_dimension_sql=SqlQueries.artist_table_insert,
    dimension_table="artists"
)

load_time_dimension_table = LoadDimensionOperator(
    task_id='Load_time_dim_table',
    dag=dag,
    redshift_conn_id="redshift",
    load_dimension_sql=SqlQueries.time_table_insert,
    dimension_table="time"
)

run_quality_checks = DataQualityOperator(
    task_id='Run_data_quality_checks',
    dag=dag,
    redshift_conn_id="redshift",
    query_results=[
        # Tables contain records
        (
            "SELECT count(*) FROM songplays",
            0,
            DataQualityOperator.EqOperation.GT
        ),
        (
            "SELECT count(*) FROM users",
            0,
            DataQualityOperator.EqOperation.GT
        ),
        (
            "SELECT count(*) FROM time",
            0,
            DataQualityOperator.EqOperation.GT
        ),
        (
            "SELECT count(*) FROM artists",
            0,
            DataQualityOperator.EqOperation.GT
        ),
        (
            "SELECT count(*) FROM songs",
            0,
            DataQualityOperator.EqOperation.GT
        ),
        # Check specific columns do not have null values
        (
            "SELECT count(*) FROM songs WHERE artistid IS NULL",
            0,
            DataQualityOperator.EqOperation.EQ
        ),
        (
            "SELECT count(*) FROM artists WHERE name IS NULL",
            0,
            DataQualityOperator.EqOperation.EQ
        ),
        (
            "SELECT count(*) FROM users WHERE first_name IS NULL",
            0,
            DataQualityOperator.EqOperation.EQ
        ),
        (
            "SELECT count(*) FROM songplays WHERE sessionid IS NULL",
            0,
            DataQualityOperator.EqOperation.EQ
        ),
        (
            "SELECT count(*) FROM songplays WHERE userid IS NULL",
            0,
            DataQualityOperator.EqOperation.EQ
        ),
        (
            "SELECT count(*) FROM time WHERE hour IS NULL",
            0,
            DataQualityOperator.EqOperation.EQ
        )
    ]
)

end_operator = DummyOperator(task_id='Stop_execution',  dag=dag)

start_operator >> stage_events_to_redshift
start_operator >> stage_songs_to_redshift

stage_events_to_redshift >> load_songplays_table
stage_songs_to_redshift >> load_songplays_table

load_songplays_table >> load_user_dimension_table
load_songplays_table >> load_song_dimension_table
load_songplays_table >> load_artist_dimension_table
load_songplays_table >> load_time_dimension_table

load_user_dimension_table >> run_quality_checks
load_song_dimension_table >> run_quality_checks
load_artist_dimension_table >> run_quality_checks
load_time_dimension_table >> run_quality_checks

run_quality_checks >> end_operator