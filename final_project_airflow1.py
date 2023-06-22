import datetime
import pendulum
import os
from airflow.decorators import dag,task
from airflow.operators.dummy_operator import DummyOperator
from final_project_operators.stage_redshift import StageToRedshiftOperator
from final_project_operators.load_fact import LoadFactOperator
from final_project_operators.load_dimension import LoadDimensionOperator
from final_project_operators.data_quality import DataQualityOperator
from udacity.common.final_project_sql_statements import SqlQueries

default_args = {
    'owner': 'udacity',
    'start_date': datetime.datetime(2018, 11, 1),
    'end_date': datetime.datetime(2018, 11, 30),
    'depends_on_past' : False,
    'retries' : 3,
    'retry_delay' : datetime.timedelta(minutes=5),
    'email_on_retry' : False,
    'catchup' : False,
    'provide_context': True,
    'description' : "DAG for project"}


#defining constants
REGION = "us-east-1"
#tables = [staging_events,staging_songs,songplay, user_table, song, artist, time_table]

@dag(
    default_args = default_args,
    schedule_interval = '@daily',
    # max_active_runs=1
)

def final_project():

    start_operator = DummyOperator(
        task_id='Begin_execution'
    )

   

    stage_events_to_redshift = StageToRedshiftOperator(
        task_id='Stage_events',
        redshift_conn_id = "redshift",
        aws_credentials_id = "aws_credentials",
        table = "staging_events",
        s3_bucket = "airflow-joerivlieghe",
        s3_key = "log-data",
        create_table = SqlQueries.staging_events_table_create,
        format = "json 's3://airflow-joerivlieghe/json-path/log_json_path.json'",
        region = REGION

    )

    stage_songs_to_redshift = StageToRedshiftOperator(
        task_id='stage_songs_to_redshift',
        redshift_conn_id = "redshift",
        aws_credentials_id = "aws_credentials",
        table = "staging_songs",
        s3_bucket = "airflow-joerivlieghe",
        s3_key = "song-data",
        create_table = SqlQueries.staging_songs_table_create,
        format = "json 'auto'",
        region = REGION
    )

    load_songplays_table = LoadFactOperator(
        task_id='load_songplays_table',
        redshift_conn_id = "redshift",
        table = "songplay",
        create_table = SqlQueries.songplay_table_create,
        load_table = SqlQueries.songplay_table_insert
    )

    load_user_dimension_table = LoadDimensionOperator(
        task_id='load_user_dimension_table',
        redshift_conn_id = "redshift",
        table = "user_table",
        create_table = SqlQueries.user_table_create,
        load_table = SqlQueries.user_table_insert
    )

    load_song_dimension_table = LoadDimensionOperator(
        task_id='load_song_dimension_table',
        redshift_conn_id = "redshift",
        table = "song",
        create_table = SqlQueries.song_table_create,
        load_table = SqlQueries.song_table_insert
    )

    load_artist_dimension_table = LoadDimensionOperator(
        task_id='load_artist_dimension_table',
        redshift_conn_id = "redshift",
        table = "artist",
        create_table = SqlQueries.artist_table_create,
        load_table = SqlQueries.artist_table_insert
    )

    load_time_dimension_table = LoadDimensionOperator(
        task_id='load_time_dimension_table',
        redshift_conn_id = "redshift",
        table = "time_table",
        create_table = SqlQueries.time_table_create,
        load_table = SqlQueries.time_table_insert
    )

    run_quality_checks = DataQualityOperator(
        task_id='run_quality_checks',
        redshift_conn_id = "redshift",
        tables = SqlQueries.tables_sql        
    )

    end_execution = DummyOperator(task_id='end_execution')

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
    run_quality_checks >> end_execution


final_project = final_project()