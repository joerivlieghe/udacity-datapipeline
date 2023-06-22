import datetime
import pendulum
import os
from airflow.decorators import dag,task
from airflow.operators.dummy_operator import DummyOperator
from final_project_operators.execute_sql import ExecuteSQLOperator

default_args = {
    'owner': 'udacity',
    'start_date' : pendulum.now(),
    'depends_on_past' : False,
    'retries' : 3,
    'retry_delay' : datetime.timedelta(minutes=5),
    'email_on_retry' : False,
    'catchup' : False,
    'provide_context': True,
    'description' : "Drop Tables"}


#defining constants
REGION = "us-east-1"
#tables = [staging_events,staging_songs,songplay, user_table, song, artist, time_table]

@dag(
    default_args = default_args,
    schedule_interval = None
)

def drop_tables():

  

    drop_tables_redshift = ExecuteSQLOperator(
        task_id = 'drop_tables_redshift',
        redshift_conn_id = "redshift",
        sql = 'DROP TABLE IF EXISTS staging_events,staging_songs,songplay, user_table, song, artist, time_table'
    )


drop_tables = drop_tables()