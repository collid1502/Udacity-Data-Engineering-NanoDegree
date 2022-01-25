from datetime import datetime, timedelta
import os
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators import (StageToRedshiftOperator, LoadFactOperator,
                                LoadDimensionOperator, DataQualityOperator)
from helpers import SqlQueries
 
# AWS_KEY = os.environ.get('AWS_KEY')
# AWS_SECRET = os.environ.get('AWS_SECRET')

default_args = {
    'owner': 'udacity',
    'start_date': datetime(2019, 1, 12),
    'depends_on_past': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
    'email_on_retry': False,
    'catchup': False
}

dag = DAG('udac_example_dag',
          default_args=default_args,
          description='Load and transform data in Redshift with Airflow',
          #schedule_interval='0 * * * *'    
          schedule_interval='@once' # change this back to hourly for submission, but for now just use @once 
        )

start_operator = DummyOperator(task_id='Begin_execution',  dag=dag)

# Create table statements from "create_tables.sql" issued from within redshift cluster - NOT part of DAG 
# This is because going forward, once the tables are built, they do not need to be dropped & re-created as part of a pipeline

# This operator will load the staging events from JSON logs. Need to pass paramters table name, AWS & Redshift vars, 
# S3 bucket & key & file path for JSON read in as default 'auto' won't work
stage_events_to_redshift = StageToRedshiftOperator(
    task_id='Stage_events',
    dag=dag,
    table="staging_events",
    aws_credentials_id="aws_credentials",
    redshift_conn_id="redshift",
    s3_bucket="udacity-dend",
    s3_key="log_data",
    json_file_path="s3://udacity-dend/log_json_path.json"
)

# can test by settting s3_key to 'song_data/A/A/A' - then when done, set back to 'song_data' 
# don't specify json_file_path as default is 'auto' 
stage_songs_to_redshift = StageToRedshiftOperator(
    task_id='Stage_songs',
    dag=dag,
    table="staging_songs",
    aws_credentials_id="aws_credentials",
    redshift_conn_id="redshift",
    s3_bucket="udacity-dend",
    s3_key="song_data/A/A/A"    
)


# load the songplays FACT table from the staging tables, using the 'songplay_table_insert' query from imported SqlQueries
# also specify redshift connection & table name 
load_songplays_table = LoadFactOperator(
    task_id='Load_songplays_fact_table',
    dag=dag,
    redshift_conn_id = "redshift",
    table = "songplays",
    sql_query = SqlQueries.songplay_table_insert 
)


# now set up the loading of the Dimension tables - leave the tableAppend as its default value of False 
load_user_dimension_table = LoadDimensionOperator(
    task_id='Load_user_dim_table',
    dag=dag,
    redshift_conn_id="redshift",
    table="users",
    sql_query=SqlQueries.user_table_insert 
)

load_song_dimension_table = LoadDimensionOperator(
    task_id='Load_song_dim_table',
    dag=dag,
    redshift_conn_id="redshift",
    table="songs",
    sql_query=SqlQueries.song_table_insert
)

load_artist_dimension_table = LoadDimensionOperator(
    task_id='Load_artist_dim_table',
    dag=dag,
    redshift_conn_id="redshift",
    table="artists",
    sql_query=SqlQueries.artist_table_insert
)

load_time_dimension_table = LoadDimensionOperator(
    task_id='Load_time_dim_table',
    dag=dag,
    redshift_conn_id="redshift",
    table="time",
    sql_query=SqlQueries.time_table_insert
)

my_dq_checks = [
    {'dq_check': 'SELECT COUNT(*) FROM songplays', 'fail_value': 0},
    {'dq_check': 'SELECT COUNT(*) FROM songs', 'fail_value': 0},
    {'dq_check': 'SELECT COUNT(*) FROM users', 'fail_value': 0}
    ] 
# Now run the DQ checks on the one FACT & four DIMENSION tables 
run_quality_checks = DataQualityOperator(
    task_id='Run_data_quality_checks',
    dag=dag,
    redshift_conn_id="redshift",
    dq_checks = my_dq_checks
)

# end the ETL pipeline 
end_operator = DummyOperator(task_id='Stop_execution',  dag=dag)


# ---------------------------------- # 
#  Set the DAG taskflow order below  # 
# ---------------------------------- #

# start operator to loading staging tables
start_operator >> [stage_events_to_redshift, stage_songs_to_redshift]

# from each staging event, load to the songplays table 
[stage_events_to_redshift, stage_songs_to_redshift] >> load_songplays_table 

# after the songplays table has been loaded, run each of the four dimension tables 
load_songplays_table >> [load_song_dimension_table, load_user_dimension_table, load_artist_dimension_table, load_time_dimension_table]

# once the FACT & DIMENSION tables are loaded, run the data quality checks on the tables 
[load_song_dimension_table, load_user_dimension_table, load_artist_dimension_table, load_time_dimension_table] >> run_quality_checks 

# after DQ checks has run, proceed to the end 
run_quality_checks >> end_operator 

# END 