from datetime import datetime, timedelta
import pendulum
from airflow.decorators import dag
from airflow.operators.dummy_operator import DummyOperator
from final_project_operators.stage_redshift import StageToRedshiftOperator
from final_project_operators.load_fact import LoadFactOperator
from final_project_operators.load_dimension import LoadDimensionOperator
from final_project_operators.data_quality import DataQualityOperator
from udacity.common.final_project_sql_statements import SqlQueries

default_args = {
    'owner': 'udacity',
    'start_date': pendulum.datetime(2023, 1, 1, tz="UTC"),
    'depends_on_past': False,  # The DAG does not have dependencies on past runs
    'retries': 3,  # On failure, the tasks are retried 3 times
    'retry_delay': timedelta(minutes=5),  # Retries happen every 5 minutes
    'email_on_retry': False,  # Do not email on retry
}

@dag(
    default_args=default_args,
    description='Load and transform data in Redshift with Airflow',
    schedule_interval='0 * * * *',  # Every hour
    catchup=False
)
def final_project():

    S3_BUCKET = "final-project-airflow-vasu" 
    SONG_S3_KEY = "song-data/A/A/A/A"  
    LOG_S3_KEY = "log-data/2018/11" 
    LOG_JSON_PATH = "log_json_path.json" 

    start_operator = DummyOperator(task_id='Begin_execution1')

    # Stage log data to Redshift
    stage_events_to_redshift = StageToRedshiftOperator(
        task_id='Stage_events',
        table='staging_events', 
        redshift_conn_id='redshift',
        aws_role_arn='arn:aws:iam::048552373269:role/my-redshift-service-role',  # Ensure this is a valid IAM role ARN
        s3_bucket=S3_BUCKET,
        s3_key=LOG_S3_KEY,
        json_path=LOG_JSON_PATH,
    )

    # Stage song data to Redshift
    stage_songs_to_redshift = StageToRedshiftOperator(
        task_id='stage_songs',
        table='staging_songs', 
        redshift_conn_id='redshift',
        aws_role_arn='arn:aws:iam::048552373269:role/my-redshift-service-role',  # Ensure this is a valid IAM role ARN
        s3_bucket=S3_BUCKET,
        s3_key=SONG_S3_KEY,
        json_path='auto', 
    )

        # Load fact tables
    load_songplay_fact = LoadFactOperator(
        task_id='Load_songplays_fact_table',
        redshift_conn_id='redshift',
        sql_query=SqlQueries.songplay_table_insert,
        table_name='songplays',
        create_table_query=SqlQueries.create_songplays_table
    )

      # Load Dimension tables
    load_user_dimension = LoadDimensionOperator(
        task_id='Load_user_dim_table',
        redshift_conn_id='redshift',
        sql_query=SqlQueries.user_table_insert,
        table_name='users',
        create_table_query=SqlQueries.create_users_table
    )

    load_song_dimension = LoadDimensionOperator(
        task_id='Load_song_dim_table',
        redshift_conn_id='redshift',
        sql_query=SqlQueries.song_table_insert,
        table_name='songs',
        create_table_query=SqlQueries.create_songs_table
    )

    load_artist_dimension = LoadDimensionOperator(
        task_id='Load_artist_dim_table',
        redshift_conn_id='redshift',
        sql_query=SqlQueries.artist_table_insert,
        table_name='artists',
        create_table_query=SqlQueries.create_artists_table
    )

    load_time_dimension = LoadDimensionOperator(
        task_id='Load_time_dim_table',
        redshift_conn_id='redshift',
        sql_query=SqlQueries.time_table_insert,
        table_name='time',
        create_table_query=SqlQueries.create_time_table
    )

    # Create a single Data Quality Check task for all dimension tables
    tables_to_check = ['songplays', 'users', 'songs', 'artists', 'time']

    data_quality_check = DataQualityOperator(
        task_id='check_data_quality',
        tables=tables_to_check
    )

    end_operator = DummyOperator(task_id='End_execution1')

    # Set task dependencies
    start_operator >> [stage_events_to_redshift, stage_songs_to_redshift]
    
    stage_events_to_redshift >> load_songplay_fact
    stage_songs_to_redshift >> load_songplay_fact

    load_songplay_fact >> load_user_dimension
    load_songplay_fact >> load_song_dimension
    load_songplay_fact >> load_artist_dimension
    load_songplay_fact >> load_time_dimension
    load_user_dimension >> data_quality_check
    load_song_dimension >> data_quality_check
    load_artist_dimension >> data_quality_check
    load_time_dimension >> data_quality_check
    data_quality_check >> end_operator

# Instantiate the DAG
final_project_dag = final_project()