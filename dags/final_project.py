from datetime import datetime, timedelta
import pendulum
from airflow.decorators import dag

from airflow.operators.dummy_operator import DummyOperator

# https://knowledge.udacity.com/questions/992336
from operators.stage_redshift import StageToRedshiftOperator
from operators.load_fact import LoadFactOperator
from operators.load_dimension import LoadDimensionOperator
from operators.data_quality import DataQualityOperator


from helpers import SqlQueries, DagConfig

default_args = {
    "owner": "ebradley",
    "depends_on_past": False,
    "retries": 3,
    "retry_delay": timedelta(minutes=5),
    "catchup": False,
    "email_on_retry": False
}

@dag(
    default_args=default_args,
    start_date=pendulum.datetime(2018, 11, 1, 0, 0, 0, 0),
    end_date=pendulum.datetime(2018, 11, 30, 0, 0, 0, 0),
    description="Load and transform data in Redshift with Airflow",
    schedule_interval='@hourly',
    max_active_runs=1
)
def final_project():

    start_operator = DummyOperator(task_id="Begin_execution")
    # https://github.com/dipenich1000/Udacity-Pipeline-Project/blob/main/dags/udac_example_dag.py
    stage_events_to_redshift = StageToRedshiftOperator(
        task_id='stage_events',
        redshift_conn_id=DagConfig.REDSHIFT_CONN_ID,
        aws_credentials_id=DagConfig.AWS_CREDENTIALS_ID,
        table='staging_events',
        s3_bucket=DagConfig.S3_BUCKET,
        s3_key=DagConfig.S3_LOG_KEY,
        region=DagConfig.REGION,
        data_format=DagConfig.DATA_FORMAT_EVENT
    )

    stage_songs_to_redshift = StageToRedshiftOperator(
        task_id="load_stage_songs",
        redshift_conn_id=DagConfig.REDSHIFT_CONN_ID,
        aws_credentials_id=DagConfig.AWS_CREDENTIALS_ID,
        table="public.staging_songs",
        s3_bucket=DagConfig.S3_BUCKET,
        s3_key="song-data/A/A/A/",
        region=DagConfig.REGION,
        data_format="format as json 'auto'",     # For song_data, you do not have to use the json path.
                                                 #                Just use json 'auto
    )

    load_songplays_table = LoadFactOperator(
        task_id="Load_songplays_fact_table",
        redshift_conn_id=DagConfig.REDSHIFT_CONN_ID,
        table="public.songplays",
        fact_sql=SqlQueries.songplay_table_insert,
        append_only = True # do not truncate
    )

    load_user_dimension_table = LoadDimensionOperator(
        task_id="Load_user_dim_table",
        redshift_conn_id=DagConfig.REDSHIFT_CONN_ID,
        append_only=False, # truncate
        sql_query=SqlQueries.user_table_insert,
        table_name="users",
    )

    load_song_dimension_table = LoadDimensionOperator(
        task_id="Load_song_dim_table",
        redshift_conn_id=DagConfig.REDSHIFT_CONN_ID,
        append_only=False, # truncate
        sql_query=SqlQueries.song_table_insert,
        table_name="songs",
    )

    load_artist_dimension_table = LoadDimensionOperator(
        task_id="Load_artist_dim_table",
        redshift_conn_id=DagConfig.REDSHIFT_CONN_ID,
        append_only=False, # truncate
        sql_query=SqlQueries.artist_table_insert,
        table_name="artists",
    )

    load_time_dimension_table = LoadDimensionOperator(
        task_id="Load_time_dim_table",
        redshift_conn_id=DagConfig.REDSHIFT_CONN_ID,

        append_only=False, # truncate
        sql_query=SqlQueries.time_table_insert,
        table_name="time",
    )
    # https://knowledge.udacity.com/questions/54406
    run_quality_checks = DataQualityOperator(
        task_id="Run_data_quality_checks",
        redshift_conn_id=DagConfig.REDSHIFT_CONN_ID,
        tables=['songplays', 'users', 'songs', 'artists', 'time'],
    )

    end_operator = DummyOperator(task_id="End_execution")

    start_operator >> stage_events_to_redshift
    start_operator >> stage_songs_to_redshift

    stage_events_to_redshift >> load_songplays_table
    stage_songs_to_redshift >> load_songplays_table

    (
    load_songplays_table
    >> [
        load_user_dimension_table,
        load_song_dimension_table,
        load_artist_dimension_table,
        load_time_dimension_table,
    ]
    >> run_quality_checks   
    >> end_operator
    )


final_project_dag = final_project()

