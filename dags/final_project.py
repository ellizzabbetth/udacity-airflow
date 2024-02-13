from datetime import datetime, timedelta
# from airflow.plugins_manager import AirflowPlugin

import pendulum
import os
from airflow.decorators import dag

from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.postgres_operator import PostgresOperator
# https://knowledge.udacity.com/questions/992336
from operators.stage_redshift import StageToRedshiftOperator
from operators.load_fact import LoadFactOperator
from operators.load_dimension import LoadDimensionOperator
from operators.data_quality import DataQualityOperator



# from final_project_operators.load_fact import LoadFactOperator

# from final_project_operators.load_dimension import LoadDimensionOperator

# from final_project_operators.data_quality import DataQualityOperator

from helpers import SqlQueries, ConfigureDataAccess

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
        redshift_conn_id=ConfigureDataAccess.REDSHIFT_CONN_ID,
        aws_credentials_id=ConfigureDataAccess.AWS_CREDENTIALS_ID,
        table='staging_events',
        s3_bucket=ConfigureDataAccess.S3_BUCKET,
        s3_key=ConfigureDataAccess.S3_LOG_KEY,
        region=ConfigureDataAccess.REGION,
        data_format=ConfigureDataAccess.DATA_FORMAT_EVENT
    )

    stage_songs_to_redshift = StageToRedshiftOperator(
        task_id="load_stage_songs",
        redshift_conn_id=ConfigureDataAccess.REDSHIFT_CONN_ID,
        aws_credentials_id=ConfigureDataAccess.AWS_CREDENTIALS_ID,
        table="public.staging_songs",
        s3_bucket=ConfigureDataAccess.S3_BUCKET,
        s3_key="song-data/A/A/A/",
        region=ConfigureDataAccess.REGION,
        data_format="format as json 'auto'",     # For song_data, you do not have to use the json path.
                                                 #                Just use json 'auto
    )

    load_songplays_table = LoadFactOperator(
        task_id="Load_songplays_fact_table",
        redshift_conn_id="redshift",
        table="public.songplays",
        fact_sql=SqlQueries.songplay_table_insert,
        append_only = True
    )

    load_user_dimension_table = LoadDimensionOperator(
        task_id="Load_user_dim_table",
        redshift_conn_id=ConfigureDataAccess.REDSHIFT_CONN_ID,
        #load_mode="truncate_insert",
        truncate=True,
        sql_query=SqlQueries.user_table_insert,
        table_name="users",
    )

    load_song_dimension_table = LoadDimensionOperator(
        task_id="Load_song_dim_table",
        redshift_conn_id=ConfigureDataAccess.REDSHIFT_CONN_ID,
       # load_mode="truncate_insert",
        truncate=True,
        sql_query=SqlQueries.song_table_insert,
        table_name="songs",
    )

    load_artist_dimension_table = LoadDimensionOperator(
        task_id="Load_artist_dim_table",
        redshift_conn_id=ConfigureDataAccess.REDSHIFT_CONN_ID,
        truncate=True,
        #load_mode="truncate_insert",
        sql_query=SqlQueries.artist_table_insert,
        table_name="artists",
    )

    load_time_dimension_table = LoadDimensionOperator(
        task_id="Load_time_dim_table",
        redshift_conn_id=ConfigureDataAccess.REDSHIFT_CONN_ID,
        # load_mode="truncate_insert",
        truncate=True,
        sql_query=SqlQueries.time_table_insert,
        table_name="time",
    )
    # https://knowledge.udacity.com/questions/54406
    run_quality_checks = DataQualityOperator(
        task_id="Run_data_quality_checks",
        redshift_conn_id=ConfigureDataAccess.REDSHIFT_CONN_ID,
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


# import pendulum


# from airflow.decorators import dag, task
# from airflow.secrets.metastore import MetastoreBackend
# from airflow.hooks.postgres_hook import PostgresHook
# from airflow.operators.postgres_operator import PostgresOperator

# from udacity.common import sql_statements

# @dag(
#     start_date=pendulum.datetime(2018, 1, 1, 0, 0, 0, 0),
#     end_date=pendulum.datetime(2018, 2, 1, 0, 0, 0, 0),
#     schedule_interval='@monthly',
#     max_active_runs=1    
# )
# def data_partitioning():


#     @task()
#     def load_trip_data_to_redshift(*args,* *kwargs):
#         metastoreBackend = MetastoreBackend()
#         aws_connection=metastoreBackend.get_connection("aws_credentials")
#         redshift_hook = PostgresHook("redshift")
#         execution_date = kwargs["execution_date"]

#         sql_stmt = sql_statements.COPY_MONTHLY_TRIPS_SQL.format(
#             aws_connection.login,
#             aws_connection.password,
#             year=execution_date.year,
#             month=execution_date.month
#         )
#         redshift_hook.run(sql_stmt)

#     load_trip_data_to_redshift_task= load_trip_data_to_redshift()

#     @task()
#     def load_station_data_to_redshift():
#         metastoreBackend = MetastoreBackend()
#         aws_connection=metastoreBackend.get_connection("aws_credentials")
#         redshift_hook = PostgresHook("redshift")
#         sql_stmt = sql_statements.COPY_STATIONS_SQL.format(
#             aws_connection.login,
#             aws_connection.password,
#         )
#         redshift_hook.run(sql_stmt)

#     load_station_data_to_redshift_task = load_station_data_to_redshift()

#     create_trips_table = PostgresOperator(
#         task_id="create_trips_table",
#         postgres_conn_id="redshift",
#         sql=sql_statements.CREATE_TRIPS_TABLE_SQL
#     )


#     create_stations_table = PostgresOperator(
#         task_id="create_stations_table",
#         postgres_conn_id="redshift",
#         sql=sql_statements.CREATE_STATIONS_TABLE_SQL,
#     )

#     create_trips_table >> load_trip_data_to_redshift_task
#     create_stations_table >> load_station_data_to_redshift_task

# data_partitioning_dag = data_partitioning()