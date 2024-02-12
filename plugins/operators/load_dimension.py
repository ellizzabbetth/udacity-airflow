from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class LoadDimensionOperator(BaseOperator):

    ui_color = '#80BD9E'

    @apply_defaults
    def __init__(self,
                 redshift_conn_id="",
                 sql_query="",
                 table_name="",
                 truncate=False,
                 *args, **kwargs):

        super(LoadDimensionOperator, self).__init__(*args, **kwargs)
        self.postgres_conn_id = redshift_conn_id
        self.insert_sql_stmt = sql_query
        self.table_name = table_name
        self.truncate = truncate

    def execute(self, context):
        postgres = PostgresHook(postgres_conn_id=self.postgres_conn_id)
        if self.truncate:
            self.log.info(f"Truncate dimension table {self.table_name}")
            postgres.run(f"TRUNCATE TABLE {self.table_name};")
            
        self.log.info(f"Load data to dimension table {self.table_name}")
        postgres.run(f"INSERT INTO {self.table_name} {self.insert_sql_stmt};")


# load_user_dimension_table = LoadDimensionOperator(
#     task_id='Load_user_dim_table',
#     dag=dag,
#     redshift_conn_id='redshift',
#     table='users',
#     sql_statement=SqlQueries.user_table_insert,
#     operation='truncate'
# )