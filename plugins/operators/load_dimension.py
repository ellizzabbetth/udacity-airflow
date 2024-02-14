from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
# https://knowledge.udacity.com/questions/979077
class LoadDimensionOperator(BaseOperator):

    ui_color = '#80BD9E'
    truncate_sql = """
        TRUNCATE TABLE {};
    """
    
    @apply_defaults
    def __init__(self,
                 redshift_conn_id="",
                 sql_query="",
                 table_name="",
                 append_only = False,
                 *args, **kwargs):

        super(LoadDimensionOperator, self).__init__(*args, **kwargs)
        self.postgres_conn_id = redshift_conn_id
        self.insert_sql_stmt = sql_query
        self.table_name = table_name
        self.append_only = append_only


    def execute(self, context):
        postgres = PostgresHook(postgres_conn_id=self.postgres_conn_id)
        # You need to add a parameter in the LoadDimension operator e.g. `append_only`. When set to FALSE, run sql command "TRUNCATE TABLE table_name"
        if not self.append_only:
            self.log.info(f"Truncate dimension table {self.table_name}")        
            postgres.run(LoadDimensionOperator.truncate_sql.format(self.table))
            self.log.info(f"Data cleared from dimension table {self.table_name}")
            
        self.log.info(f"Load data to dimension table {self.table_name}")
        postgres.run(f"INSERT INTO {self.table_name} {self.insert_sql_stmt};")


