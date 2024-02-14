from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
# https://knowledge.udacity.com/questions/905226
class LoadFactOperator(BaseOperator):

    ui_color = '#F98866'
    
    truncate_sql = """
        TRUNCATE TABLE {};
    """

    @apply_defaults
    def __init__(self,
                 redshift_conn_id, 
                 table, 
                 fact_sql,
                 append_only,
                 *args, **kwargs):

        super(LoadFactOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.table = table
        self.fact_sql = fact_sql
        self.append_only = append_only

    def execute(self, context):
        #redshift connection
        redshift = PostgresHook(postgres_conn_id = self.redshift_conn_id)

        if not self.append_only:
            self.log.info(f"TRUNCATE TABLE {self.table} fact table")
            redshift.run(LoadFactOperator.truncate_sql.format(self.table))
            # fact_sql = (f"INSERT INTO {self.table} {self.fact_sql}")
            # redshift.run(fact_sql)
            
        self.log.info(f"Insert data into {self.table} fact table")
        #if self.append_only:
        fact_sql = (f"INSERT INTO {self.table} {self.fact_sql}")
        redshift.run(fact_sql)
        self.log.info('Loaded data into {table} fact table'.format(table=self.table))

