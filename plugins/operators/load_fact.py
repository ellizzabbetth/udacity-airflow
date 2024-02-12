from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class LoadFactOperator(BaseOperator):

    ui_color = '#F98866'

    @apply_defaults
    def __init__(self,
                 redshift_conn_id, 
                 table, 
                 songplay_sql, 
                 *args, **kwargs):

        super(LoadFactOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.table_name = table
        self.songplay_sql = songplay_sql

    def execute(self, context):
        self.log.info('LoadFactOperator not implemented yet')
        
        redshift_hook = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        
        drop_table = f"DROP TABLE IF EXISTS {self.table_name}"
        create_table = f"CREATE TABLE IF NOT EXISTS {self.table_name}"
        
        table_insert_sql = f'''
            INSERT INTO {self.table_name}
            {self.songplay_sql}
        '''
        
        redshift_hook.run(drop_table)
        redshift_hook.run(create_table)
        redshift_hook.run(table_insert_sql)

