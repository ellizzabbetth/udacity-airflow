from airflow.hooks.postgres_hook import PostgresHook
from airflow.contrib.hooks.aws_hook import AwsHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
import pendulum

#https://knowledge.udacity.com/questions/838478
class StageToRedshiftOperator(BaseOperator):
    ui_color = '#358140'
    template_fields = ('s3_key',)
    copy_sql = """
        COPY {}
        FROM '{}'
        ACCESS_KEY_ID '{}'
        SECRET_ACCESS_KEY '{}'
        JSON '{}'
        REGION '{}'
    """
    @apply_defaults
    def __init__(self,
                 redshift_conn_id='',
                 aws_credentials_id='',
                 table='',
                 s3_bucket='',
                 s3_key='',
                 region='',
                 truncate=False,
                 copy_json_option='',
                 *args, **kwargs):
        super(StageToRedshiftOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.aws_credentials_id = aws_credentials_id
        self.table = table
        self.s3_bucket = s3_bucket
        self.s3_key = s3_key
        self.region = region
        self.truncate = truncate
        self.copy_json_option = copy_json_option

    def execute(self, context):
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        aws_hook = AwsHook(self.aws_credentials_id)
        credentials = aws_hook.get_credentials()
        execution_date = pendulum.parse('2018-11-01')
        year = execution_date.year
        month = execution_date.month
        ds = execution_date.strftime("%Y-%m-%d")

        self.log.info("Clearing data from destination Redshift table")
        redshift.run(f"DELETE FROM {self.table}")

        if self.truncate:
            self.log.info(f'Truncate Redshift table {self.table}')
            redshift.run(f'TRUNCATE {self.table}')


        self.log.info("Copying data from S3 to Redshift") 
        # SQL query parameters
        rendered_s3_key = self.s3_key.format(**context)
        s3_path = f's3://{self.s3_bucket}/{rendered_s3_key}'
        formatted_sql = StageToRedshiftOperator.copy_sql.format(
            self.table, s3_path, credentials.access_key,
            credentials.secret_key, self.copy_json_option, self.region,
        )
        # Run query
        self.log.info('Copy data from {s3_path} to Redshift table {self.table}')
        redshift.run(formatted_sql)