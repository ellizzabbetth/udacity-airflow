from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class DataQualityOperator(BaseOperator):

    ui_color = '#89DA59'

    @apply_defaults
    def __init__(self,
                 redshift_conn_id="",
                 dq_checks_list = [],
                 *args, **kwargs):
        super(DataQualityOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.dq_checks_list = dq_checks_list
    
    def execute(self, context):
        if not self.dq_checks_list:
            self.log.info("Empty test case for check data quality, \
                please check your test case again!")
            return
        else:
            error_found = 0
            redshift = PostgresHook(self.redshift_conn_id)
            for testcase_number, testcase_value in enumerate(self.dq_checks_list):
                testcase = testcase_value.get('sql_testcase')
                exected_output = testcase_value.get('expected_result')
                
                try:
                    self.log.info(f"Running testcase #{testcase_number}")
                    records = redshift.get_records(testcase)
                    if not exected_output == records[0][0]:
                        error_found += 1
                        raise ValueError(f"Data quality run testcase #{testcase_number} failed. \
                                     value should be {exected_output}!")
                    else:
                        self.log.info(f"You have passed testcase #{testcase_number}")
                except Exception as e:
                    self.log.info(f"Running testcase #{testcase_number} cannot run because '{e}'!\
                        please try to fix before the next run.")
                    
            if error_found:
                self.log.info(f"You have passed {len(self.dq_checks_list) - error_found}/{len(self.dq_checks_list)}")
            else:
                self.log.info(f"Congratulations, you have passed all test cases!!!")