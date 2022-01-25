from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults


# create an empty list for the tables to check. So we can then iterate over each table passed into the parameter via a list

class DataQualityOperator(BaseOperator):

    ui_color = '#89DA59'

    @apply_defaults
    def __init__(self,
                 redshift_conn_id="",
                 dq_checks=[],
                 *args, **kwargs):

        super(DataQualityOperator, self).__init__(*args, **kwargs)
        
        self.redshift_conn_id = redshift_conn_id 
        self.dq_checks = dq_checks 

    def execute(self, context):
        
        redshift = PostgresHook(self.redshift_conn_id)
        for check in self.dq_checks:
            sql_query = check.get('dq_check')
            result_val = check.get('fail_value') 
            numObs = redshift.get_records(sql_query)
            
            if numObs[0][0] == result_val:
                raise ValueError(f"DQ check {sql_query} has failed") 
                
            self.log.info(f"DQ check {sql_query} has pased")       
               