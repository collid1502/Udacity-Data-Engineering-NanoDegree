from airflow.contrib.hooks.aws_hook import AwsHook
from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class StageToRedshiftOperator(BaseOperator):
    ui_color = '#358140'

    @apply_defaults
    def __init__(self,
                 table = "",
                 aws_credentials_id = "",
                 redshift_conn_id = "",
                 s3_bucket = "",
                 s3_key = "",
                 json_file_path = "auto",
                 *args, **kwargs):

        super(StageToRedshiftOperator, self).__init__(*args, **kwargs)
        
        self.table = table 
        self.aws_credentials_id = aws_credentials_id 
        self.redshift_conn_id = redshift_conn_id 
        self.s3_bucket = s3_bucket 
        self.s3_key = s3_key 
        self.json_file_path = json_file_path 

    def execute(self, context):
        aws_hook = AwsHook(self.aws_credentials_id)
        credentials = aws_hook.get_credentials()
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)

        self.log.info("Clear any current data from staging table")
        redshift.run(f"DELETE FROM {self.table}") 
        
        self.log.info("Copying data from S3 to Redshift")
        rendered_key = self.s3_key.format(**context)
        s3_path = "s3://{}/{}".format(self.s3_bucket, rendered_key)
        redshift.run(f"COPY {self.table} FROM '{s3_path}' ACCESS_KEY_ID '{credentials.access_key}' SECRET_ACCESS_KEY '{credentials.secret_key}' JSON '{self.json_file_path}' \
                       COMPUPDATE OFF REGION 'us-west-2'") 
                            
