from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class LoadDimensionOperator(BaseOperator):

    ui_color = '#80BD9E'

    @apply_defaults
    def __init__(self,
                 redshift_conn_id = "",
                 table = "",
                 tableAppend = False,
                 sql_query = "",
                 *args, **kwargs):

        super(LoadDimensionOperator, self).__init__(*args, **kwargs)
        
        self.redshift_conn_id = redshift_conn_id 
        self.table = table
        self.tableAppend = tableAppend 
        self.sql_query = sql_query

    def execute(self, context):
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        self.log.info(f"Loading Dimension table {self.table}") 
        execute_sql = "" 
        
        # Create an IF ELSE condition based on whether the use tableAppend is set to True or False (default is False) 
        # This will determin the sql statement used
        if self.tableAppend:
            execute_sql = """
            INSERT INTO {}
            {} ;
            """.format(self.table, self.sql_query) 
 
        # If Append is False, we want to clear the table first. Use the 'TRUNCATE' statment from postgres which is more efficient to wipe data from table 
        # as all data is wiped without scanning the table first, rather than using 'DELETE' 
        else:
            execute_sql = """
            TRUNCATE TABLE {} ;
            INSERT INTO {}
            {} ;
            """.format(self.table, self.table, self.sql_query)          
          
        redshift.run(execute_sql)
       
            
