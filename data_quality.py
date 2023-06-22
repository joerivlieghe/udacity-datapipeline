# from airflow.hooks.postgres_hook import PostgresHook
# from airflow.models import BaseOperator
# from airflow.utils.decorators import apply_defaults

# class DataQualityOperator(BaseOperator):

#     ui_color = '#89DA59'

#     @apply_defaults
#     def __init__(self,
#                  redshift_conn_id ="",
#                  quality_tests = [],
#                  expected_results = [],
#                  result='',
#                  *args, **kwargs):

#         super(DataQualityOperator, self).__init__(*args, **kwargs)
#         self.redshift_conn_id = redshift_conn_id,
#         self.quality_tests = quality_tests,
#         self.expected_results = expected_results
#         self.result = result

#     def execute(self, context):
#         redshift = PostgresHook(postgres_conn_id = self.redshift_conn_id)

#         self.log.info("Running Quality Tests")
        
#         for i in range(len(self.quality_tests)):
#             self.result = redshift.run(self.quality_tests[i])
#             self.log.info(self.result)

#             if self.result != self.expected_results[i]:
#                 raise ValueError('Quality test {} failed'.format(i))
#                 break
            
#             else:
#                 self.log.info("Quality test {} has passed".format(i))


from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class DataQualityOperator(BaseOperator):

    ui_color = '#89DA59'

    @apply_defaults
    def __init__(self,
                 redshift_conn_id="", 
                 tables=[],
                 *args, **kwargs):

        super(DataQualityOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.tables = tables

    def execute(self, context):
    
        redshift_hook = PostgresHook(self.redshift_conn_id)
        for table in self.tables:
            self.log.info(f"counting records from {table}")
            records = redshift_hook.get_records(f"SELECT COUNT(*) FROM {table}")
            if len(records) < 1 or len(records[0]) < 1:
                raise ValueError(f"Data quality check failed. {table} returned no results")
            
            if records[0][0] < 1:
                raise ValueError(f"Data quality check failed. {table} contained 0 rows")
            self.log.info(f"Data quality check on {table} passed and has {records[0][0]} records")