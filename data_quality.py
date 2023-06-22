from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class DataQualityOperator(BaseOperator):

    ui_color = '#89DA59'

    @apply_defaults
    def __init__(self,
                 redshift_conn_id ="",
                 quality_tests = [],
                 expected_results = [],
                 *args, **kwargs):

        super(DataQualityOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id,
        self.quality_tests = quality_tests,
        self.expected_results = expected_results

    def execute(self, context):
        redshift = PostgresHook(postgres_conn_id = self.redshift_conn_id)

        self.log.info("Running Quality Tests")
        
        for i in range(len(self.quality_tests)):
            result = redshift.run(self.quality_tests[i])

            if result[0] != self.expected_results[i]:
                raise ValueError('Quality test {} failed'.format(i))
            
            else:
                self.log.info("Quality test {} has passed".format(i))

