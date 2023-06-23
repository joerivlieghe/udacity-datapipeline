from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class LoadDimensionOperator(BaseOperator):

    ui_color = '#80BD9E'

    @apply_defaults
    def __init__(self,
                 redshift_conn_id="",
                 table="",
                 create_table="",
                 load_table="",
                 truncate_append="",
                 *args, **kwargs):

        super(LoadDimensionOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.table = table
        self.create_table = create_table
        self.load_table = load_table
        self.truncate_append = truncate_append

    def execute(self, context):
        redshift = PostgresHook(postgres_conn_id = self.redshift_conn_id)

        # self.log.info("Clearing table")
        # redshift.run("DROP TABLE IF EXISTS {}".format(self.table))
        redshift.run(self.create_table)

        if self.truncate_append == "truncate":
            self.log.info("clearing table")
            redshift.run("TRUNCATE {self.table}")

        self.log.info("Inserting data into redshift table")
        redshift.run(self.load_table)
