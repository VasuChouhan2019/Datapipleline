from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class LoadDimensionOperator(BaseOperator):

    ui_color = '#80BD9E'

    @apply_defaults
    def __init__(self,
                 redshift_conn_id="",
                 sql_query="",
                 table_name="",
                 create_table_query="",
                 *args, **kwargs):

        super(LoadDimensionOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.sql_query = sql_query
        self.table_name = table_name
        self.create_table_query = create_table_query

    def execute(self, context):
        self.log.info('Starting LoadDimensionOperator')
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)

        # Check and create the table if it doesn't exist
        self.log.info(f"Checking if table {self.table_name} exists and creating it if not.")
        redshift.run(self.create_table_query)

        self.log.info(f"Loading data into {self.table_name} table")
        redshift.run(self.sql_query)

        self.log.info("Committing transaction")
        redshift.run("COMMIT;")