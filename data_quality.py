from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class DataQualityOperator(BaseOperator):

    ui_color = '#89DA59'

    @apply_defaults
    def __init__(self,
                 tables=None,
                 *args, **kwargs):
        super(DataQualityOperator, self).__init__(*args, **kwargs)
        self.tables = tables if tables is not None else []

    def execute(self, context):
        self.log.info('Starting DataQualityOperator')

        # Initialize PostgresHook
        redshift_hook = PostgresHook(postgres_conn_id="redshift")

        for table in self.tables:
            self.log.info(f"Checking data quality for table {table}")
            
            # Query to count rows in the table
            records = redshift_hook.get_records(f"SELECT COUNT(*) FROM {table}")
            
            # Check if any records were returned
            if len(records) < 1 or len(records[0]) < 1:
                raise ValueError(f"Data quality check failed. {table} returned no results")
            
            # Check if the number of records is greater than zero
            num_records = records[0][0]
            if num_records < 1:
                raise ValueError(f"Data quality check failed. {table} contained 0 rows")
            
            self.log.info(f"Data quality on table {table} check passed with {num_records} records")