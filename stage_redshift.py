from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class StageToRedshiftOperator(BaseOperator):
    ui_color = '#358140'
    
    # SQL templates for creating tables
    create_song_table_sql = """
        CREATE TABLE IF NOT EXISTS staging_songs (
            song_id VARCHAR(255),
            num_songs INT,
            title VARCHAR(255),
            artist_name VARCHAR(255),
            artist_latitude FLOAT,
            year INT,
            duration FLOAT,
            artist_id VARCHAR(255),
            artist_longitude FLOAT,
            artist_location VARCHAR(255)
        );
    """
    
    create_log_table_sql = """
        CREATE TABLE IF NOT EXISTS staging_events (
            event_id INT IDENTITY(0,1),
            artist VARCHAR(255),
            auth VARCHAR(50),
            firstName VARCHAR(255),
            gender VARCHAR(1),
            itemInSession INT,
            lastName VARCHAR(255),
            length FLOAT,
            level VARCHAR(50),
            location VARCHAR(255),
            method VARCHAR(25),
            page VARCHAR(25),
            registration BIGINT,
            sessionId INT,
            song VARCHAR(255),
            status INT,
            ts BIGINT,
            userAgent VARCHAR(255),
            userId INT
        );
    """
    
    copy_sql_template = """        
        COPY {}
        FROM '{}'  
        IAM_ROLE '{}'
        JSON '{}'
        REGION '{}';
    """
    
    @apply_defaults
    def __init__(self,
                 redshift_conn_id="",
                 aws_role_arn="",
                 table="",
                 s3_bucket="",
                 s3_key="",
                 json_path="auto", 
                 region="us-east-1", 
                 *args, **kwargs):
        super(StageToRedshiftOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.aws_role_arn = aws_role_arn
        self.table = table
        self.s3_bucket = s3_bucket
        self.s3_key = s3_key
        self.json_path = json_path
        self.region = region
        
    def execute(self, context):
        self.log.info('Starting StageToRedshiftOperator')
        
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id) 
        
        # Create table if it does not exist based on the table name
        self.log.info("Creating table in Redshift")
        if self.table == 'staging_songs':
            create_table_sql = StageToRedshiftOperator.create_song_table_sql
        elif self.table == 'staging_events':
            create_table_sql = StageToRedshiftOperator.create_log_table_sql
        else:
            raise ValueError(f"Unknown table name: {self.table}")
        
        redshift.run(create_table_sql)
        
        self.log.info("Copying data from S3 to Redshift")
        rendered_key = self.s3_key.format(**context)
        s3_path = f"s3://{self.s3_bucket}/{rendered_key}"
        
        # Format JSON path for the COPY command
        if self.json_path == 'auto':
            json_path = 'auto'
        else:
            json_path = f"s3://{self.s3_bucket}/{self.json_path}"
        
        # Format COPY command with the proper IAM role and JSON path
        formatted_sql = StageToRedshiftOperator.copy_sql_template.format(
            self.table,
            s3_path,
            self.aws_role_arn,
            json_path,
            self.region
        )
        self.log.info(f"Running copy command: {formatted_sql}")
        
        try:
            redshift.run(formatted_sql)
        except Exception as e:
            self.log.error(f"Error running COPY command: {str(e)}")
            raise