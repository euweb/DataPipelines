#from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.hooks.postgres_hook import PostgresHook
from airflow.hooks.S3_hook import S3Hook
from airflow.models import BaseOperator

class StageToRedshiftOperator(BaseOperator):
    """
    Copies JSON data from S3 to a Redshift Cluster
    """
    ui_color = '#358140'

    copy_sql = """
        COPY {}
        FROM '{}'
        ACCESS_KEY_ID '{}'
        SECRET_ACCESS_KEY '{}'
        REGION '{}'
        FORMAT AS JSON '{}'
    """


    def __init__(self,
                 redshift_conn_id="redshift",
                 aws_credentials_id="aws_credentials",
                 table="",
                 s3_bucket="",
                 s3_key="",
                 json_path="auto",
                 region="eu-west-1",
                 *args, **kwargs):
        """Initializes the operator

        Args:
            redshift_conn_id (str, optional): name of the connection created in Airflow. Defaults to "redshift".
            aws_credentials_id (str, optional): name of the connection created in Airflow. Defaults to "aws_credentials".
            table (str, optional): destination table. Defaults to "".
            s3_bucket (str, optional): source s3 bucket. Defaults to "".
            s3_key (str, optional): Folder inside the s3 bucket containing the data. Defaults to "".
            json_path (str, optional): Path tho the format description of the data. Defaults to "auto".
            region (str, optional): AWS region of s3 bucket and the Redshift Cluster. Defaults to "eu-west-1".
        """
        super(StageToRedshiftOperator, self).__init__(*args, **kwargs)
        self.table = table
        self.redshift_conn_id = redshift_conn_id
        self.s3_bucket = s3_bucket
        self.s3_key = s3_key
        self.aws_credentials_id = aws_credentials_id
        self.json_path = json_path
        self.region = region
  
    def execute(self, context):
        aws_hook = S3Hook(aws_conn_id=self.aws_credentials_id)
        credentials = aws_hook.get_credentials()
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)

        self.log.info("Clearing data from destination Redshift table {self.table}")
        redshift.run("DELETE FROM {}".format(self.table))

        self.log.info("Copying data from S3 to Redshift")
        rendered_key = self.s3_key.format(**context)
        s3_path = "s3://{}/{}".format(self.s3_bucket, rendered_key)
        formatted_sql = StageToRedshiftOperator.copy_sql.format(
            self.table,
            s3_path,
            credentials.access_key,
            credentials.secret_key,
            self.region,
            self.json_path
        )
        redshift.run(formatted_sql)