from snowflake.snowpark import Session
from snowflake.snowpark.functions import *
from snowflake.snowpark.types import *
from snowflake.snowpark.files import *

with open('config.json') as f:
    config = json.load(f)

session = Session.builder.configs(config).create()
emp_df = session.read.option('file_format','CSV_FORMAT_SKIP_HEADER').csv('@aws_s3_stage/csv/EMP.csv')
emp_df.show()
session.close()