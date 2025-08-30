import snowflake.connector as sf
import pandas as pd
from sqlalchemy import create_engine 

user = "asvr410"
password = "RA-one41132025"
warehouse = "my_warehouse"
database = "my_db"
schema = "my_schema"
account = "VYLRAHU-UG05883"
role = "ACCOUNTADMIN"

conn = create_engine('snowflake://{user}:{password}@{account}/'.
                        format(user=user,password=password,account=account,))
