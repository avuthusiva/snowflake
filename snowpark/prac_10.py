from snowflake.snowpark import Session
from snowflake.snowpark.functions import *
from snowflake.snowpark.types import *
from snowflake.snowpark.files import *
import json

with open("config.json") as f:
    config = json.load(f)
    
session = Session.builder.configs(config).create()
emp_df = session.table("emp")
emp_df.with_column("new_sal",when(col("SAL") < 1500, col("SAL") + 200).
                   when((col("SAL") >= 1500) & (col("SAL") <= 4500),col("SAL")+500).
                   otherwise(col("SAL") + 1000)).select(col("EMPNO"),col("ENAME"),col("SAL"),col("new_sal")).show()
session.close()