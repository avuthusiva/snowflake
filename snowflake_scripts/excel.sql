use role accountadmin;
use warehouse my_warehouse;
use database my_db;
use schema my_schema;
show stages;
ls @int_stage;

create or replace procedure sp_load_excel_data_to_tables(file_name string,sheet_name string,table_name string)
returns string
language python
runtime_version='3.12'
packages=('snowflake-snowpark-python','openpyxl')
handler='run'
as
$$
from snowflake.snowpark import Session
from snowflake.snowpark.functions import *
from snowflake.snowpark.types import *
from snowflake.snowpark.files import *
from openpyxl import load_workbook

def run(session,file_name,sheet_name,table_name):
    parts = table_name.split('.')
    if len(parts) != 3:
        return "Please provide the qulified shared table name in the format of database.schema.table_name"
    db,schema,table = parts
    table_exists = session.sql(f"show tables like '{table}' in schema {db}.{schema}").collect()
    if len(table_exists) < 1:
        return f"{table_name} table does not exist"

    with SnowflakeFile.open(file_name,'rb') as f:
        wb = load_workbook(f,data_only=True)
        if sheet_name not in wb.sheetnames:
            raise ValueError(f"Sheet {sheet_name} not found in the Excel file")
        ws = wb[sheet_name]
        rows = list(ws.iter_rows(values_only=True))
        if not rows or len(rows) < 2:
            return "Please provide a valid excel file with at least 2 rows"
        header = [str(row).strip() for row in rows[0]]
        data = [list(data) for data in rows[1:] if any(data)]
        schema = StructType([StructField(col,StringType()) for col in header])
        string_data = [ [ str(cell) if cell is not None else None for cell in row] for row in data]
        df = session.create_dataframe(string_data,schema=schema)
        df.write.mode("append").save_as_table(table_name)
        return f"Data loaded into {table_name} table Successfully"
    return f"Error in loading data into {table_name} table"
$$;

call sp_load_excel_data_to_tables('excel/emp.xlsx','Sheet1','my_db.my_schema.employees');

call sp_load_excel_data_to_tables(build_scoped_file_url(@int_stage,'excel/emp.xlsx'),'Sheet1','my_db.my_schema.employees');
create or replace table employees as select * from emp where 1=2;

select * from employees;

desc table employees;
