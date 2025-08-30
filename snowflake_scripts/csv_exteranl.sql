use role accountadmin;
use warehouse my_warehouse;
use database my_db;
create schema external_schema;
use schema my_db.external_schema;
show file formats;
create file format csv_without_header
type = 'csv'
field_delimiter = ','
skip_header = 0
field_optionally_enclosed_by = '"'
null_if = ('', 'NULL','null','Null');
desc file format csv_without_header;
create or replace stage external_tables_stage
storage_integration = aws_int
url = 's3://snowflake20072025/testdata/snowflake_external/csv/';
list @external_tables_stage;
desc stage external_tables_stage;
alter stage external_tables_stage set directory = (enable = true);
alter stage external_tables_stage set url = 's3://snowflake20072025/testdata/snowflake_external/';
alter stage external_tables_stage refresh;
select * from directory(@external_tables_stage);
select $1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,metadata$filename,metadata$file_row_number
from @external_tables_stage/csv/
(file_format => 'csv_without_header',pattern=> '.*emp.*csv.*');

create or replace external table emp_ext
(
    employee_id     varchar as (value:c1::varchar),
    first_name      varchar as (value:c2::varchar),
    last_name       varchar as (value:c3::varchar),
    email           varchar as (value:c4::varchar),
    phone_number    varchar as (value:c5::varchar),
    hiredate        varchar as (value:c6::varchar),
    job_id          varchar as (value:c7::varchar),
    salary          varchar as (value:c8::varchar),
    commission_pct  varchar as (value:c9::varchar),
    manager_id      varchar as (value:c10::varchar),
    department_id   varchar as (value:c11::varchar),
    file_name       varchar as (metadata$filename),
    file_row_number varchar as (metadata$file_row_number::varchar)
) 
location = @external_tables_stage/csv/
pattern = '.*emp.*csv.*'
file_format = 'csv_without_header';
select * from emp_ext;
select metadata$filename, metadata$file_row_number from emp_ext;
select distinct metadata$filename from emp_ext;
select metadata$filename filename,max(metadata$file_row_number) rows_count from emp_ext
group by metadata$filename;
select * from table(information_schema.external_table_files('EMP_EXT'));
select * from table(information_schema.external_table_file_registration_history('EMP_EXT'));
alter external table emp_ext refresh;
list @external_tables_stage;
select $1,$2,$3,$4,$5,$6,$7,$8 from @external_tables_stage/csv/ 
(file_format=>'csv_without_header', pattern=>'.*customer.*');
create file format csv_skip_header
type = 'csv'
field_delimiter = ','
field_optionally_enclosed_by = '"'
null_if = ('Null','','NULL','null')
skip_header = 1;
create external table customers
location = @external_tables_stage/csv/
pattern = '.*customer.*'
file_format = 'csv_skip_header';
select * from customers;
select * from table(information_schema.external_table_files('CUSTOMERS'));
select * from table(information_schema.external_table_file_registration_history('CUSTOMERS'));
select c.value:c1,c.value:v2,c.value:v3,c.value:v4,c.value:v5,c.value:v6,c.value:v7,c.value:v8 from customers c;
desc external table customers;
show external tables;

