use role accountadmin;
use warehouse my_warehouse;
use database my_db;
create schema dynamic_schema;
use schema my_db.dynamic_schema;
create or replace file format csv_skip_header
type = csv
field_delimiter = ','
skip_header = 1
field_optionally_enclosed_by = '"'
null_if = ('','Null','NULL','null');
create or replace file format csv_parse_header
type = csv
field_delimiter = ','
parse_header = true
field_optionally_enclosed_by = '"'
null_if = ('','Null','NULL','null');
create or replace stage dyn_stage
url = 's3://snowflake20072025/testdata/Dynamic/'
storage_integration = aws_int
directory = (enable = true);
select * from directory(@dyn_stage);
select $1,$2,$3,$4,$5,$6,$7,$8 from @dyn_stage/csv 
(file_format => csv_skip_header,pattern => '.*emp.*');
create table emp_tab
(
    emp_id int,
    emp_name varchar(100),
    emp_job varchar(100),
    emp_mgr int,
    emp_hiredate date,
    emp_salary int,
    emp_comm int,
    emp_deptno int
);
select * from emp_tab;
desc table emp_tab;
copy into emp_tab from '@dyn_stage/csv'
file_format =(format_name = csv_skip_header) 
pattern = '.*emp.*';
select * from emp_tab;
select count(*) from emp_tab;
create dynamic table emp_dyn 
target_lag = '2 minutes'
warehouse = my_warehouse
refresh_mode = auto
initialize = on_create
as
select * from emp_tab;

select * from emp_dyn 
order by 1 desc;
select count(*) from emp_dyn;
desc dynamic table emp_dyn;
show dynamic tables;
select * from table(information_schema.copy_history(table_name => 'emp_tab',start_time => dateadd(day,-1,getdate())));
select * from table(information_schema.dynamic_table_refresh_history());
select * from table(information_schema.dynamic_table_graph_history());