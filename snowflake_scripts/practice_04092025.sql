use role accountadmin;
use warehouse my_warehouse;
use database my_db;
create schema new_data;
use schema my_db.new_data;
create or replace stage read_new_stage
url = 's3://snowflake20072025/testdata/data/'
storage_integration = aws_int
directory = (enable = true);
list @read_new_stage;
select * from directory(@read_new_stage);
select build_stage_file_url(@read_new_stage,'AWBuildVersion.csv') stage_url;
select build_scoped_file_url(@read_new_stage,'AWBuildVersion.csv') file_url;
select GET_PRESIGNED_URL(@read_new_stage,'AWBuildVersion.csv') file_url;
select $1,$2,$3,$4,$5,$6,$7,$8,$9 from @read_new_stage/Address.csv
(file_format => 'csv_format_tab');
select $1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13 from @read_new_stage/Person.csv
(file_format => 'csv_format_tab');
create or replace file format csv_format_tab
type = csv
field_delimiter = '\t'
skip_header = 0
field_optionally_enclosed_by = '"'
null_if = ('Null','NULL','NULL')
ENCODING = 'iso-8859-1';
create or replace file format csv_format_comma
type = csv
field_delimiter = ','
skip_header = 0
field_optionally_enclosed_by = '"'
null_if = ('Null','NULL','NULL')
encoding = 'iso-8859-1';

with cte as (
select 'AVUTHU SIVA VARDHANA REDDY' str from dual)
select str2.value names from cte,lateral split_to_table(str,' ') str2;

use schema my_db.my_schema;

select * from emp;
select last_query_id();
select * from table(result_scan(last_query_id(-2)));
show primary keys in table emp;
show unique keys in table emp;
alter table emp add constraint pk_emp_empno primary key(empno);
begin
    show primary keys in table emp;
    create or replace table get_primary_key as select * from table(result_scan(last_query_id()));
end;
select * from get_primary_key;
declare
    v_table_name varchar(10) := 'emp';
    v_sql text;
    v_res resultset;
begin
    v_sql := $$select * from $$ || v_table_name;
    execute immediate v_sql;
    v_res := (select * from  table(result_scan(last_query_id())));
    return table(v_res);
end;

select * from table(result_scan(last_query_id()));

with recursive cte as 
(
    select empno,ename, 1 l from emp 
    where mgr is null
    union all
    select e.empno,c.ename || '->' || e.ename,l + 1  from emp e,cte c
    where e.mgr = c.empno
)
select * from cte;
select e.ename,m.ename from emp e join emp m
on (e.mgr = m.empno);