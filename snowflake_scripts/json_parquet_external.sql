use role accountadmin;
use warehouse my_warehouse;
use schema my_db.external_schema;

--parquet
create file format parquet_format
type = 'PARQUET';
create or replace stage parquet_stage
url = 's3://snowflake20072025/testdata/snowflake_external/parquet/'
storage_integration = aws_int
directory= (enable = true);
select * from directory('@parquet_stage');
select $1:variety,$1:"petal.length",$1:"petal.width",$1:"sepal.length",$1:"sepal.width"
 from @parquet_stage/iris.parquet 
(file_format => parquet_format);

select $1:FL_DATE,$1:DISTANCE,$1:AIR_TIME,$1:ARR_DELAY,$1:ARR_TIME,$1:DEP_TIME,$1:DEP_DELAY
 from @parquet_stage/flights-1m.parquet 
(file_format => parquet_format);
create external table flights using template(
select array_agg(object_construct(*)) from table(infer_schema(location =>'@parquet_stage/flights-1m.parquet',
file_format => 'parquet_format')))
location = @parquet_stage
pattern = '.*flight.*'
file_format = 'parquet_format';
show tables;
select *,metadata$filename,metadata$file_row_number from flights;
select $1 from @parquet_stage/house-price.parquet
(file_format => parquet_format);
create external table house_price using template(
select array_agg(object_construct(*)) from 
table(infer_schema(location => '@parquet_stage/house-price.parquet',file_format => 'parquet_format')))
location = @parquet_stage
pattern = '.*house.*'
file_format = 'parquet_format';
select * from house_price;

--json
show stages;
alter stage external_tables_stage refresh;
select * from directory(@external_tables_stage);
create file format json_format
type = 'JSON'
strip_outer_array=true;
select $1 from @external_tables_stage/json/healthcare_facilities.json
(file_format => json_format);
create external table healthcare_facilities using template(
select array_agg(object_construct(*)) from table(infer_schema(location => '@external_tables_stage/json/healthcare_facilities.json',
file_format => 'json_format')))
location = @external_tables_stage
pattern = '.*healthcare.*'
file_format = 'json_format';
select * from healthcare_facilities;
select $1 from @external_tables_stage/json/healthcare_providers.json
(file_format => json_format);
create external table healthcare_providers using template(
select array_agg(object_construct(*)) from 
table(infer_schema(location => '@external_tables_stage/json/healthcare_providers.json',file_format => 'json_format')))
location = @external_tables_stage
pattern = '.*healthcare.*'
file_format = 'json_format';
select * from healthcare_providers;
