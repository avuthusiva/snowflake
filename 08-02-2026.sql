use role accountadmin;
use warehouse my_warehouse;
use schema my_db.my_schema;
list @int_stage;
select $1,$2,$3,$4,$5,$6,$7,$8,$9,$10 from @int_stage/files/csv/JC-202601-citibike-tripdata.csv.gz
(file_format=>'csv_skip_header');
drop table citybike_tripdata;
-- using template(
select listagg(column_name || ' '|| type,',\n') within group(order by order_id) from table(infer_schema(
location=>'@int_stage/files/csv/JC-202601-citibike-tripdata.csv.gz',
file_format=>'csv_parse_header'));
create table citybike_tripdata
(
ride_id TEXT,
rideable_type TEXT,
started_at TIMESTAMP_NTZ,
ended_at TIMESTAMP_NTZ,
start_station_name TEXT,
start_station_id TEXT,
end_station_name TEXT,
end_station_id TEXT,
start_lat NUMBER(17, 15),
start_lng NUMBER(16, 14),
end_lat NUMBER(17, 15),
end_lng NUMBER(16, 14),
member_casual TEXT);
select * from citybike_tripdata;
copy into citybike_tripdata 
from @int_stage/files/csv/JC-202601-citibike-tripdata.csv.gz
file_format=(format_name = csv_skip_header);
select * from citybike_tripdata limit 10;
show tables like 'my_%';
show views;
select * from MY_FIRST_DBT_MODEL;
select started_at,extract(quarter from started_at) from citybike_tripdata;
