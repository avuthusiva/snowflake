use role accountadmin;
use warehouse my_warehouse;
use schema my_db.my_schema;
show stages;
list @int_stage;
alter stage int_stage refresh;
select * from directory(@int_stage);
show file formats;
select $1,metadata$filename,metadata$file_row_number from @int_stage/files/csv/hotel_bookings_raw.csv (file_format =>csv_skip_header);
select * from table(infer_schema(location=> '@int_stage/files/csv/hotel_bookings_raw.csv',file_format=>'csv_parse_header'));
create table hotel_bookings using template(
select array_agg(object_construct(*)) from
table(infer_schema(location=> '@int_stage/files/csv/hotel_bookings_raw.csv',file_format=>'csv_parse_header')));
select * from hotel_bookings;
alter table hotel_bookings add column filename text;
alter table hotel_bookings add column file_row_number number;
copy into hotel_bookings from 
(select $1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,metadata$filename,metadata$file_row_number 
from @int_stage/files/csv/hotel_bookings_raw.csv (file_format =>csv_skip_header)); 
select * from hotel_bookings;