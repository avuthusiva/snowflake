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
select * from table(information_schema.copy_history(table_name=>'hotel_bookings'
,start_time=>dateadd(hour,-1,current_timestamp())));
select * from hotel_bookings;
select * from hotel_bookings where "booking_id" is null;
delete from hotel_bookings where "booking_id" is null;
select * from hotel_bookings where "hotel_id" is null;
delete from hotel_bookings where "hotel_id" is null;
select * from hotel_bookings;
select case when "check_in_date" like '%/%' then to_char(to_date("check_out_date"),'YYYY-MM-DD')
            when "check_in_date" like '%-%' then to_char(to_date("check_out_date"),'YYYY-MM-DD') end check_in_date
 from hotel_bookings;    
select 'Hi SIVA REDDY' txt,snowflake.cortex.translate(txt,'en','hi');
select * from directory(@int_stage);
select * from table(infer_schema(location=>'@int_stage/files/csv/swiggy_data.csv',file_format=>'csv_parse_header'));
select $1,$2,$3,$4,$5,$6,$7,$8,$9,$10 from @int_stage/files/csv/swiggy_data.csv (file_format=>'csv_skip_header');
create table swiggy_data
(
    state text,
    city text,
    order_date date,
    restaurant_name text,
    location text,
    category text,
    dish_name text,
    price number(6,2),
    rating number(2,1),
    rating_count number,
    filename text,
    file_row_number number,
    create_date timestamp default current_timestamp
);
copy into swiggy_data from (
select $1,$2,$3,$4,$5,$6,$7,$8,$9,$10,metadata$filename,metadata$file_row_number,current_timestamp() from 
@int_stage/files/csv/swiggy_data.csv (file_format=>'csv_skip_header'));
select * from swiggy_data;
select state,city,restaurant_name,count(*) from swiggy_data
group by 1,2,3
order by 1;