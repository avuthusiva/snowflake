use role accountadmin;
use warehouse my_warehouse;
use schema my_db.my_schema;
list @int_stage;
alter stage int_stage refresh;
select * from directory(@int_stage) where relative_path like '%.parquet';
create or replace file format parquet_format
type = parquet;
select $1:id id,$1:created_at created_at,$1:user_id user_id,$1:product_id product_id,$1:quantity quantity,
$1:unit_price unit_price
from @int_stage/files/parquet/orders.parquet 
(file_format=>parquet_format);
select listagg(column_name || ' ' || type,',\n') within group(order by order_id)
from table(infer_schema(location=>'@int_stage/files/parquet/orders.parquet',
file_format=>'parquet_format'));

create table orders
(
id NUMBER(38, 0),
created_at TIMESTAMP_NTZ,
user_id NUMBER(38, 0),
product_id NUMBER(38, 0),
quantity NUMBER(38, 0),
unit_price NUMBER(34, 6)
);
copy into orders from (select $1:id id,$1:created_at created_at,$1:user_id user_id,$1:product_id product_id,$1:quantity quantity,
$1:unit_price unit_price
from @int_stage/files/parquet/orders.parquet 
(file_format=>parquet_format));
select * from orders order by 1;
select $1:id id,$1:created_at created_at,$1:name name,$1:email email,$1:city city,
$1:state state,$1:zip zip,$1:birth_date birth_date,$1:source source
from @int_stage/files/parquet/users.parquet 
(file_format=>parquet_format);
select listagg(column_name || ' ' || type,',\n') within group(order by order_id)
from table(infer_schema(location=>'@int_stage/files/parquet/users.parquet',
file_format=>'parquet_format'));
create table users
(
    id NUMBER(38, 0),
created_at TIMESTAMP_NTZ,
name TEXT,
email TEXT,
city TEXT,
state TEXT,
zip TEXT,
birth_date TEXT,
source TEXT
);
select * from users;
copy into users 
from (select $1:id id,$1:created_at created_at,$1:name name,$1:email email,$1:city city,
$1:state state,$1:zip zip,$1:birth_date birth_date,$1:source source
from @int_stage/files/parquet/users.parquet 
(file_format=>parquet_format));
select * from users order by 1;
create table products as 
select $1:id id,$1:created_at created_at,$1:title title,$1:category category,
$1:ean ean,$1:vendor vendor,$1:price price
from @int_stage/files/parquet/products.parquet
(file_format=>parquet_format);
select * from table(infer_schema(location=>'@int_stage/files/parquet/products.parquet',
file_format=>'parquet_format'));
select * from products;
create table reviews as
select $1:id id,$1:created_at created_at,$1:reviewer reviewer,$1:product_id product_id,
$1:rating rating,$1:body body
from @int_stage/files/parquet/reviews.parquet
(file_format=>parquet_format);
select * from table(infer_schema(location=>'@int_stage/files/parquet/reviews.parquet',
file_format=>'parquet_format'));
select count(*) from reviews;
show tables;
select datediff('days',created_at,current_date()) from users;
show views;
select * from vw_users;
select * from products;
select * from reviews;
select to_char(p.created_at::timestamp,'YYYY-MM-DD')::date created_date,p.id,
p.title,avg(r.rating) avg_rating
from products p,reviews r 
where p.id = r.product_id
group by all
order by 1,2;
