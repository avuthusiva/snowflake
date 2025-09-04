use role accountadmin;
use warehouse my_warehouse;
use schema my_db.practice;

create or replace TABLE REGION (
	R_REGIONKEY NUMBER(38,0),
	R_NAME VARCHAR(25),
	R_COMMENT VARCHAR(152)
);
create or replace TABLE NATION (
	N_NATIONKEY NUMBER(38,0),
	N_NAME VARCHAR(25),
	N_REGIONKEY NUMBER(38,0),
	N_COMMENT VARCHAR(152)
);
create or replace TABLE SUPPLIER (
	S_SUPPKEY NUMBER(38,0),
	S_NAME VARCHAR(25),
	S_ADDRESS VARCHAR(40),
	S_NATIONKEY NUMBER(38,0),
	S_PHONE VARCHAR(15),
	S_ACCTBAL NUMBER(12,2),
	S_COMMENT VARCHAR(101)
);
create or replace TABLE CUSTOMER (
	C_CUSTKEY NUMBER(38,0),
	C_NAME VARCHAR(25),
	C_ADDRESS VARCHAR(40),
	C_NATIONKEY NUMBER(38,0),
	C_PHONE VARCHAR(15),
	C_ACCTBAL NUMBER(12,2),
	C_MKTSEGMENT VARCHAR(10),
	C_COMMENT VARCHAR(117)
);
create or replace TABLE PART (
	P_PARTKEY NUMBER(38,0),
	P_NAME VARCHAR(55),
	P_MFGR VARCHAR(25),
	P_BRAND VARCHAR(10),
	P_TYPE VARCHAR(25),
	P_SIZE NUMBER(38,0),
	P_CONTAINER VARCHAR(10),
	P_RETAILPRICE NUMBER(12,2),
	P_COMMENT VARCHAR(23)
);
create or replace TABLE ORDERS (
	O_ORDERKEY NUMBER(38,0),
	O_CUSTKEY NUMBER(38,0),
	O_ORDERSTATUS VARCHAR(1),
	O_TOTALPRICE NUMBER(12,2),
	O_ORDERDATE DATE,
	O_ORDERPRIORITY VARCHAR(15),
	O_CLERK VARCHAR(15),
	O_SHIPPRIORITY NUMBER(38,0),
	O_COMMENT VARCHAR(79)
);
create or replace TABLE LINEITEM (
	L_ORDERKEY NUMBER(38,0),
	L_PARTKEY NUMBER(38,0),
	L_SUPPKEY NUMBER(38,0),
	L_LINENUMBER NUMBER(38,0),
	L_QUANTITY NUMBER(12,2),
	L_EXTENDEDPRICE NUMBER(12,2),
	L_DISCOUNT NUMBER(12,2),
	L_TAX NUMBER(12,2),
	L_RETURNFLAG VARCHAR(1),
	L_LINESTATUS VARCHAR(1),
	L_SHIPDATE DATE,
	L_COMMITDATE DATE,
	L_RECEIPTDATE DATE,
	L_SHIPINSTRUCT VARCHAR(25),
	L_SHIPMODE VARCHAR(10),
	L_COMMENT VARCHAR(44)
);
create or replace TABLE PARTSUPP (
	PS_PARTKEY NUMBER(38,0),
	PS_SUPPKEY NUMBER(38,0),
	PS_AVAILQTY NUMBER(38,0),
	PS_SUPPLYCOST NUMBER(12,2),
	PS_COMMENT VARCHAR(199)
);

show integrations;

create or replace file format csv_format_skip_header
type = csv
skip_header = 1
field_delimiter = ','
field_optionally_enclosed_by = '"'
null_if = ('null','NULL','Null');

create or replace file format csv_format_no_header
type = csv
skip_header = 0
field_delimiter = ','
field_optionally_enclosed_by = '"'
null_if = ('null','NULL','Null');

create or replace file format csv_format_parse_header
type = csv
parse_header = true
field_delimiter = ','
field_optionally_enclosed_by = '"'
null_if = ('null','NULL','Null');

create or replace stage prac_stage
storage_integration = aws_int
url = 's3://snowflake20072025/testdata/practice/'
directory = (enable = true);

select * from directory(@prac_stage);
select * from customer;
desc table customer;
select $1,$2 from @prac_stage/region.csv.gz (file_format => 'csv_format_no_header');
copy into region from @prac_stage/region.csv.gz file_format = (format_name = 'csv_format_no_header');
select * from region;
select $1,$2,$3,$4 from @prac_stage/nation.csv.gz (file_format => 'csv_format_no_header');
copy into nation from @prac_stage/nation.csv.gz file_format = (format_name = 'csv_format_no_header');
select * from nation;
select $1,$2,$3,$4,$5,$6,$7 from @prac_stage/supplier.csv.gz (file_format => 'csv_format_no_header');
copy into supplier from @prac_stage/supplier.csv.gz file_format = (format_name = 'csv_format_no_header');
select * from supplier;
select $1,$2,$3,$4,$5,$6,$7,$8,$9 from @prac_stage/customer.csv.gz (file_format => 'csv_format_no_header');
copy into customer from @prac_stage/customer.csv.gz file_format = (format_name = 'csv_format_no_header');
select * from customer;
select $1,$2,$3,$4,$5,$6,$7,$8,$9 from @prac_stage/part.csv.gz (file_format => 'csv_format_no_header');
copy into part from @prac_stage/part.csv.gz file_format = (format_name = 'csv_format_no_header');
select * from part;
select $1,$2,$3,$4,$5,$6,$7,$8,$9 from @prac_stage/orders.csv.gz (file_format => 'csv_format_no_header');
copy into orders from @prac_stage/orders.csv.gz file_format = (format_name = 'csv_format_no_header');
select * from orders;
select $1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14 from @prac_stage/lineitem.csv.gz 
(file_format => 'csv_format_no_header');
copy into lineitem from @prac_stage/lineitem.csv.gz file_format = (format_name = 'csv_format_no_header');
select * from lineitem;
select $1,$2,$3,$4,$5 from @prac_stage/partsupp.csv.gz (file_format => 'csv_format_no_header');
copy into partsupp from @prac_stage/partsupp.csv.gz file_format = (format_name = 'csv_format_no_header');
select * from partsupp;
declare
    sql_text text;
    v_table_name varchar(25);
    v_res resultset;
    v_table_columns varchar(2000);
begin
    v_table_name := 'region';
    sql_text := 'select listagg(column_name, '','') within group (order by ordinal_position) as columns from 
                    information_schema.columns where table_name = upper(:1)';
    v_res := (execute immediate :sql_text using (v_table_name));
    return table(v_res);
end;

select * from directory(@prac_stage);
select build_stage_file_url(@prac_stage, 'region.csv.gz'),build_scoped_file_url(@prac_stage, 'region.csv.gz');

