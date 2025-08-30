use role accountadmin;
use warehouse my_warehouse;
use schema my_db.my_schema;
show stages;
select * from directory('@aws_s3_stage');
list @aws_s3_stage;
show file formats;

drop table if exists students;
CREATE TABLE students(
   sno INT,
   sname VARCHAR(100),
   sub1 INT,
   sub2 INT,
   sub3 INT,
   sub4 INT
);


INSERT INTO students VALUES
(1, 'Rahul',   85, 78, 92, 88),
(2, 'Priya',   67, 72, 70, 66),
(3, 'Amit',    90, 94, 89, 92),
(4, 'Sneha',   55, 60, 58, 63 )
;

select * from students;
select sno,sname,least(sub1,sub2,sub3,sub4) as least_marks,greatest(sub1,sub2,sub3,sub4) as greatest_marks from students;
with cte as (
    select date_trunc('YEAR',current_date()) dt
)
select dt,to_char(dt,'DY'),week(dt),month(dt),year(dt) from (
select dateadd('day',n,dt) dt from cte,
(select row_number() over(order by 1) - 1 as n from table(generator(rowcount => 365))));
