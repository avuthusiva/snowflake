use role accountadmin;
use warehouse my_warehouse;
use schema my_db.my_schema;
select * from emp e,dept d
where e.deptno = d.deptno;
set query_id = '01be7eb5-3201-e221-000e-cef60006995a';
select * from table(get_query_operator_stats($query_id));

show tables;
use schema my_db.github_schema;
show tables;
select * from orders sample(10 rows);
select * from orders sample row (10);
select * from orders sample row (5) seed(1);
select * from orders sample block(5) seed(2);