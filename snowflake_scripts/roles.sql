use role accountadmin;
select current_user(), current_role(), current_database(), current_schema();
grant usage on warehouse my_warehouse to securityadmin;
show grants on warehouse my_warehouse;
use role securityadmin;
create user siva 
password = 'siva@41132025'
must_change_password = false
default_role = public;
grant usage on warehouse my_warehouse to user siva;
show users;
create role my_role;
show roles;
show grants to role my_role;
grant usage on warehouse my_warehouse to role my_role;
grant usage on database my_db to role my_role;
grant usage on schema my_db.my_schema to role my_role;
grant select on all tables in schema my_db.my_schema to role my_role;
grant role my_role to user siva;
