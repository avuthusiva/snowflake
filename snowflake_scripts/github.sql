use role accountadmin;
use warehouse my_warehouse;
use database my_db;
use schema GITHUB_SCHEMA;
show integrations;
show api integrations;
show git repositories;
desc git repository github_repo;
create or replace api integration github_int
enabled = true
api_provider = git_https_api
api_allowed_prefixes = ('https://github.com/avuthusiva/retail_db');
show api integrations;
desc api integration github_int;

create or replace git repository github_repo
api_integration = github_int
origin = 'https://github.com/avuthusiva/retail_db';
show git repositories;
desc git repository github_repo;
show git branches in github_repo;
list @github_repo/branches/master;
alter git repository github_repo fetch;

create or replace procedure pr_hello_world(message varchar)
returns varchar
language python
runtime_version = '3.10'
handler = 'hello.hello_world'
packages = ('snowflake-snowpark-python')
imports = ('@my_db.github_schema.github_repo/branches/master/hello.py');

call pr_hello_world('Hi this is siva');