SELECT * FROM `information_schema`.`character_sets` LIMIT 0,1000;
SELECT * FROM `information_schema`.`columns` LIMIT 0,1000;
SELECT * FROM `information_schema`.`key_column_usage` LIMIT 0,1000;
SELECT * FROM `information_schema`.`PROCESSLIST` LIMIT 0,1000;
SELECT * FROM `information_schema`.`profiling` LIMIT 0,1000;
SELECT * FROM `information_schema`.`schemata` LIMIT 0,1000;
SELECT * FROM `information_schema`.`tables` LIMIT 0,1000;
SELECT * FROM `information_schema`.`triggers` LIMIT 0,1000;
SELECT * FROM `information_schema`.`user_privileges` LIMIT 0,1000;
SELECT * FROM `mysql`.`columns_priv` LIMIT 0,1000;
SELECT * FROM `mysql`.`db` LIMIT 0,1000;
SELECT * FROM `mysql`.`procs_priv` LIMIT 0,1000;
SELECT * FROM `mysql`.`tables_priv` LIMIT 0,1000;
SELECT * FROM `mysql`.`user` LIMIT 0,1000;
use mysql;
show tables;
show columns from `user`;
show columns from `db`;
show columns from `procs_priv`;
show columns from `columns_priv`;
show columns from `tables_priv`;
use information_schema;
show tables;
show columns from `KEY_COLUMN_USAGE`;
show columns from `COLUMNS`;
show columns from `PROFILING`;
show columns from `PROCESSLIST`;
show columns from `USER_PRIVILEGES`;
show columns from `SCHEMATA`;
show columns from `CHARACTER_SETS`;
show columns from `TRIGGERS`;
show columns from `TABLES`;