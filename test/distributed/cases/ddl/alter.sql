-- @suit
-- @case
-- @desc:alter table add/drop foreign key,alter table add/drop index
-- @label:bvt
create database test;
use test;

-- alter table drop foreign key
drop table if exists foreign01;
create table foreign01(col1 int primary key,
                       col2 varchar(20),
                       col3 int,
                       col4 bigint);
insert into foreign01 values(1,'shujuku',100,3247984);
insert into foreign01 values(2,'数据库',328932,32324423432);
drop table if exists foreign02;
create table foreign02(col1 int,
                       col2 int,
                       col3 int primary key,
                       constraint fk foreign key fk(col1) references foreign01(col1));
insert into foreign02 values(1,1,1);
insert into foreign02 values(2,2,2);
delete from foreign01 where col3 = 100;

show create table foreign02;
alter table foreign02 drop foreign key fk;
show create table foreign02;
drop table foreign01;
drop table foreign02;

-- alter table add/drop foreign key
drop table if exists ti1;
drop table if exists tm1;
drop table if exists ti2;
drop table if exists tm2;

create  table ti1(a INT not null, b INT, c INT);
create  table tm1(a INT not null, b INT, c INT);
create  table ti2(a INT primary key AUTO_INCREMENT, b INT, c INT);
create  table tm2(a INT primary key AUTO_INCREMENT, b INT, c INT);
insert into ti1 values (1,1,1), (2,2,2);
insert into ti2 values (1,1,1), (2,2,2);
insert into tm1 values (1,1,1), (2,2,2);
insert into tm2 values (1,1,1), (2,2,2);

alter table ti1 add constraint fi1 foreign key (b) references ti2(a);
alter table tm1 add constraint fm1 foreign key (b) references tm2(a);
show create table ti1;
show create table tm1;

delete from ti2 where c = 1;
delete from tm2 where c = 1;

alter table ti1 drop foreign key fi1;
alter table tm1 drop foreign key fm1;

show create table ti1;
show create table tm1;

delete from ti2 where c = 1;
delete from tm2 where c = 1;

drop table  ti1;
drop table  tm1;
drop table  ti2;
drop table  tm2;

-- alter table add/drop foreign key in temporary table
-- @bvt:issue#9282
drop table if exists foreign01;
create temporary table foreign01(col1 int primary key,
					 col2 varchar(20),
					 col3 int,
					 col4 bigint);
insert into foreign01 values(1,'shujuku',100,3247984);
insert into foreign01 values(2,'数据库',328932,32324423432);
drop table if exists foreign02;
create temporary table foreign02(col1 int,
					 col2 int,
					 col3 int primary key,
					 constraint fk foreign key fk(col1) references foreign01(col1));
insert into foreign02 values(1,1,1);
insert into foreign02 values(2,2,2);
delete from foreign01 where col3 = 100;

show create table foreign02;
alter table foreign02 drop foreign key fk;
show create table foreign02;

drop table foreign01;
drop table foreign02;

-- alter table add/drop foreign key
drop table if exists ti1;
drop table if exists tm1;
drop table if exists ti2;
drop table if exists tm2;

create temporary table ti1(a INT not null, b INT, c INT);
create temporary table tm1(a INT not null, b INT, c INT);
create temporary table ti2(a INT primary key AUTO_INCREMENT, b INT, c INT);
create temporary table tm2(a INT primary key AUTO_INCREMENT, b INT, c INT);
insert into ti1 values (1,1,1), (2,2,2);
insert into ti2 values (1,1,1), (2,2,2);
insert into tm1 values (1,1,1), (2,2,2);
insert into tm2 values (1,1,1), (2,2,2);

alter table ti1 add constraint fi1 foreign key (b) references ti2(a);
alter table tm1 add constraint fm1 foreign key (b) references tm2(a);
show create table ti1;
show create table tm1;

delete from ti2 where c = 1;
delete from tm2 where c = 1;

alter table ti1 drop foreign key fi1;
alter table tm1 drop foreign key fm1;

show create table ti1;
show create table tm1;

delete from ti2 where c = 1;
delete from tm2 where c = 1;

drop table  ti1;
drop table  tm1;
drop table  ti2;
drop table  tm2;
-- @bvt:issue

-- alter table add index
drop table if exists index01;
create table index01(col1 int,key key1(col1));
show index from index01;
alter table index01 drop index key1;
show index from index01;
alter table Index01 add index key1(col1) comment 'test';
show index from index01;
alter table index01 alter index key1 invisible;
show index from index01;
drop table index01;

drop table if exists index02;
create table index02(col1 int,key key1(col1) comment 'test');
alter table index02 drop index key1;
show index from index02;
alter table index02 add index Key1(col1);
alter table index02 alter index key1 invisible;
show index from index02;
drop table index02;

drop table if exists index03;
create table index03(col1 int, col2 int, col3 int);
alter table index03 add unique key(col1,col2) comment 'abcTest';
show index from index03;
alter table index03 alter index col1 invisible;
show index from index03;
drop table index03;

drop table if exists index03;
create table index03(fld1 int, key key1(fld1) comment 'test');
show index from index03;
alter table index03 drop index key1;
show index from index03;
drop table index03;

drop table if exists index04;
create table index04(col1 int, col2 char, col3 varchar(10),primary key(col1,col2));
show index from index04;
alter table index04 add index(col1);
alter table index04 alter index col1 invisible;
show index from index04;
alter table index04 alter index col1 visible;
show index from index04;
drop table index04;

drop table if exists index05;
create table index05(col1 int, col2 bigint, col3 decimal);
show index from index05;
alter table index05 add unique key(col1,col2);
show index from index05;
alter table index05 alter index col1 invisible;
show index from index05;
drop table index05;

drop table if exists index06;
create table index06(col1 int not null,col2 binary, col3 float,unique key(col1));
alter table index06 add unique index(col2);
show index from index06;
alter table index06 alter index col2 invisible;
show index from index06;
drop table index06;

drop table if exists index07;
CREATE TABLE index07(
                        col1 INT NOT NULL,
                        col2 DATE NOT NULL,
                        col3 VARCHAR(16) NOT NULL,
                        col4 int unsigned NOT NULL,
                        PRIMARY KEY(col1)
);

insert into index07 values(1, '1980-12-17','Abby', 21);
insert into index07 values(2, '1981-02-20','Bob', 22);
insert into index07 values(3, '1981-02-22','Carol', 23);

alter table index07 add constraint unique key (col3, col4);
alter table index07 add constraint unique key wwwww (col3, col4);
alter table index07 add constraint abctestabbc unique key zxxxxx (col3);
show index from index07;
alter table index07 add unique key idx1(col3);
show index from index07;
alter table index07 add constraint idx2 unique key (col3);
alter table index07 add constraint idx2 unique key (col4);
alter table index07 alter index wwwww invisible;
show index from index07;
drop table index07;

drop table if exists index08;
CREATE TABLE index08(
                        col1 INT NOT NULL,
                        col2 DATE NOT NULL,
                        col3 VARCHAR(16) NOT NULL,
                        col4 int unsigned NOT NULL,
                        PRIMARY KEY(col1)
);

insert into index08 values(1, '1980-12-17','Abby', 21);
insert into index08 values(2, '1981-02-20','Bob', 22);

alter table index08 add constraint unique index (col3, col4);
alter table index08 add constraint index wwwww (col3, col4);
alter table index08 add constraint unique index zxxxxx (col3);
show index from index08;
alter table index08 add index zxxxx(col3);
show index from index08;
drop table index08;

drop database test;
