show databases;
-- 切换数据库
use demo;
create table t_alter(
    id int comment "ID",
    name string comment "英雄名称",
    hp_max int comment "最大生命",
    mp_max int comment "最大法力",
    attack_max int comment "最高物攻",
    defense_max int comment "最大物防",
    attack_range string comment "攻击范围",
    role_main string comment "主要定位",
    role_assist string comment "次要定位"
) comment "王者荣耀射手信息"
    row format delimited fields terminated by "\t";

show tables ;
select * from t_alter;

--复杂数据类型建表

create table t_hot_hero_skin_price(
    id int,
    name string,
    win_rate int,
    skin_price map<string,int>

) row format delimited fields terminated by ',' --指定字段之间分隔符
collection items terminated by '-' --指定集合元素之间的分隔符
map keys terminated by ':';     --指定map元素kv之间的分隔符
show tables ;
select * from t_hot_hero_skin_price;

-- 内部表和外部表的区别
--创建内部表
create table student(
    num int,
    name string,
    sex string,
    age int,
    dept string
)row format delimited fields terminated by ',';

describe formatted demo.student;

--创建外部表
create EXTERNAL table student(
                        num int,
                        name string,
                        sex string,
                        age int,
                        dept string
)row format delimited fields terminated by ',';


--注意分区表创建语法规则
--分区表建表
create table t_all_hero_part(
                                id int,
                                name string,
                                hp_max int,
                                mp_max int,
                                attack_max int,
                                defense_max int,
                                attack_range string,
                                role_main string,
                                role_assist string
) partitioned by (role string)--注意哦 这里是分区字段
    row format delimited
        fields terminated by "\t";

-- 分桶表
create table if not exists t_usa_covid19_bucket(
    count_date string,
    country string,
    state string,
    fips int,
    cases int,
    deaths int)
    clustered by (state) into 5 buckets ; -- 分桶的字段一定是表中已经存在的字段

-- 根据case倒序排序
create table t_usa_covid19_bucket_sort(
                                     count_date string,
                                     country string,
                                     state string,
                                     fips int,
                                     cases int,
                                     deaths int)
    clustered by (state)
        sorted by (cases desc ) into 5 buckets ;

-- 分桶表的数据加载
--step1：开启分桶的功能，从hive2.0开始不需要设置
set hive.enforce.bucketing=true;
-- step2：把源数据加载到普通hive表中
create table t_usa_covid19(
                                     count_date string,
                                     country string,
                                     state string,
                                     fips int,
                                     cases int,
                                     deaths int)
    row format delimited fields terminated by ",";
-- 将源数据上传到普通表中
--step3：使用insert+select语法加载到分桶表中
insert into t_usa_covid19_bucket select * from t_usa_covid19;

---Hive View视图相关语法
--hive中有一张真实的基础表t_usa_covid19
--select *
--from itheima.t_usa_covid19;

--1、创建视图
--create view v_usa_covid19 as select count_date, county,state,deaths from t_usa_covid19 limit 5;

--能否从已有的视图中创建视图呢  可以的
--create view v_usa_covid19_from_view as select * from v_usa_covid19 limit 2;

--2、显示当前已有的视图
--show tables;
--show views;--hive v2.2.0之后支持

--3、视图的查询使用
--select *
--from v_usa_covid19;

--能否插入数据到视图中呢？
--不行 报错  SemanticException:A view cannot be used as target table for LOAD or INSERT
--insert into v_usa_covid19 select count_date,county,state,deaths from t_usa_covid19;

--4、查看视图定义
--show create table v_usa_covid19;

--5、删除视图
--drop view v_usa_covid19_from_view;
--6、更改视图属性
  --  alter view v_usa_covid19 set TBLPROPERTIES ('comment' = 'This is a view');
--7、更改视图定义
    --alter view v_usa_covid19 as  select county,deaths from t_usa_covid19 limit 2;



--通过视图来限制数据访问可以用来保护信息不被随意查询:
create table userinfo(firstname string, lastname string, ssn string, password string);

create view safer_user_info as select firstname, lastname from userinfo;

--可以通过where子句限制数据访问，比如，提供一个员工表视图，只暴露来自特定部门的员工信息:
create table employee(firstname string, lastname string, ssn string, password string, department string);

--create view techops_employee as select firstname, lastname, ssn from userinfo where department = 'java';


--使用视图优化嵌套查询
--from (
 --        select * from people join cart
   --                                on(cart.pepople_id = people.id) where firstname = 'join'
     --)a select a.lastname where a.id = 3;

--把嵌套子查询变成一个视图
--create view shorter_join as
--select * from people join cart
  --                        on (cart.pepople_id = people.id) where firstname = 'join';
--基于视图查询
--select lastname from shorter_join where id = 3;


--事务表
--Hive中事务表的创建使用
--1、开启事务配置（可以使用set设置当前session生效 也可以配置在hive-site.xml中）
--set hive.support.concurrency = true; --Hive是否支持并发
--set hive.enforce.bucketing = true; --从Hive2.0开始不再需要  是否开启分桶功能
--set hive.exec.dynamic.partition.mode = nonstrict; --动态分区模式  非严格
--set hive.txn.manager = org.apache.hadoop.hive.ql.lockmgr.DbTxnManager; --
--set hive.compactor.initiator.on = true; --是否在Metastore实例上运行启动线程和清理线程
--set hive.compactor.worker.threads = 1; --在此metastore实例上运行多少个压缩程序工作线程。

--2、创建Hive事务表
drop table if exists trans_student;
create table trans_student(
                              id int,
                              name String,
                              age int
)clustered by (id) into 2 buckets stored as orc TBLPROPERTIES('transactional'='true');
--注意 事务表创建几个要素：开启参数、分桶表、存储格式orc、表属性

--物化视图
--先创建一张事务表
set hive.support.concurrency = true; --Hive是否支持并发
set hive.enforce.bucketing = true; --从Hive2.0开始不再需要  是否开启分桶功能
set hive.exec.dynamic.partition.mode = nonstrict; --动态分区模式  非严格
set hive.txn.manager = org.apache.hadoop.hive.ql.lockmgr.DbTxnManager; --
set hive.compactor.initiator.on = true; --是否在Metastore实例上运行启动线程和清理线程
set hive.compactor.worker.threads = 1; --在此metastore实例上运行多少个压缩程序工作线程

CREATE TABLE student_trans (
                               sno int,
                               sname string,
                               sdept string)
    clustered by (sno) into 2 buckets stored as orc TBLPROPERTIES('transactional'='true');

--2、导入数据到student_trans中
insert overwrite table student_trans
select num,name,dept
from student;

select *
from student_trans;

--3、对student_trans建立聚合物化视图
CREATE MATERIALIZED VIEW student_trans_agg
AS SELECT sdept, count(*) as sdept_cnt from student_trans group by sdept;

--注意 这里当执行CREATE MATERIALIZED VIEW，会启动一个MR对物化视图进行构建
--可以发现当下的数据库中有了一个物化视图
show tables;
show materialized views;

--4、对原始表student_trans查询
--由于会命中物化视图，重写query查询物化视图，查询速度会加快（没有启动MR，只是普通的table scan）
SELECT sdept, count(*) as sdept_cnt from student_trans group by sdept;

--5、查询执行计划可以发现 查询被自动重写为TableScan alias: itcast.student_trans_agg
--转换成了对物化视图的查询  提高了查询效率
explain SELECT sdept, count(*) as sdept_cnt from student_trans group by sdept;


--验证禁用物化视图自动重写
ALTER MATERIALIZED VIEW student_trans_agg DISABLE REWRITE;

--删除物化视图
drop materialized view student_trans_agg;

-------------------Database 数据库 DDL操作---------------------------------------
--创建数据库
create database if not exists itcast
    comment "this is my first db"
    with dbproperties ('createdBy'='Allen');

--描述数据库信息
describe database itcast;
describe database extended itcast;
desc database extended itcast;

--切换数据库
use default;
use itcast;
create table t_1(id int);

--删除数据库
--注意 CASCADE关键字慎重使用
--DROP (DATABASE|SCHEMA) [IF EXISTS] database_name [RESTRICT|CASCADE];
drop database itcast cascade ;


--更改数据库属性
--ALTER (DATABASE|SCHEMA) database_name SET DBPROPERTIES (property_name=property_value, ...);
--更改数据库所有者
--ALTER (DATABASE|SCHEMA) database_name SET OWNER [USER|ROLE] user_or_role;
--更改数据库位置
--ALTER (DATABASE|SCHEMA) database_name SET LOCATION hdfs_path;


-------------------Table 表 DDL操作---------------------------------------

--查询指定表的元数据信息
--describe formatted itheima.student_partition;

--1、更改表名
--ALTER TABLE table_name RENAME TO new_table_name;
--2、更改表属性
--ALTER TABLE table_name SET TBLPROPERTIES (property_name = property_value, ... );
--更改表注释
--ALTER TABLE student SET TBLPROPERTIES ('comment' = "new comment for student table");
--3、更改SerDe属性
--ALTER TABLE table_name SET SERDE serde_class_name [WITH SERDEPROPERTIES (property_name = property_value, ... )];
--ALTER TABLE table_name [PARTITION partition_spec] SET SERDEPROPERTIES serde_properties;
--ALTER TABLE table_name SET SERDEPROPERTIES ('field.delim' = ',');
--移除SerDe属性
--ALTER TABLE table_name [PARTITION partition_spec] UNSET SERDEPROPERTIES (property_name, ... );

--4、更改表的文件存储格式 该操作仅更改表元数据。现有数据的任何转换都必须在Hive之外进行。
--ALTER TABLE table_name  SET FILEFORMAT file_format;
--5、更改表的存储位置路径
--ALTER TABLE table_name SET LOCATION "new location";
-------------------Partition分区 DDL操作---------------------------------------
--1、增加分区
--step1: 创建表 手动加载分区数据
drop table if exists t_user_province;
create table t_user_province (
                                 num int,
                                 name string,
                                 sex string,
                                 age int,
                                 dept string) partitioned by (province string);

load data local inpath '/home/zz/bin/hive_sets/students.txt' into table t_user_province partition(province ="SH");

