show databases;
USE demo;
SHOW TABLES ;

-------load 语法规则-------

----------------练习：load data from Local FS or HDFS ------------------
--step1 :建表
-- student_local用于测试从本地加载数据
create table student_local(num int,name string,sex string,age int,dept string)row format delimited fields terminated by ',';
-- student_HDFS 用于测试从HDFS加载数据
create external table student_HDFS(num int,name string,sex string,age int,dept string)row format delimited fields terminated by ',';
-- student_HDFS_P 用于测试从HDFS加载数据到分区表
create table student_HDFS_P(num int,name string,sex string,age int,dept string)partitioned by (country string)  row format delimited fields terminated by ',';

--建议使用beeline客户端 可以显示出加载过程日志信息
--step2:加载数据
-- 从本地加载数据  数据位于HS2（node1）本地文件系统  本质是hadoop fs -put上传操作
LOAD DATA LOCAL INPATH '/home/zz/bin/hive_sets/students.txt' INTO TABLE student_local;

--从HDFS加载数据  数据位于HDFS文件系统根目录下  本质是hadoop fs -mv 移动操作
--先把数据上传到HDFS上  hadoop fs -put /home/zz/bin/hive_sets/students.txt /
LOAD DATA INPATH '/students.txt' INTO TABLE student_HDFS;

----从HDFS加载数据到分区表中并制定分区  数据位于HDFS文件系统根目录下
--先把数据上传到HDFS上 hadoop fs -put /home/zz/bin/hive_sets/students.txt /
LOAD DATA INPATH '/students.txt' INTO TABLE student_HDFS_p partition(country ="China");


-------hive 3.0 load命令新特性------------------
CREATE TABLE if not exists tab1 (col1 int, col2 int)
    PARTITIONED BY (col3 int)
    row format delimited fields terminated by ',';

--正常情况下  数据格式如下
--11,22
--33,44
LOAD DATA LOCAL INPATH '/root/hivedata/xxx.txt' INTO TABLE tab1 partition(col3="1");

--在hive3.0之后 新特性可以帮助我们把load改写为insert as select
LOAD DATA LOCAL INPATH '/root/hivedata/tab1.txt' INTO TABLE tab1;

--tab1.txt内容如下
--11,22,1
--33,44,2

select * from tab1;