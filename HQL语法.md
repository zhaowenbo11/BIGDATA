# HQL

## DDL（数据定义语言），是SQL语言集中对数据库内部的对象结构进行创建，删除，修改等的操作语言

## HIVE中DDL语法的使用
- HQL 与标准的 SQL的语法大同小异，基本相通，注意差异即可
- 基于Hive的设计、使用特点，**HQL中create语法（尤其create table）将是学习掌握HIve DDL语法的重中之重**

![](F:/大数据开发/Hive3.x/HQL语法.jpg)

# Hive数据类型详解
## 整体概述
- Hive数据类型指的是表中列的字段类型
- 整体分为两类：**原生数据类型(primitive data type)**和**复杂数据类型(complex data type)**
- 原生数据类型包括：数值类型、时间日期类型、字符串类型、杂项数据类型
- 复杂数据类型包括：array数组、map映射、struct结构、union联合体

## 注意事项
- HQL中，数据类型英文字母大小写不敏感
- 除SQL数据类型外，还支持JAVA数据类型，比如字符串string
- 复杂数据类型的使用通常需要和**分隔符指定语法**配合使用
- 如果定义的数据类型和文件不一致，Hive会尝试隐式转换，但是不保证成功
- 显示类型转换使用**CAST函数**，例如，CAST（‘100’ AS INT)

# Hive读写文件机制

## SerDe是什么
- SerDe 是Serializer、Deserializer的简称，目的是用于序列化和反序列化
- 序列化是对象转化为字节码的过程，反序列化是字节码转化为对象的过程
- Hive使用SerDe（包括FileFormat）读取和写入表**行对象**。需要注意的是，“key”部分在读取时会被忽略，而在写入时key始终是常数。基本上**行对象存储在“value”中**
- Read：HDFS files --> InputFileFormat--><key,value>-->Deserializer-->Row object
- Write：Row object-->Serializer--><key,value>-->OutputFileFormat-->HDFS files
- 可以通过 desc formatted tablename查看表的相关SerDe信息

## SerDe相关语法
- **ROW FORMAT**这一行所代表的是跟读写文件、序列化SerDe相关的语法
- 如果使用delimited表示使用默认的LazySimpleSerDe类来处理数据
- 如果数据文件格式比较特殊可以使用ROW FORMAT SERDR serdes_name指定其他的Serdr类来处理数据，甚至支持用户自定义SerDe类

## LazySimpleSerDe分隔符指定
ROW FORMAT DELIMITED
		[FIELDS TERMINATED BY char]——字段之间分隔符
		[COLLECTION ITEMS TERMINATED BY char]——集合元素之间分隔符
		[MAP KEYS TERMINATED BY char]——Map映射kv之间分隔符
		[LINES TERMINATED BY char]——行数据之间分隔符
		
## Hive默认分隔符
- hive建表时如果没有ROW FORMAT语法指定分隔符，则采用默认分隔符；
- 默认的分隔符是'\001'

# Hive数据存储路径
## 默认存储路径是hdfs:/usr/hive/warehouse
## 指定存储路径 LOCATION '<hdfs_location>'

# Hive内、外部表

## 什么是内部表
- 内部表（Internal table）也称为被Hive拥有和管理的托管表（Managed table）
- 默认情况下创建的表是内部表，Hive拥有该表的结构和文件。换句话说，hive完全管理表（元数据和数据）的生命周期，类似于RDBMS中的表
- 当删除内部表是，它会删除数据以及表的元数据
- 可以用DESCRIBE FORMATTED  tablename查看

## 什么是外部表
- 外部表（External table）中的数据不是hive拥有或管理的，只管理表元数据的生命周期
- 创建外部表需要使用EXTERNAL语法关键字
- 删除外部表只会删除元数据，不会删除实际数据。在Hive外部仍然可以访问实际数据
- 实际场景中，外部表搭配location语法指定数据的路径，可以让数据更安全

## 如何选择内、外部表
- 当需要通过hive完全管理控制表的整个生命周期时，使用内部表
- 当数据来之不易，防止误删，使用外部表，即使删除表，文件也会被保留

# Hive分区表
## 概念
- 当Hive表对应的数据量大、文件个数多时，为了避免查询全表扫描数据，HIve支持**根据指定的字段对表进行分区**，分区的字段可以是日期、地域、种类等具有标识意义的字段
- 比如把一整年的数据根据月份划分12个月（12个分区），后续就可以查询指定月份分区的数据，尽可能避免了全表扫描查询。

## 分区表数据加载--静态分区
- 所谓静态分区指的是分区的属性值是由用户加载数据的时候手动指定的
- 语法：load data [local] inpath 'filepath' into table tablename partition(分区字段='分区值')
- local参数用于指定待加载的数据是位于本地文件系统还是HDFS文件系统

### 非分区表 全表扫描过滤查询
select count(*) from tablename where role_main="" and hx_max >6000;
### 分区表 先基于分区过滤再查询
select count(*) from tablename where role="" and hp_max> 6000;
## 多重分区表
- -----多重分区表
- 单分区表，按省份分区
- create table t_user_province (id int, name string,age int) partitioned by (province string);
- 双分区表，按省份和市分区
- 分区字段之间是一种递进的关系 因此要注意分区字段的顺序 谁在前在后
- create table t_user_province_city (id int, name string,age int) partitioned by (province string, city string);

--双分区表的数据加载 静态分区加载数据
load data local inpath '/root/hivedata/user.txt' into table t_user_province_city
    partition(province='zhejiang',city='hangzhou');
load data local inpath '/root/hivedata/user.txt' into table t_user_province_city
    partition(province='zhejiang',city='ningbo');
load data local inpath '/root/hivedata/user.txt' into table t_user_province_city
    partition(province='shanghai',city='pudong');

## 分区表数据加载--动态分区
- set hive.exec.dynamic.partition=true;
- set hive.exec.dynamic.partition.mode=nonstrict; 严格模式下，用户必须至少指定一个静态分区，非严格模式下，允许所有分区都是动态的
- --执行动态分区插入
- insert into table t_all_hero_part_dynamic partition(role) --注意这里 分区值并没有手动写死指定
- select tmp.*,tmp.role_main from t_all_hero tmp;


## 分区表的注意事项
#### 一、分区表不是建表的必要语法规则，是一种优化手段表
#### 二、分区字段不能是表中已有的字段，不能重复
#### 三、分区字段是虚拟字段，其数据并不存储在底层文件中
#### 四、分区字段的确定来自于用户价值数据手动指定（静态分区）或者根据查询结果位置自动推断（动态分区）
#### 五、Hive支持多重分区，也就是说在分区的基础上继续分区，划分更加细粒度

# 分桶表（BUCKET)
## 概念
- 是一种用于优化查询而设计的表
- 分桶表对应的数据文件在底层会被分解为若干个部分，通俗来说就是被拆分成若干个独立的小文件
- 在分桶时，要指定根据哪个字段将数据分为几个桶

## 规则
- 分桶规则如下：桶编号相同的数据会被分到同一个桶
- Bucket number = hash_function(bucketing_column) mod num_buckets
- 分桶编号       = 哈希方法       （分桶字段）         取模  分桶个数
- hash_function取决于分桶字段bucketing_column的类型：
	1. 如果是int类型，hash_function(int) == int
	2. 如果是其他比如：bigint，string或者复杂数据类型，hash_function比较棘手、
- 建表语法：[CLUSTERED BY(col_name,col_name,...)[SORTED BY(col_name[ASC|DESC],...)] INTO num_buckets BUCKETS]
- 需要注意的是分桶字段必须是表中存在的字段

## 分桶表的数据加载 （hive.sql)

## 使用分桶表的好处
1. 基于分桶字段查询时，减少全表扫描
2. JION时可以提高MR程序效率，减少笛卡尔积数量
	- 根据join 的字段对表进行分桶操作
3. 分桶表数据进行高效抽样
	当数据量特别大时，对全体数据进行处理存在困难时，抽样就显得尤其重要。抽样可以从被抽取的数据中估计和推断出整体的特性，是科学实验、质量检验、社会调查普遍采用的一种经济有效的工作和研究方法

# 事务表（ACID）
## 背景
- hive本身设计之初，并不支持事务。因为hive的核心目标是将已经存在的结构化数据文件映射成为表，然后提供基于表的SQL分析处理，是一款面向分析的工具。且映射的数据通常存储于HDFS上，而HDFS不支持修改文件数据

## 局限性
1. 尚不支持BEGIN、COMMIT和ROLLBACK。所有语言操作都是自动提交的
2. 仅支持ORC文件格式（STORED AS ORC）
3. 默认情况下事务配置为关闭，需要配置参数开启使用
4. 表必须是分桶表才可以使用事务功能
5. 表参数transactional必须为true
6. 外部表不能成为ACID表，不允许从非ACID会话读取/写入ACID表

## 语法：clustered by (id) into 2 buckets stored as orc TBLPROPERTIES('transactional'='true');

# Hive Views 视图
## 概念
1. Hive中的视图（view）是一种虚拟表，只保存定义，不实际存储数据
2. 通常从真实的物理表查询中创建生成视图，也可以从已经存在的视图上创建新视图
3. 创建视图时，将冻结视图的架构，如果删除或更改基础表，则视图将失败
4. 视图是用来简化操作的，不缓冲记录，也没有提高查询效率

## 使用视图的好处
1. 将真实表中特定的列数据提供给用户，保护数据隐式
2. 降低查询的复杂度，优化查询语句  

# Hive3.0新特性：Materialized Views 物化视图
## 概念
- 物化视图是一个包括查询结果的数据库对象，可以用于预先计算并保存表连接或聚集等耗时较多的操作的结果。在执行查询时，就可以避免进行这些耗时的操作，从而快速的得到结果
- 使用物化视图的目的就是通过预计算，提高查询性能，当然需要占用一定的存储空间
- 物化视图的查询自动重写机制
- 还提供了物化视图存储选择机制，可以本地存储在Hive，也可以通过用户自定义storage handlers存储在其他系统（如Druid）

## 物化视图、视图的区别
- view是虚拟的，逻辑存在的，只有定义没有存储数据
- 物化视图是真实的，物理存在的，里面存储着预计算的数据
- 视图的目的简化降低查询的复杂度，而物化视图的目的是提高查询性能

## 基于物化视图的查询重写
- 物化视图创建后即可用于相关查询的加速，即：用户提交查询query，若该query经过重写后可以命中已经存在的物化视图，则直接通过物化视图查询数据返回结果，以实现查询加速
- 是否重写查询使用物化视图可以通过全局参数控制，默认为true：hive.materializedview.rewriting=true
- 用户可选择性的控制指定的物化视图查询重写机制，语法：ALTER MATERIALIZED VIEW[db_name.]materialized_view_name ENABLE|DISABLE REWRITE;


# database DDL
## 整体概述
- 在HIve中，DATABASE和RDBMS中类似，可以互换
- 默认数据库为defalt

## create database
- CREATE DATABASE
- desc database

# Table DDL操作
- describe formatted table_name 查看表中元数据信息（formatted关键字将以表格格式显示元数据）
- drop table [if exists] table_name[purge] 如果指定了purge，则表数据跳过hdfs的垃圾桶
- truncate table 从表中删除所有行，清空表的所有数据但是保留表的元数据结构
- alter table table_name rename to new_table_name 修改表名
- alter table table_name set tblproperties(property_name = property_value,...) 更改表属性
- alter table table_name set tblproperties('comment'="new comment")更改表注释
- alter table table_name set serde serde_class_name[WITH SERDEPROPERTIES(PROPERTY_NAME=PROPERTY_VALUE,...)] 更改serde属性

# Partition(分区) DDL
## 整体概述
- 针对partition的操作主要包括：增加分区、删除分区、重命名分区、修复分区、修改分区
1. add partition
	- app partition会更改表元数据，但不会加载数据。如果分区位置中不存在数据，查询时将不会返回结果，因此需要保证在增加的分区位置路径下，数据已经存在，或者增加完分区之后导入分区数据
2. rename partition
	- ALTER TABLE table_name PARTITION partition_spec RENAME TO PARTITION partition_spec
3. delete partition  这将删除该分区的数据和元数据
	- ALTER TABLE table_name DROP [IF EXISTS] PARTITION (dt='',country='')
	- ALTER TABLE table_name DROP [IF EXISTS] PARTITION (dt='',country='')PURGE 直接删除数据，不进垃圾桶

4.alter partition
	- 更改分区文件存储格式
	- ALTER TABLE table_name PARTITION(dt='')SET FILEFORMAT file_format
	- 更改分区位置
	- ALTER TABLE table_name PARTITION(dt='')SET LOCATION "new location"
5. MSCK partition
	- hive将每个表的分区列表信息存储在其meatstore中。但是，如果将新分区直接添加到hdfs下（例如通过使用hdfs dfs -put命令）或从HDFS中直接删除分区文件夹，则除非用户ALTER TABLE table_name ADD/DROP PARTITION在每个新添加的分区上运行命令，否则，meta store将不会意识到分区信息的这些修改
	- MSCK是meta store的缩写，表示元数据检查操作，可用于元数据的修复
	- MSCK [REPAIR] TABLE table_name [ADD/DROP/SYNC PARTITION]
	- MSCK默认行为ADD PARTITION 它将把HDFS上存在元存储中不存在的所有分区添加到meta store
	- DROP PARTITION 选项将从已经从HDFS中删除的meta store中删除分区信息
	- 如果存在大量未跟踪的分区，则可以批量运行MSCK REPAIR TABLE,以避免OOME（内存不足错误）

# Hive Show语法
1.显示所有数据库：show databases / show schemas
2.显示当前数据库所有表/视图/物化视图/分区/索引
	- show tables;
	- SHOW TABLES [IN database_name]; --指定某个数据库
3. 显示当前数据库下所有视图
	- Show Views;
	- SHOW VIEWS 'test_*'; -- show all views that start with "test_"
	- SHOW VIEWS FROM test1; -- show views from database test1
	- SHOW VIEWS [IN/FROM database_name];
4. 显示当前数据库下所有物化视图

  - SHOW MATERIALIZED VIEWS [IN/FROM database_name];
5. 显示表分区信息，分区按字母顺序列出，不是分区表执行该语句会报错
	- show partitions table_name;
	- show partitions itheima.student_partition;
6. 显示表/分区的扩展信息
	- SHOW TABLE EXTENDED [IN|FROM database_name] LIKE table_name;
	- show table extended like student;
	- describe formatted itheima.student;
7. 显示表的属性信息

  - SHOW TBLPROPERTIES table_name;
8. 显示表、视图的创建语句
	- show create table student;
	- SHOW CREATE TABLE ([db_name.]table_name|view_name);
9. 显示表中的所有列，包括分区列

  - show columns  in student;
10. 显示当前支持的所有自定义和内置的函数
	- show functions;
# Hive SQL-DML语法
# DML-Load加载数据

## Load语法
- **LOAD DATA [LOCAL] INPATH 'filepath' [OVERWRITE] INTO TABLE tablename [PARTITION (partcol1=val1, partcol2=val2 ...)]**
- 在将数据load加载到表中，hive不会进行任何的转换，移动时是纯复制、移动操作
- **filepath**:表示待移动数据的路径
- **LOCAL**：如果指定了LOCAL,load命令将在本地文件系统中查找文件路径，这里的**本地文件系统指的是hiveserver2服务所在机器的本地linux文件系统**
- **OVERWRITE**:如果使用OVERWRITE关键字，则目标表（或分区）中的内容会被删除，然后再将filepath指向的文件/目录中的内容添加到表/分区中

## Hive3.0 LOAD 新特性
- 在某些场合下，还会将加载重写为 INSERT AS SELECT
- 还支持使用imputformat、SerDe指定输入格式，如Text、ORC等
- 比如，如果表具有分区，则load命令没有指定分区，则将load转换为INSERT AS SELECT 并假定最后一组列为分区列，如果文件不符合预期，则报错

# Hive insert
## insert+value 方式插入数据效率低，耗时长
## insert+select
- 表示后面查询返回的结果作为内容插入到指定表中
- 需要保证查询结果列的数目和需要插入数据表格的列目录一致
- 如果查询处理的数据类型和插入表格对应的列数据类型不一致，将会转换，但不一定能成功
- ** INSERT OVERWRITE TABLE table_name [partition (partcol1=val1,partcol2=val2...)[IF NOT EXISTS] select_statement1 FROM from_statement]
- INSERT INTO TABLE table_name [partition (partcol1=val1,partcol2=val2...)] select_statement1 FROM from_statement **

## multiple inserts 多重插入
- 核心功能：一次扫描，多次插入
- 目的就是减少扫描次数

## partition insert 动态分区插入
- 通过load命令加载的数据的过程中，分区值是手动指定写死的，叫做静态分区
- 动态分区插入指的是：分区的值是由后续的select查询语句的结果来动态确定的

## insert directory 导出数据
- hive支持将select查询的结果导出成文件存放在文件系统中
- 注意：导出操作是一个overwrite覆盖操作，慎重

# Hive Transaction 事务表
## 实现原理
- 用HDFS文件作为原始数据（基础数据），用delta保存事务操作的记录增量数据
- 正在执行中的事务，是以一个staging开头的文件夹维护的，执行结束就是delta文件夹。每次执行一次事务操作都会有这样的一个delta增量文件夹
- 当访问hive数据时，根据HDFS原始文件和delta增量文件做合并，查询最新的数据
- INSERT 语句会直接创建delta目录
- DELETE目录的前缀是delete_data
- UPDATE语句采用了split-update特性，即先删除、后插入

### delta文件夹命名格式
- delta_minWID_maxWID_stmtID
- hive会为写事务创建一个写事务ID（Write ID），该ID在表范围内唯一
- 语句ID（stmtID）则是当一个事务中有多条写入语句时使用的，用作唯一标识
- bucket_00000文件是写入的数据内容，需要使用ORC TOOLS查看
- operation:0表示插入，2表示删除，
- originalTransaction\currentTransaction：该条记录的原始写事务ID、当前的写事务ID
- rowId：一个自增的唯一ID，在写事务和分桶的组合中唯一
- row：具体数据。对于DELETE语句，则为null；对于INSERT语句，则为插入的数据；对于UPDATE就是更新后的数据

### 合并器（Compactor）
- 随着表的修改操作，创建了越来越多的delta增量文件，就需要合并以保持足够的性能
- 合并器是一套在Hive Metastore内运行
- 合并操作分为两种

### 事务表的设置与局限性
1. 使用起来没有像MySQL中使用那样方便
2. 不支持BEGIN、COMMIT和ROLLBACK
3. 表文件存储格式仅支持ORC（stored as ORC）

# HQL DML-UPDATE、DELETE
## 概述
- hive是基于Hadoop的数据仓库，是面向分析支持分析工具
- 因此在hive中常见的操作就是分析查询select操作
## **只有在事务表中才可以进行update和delete操作**

# HQL-DML-SELECT 查询数据
## 语法树
- 从哪里查询取决于FROM关键字后面的table_referencr。可以是普通物理表、视图、join结果或者子查询结果
- 表名和列名不区分大小写
- 和sql select语法相似
- [WITH Common TabkeExpression,(CommonTableExpression)*]
- SELECT [ALL|DISTINCT] select_expr,select_expr,...
- FROM table_reference
- [WHERE where_condition]
- [GROUP BY col_list]
- [ORDER BY col_list]
- [CLUSTER BY col_list
-   | DISTRIBUTE BY col_list][SORT BY col_list]
- ]
- [LIMIT[offset,]rows];

## having和where的区别
- having是在分组后对数据进行过滤
- where是在分组前对数据进行过滤
- having后面可以使用聚合函数
- where后面不可以使用聚合函数

## 执行顺序
- from > where > group by(含聚合) > having > order by > select
- 聚合语句（sum,min,max,avg,count)要比having子句优先执行
- where 优先于 聚合函数

## CLUSTER BY
- 根据指定字段将数据分组，每组内再根据字段正序排序（只能正序）
- 分组规则（hash）

## DISTRIBUTE BY + SORT BY
- 相当于把 CLUSTER BY的功能一分为二
- DISTRIBUTE BY负责根据指定字段分组
- SORT BY负责分组内排序规则
- 分组和排序的字段可以不同

### ORDER BY全局排序，因此只有一个reducer，结果输出在一个文件中，当输入规模大时，需要较长的计算时间
### DISTRIBUTE BY根据指定字段将数据分组，算法是hash散列。SORT BY是在分组之后，每个组内局部排序
### CLUSTER BY 既有分组又有排序，但是两个字段只能是同一个字段

## union 联合查询
- 用于将来自于多个select语句的结果合并为一个结果集
- 使用distinct关键字与只使用union默认值效果一样，都会删除重复行
- 使用all关键字，不会删除重复行，结果集包括所有select语句的匹配行
- 每个select_statement返回的列的数量和名称必须相同

# HQL-DML-JOIN 连接操作
## join概念
- 根据数据库的三范式设计要求会设计不同类型的数据设计不同的表存储
- join语法的出现是用于根据两个或多个表中的列之间的关系，从这些表中共同组合查询数据

## join语法规则
- inner join(内连接）,left join（左连接）,right join（右连接）,full outer join（全外连接）,left semi join（左半开连接）,cross join（交叉连接，也叫笛卡尔乘积）
- 内连接和左连接使用较多
- table_reference:是join查询中使用的表名，也可以是子查询别名
- table_factor:与table_reference相同，是连接查询中使用的表明，也可以是子查询别名
- join_condition:join查询关联的条件，如果在两个以上的表需要连接，则使用AND关键字
- **join_table:
    table_reference [INNER] JOIN table_factor [join_condition]
  | table_reference {LEFT|RIGHT|FULL} [OUTER] JOIN table_reference join_condition
  | table_reference LEFT SEMI JOIN table_reference join_condition
  | table_reference CROSS JOIN table_reference [join_condition] (as of Hive 0.10)
  join_condition:
    ON expression**

### inner join:内连接，两个表中相同的数据才可以留下
### left join:左连接，join时以左边的全部数据为准，右边与之关联；左表数据全部返回，右表没有join上的返回null
### right join:右连接，join时以右边的全部数据为准
### full outer join:全外连接，相当于进行左右连接后，消去重复行
### left semi join：左半开连接，会返回左边表的记录，前提是其记录对于右边的表满足ON语句中的判定条件。相当于内连接，但是只返回左表的全部数据
### full outer join:笛卡尔乘积，将返回被连接的两个表的笛卡尔积

