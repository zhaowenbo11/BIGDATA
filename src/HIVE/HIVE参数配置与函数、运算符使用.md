# 客户端和属性配置

## Hive CLI
- $HIVE_HOME/bin/hive是一个shell util，称之为第一代客户端
	1. 交互式或批处理模式运行hive查询。注意此时作为客户端，需要并且能够访问的是Hive meta store服务，而不是hiveserver2
	2. hive相关服务的启动，比如metastore服务
- 功能一：batch mode 批处理模式。当使用-e或-f选项运行bin/hive时，它将以批处理模式执行SQL命令
- 		-e+SQL命令  	|  		-f + sql文件
- 		所谓的批处理可以理解为**一次执行，执行完毕退出**
- 		-i 进入交互式模式之前运行初始化脚本
- 		-S -e 使用静默模式将数据从查询中存储到文件
- 功能二：Interactive Shell 交互式模式
- 		可以理解为客户端和hive服务一直保持连接除非手动退出客户端
- 功能三：启动hive服务
- 		比如meta store服务和hiveservee2服务的启动

## Beeline CLI
- 第二代客户端，在嵌入式模式和远程模式下均可工作

## 属性配置
- 作为用户，在修改hive属性之前要明白：1.有哪些属性支持用户修改，属性的功能，作用是什么。 2. 支持哪种方式进行修改，是临时生效还是永久生效
- hive配置属性是在HiveConf.JAVA类中进行管理
- 通常去hive官方CONFLUENCE去查看更为详细、更好理解
## 配置方式
- 方式一：hive-site.xml  影响方式广泛，影响整个hive安装包的配置
- 方式二： --hiveconf   是一个命令行参数，会在整个会话session中有效
- 方式三：set命令   在HIVE CLI或Beeline中使用set命令。Hive倡导：谁需要、谁配置、谁使用。
- 方式四： 服务特定配置文件 hivemetastore-sit.xml 、hiveserver2-site.xml 不同服务使用不同配置文件
- 配置方式优先级：set命令 >  hiveconf参数  >  hive-site.xml配置文件
- 日常开发使用中，如果不是核心的需要全局修改的参数属性，建议使用set命令
- 同时，hive也会读入Hadoop配置，如果有冲突，hive会覆盖Hadoop

# HIVE 内置运算符

## 概述
- 可以分为三大类：关系运算、算术运算、逻辑运算
## 查看运算符使用说明
- show function ；————显示所有的函数和运算符
- describe function count; ————查看运算符或函数的使用说明

## 关系运算符
- 是二元运算符
- 每个关系运算符都返回Boolean类型结果

## 算术运算符
- 操作数必须是数值类型

## 逻辑运算符
- 与、或、非、在、不在

# HIVE Function 函数入门
## hive函数概述与分类
#### 概述
- 使用 show function 查看当下可用的函数
- 通过describe function extended funcname 查看函数的使用方式
#### 分类标准
- 分为两大类：内置函数（built -in function）、用户定义函数 （user-defined functions）
- 内置函数可分为：数值类型函数、日期类型、字符串类型、集合函数、条件函数等
- 用户定义函数根据输入输出行数可分为：UDF、UDAF、UDTF
- UDF：普通函数：一进一出
- UDAF:聚合函数，多进一出
- UDTF：表生产函数，一进多出

## UDF分类标准扩大化
- UDF分类标准可以扩大到HIVE的所有函数中，包括内置函数和用户自定义函数

## 内置函数
- 是指hive开发实现好，直接可以使用
- 根据应用类型整体可以分为8大种类型

## 案例：开发hive UDF实现手机号****加密
### UDF实现步骤
1. 写一个Java类，继承UDF，并重载evaluate方法，方法中实现函数的业务逻辑
2. 重载意味着可以在一个Java类中实现多个函数功能
3. 程序打成jar包，上传到HS2服务器本地或者HDFS
4. 客户端命令行中添加jar包到hive的class path：hive > add JAR /xxxx/udf.jar
5. 注册成为临时函数（给UDF命名）： create temporary function 函数名 as 'UDF类全路径'
6. HQL中使用函数

## HIVE UDTF 之 explode 函数
- explode接收map、array类型的数据作为输入，然后把输入数据中的每个元素拆开变成一行数据
- explode(`array`) 
- explode(`map`)  k一列，v一列

## UDTF语法限制
1. explode函数属于UDTF表生成函数，explode执行返回的结果可以理解为一张虚拟的表，其数据来源于源表
2. 在select中只查询源表数据没有问题，只查询explode生成的虚拟表数据也没有问题，但是不能在只查询源表的时候，既想返回源表字段又想返回explode生成的虚拟表字段；通俗来讲，有两张表，不能只查询一张表但是又想返回分别属于两张表的字段

## UDTF语法限制解决
1. 从SQL层面上来说上述问题的解决方案是：对两张表进行join关联查询
2. hive专门提供了语法 ** lateral View侧视图**，专门用于搭配explode这样的UDTF函数，以满足上述需要

## hive lateral view 侧视图
- lateral view 是一种特殊的语法，主要搭配UDTF类型函数一起使用
- 一般只要使用UDTF，就会固定搭配lateral view来使用
- lateral view自带连接，不需要再去join

## hive aggregation 聚合函数
- 属于UDAF类型函数
- HQL 提供了几种内置的UDAF聚合函数，例如max（）、min（）和avg（）。这些称之为基础的聚合函数
- 通常情况下聚合函数会与group by子句一起使用。如果未指定GROUP BY子句，默认情况下，会汇总所有行数据