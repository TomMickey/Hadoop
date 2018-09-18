东方国信所用hive库

dfgx_tour_db --旅游大数据

dfgx_wo_db --沃风云榜，上网日志，4G信令

dfgx_wo_db_test --渠道选址用，234G信令，全广东省的基站信息

show partitions 表名; --显示表的分区信息

给表加载文件数据：
（1）load data local inpath '文件所在的本地地址' into table 表名;

（2）hadoop fs -put 本地地址 HDFS地址 如：hadoop fs -put ./zhuhai.txt /NS3/user/hive/warehouse/dfgx_tour_db.db/dfgx_code_prov

表名：dfgx_建表人_需求方_表具体信息

信令数据里面存储的lac,ci是16进制，我们自己的基站表10进制，转换进制函数conv(字段,16,10)--从16进制转为10进制

concat(lac,ci)=lacci --关联lac和ci不分开的时候


CREATE EXTERNAL TABLE dfgx_brd_sdtp(   --EXTERNAL表示外部表（drop掉这个表，他的数据仍然存在hdfs上，不能truncate），没有则是内部表，drop掉后，hdfs上的数据也删除掉
  isdn string,  --字段类型 string int double
  imsi string, 
  year string, 
  month string, 
  day string, 
  time string, 
  lac string, 
  ci string, 
  imei string, 
  flag string, 
  type string, 
  len string, 
  msisdn string, 
  imsi2 string, 
  result string, 
  errorcode string, 
  subevttype string)
PARTITIONED BY (  --指明分区的字段。独立的
  deal_year string, 
  deal_month string, 
  deal_day string)
ROW FORMAT DELIMITED 
  FIELDS TERMINATED BY ','  --指定分隔符 | \t , \u0001（默认）
STORED AS INPUTFORMAT  --导入格式 
  'org.apache.hadoop.mapred.TextInputFormat'  
OUTPUTFORMAT  --导出格式 比如从hdfs获取到这个hive表的数据，如果是RCFile格式，那么是不可读的，如果是TEXT格式，就是可读的。
  'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'
LOCATION  --HDFS路径，数据存储位置， 。。。。 库.db/表名
  'viewfs://beh/NS3/user/hive/warehouse/dfgx_tour_db.db/dfgx_brd_sdtp'
;

load data local inpath '/data2/USERS/hadoop_dfgx_A/zk/20180702-fs_xzzx/baseinfo/user.txt' into table users partition (city=上海);

创建分区表：create table employee (name string,age int,sex string) partitioned by (city string) row format delimited fields terminated by '\t';
分区表装载数据：load data local inpath '/usr/local/lee/employee' into table employee partition (city='hubei');

添加分区并且指定分区数据目录
alter table dfgx_wo_db_test.dfgx_brd_234G add if not exists partition(match_day='$v_date') location '/NS1/DATA/hajx_brd/$v_date';


dfgx_tour_db库的表说明：
dfgx_brd_sdtp  --23G信令，isdn手机号,imsi国际移动识别码，lac基站，ci基站，imei终端设备号
dfgx_code_prov --全国的城市地区编码
dfgx_mcc_mapping --国际mcc识别编码 关联国籍用
dfgx_zk_app_info --app编码表，关联上网日志的app信息用。
dfgx_zk_city_lonlat --城市区县的中心位置
dfgx_zk_gaosu_lacci_mapping --广东省高速路的基站信息
dfgx_zk_msisdn_meg --手机号段对应的归属地和运营商
dim_lac_ci_spot_mapping --旅游大数据景区的基站信息
dim_lac_ci_traffic_mapping --广东省火车站、飞机场、汽车站基站信息
dim_msisdn_seg ，dim_bi_area_oracle 这两种表用于旅游大数据识别用户的归属地
dim_spot_info --旅游大数据景区信息
dim_trip_spot_keyword --旅游大数据搜索关键词

dfgx_wo_db库的表说明：
4g_s1mme  --4G信令，msisdn手机号，sai_cgi_ecgi这个字段包含lac和ci，substr(SAI_CGI_ECGI,6,5) lac，substr(SAI_CGI_ECGI,11,2) ci
dfgx_city_message --手机段归属地
dfgx_dpi_data_match --上网日志，phone_id手机号，proc_code是lac，ci_num是ci，starttime上网开始时间，endtime上网结束时间，usetime上网使用时间，upload_bytes上行流量，download_bytes下行流量，allflow使用流量，url上网的网址，由于数据量大只能一天天查
dfgx_user_info --用户的信息，sex性别，age年龄，serial_number手机号

dfgx_wo_db_test库的表说明：
dfgx_brd_234g --234G信令数据表
dsi_o_day --渠道选址日沉淀表
dsi_o_month --渠道选址月沉淀表
dsi_o_month_user --用户位置表 月更新
ods_cell_info  --全广东省基站信息每月更新
td_s_holiday  --假期表只有2017和18年数据



hive中应该注意的地方：
1、hive 不能delete 也不能update 不能进行修改操作，如果一定要进行修改操作，insert overwrite table select * from table where col!='xxx';
2、hive默认安全模式：（1）对分区表不允许没有限制分区查询，（2）不能直接order by操作，被限制为只能和limit 配合使用。（3）安全模式不允许进行笛卡尔积连接。
   取消安全模式：set hive.mapred.mode=nostrict; 
3、explain sql语句；进入解释计划中。
4、hive优化：（1）第一步要把数据量尽可能减少，（2）避免使用count（distinct 字段）这种操作，（3）不能像oracle一样使用where关联 （4）limit 限制行记录 
5、窗口函数：
sum(字段) over(partition by 字段 order by 字段 asc) 求和
lead(字段) ... 取下一条记录中该字段的值
row_number() ... 标记序号
rank() ... 
6、hive -e "语句、设置参数、进入库"    --后台操作模式。 写数据文件时可以通过重定向符号输出到文件 >aa.txt
7、hive是不允许不相等关联。


sqoop 使用 --把hdfs的数据或者说hive的数据传到oracle表中
sqoop export -D mapred.job.queue.name=root.B --connect jdbc:oracle:thin:@132.98.23.28:1521:dtcentapp --username uif_qdxz_yy --password Ora_123QWE --table ODS_USER_WORK_REST --export-dir 'viewfs://beh/NS3/user/hive/warehouse/dfgx_wo_db_test.db/dsi_o_month_user/match_mon=$v_acct_date' --columns month_id,serial_number,work_lon,work_lat,rest_lon,rest_lat --input-fields-terminated-by '|' --input-lines-terminated-by '\n' --input-null-string '0.0'


spark的使用
spark-sql --master yarn --queue root.B --num-executors 400 --executor-cores 3 --executor-memory 10G
复杂使用：200*2 *30G-40G
一般使用：300-400*3 *10G-15G
数据较少使用：300-400*3 *5G-10G-15G

spark-sql --master yarn --queue root.B --num-executors 400 --executor-cores 3 --executor-memory 10G -e 后台执行



dfgx_tour_db.dfgx_brd_sdtp --23G信令数据 实时
desc 表名 --显示字段
conv(转换字段，转换之前进制，转换后进制)
--通过基站联表查数
查询某年某月某日某个行政中心的手机号
select xzzx_id,xzzx_name,isdn from dfgx_brd_sdtp a join dfgx_zk_fs_xzzx_lacci b on 
conv(a.lac,16,10)=b.lac and conv(a.ci,16,10)=b.ci 
where concat(a.deal_year,a.deal_month,a.deal_day)='20180705' and b.xzzx_id=19 limit 200;
dfgx_zk_fs_xzzx_code--行政中心码表
xzzx_id             	int                 	                    
xzzx_name           	string              	                    
lon                 	double              	                    
lat                 	double 
dfgx_zk_fs_xzzx_lacci -- 行政中心基站表
xzzx_id 
zxxz_name
lac
ci
net_type


dfgx_wo_db_test.td_s_holiday  --判断是否是工作日的表

select * from dfgx_wo_db_test.dsi_o_month_user where match_mon='201803' limit 10; 
--用户位置判定 选择休息地或者工作地位置
汇聚图数据参考脚本/data2/USERS/hadoop_dfgx_A/fs_xzzx/sh/library/dfgx_fsac_d_tsg_trail.sh
/data2/USERS/hadoop_dfgx_A/zk/20180103-qingyuantrip/shell/sql/ --可参考脚本 
s_flow.sh 或d_user_analy.sh
spark-sql --master yarn --queue root.B --num-executors 400 --executor-cores 2 --executor-memory 10G mapreduce.job.queuename root.B


load data local inpath 'tb_emp_info.txt' into table tb_emp_info;--hadoop加载本地文件入库
nohup hive -e "" &  后台执行sql
alter table 表 change 旧字段 新字段 类型
hdfs dfs -ls /NS3/user/hive/warehouse/dfgx_tour_db.db  查看dfgx_tour_db.db库下面的表使用row_number（）函数进行编号

hdfs dfs -text /NS3/user/hive/warehouse/dfgx_tour_db.db/dfgx_zk_qy_spot_xishu/spot_xishu.txt
如下:
在订单中按价格的升序进行排序，并给每条记录进行排序代码如下：

select DID,customerID,totalPrice,ROW_NUMBER() over(order by totalPrice) as rows from OP_Order
连接函数
concat_ws()
填充函数
lpad  rpad
查找字符串函数
instr()
FIRST_VALUE 返回组中数据窗口的第一个值 FIRST_VALUE
 ( [scalar_expression )OVER ( [ partition_by_clause ] order_by_clause ) 
LAST_VALUE   返回组中数据窗口的最后一个值  LAST_VALUE 
( [scalar_expression )OVER ( [ partition_by_clause order_by_clause ) 
DATEDIFF() 函数返回两个日期之间的天数。


