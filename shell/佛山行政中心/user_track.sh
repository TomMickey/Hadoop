#!/bin/bash
############################################################
#说明：佛山行政中心，人流轨迹（一小时）
#执行周期：每一小时一次，示例：08,09,10,...,17,18
#来源表1：dfgx_tour_db.dfgx_brd_sdtp 234G信令数据
#来源表2：dfgx_zk_fs_xzzx_lacci -- 行政中心基站表
#来源表3: dfgx_fs_changzhu 佛山常驻人员表
#来源表4: dfgx_wo_db_test.td_s_holiday 工作日表
#来源表5：dfgx_wo_db_test.dsi_o_month_user 位置表
############################################################

#设置环境变量
source ~/.bashrc

v_date=`date +%Y%m%d` #当天时间 例如20180705
v_hour=`date -d "-2 hour" +%H` #前一个小时 例如14
v_second=60
v_leave_minute=15
v_leave_second=`expr $v_second \* $v_leave_minute`
v_minute=`expr $v_second - $v_leave_minute`
v_acct_hour=`date +%H` #当前小时段
v_last_hour=$v_hour:$v_minute:00 #时间段 小
v_next_hour=`date +%H`:00:00 #时间段 大
v_data_file=/data2/USERS/hadoop_dfgx_A/zk/20180702-fs_xzzx/data/fs_xzzx_track.txt #数据存放文件
v_log_file=/data2/USERS/hadoop_dfgx_A/zk/20180702-fs_xzzx/log/fs_xzzx_track.log #日志记录文件

#v_state=`hive -e "select is_work from dfgx_wo_db_test.td_s_holiday where day_id=$v_date;"`
#if [ "$v_state"  -ne "1" ]; then
#	exit 1
#fi

hive -e "
	use dfgx_tour_db;
	add jar /data2/USERS/hadoop_dfgx_A/zk/20180702-fs_xzzx/shell/getGridIdUDF.jar;
	create temporary function get_grid_id as 'com.bonc.udf.GetGridIdUDF';
	add jar /data2/USERS/hadoop_dfgx_A/time_interval.jar;
	create temporary function td as 'time_interval.test1';
	add jar /data2/USERS/hadoop_dfgx_A/boncUDF.jar;
	create temporary function get_length as 'com.bonc.udf.GetLonLatLength';
	select g.xzzx_id,g.xzzx_name,g.isdn,g.lon1 as lon,g.lat1 as lat,g.day_id from 
			(select e.xzzx_id,e.xzzx_name,e.isdn,case when f.work_lon=0 then f.rest_lon else f.work_lon end as lon1,case when f.work_lat=0 then f.rest_lat else f.work_lat end as lat1,e.lon,e.lat,e.day_id from
				(select ee.day_id,ee.xzzx_id,ee.xzzx_name,ee.isdn,ee.lon,ee.lat from 
						(select dd.xzzx_id,dd.xzzx_name,ddd.day_id,ddd.isdn,dd.lon,dd.lat from 
								(select d.xzzx_id,d.xzzx_name,d.day_id,d.isdn from 
										(select c.day_id,c.isdn,c.xzzx_id,c.xzzx_name,sum(c.stay_time) sum_time from
												(select bb.xzzx_id,bb.xzzx_name,b.day_id,b.isdn,b.next_time,td(b.time,nvl(b.next_time,b.time)) as stay_time from 
														(select  a.day_id,a.isdn,a.lac,a.ci,a.time,lead(a.time) over(partition by a.isdn order by a.time) as next_time from
																(select concat(aa.year,lpad(aa.month,2,0),lpad(aa.day,2,0)) day_id,aa.isdn,aa.lac,aa.ci,
																      concat(concat_ws('-',year,lpad(month,2,0),lpad(day,2,0)),' ',regexp_replace(time,'-',':')) as time 
																      from dfgx_brd_sdtp aa 
																      where concat(aa.deal_year,aa.deal_month,aa.deal_day)='$v_date' 
																      and length(aa.isdn)=11 and concat(aa.year,lpad(aa.month,2,0),lpad(aa.day,2,0))='$v_date'  
																      and regexp_replace(aa.time,'-',':')>='$v_last_hour' and regexp_replace(aa.time,'-',':')<'$v_next_hour'
																) a  
														) b join dfgx_zk_fs_xzzx_lacci bb on conv(b.lac,16,10)=bb.lac and conv(b.ci,16,10)=bb.ci
												) c group by c.day_id,c.isdn,c.xzzx_id,c.xzzx_name
										) d where d.sum_time>$v_leave_second
								) ddd right outer join dfgx_zk_fs_xzzx_code dd on ddd.xzzx_id=dd.xzzx_id and ddd.xzzx_name=dd.xzzx_name where length(cast(dd.xzzx_id as string))=14
								and dd.xzzx_id not in  (44060512400007,44060512300001)
						) ee left join dfgx_fs_changzhu j on ee.xzzx_name=j.xzzx_name and ee.isdn=j.isdn where j.isdn is null and j.xzzx_name is null group by ee.day_id,ee.xzzx_id,ee.xzzx_name,ee.isdn,ee.lon,ee.lat
				) e join dfgx_wo_db_test.dsi_o_month_user f on e.isdn=f.isdn where f.match_mon='201806'
			) g where get_length(g.lat1,g.lon1,g.lat,g.lon)<10000;"> $v_data_file

file_row=`wc -l $v_data_file| awk -F ' ' '{print$1}'`

v_ctl="LOAD DATA \n
CHARACTERSET 'UTF8' \n
INFILE \"$v_data_file\" \"str '\n'\" \n
append INTO TABLE FSAC_XZZX_TRACK \n
Fields terminated by \"\t\"  \n
trailing nullcols \n
( \n
XZZX_ID, \n
XZZX_NAME, \n
ISDN, \n
FROMLON, \n
FROMLAT, \n
DAY_ID \n
)
"
#
v_ctl_file=/data2/USERS/hadoop_dfgx_A/zk/20180702-fs_xzzx/shell/fsac_xzzx_track.ctl
#
v_ctl_log=/data2/USERS/hadoop_dfgx_A/zk/20180702-fs_xzzx/shell/fsac_xzzx_track.log

#写入ctl控制文件
echo -e $v_ctl > $v_ctl_file


#定义数据库
ORA_LINK=ucr_tourdb_test/oracle_123QWEBSBA@DTCENTNEW2

#清除之前的数据
#echo "BEGIN EXECUTE IMMEDIATE 'TRUNCATE TABLE FSAC_XZZX_TRACK'; END;" | sqlplus -S $ORA_LINK

#利用oracle导入工具sqlldr入库文件
sqlldr $ORA_LINK control=$v_ctl_file  log=$v_ctl_log

#判断入了多少条记录
load_rows=`awk '/ successfully loaded/{print}' $v_ctl_log|sed 's/[^0-9]//g'`

if [ "$load_rows" -ne "$file_row" ] 
then
	echo `date +'%x %A %T'`"入库失败，文件记录数:$file_row，入库记录数:$load_rows" >> $v_log_file
else
	echo `date +'%x %A %T'`"入库成功，文件记录数:$file_row，入库记录数:$load_rows" >> $v_log_file
fi
