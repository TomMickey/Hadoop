--首先查询出常住人口
常住人口口径:在过去的4个月内每天出现在景区的时间超过一定的时间(时间待定)
以佛山高明区为例
dfgx_tour_db.dfgx_brd_sdtp  23G信令表
dfgx_wo_db.4g_s1mme 4G信令表
dfgx_tour_db.dfgx_lzy_fsgm_changzhu_mid 常住人口中间表
dfgx_tour_db.dfgx_lzy_fsgm_spot_lacci 景区基站表
dfgx_tour_db.dfgx_lzy_fsgm_spot  景区表
表说明:
dfgx_tour_db.dfgx_lzy_fsgm_changzhu_mid		常住人口中间表
字段:day_id,isdn,spot_name,stay_time		日期-号码-景区名-停留时间
dfgx_tour_db.dfgx_lzy_fsgm_spot_lacci 景区基站表
字段:month_id,day,pro_id,area,spot_id,lac,ci,lon,lat		月份,天,省份id,地域,景区id,lac,ci,经度,纬度
dfgx_tour_db.dfgx_lzy_fsgm_spot  景区表
字段:spot_id,spot_name	景区id,景区名称
dfgx_tour_db.dfgx_lzy_fsgm_changzhu		常驻人口表
字段:isdn,spot_name,,stay_day		号码-景区名-停留天数
dfgx_tour_db.dfgx_lzy_fsgm_spot_lacci_tran  交通表
字段:month_id,day,pro_id,area,lac,ci,lon,lat,transpot,station_name
dfgx_tour_db.dfgx_lzy_fsgm_tour_week	一周游客表
字段:day_id,spot_id,spot_name,isdn		日期-景区id-景区名称-号码
create table dfgx_tour_db.dfgx_lzy_fsgm_changzhu_mid as
insert into table dfgx_tour_db.dfgx_lzy_fsgm_changzhu_mid
select g.day_id,g.isdn,h.spot_name,g.stay_time from 
	(select f.day_id,f.isdn,f.spot_id,sum(f.stay_time) as stay_time from 
		(select substring(d.t_time,1,10) as day_id,e.spot_id,d.isdn,d.stay_time from 
			(select c.isdn,c.lac,c.ci,c.t_time,(nvl(c.last_dura,c.dura)-c.dura) as stay_time
			from
				(select b.isdn,b.lac,b.ci,b.t_time,b.dura,lead(b.dura) over(partition by b.isdn order by b.t_time) as last_dura
				from
					(select a.isdn,a.lac,a.ci,a.t_time,unix_timestamp(a.t_time) as dura
					from
						(select concat(concat_ws('-',year,lpad(month,2,0),lpad(day,2,0)),' ',regexp_replace(time,'-',':')) as t_time,lac,ci,isdn
						from dfgx_tour_db.dfgx_brd_sdtp 
						where concat(deal_year,deal_month)='201807'
						and concat(year,lpad(month,2,'0'))='201807'
						and length(isdn)=11
						union all
						select from_unixtime(cast(proc_starttime as bigint),'yyyy-MM-dd HH:mm:ss') as t_time,substr(SAI_CGI_ECGI,6,5) as lac,substr(SAI_CGI_ECGI,11,2) as ci,msisdn as isdn
						from dfgx_wo_db.4g_s1mme
						where substring(match_day,1,6)='201807'
						and from_unixtime(cast(proc_starttime as bigint),'yyyyMM')='201807'
						and sai_cgi_ecgi<>''
						and length(msisdn)=11
						) a
					) b
				) c
			) d join dfgx_tour_db.dfgx_lzy_fsgm_spot_lacci e on conv(d.lac,16,10)=e.lac and conv(d.ci,16,10)=e.ci
		) f group by f.day_id,f.isdn,f.spot_id
	) g join dfgx_tour_db.dfgx_lzy_fsgm_spot h on g.spot_id=h.spot_id;
	
	
建立景区常住人口表
create table dfgx_tour_db.dfgx_lzy_fsgm_changzhu as 
select isdn,spot_name,count(day_id) as stay_day
from
	(select day_id,isdn,spot_name,sum(stay_time) as day_stay 
	from dfgx_tour_db.dfgx_lzy_fsgm_changzhu_mid 
		group by day_id,isdn,spot_name
	) a
where day_stay>=7200
group by isdn,spot_name
having count(day_id) >=75;


每日游客接待数量(分景区和地市)
创建一周之内的游客表
dfgx_tour_db.dfgx_lzy_fsgm_tour_week  创建的周接待游客表(把每天的数据插入到这个表里)
每天游客到一个景区旅游,记录该游客的旅游记录
剔除过路人群和常住人口
add jar /data2/USERS/hadoop_dfgx_A/time_interval.jar;
create temporary function td as 'time_interval.test1';
create table dfgx_tour_db.dfgx_lzy_fsgm_tour_week as 
insert into table dfgx_tour_db.dfgx_lzy_fsgm_tour_week
select d.day_id,d.spot_id,d.spot_name,d.isdn,d.lon,d.lat from 
	(select b.day_id,b.isdn,b.spot_id,c.spot_name,b.ci,b.lat,b.lon from 
		(select a2.day_id,a3.spot_id,a2.isdn,a2.lac,a2.ci,a3.lon,a3.lat,a2.t_time,a2.last_dura,td(a2.t_time,nvl(a2.last_dura,a2.t_time)) as stay_time
			from(select a1.day_id,a1.isdn,a1.lac,a1.ci,a1.t_time,lead(a1.t_time) over(partition by a1.day_id,a1.isdn order by a1.t_time) as last_dura
				from(select concat(year,lpad(month,2,'0'),lpad(day,2,'0')) as day_id, concat(concat_ws('-',year,lpad(month,2,0),lpad(day,2,0)),' ',regexp_replace(time,'-',':')) as t_time,lac,ci, isdn
					from dfgx_tour_db.dfgx_brd_sdtp 
					where concat(deal_year,deal_month,deal_day)='20180821'
					and concat(year,lpad(month,2,'0'),lpad(day,2,'0'))='20180821'
					and length(isdn)=11
					union all
					select from_unixtime(cast(proc_starttime as bigint),'yyyyMMdd') as day_id,from_unixtime(cast(proc_starttime as bigint),'yyyy-MM-dd HH:mm:ss') as t_time,substr(SAI_CGI_ECGI,6,5) as lac,substr(SAI_CGI_ECGI,11,2) as ci,msisdn as isdn
					from dfgx_wo_db.4g_s1mme
					where match_day='20180821'
					and from_unixtime(cast(proc_starttime as bigint),'yyyyMMdd')='20180821'
					and sai_cgi_ecgi<>''
					and length(msisdn)=11
					) a1
				) a2
				join dfgx_tour_db.dfgx_lzy_fsgm_spot_lacci a3 on conv(a2.lac,16,10)=a3.lac and conv(a2.ci,16,10)=a3.ci
		) b join dfgx_tour_db.dfgx_lzy_fsgm_spot c on b.spot_id=c.spot_id and b.stay_time>3600
	) d left join dfgx_tour_db.dfgx_lzy_fsgm_changzhu e on e.isdn = d.isdn and e.spot_name=d.spot_name where e.isdn is null;

求出景区日数据
select day_id,spot_id,spot_name,count(distinct isdn) from dfgx_tour_db.dfgx_lzy_fsgm_tour_week where day_id='20180821' group by day_id,spot_id,spot_name;

地市每日接待
select day_id,count(distinct isdn) from dfgx_tour_db.dfgx_lzy_fsgm_tour_week where day_id='20180823' group by day_id;


小时热力图 实时人数
日期--景区ID--景区名称--数量
add jar /data2/USERS/hadoop_dfgx_A/time_interval.jar;
create temporary function td as "time_interval.test1";
add jar /data2/USERS/hadoop_dfgx_A/time_interval.jar;
create temporary function td as 'time_interval.test1';
select f.day_id,f.spot_id,f.spot_name,f.lon,f.lat,count(distinct f.isdn) from 
	(select d.day_id,d.spot_id,d.spot_name,d.isdn,d.lon,d.lat from 
		(select b.day_id,b.spot_id,c.spot_name,b.isdn,b.lon,b.lat from 
			(select a2.day_id,a3.spot_id,a2.isdn,a2.lac,a2.ci,a3.lon,a3.lat,a2.t_time,a2.last_dura,td(a2.t_time,nvl(a2.last_dura,a2.t_time)) as stay_time
				from(select a1.day_id,a1.isdn,a1.lac,a1.ci,a1.t_time,lead(a1.t_time) over(partition bya1.day_id, a1.isdn order by a1.t_time) as last_dura
					from(select concat(year,lpad(month,2,'0'),lpad(day,2,'0')) as day_id, concat(concat_ws('-',year,lpad(month,2,0),lpad(day,2,0)),' ',regexp_replace(time,'-',':')) as t_time, lac, ci, isdn
						from dfgx_tour_db.dfgx_brd_sdtp 
						where concat(deal_year,deal_month,deal_day)='20180821'
						and concat(year,lpad(month,2,'0'),lpad(day,2,'0'))='20180821'
						and regexp_replace(time,'-',':')>='07:30:00' and regexp_replace(time,'-',':')<'09:00:00'
						and length(isdn)=11
						) a1
					) a2
					join dfgx_tour_db.dfgx_lzy_fsgm_spot_lacci a3 on conv(a2.lac,16,10)=a3.lac and conv(a2.ci,16,10)=a3.ci
			) b join dfgx_tour_db.dfgx_lzy_fsgm_spot c on b.spot_id=c.spot_id and b.stay_time>3600
		) d left join dfgx_tour_db.dfgx_lzy_fsgm_changzhu e on e.isdn = d.isdn and e.spot_name=d.spot_name where e.isdn is null
	) f group by f.day_id,f.spot_id,f.spot_name,f.lon,f.lat;

游客来源地
--创建周表
日期-景区id-景区-手机号
add jar /data2/USERS/hadoop_dfgx_A/time_interval.jar;
create temporary function td as "time_interval.test1";
add jar /data2/USERS/hadoop_dfgx_A/time_interval.jar;
create temporary function td as 'time_interval.test1';
create table dfgx_tour_db.dfgx_lzy_fsgm_tour_area_week as
insert into table dfgx_tour_db.dfgx_lzy_fsgm_tour_area_week
select d.day_id,d.spot_id,d.spot_name,d.isdn from 
	(select b.day_id,b.spot_id,c.spot_name,b.isdn from 
		(select a4.day_id ,a4.spot_id,a4.isdn,sum(a4.stay_time) as sum_time from 
			(select a2.day_id,a3.spot_id,a2.isdn,td(a2.t_time,nvl(a2.last_dura,a2.t_time)) as stay_time
			from(select a1.day_id, a1.isdn,a1.lac,a1.ci,a1.t_time,lead(a1.t_time) over(partition by a1.day_id,a1.isdn order by a1.t_time) as last_dura
				from(select concat(year,lpad(month,2,'0'),lpad(day,2,'0')) as day_id, concat(concat_ws('-',year,lpad(month,2,0),lpad(day,2,0)),' ',regexp_replace(time,'-',':')) as t_time, lac, ci, isdn
					from dfgx_tour_db.dfgx_brd_sdtp 
					where concat(deal_year,deal_month,deal_day)>='20180819' and concat(deal_year,deal_month,deal_day)<='20180825'
					and concat(year,lpad(month,2,'0'),lpad(day,2,'0'))>='20180819' and concat(year,lpad(month,2,'0'),lpad(day,2,'0'))<='20180825'
					and length(isdn)=11
					union all
					select from_unixtime(cast(proc_starttime as bigint),'yyyyMMdd') as day_id,from_unixtime(cast(proc_starttime as bigint),'yyyy-MM-dd HH:mm:ss') as t_time,substr(SAI_CGI_ECGI,6,5) as lac,substr(SAI_CGI_ECGI,11,2) as ci,msisdn as isdn
					from dfgx_wo_db.4g_s1mme
					where match_day>='20180819' and match_day<='20180825'
					and from_unixtime(cast(proc_starttime as bigint),'yyyyMMdd')>='20180819' and from_unixtime(cast(proc_starttime as bigint),'yyyyMMdd')<='20180825'
					and sai_cgi_ecgi<>''
					and length(msisdn)=11
					) a1
				) a2
				join dfgx_tour_db.dfgx_lzy_fsgm_spot_lacci a3 on conv(a2.lac,16,10)=a3.lac and conv(a2.ci,16,10)=a3.ci
			) a4 group by a4.day_id,a4.spot_id,a4.isdn
		) b join dfgx_tour_db.dfgx_lzy_fsgm_spot c on b.spot_id=c.spot_id and b.sum_time>3600
	) d left join dfgx_tour_db.dfgx_lzy_fsgm_changzhu e on e.isdn = d.isdn and e.spot_name=d.spot_name where e.isdn is null;
	

--省内客源
每个景区的手机号关联手机号码表
select a.isdn,a.spot_id,a.spot_name,b.province,b.city from dfgx_tour_db.dfgx_lzy_fsgm_tour_area_week a join dfgx_tour_db.dfgx_zk_msisdn_meg b on substring(a.isdn,1,7)=b.msisdn_meg and b.province='广东省';

城区游客省内来源
select a.isdn,b.province,b.city from dfgx_tour_db.dfgx_lzy_fsgm_tour_area_week a join dfgx_tour_db.dfgx_zk_msisdn_meg b on substring(a.isdn,1,7)=b.msisdn_meg and b.province='广东省'  group by a.isdn,b.province;

省外来源
景区游客
select a.isdn,a.spot_id,a.spot_name,b.province,b.city from dfgx_tour_db.dfgx_lzy_fsgm_tour_area_week a join dfgx_tour_db.dfgx_zk_msisdn_meg b on substring(a.isdn,1,7)=b.msisdn_meg and b.province!='广东省';


省外游客
城区游客
select a.isdn,b.province from dfgx_tour_db.dfgx_lzy_fsgm_tour_area_week a join dfgx_tour_db.dfgx_zk_msisdn_meg b on substring(a.isdn,1,7)=b.msisdn_meg and b.province!='广东省'  group by a.isdn;

游客画像-景区
add jar /data2/USERS/hadoop_dfgx_A/time_interval.jar;
create temporary function td as "time_interval.test1";
select f.day_id,f.spot_id,f.spot_name,f.isdn,g.age,g.sex from 
	(select d.day_id,d.spot_id,d.spot_name,d.isdn from 
		(select b.day_id,b.spot_id,c.spot_name,b.isdn,b.lac,b.ci from 
			(select a2.day_id,a3.spot_id,a2.isdn,a2.lac,a2.ci,a2.t_time,a2.last_dura,td(a2.t_time,nvl(a2.last_dura,a2.t_time)) as stay_time
				from(select a1.day_id, a1.isdn,a1.lac,a1.ci,a1.t_time,lead(a1.t_time) over(partition by a1.isdn order by a1.t_time) as last_dura
					from(select concat(year,lpad(month,2,'0'),lpad(day,2,'0')) as day_id, concat(concat_ws('-',year,lpad(month,2,0),lpad(day,2,0)),' ',regexp_replace(time,'-',':')) as t_time, lac, ci, isdn
						from dfgx_tour_db.dfgx_brd_sdtp 
						where concat(deal_year,deal_month,deal_day)='20180819'
						and concat(year,lpad(month,2,'0'),lpad(day,2,'0'))='20180819'
						and length(isdn)=11
						union all
						select from_unixtime(cast(proc_starttime as bigint),'yyyyMMdd') as day_id,from_unixtime(cast(proc_starttime as bigint),'yyyy-MM-dd HH:mm:ss') as t_time,substr(SAI_CGI_ECGI,6,5) as lac,substr(SAI_CGI_ECGI,11,2) as ci,msisdn as isdn
						from dfgx_wo_db.4g_s1mme
						where match_day='20180819'
						and from_unixtime(cast(proc_starttime as bigint),'yyyyMMdd')='20180819'
						and sai_cgi_ecgi<>''
						and length(msisdn)=11
						) a1
					) a2
					join dfgx_tour_db.dfgx_lzy_fsgm_spot_lacci a3 on a2.lac=conv(a3.lac,10,16) and a2.ci=conv(a3.ci,10,16)
			) b join dfgx_tour_db.dfgx_lzy_fsgm_spot c on b.spot_id=c.spot_id and b.stay_time>3600
		) d left join dfgx_tour_db.dfgx_lzy_fsgm_changzhu e on e.isdn = d.isdn and e.spot_name=d.spot_name where e.isdn is null
	)  f join dfgx_wo_db.dfgx_user_info g on f.isdn=g.serial_number;
	
游客画像-城区
add jar /data2/USERS/hadoop_dfgx_A/time_interval.jar;
create temporary function td as "time_interval.test1";
select i.day_id,i.isdn,g.sex,g.age from 
	(select f.day_id,distinct f.isdn from 
		(select d.day_id,d.spot_name,d.isdn from 
			(select b.day_id,b.area,c.spot_name,b.isdn,b.lac,b.ci,b.lat,b.lon from 
				(select a2.day_id,a3.area,a3.spot_id,a2.isdn,a2.lac,a2.ci,a3.lon,a3.lat,a2.t_time,a2.last_dura,td(a2.t_time,nvl(a2.last_dura,a2.t_time)) as stay_time
					from(select a1.day_id, a1.isdn,a1.lac,a1.ci,a1.t_time,lead(a1.t_time) over(partition by a1.isdn order by a1.t_time) as last_dura
						from(select concat(year,lpad(month,2,'0'),lpad(day,2,'0')) as day_id, concat(concat_ws('-',year,lpad(month,2,0),lpad(day,2,0)),' ',regexp_replace(time,'-',':')) as t_time, lac, ci, isdn
							from dfgx_tour_db.dfgx_brd_sdtp
							where concat(deal_year,deal_month,deal_day)>='20180819' and concat(deal_year,deal_month,deal_day)<='20180825'
							and concat(year,lpad(month,2,'0'),lpad(day,2,'0'))>='20180819' and concat(year,lpad(month,2,'0'),lpad(day,2,'0'))<='20180825'
							and length(isdn)=11
							union all
							select from_unixtime(cast(proc_starttime as bigint),'yyyyMMdd') as day_id,from_unixtime(cast(proc_starttime as bigint),'yyyy-MM-dd HH:mm:ss') as t_time,substr(SAI_CGI_ECGI,6,5) as lac,substr(SAI_CGI_ECGI,11,2) as ci,msisdn as isdn
							from dfgx_wo_db.4g_s1mme
							where match_day='20180821'
							and from_unixtime(cast(proc_starttime as bigint),'yyyyMMdd')='20180821'
							and sai_cgi_ecgi<>''
							and length(msisdn)=11
							) a1
						) a2
						join dfgx_tour_db.dfgx_lzy_fsgm_spot_lacci a3 on a2.lac=conv(a3.lac,10,16) and a2.ci=conv(a3.ci,10,16)
				) b join dfgx_tour_db.dfgx_lzy_fsgm_spot c on b.spot_id=c.spot_id and b.stay_time>3600
			) d left dfgx_tour_db.dfgx_lzy_fsgm_changzhu e on e.isdn = d.isdn and e.spot_name=d.spot_name where e.isdn is null
		) f group by f.day_id
	) i join dfgx_wo_db.dfgx_user_info g on i.isdn=g.serial_number;
	
	
	20180831
	
交通工具--景区
add jar /data2/USERS/hadoop_dfgx_A/time_interval.jar;
create temporary function td as 'time_interval.test1';
select f.day_id,f.isdn,f.spot_id,f.spot_name,f.transpot from 
	(select d.day_id,d.isdn,d.spot_id,e.spot_name,d.transpot from 
		(select cc.day_id,cc.isdn,cc.transpot,cc.spot_id,sum(cc.stay_time) as stay_time from 
			(select b.day_id,b.isdn,b.transpot,c.spot_id,b.stay_time from 
				(select a2.day_id,a2.isdn,a2.lac,a2.ci,a2.t_time,a2.last_dura,td(a2.t_time,nvl(a2.last_dura,a2.t_time)) as stay_time,
				case
					when a3.transpot==2 then
					'火车'
					when a3.transpot==3 then
					'汽车'
					when a3.transpot==4 then
					'自驾'
					end as transpot
				from 
					(select a1.day_id,a1.isdn,a1.lac,a1.ci,first_value(a1.lac) over(partition by a1.isdn order by a1.t_time) as first_lac,first_value(a1.ci) over(partition by a1.isdn order by a1.t_time) as first_ci,a1.t_time,lead(a1.t_time) over(partition by a1.isdn order by a1.t_time) as last_dura from 
						(select concat(year,lpad(month,2,'0'),lpad(day,2,'0')) as day_id, concat(concat_ws('-',year,lpad(month,2,0),lpad(day,2,0)),' ',regexp_replace(time,'-',':')) as t_time,lac,ci,isdn
						from dfgx_tour_db.dfgx_brd_sdtp
						where concat(deal_year,deal_month,deal_day)>='20180819' and concat(deal_year,deal_month,deal_day)<='20180825'
						and concat(year,lpad(month,2,'0'),lpad(day,2,'0'))>='20180819' and concat(year,lpad(month,2,'0'),lpad(day,2,'0'))<='20180825'
						and length(isdn)=11
						union all
						select from_unixtime(cast(proc_starttime as bigint),'yyyyMMdd') as day_id,from_unixtime(cast(proc_starttime as bigint),'yyyy-MM-dd HH:mm:ss') as t_time,substr(SAI_CGI_ECGI,6,5) as lac,substr(SAI_CGI_ECGI,11,2) as ci,msisdn as isdn
						from dfgx_wo_db.4g_s1mme
						where match_day>='20180819' and match_day<='20180825'
						and from_unixtime(cast(proc_starttime as bigint),'yyyyMMdd')>='20180819' and from_unixtime(cast(proc_starttime as bigint),'yyyyMMdd')<='20180825'
						and sai_cgi_ecgi<>''
						and length(msisdn)=11
						) a1
					) a2 join dfgx_tour_db.dfgx_lzy_fsgm_spot_lacci_tran a3 on conv(a2.first_lac,16,10)=a3.lac and conv(a2.first_ci,16,10)=a3.ci  
				) b join dfgx_tour_db.dfgx_lzy_fsgm_spot_lacci c on conv(b.lac,16,10)=c.lac and conv(b.ci,16,10)=c.ci 
			) cc group by cc.day_id,cc.isdn,cc.transpot,cc.spot_id
		) d join dfgx_tour_db.dfgx_lzy_fsgm_spot e on d.spot_id=e.spot_id and d.stay_time>1200
	) f left join dfgx_tour_db.dfgx_lzy_fsgm_changzhu g on f.isdn =g.isdn and f.spot_name=g.spot_name and g.isdn is null;
	
	
	
交通工具--城区
add jar /data2/USERS/hadoop_dfgx_A/time_interval.jar;
create temporary function td as "time_interval.test1";
select h.day_id,h.isdn,distinct h.transpot from 
	(select f.day_id,f.isdn,f.spot_id,f.spot_name,f.transpot from 
		(select d.day_id,d.isdn,d.spot_id,e.spot_name,d.transpot from 
			(select cc.day_id,cc.isdn,cc.transpot,cc.spot_id,sum(cc.stay_time) as stay_time  from 
				(select b.day_id,b.isdn,b.transpot,c.spot_id,b.stay_time from 
					(select a2.day_id,a2.isdn,a2.lac,a2.ci,a2.t_time,a2.last_dura,td(a2.t_time,nvl(a2.last_dura,a2.t_time)) as stay_time,
					case
						when a3.transpot==2 then
						'火车'
						when a3.transpot==3 then
						'汽车'
						when a3.transpot==4 then
						'自驾'
						end as transpot
					from 
						(select a1.day_id, a1.isdn,a1.lac,a1.ci,first_value(a1.lac) over(partition by a1.day_id,a1.isdn order by a1.t_time) as first_lac,first_value(a1.ci) over(partition by a1.day_id,a1.isdn order by a1.t_time) as first_ci,a1.t_time,lead(a1.t_time) over(partition by a1.isdn order by a1.t_time) as last_dura from 
							(select concat(year,lpad(month,2,'0'),lpad(day,2,'0')) as day_id, concat(concat_ws('-',year,lpad(month,2,0),lpad(day,2,0)),' ',regexp_replace(time,'-',':')) as t_time, lac, ci, isdn
							from dfgx_tour_db.dfgx_brd_sdtp
							where concat(deal_year,deal_month,deal_day)>='20180819' and concat(deal_year,deal_month,deal_day)<='20180825'
							and concat(year,lpad(month,2,'0'),lpad(day,2,'0'))>='20180819' and concat(year,lpad(month,2,'0'),lpad(day,2,'0'))<='20180825'
							and length(isdn)=11
							union all
							select from_unixtime(cast(proc_starttime as bigint),'yyyyMMdd') as day_id,from_unixtime(cast(proc_starttime as bigint),'yyyy-MM-dd HH:mm:ss') as t_time,substr(SAI_CGI_ECGI,6,5) as lac,substr(SAI_CGI_ECGI,11,2) as ci,msisdn as isdn
							from dfgx_wo_db.4g_s1mme
							where match_day>='20180819' and match_day<='20180825'
							and from_unixtime(cast(proc_starttime as bigint),'yyyyMMdd')>='20180819' and from_unixtime(cast(proc_starttime as bigint),'yyyyMMdd')<='20180825'
							and sai_cgi_ecgi<>''
							and length(msisdn)=11
							) a1
						) a2 join dfgx_tour_db.dfgx_lzy_fsgm_spot_lacci_tran a3 on conv(a2.first_lac,16,10)=a3.lac and conv(a2.first_ci,16,10)=a3.ci
					) b join dfgx_tour_db.dfgx_lzy_fsgm_spot_lacci c on b.lac=conv(c.lac,10,16) and b.ci=conv(c.ci,10,16)
				) cc group by cc.day_id,cc.isdn,cc.transpot,cc.spot_id
			) d join dfgx_tour_db.dfgx_lzy_fsgm_spot e on d.spot_id=e.spot_id and d.stay_time>3600
		) f left join dfgx_tour_db.dfgx_lzy_fsgm_changzhu g on f.isdn =g.isdn and f.spot_name=g.spot_name where g.isdn is null
	) h group by h.day_id,h.isdn;
	
	
	
--游客逗留天数
景区游客
add jar /data2/USERS/hadoop_dfgx_A/time_interval.jar;
create temporary function td as "time_interval.test1";
select h.spot_id,h.spot_name,h.isdn,(h.start_date-h.end_date) as stay_day from 
	(select f.day_id,f.spot_id,f.spot_name,f.isdn,f.end_date,f.start_date from 
		(select d.day_id,d.spot_id,e.spot_name,d.isdn,d.start_date,d.end_date from 
			(select cc.day_id,cc.spot_id,cc.isdn,cc.start_date,cc.end_date,sum(cc.stay_time) as stay_time from 
				(select b.day_id,c.spot_id,b.isdn,b.start_date,b.end_date,b.t_time,b.last_dura,td(b.t_time,nvl(b.last_dura,b.t_time)) as stay_time
					(select a.day_id,a.t_time,a.lac,a.ci,a.isdn,first_value(a.day_id) OVER (PARTITION BY a.isdn ORDER BY a.day_id) as start_date,
					last_value(a.day_id) OVER (PARTITION BY a.isdn ORDER BY a.day_id) as end_date,lead(a.t_time) over(partition by a1.isdn order by a.t_time) as last_dura from 
						(select concat(year,lpad(month,2,'0'),lpad(day,2,'0')) as day_id, concat(concat_ws('-',year,lpad(month,2,0),lpad(day,2,0)),' ',regexp_replace(time,'-',':')) as t_time,lac,ci,isdn
						from dfgx_tour_db.dfgx_brd_sdtp
						where concat(deal_year,deal_month,deal_day)>='20180819' and concat(deal_year,deal_month,deal_day)<='20180825'
						and concat(year,lpad(month,2,'0'),lpad(day,2,'0'))>='20180819' and concat(year,lpad(month,2,'0'),lpad(day,2,'0'))<='20180825'
						and length(isdn)=11
						union all
						select from_unixtime(cast(proc_starttime as bigint),'yyyyMMdd') as day_id,from_unixtime(cast(proc_starttime as bigint),'yyyy-MM-dd HH:mm:ss') as t_time,substr(SAI_CGI_ECGI,6,5) as lac,substr(SAI_CGI_ECGI,11,2) as ci,msisdn as isdn
						from dfgx_wo_db.4g_s1mme
						where match_day>='20180819' and match_day<='20180825'
						and from_unixtime(cast(proc_starttime as bigint),'yyyyMMdd')>='20180819' and from_unixtime(cast(proc_starttime as bigint),'yyyyMMdd')<='20180825'
						and sai_cgi_ecgi<>''
						and length(msisdn)=11
						) a 
					) b join dfgx_tour_db.dfgx_lzy_fsgm_spot_lacci c on conv(b.lac,16,10)=c.lac and conv(b.ci,16,10)=c.ci
				) cc group by cc.day_id,cc.spot_id,cc.isdn,cc.start_date,cc.end_date
			) d join dfgx_tour_db.dfgx_lzy_fsgm_spot e on d.spot_id=e.spot_id where d.stay_time>3600
		) f left join dfgx_tour_db.dfgx_lzy_fsgm_changzhu g on f.spot_name=g.spot_name and f.isdn=g.isdn where g.isdn is null
	) h group by h.spot_id,h.spot_name,h.isdn;
	
	
	
游客逗留天数
景区游客
add jar /data2/USERS/hadoop_dfgx_A/time_interval.jar;
create temporary function td as "time_interval.test1";
select h.isdn,(h.start_date-h.end_date) as stay_day from 
	(select f.isdn,f.end_date,f.start_date from 
		(select d.spot_id,e.spot_name,d.isdn,d.start_date,d.end_date from 
			(select cc.spot_id,cc.isdn,cc.start_date,cc.end_date,sum(cc.stay_time) as stay_time from 
				(select c.spot_id,b.isdn,b.start_date,b.end_date,b.t_time,b.last_dura,td(b.t_time,nvl(b.last_dura,b.t_time)) as stay_time
					(select a.t_time,a.lac,a.ci,a.isdn,first_value(a.day_id) OVER (PARTITION BY a.isdn ORDER BY a.day_id) as start_date,
					last_value(a.day_id) OVER (PARTITION BY a.isdn ORDER BY a.day_id) as end_date,lead(a.t_time) over(partition by a1.isdn order by a.t_time) as last_dura from 
						(select concat(year,lpad(month,2,'0'),lpad(day,2,'0')) as day_id, concat(concat_ws('-',year,lpad(month,2,0),lpad(day,2,0)),' ',regexp_replace(time,'-',':')) as t_time,lac,ci,isdn
						from dfgx_tour_db.dfgx_brd_sdtp
						where concat(deal_year,deal_month,deal_day)>='20180819' and concat(deal_year,deal_month,deal_day)<='20180825'
						and concat(year,lpad(month,2,'0'),lpad(day,2,'0'))>='20180819' and concat(year,lpad(month,2,'0'),lpad(day,2,'0'))<='20180825'
						and length(isdn)=11
						union all
						select from_unixtime(cast(proc_starttime as bigint),'yyyyMMdd') as day_id,from_unixtime(cast(proc_starttime as bigint),'yyyy-MM-dd HH:mm:ss') as t_time,substr(SAI_CGI_ECGI,6,5) as lac,substr(SAI_CGI_ECGI,11,2) as ci,msisdn as isdn
						from dfgx_wo_db.4g_s1mme
						where match_day>='20180819' and match_day<='20180825'
						and from_unixtime(cast(proc_starttime as bigint),'yyyyMMdd')>='20180819' and from_unixtime(cast(proc_starttime as bigint),'yyyyMMdd')<='20180825'
						and sai_cgi_ecgi<>''
						and length(msisdn)=11
						) a 
					) b join dfgx_tour_db.dfgx_lzy_fsgm_spot_lacci c on conv(b.lac,16,10)=c.lac and conv(b.ci,16,10)=c.ci
				) cc group by  cc.spot_id,cc.isdn,cc.start_date,cc.end_date
			) d join dfgx_tour_db.dfgx_lzy_fsgm_spot e on d.spot_id=e.spot_id where d.stay_time>3600
		) f left join dfgx_tour_db.dfgx_lzy_fsgm_changzhu g on f.spot_name=g.spot_name and f.isdn=g.isdn where g.isdn is null
	) h group by h.isdn;
	
	
	
--轨迹
select '$v_day_format' as day_id, h.track, count(h.isdn) as user_num
from (select g.isdn,concat_ws('==>',collect_list(g.spot_name)) as track ,count(g.spot_name) as rn2
		(select f.day_id,f.isdn,f.spot_id,f.spot_name from 
			(select d.day_id,d.spot_id,e.spot_name,d.isdn,d.t_time,row_number() over(partition by d.spot_name,d.isdn order by d.t_time) as rn from 
				(select b.day_id,c.spot_id,b.isdn,b.t_time,b.last_dura,td(b.t_time,nvl(b.last_dura,b.t_time)) as stay_time
					(select a.day_id,a.t_time,a.lac,a.ci,a.isdn,lead(a.t_time) over(partition by a1.isdn order by a.t_time) as last_dura from 
						(select concat(year,lpad(month,2,'0'),lpad(day,2,'0')) as day_id, concat(concat_ws('-',year,lpad(month,2,0),lpad(day,2,0)),' ',regexp_replace(time,'-',':')) as t_time,lac,ci,isdn
						from dfgx_tour_db.dfgx_brd_sdtp
						where concat(deal_year,deal_month,deal_day)='$v_day_format'
						and concat(year,lpad(month,2,'0'),lpad(day,2,'0'))='$v_day_format' 
						and length(isdn)=11
						) a 
					) b join dfgx_tour_db.dfgx_lzy_fsgm_spot_lacci c on conv(b.lac,16,10)=c.lac and conv(b.ci,16,10)=c.ci
				) d join dfgx_tour_db.dfgx_lzy_fsgm_spot e on d.spot_id=e.spot_id where d.stay_time>3600
			) f left join dfgx_tour_db.dfgx_lzy_fsgm_changzhu g on f.spot_name=g.spot_name and f.isdn=g.isdn where g.isdn is null and rn=1
		) g group by g.isdn having count(g.spot_name)>1
	) h group by h.track order by user_num desc;
	
	
	
	
---热门app
SELECT d.app_name,count(*) AS ant
FROM
	(SELECT bb.phone_id,cc.app_name 
	from 
		dfgx_tour_db.dfgx_lzy_fsgm_tour_week aa 
		join dfgx_wo_db.dfgx_dpi_data_match bb 
		join dfgx_tour_db.dfgx_zk_app_info cc on aa.isdn=bb.phone_id and bb.type=cc.id 
		where bb.match_day='20180701' 
		bb.phone_id <> ''and cc.subclass like '%旅行攻略类%'
	) dd
GROUP BY dd.app_name
ORDER BY ant desc;




--游客出游时间
select c.day_id,c.isdn from 
	(select b.day_id,b.isdn from 
		(select a.day_id,a.isdn,a.spot_name,row_number() over(partition by a.isdn order by a.day_id) as rn 
			from dfgx_tour_db.dfgx_lzy_fsgm_tour_week a
		) b where b.rn=1
	) c group by c.day_id;