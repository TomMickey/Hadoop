#!/bin/bash

hive -e "
	set mapreduce.job.name='热门商圈、景区排行-生成原始数据';
	use dfgx_tour_db;
	insert overwrite table dfgx_tour_db.dfgx_sz_hotviews_2018gq select e.isdn,case when e.province='广东' then '省内' else '省外' end as province_type,'051' as proc_code,'V0440300' as city_code,e.t_time,e.spot_name,case when e.spot_name like '%商圈%' then '商圈' else '景区' end as spot_type,e.lon,e.lat from (select a.isdn,d.province,a.lac,a.ci,concat(a.year,lpad(a.month,2,0),lpad(a.day,2,0),regexp_replace(substr(a.time,1,5),'-','')) as t_time,b.spot_name,b.lon,b.lat from dfgx_tour_db.dfgx_brd_sdtp a join dfgx_tour_db.sz_spot_mid b join dfgx_tour_db.to_sz_travel_people_2018gq c join stq_location.stq_isdn_city_area_info d on conv(a.lac,16,10)=b.lac and conv(a.ci,16,10)=b.ci and a.isdn=c.isdn and d.init_isdn=substr(a.isdn,1,7) where concat (a.deal_year,lpad(a.deal_month,2,0),lpad (a.deal_day,2,0))>='20180901' and concat(a.deal_year,lpad(a.deal_month,2,0),lpad(a.deal_day,2,0))<='20180905' and length(a.isdn) =11) e group by  e.isdn,e.province,e.t_time,e.spot_name,e.lon,e.lat;
"