#!/bin/bash

v_file_route=`sh /data2/USERS/hadoop_dfgx_A/zk/20180112-shenzhengtrip/shell/shenzhen/file.sh`
v_file=$v_file_route/sz_to_sw_day.txt

hive -e "
	set mapreduce.job.name='深圳省外游-日数据';
	use dfgxtour_db;
	select d.first_day,count(*) from (select b.first_day,c.province,c.city,b.stay_day,b.age_type,b.sex_type,b.msisdn from (select a.first_day,a.visit_area_code,a.stay_day,a.age_type,a.sex_type,a.msisdn from (select split(first_date,' ')[0] as first_day,msisdn,visit_area_code,stay_day,age_type,case when sex like '%M%' then '男' when sex like '%F%' then '女' else '未知' end as sex_type from dfgx_tour_db.dfgx_sz_to_sw_people_result_2018gq) a  group by a.first_day,a.visit_area_code,a.stay_day,a.age_type,a.sex_type,a.msisdn) b join stq_location.stq_isdn_city_area_info c on b.visit_area_code=c.city_code where c.province not like '%广东%' group by b.first_day,c.province,c.city,b.stay_day,b.age_type,b.sex_type,b.msisdn) d group by d.first_day order by d.first_day;
"