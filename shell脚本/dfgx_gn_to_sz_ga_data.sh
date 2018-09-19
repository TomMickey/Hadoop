#!/bin/bash

hive -e "
	select a.province,a.from_city,a.first_day,a.stay_day,a.last_traffic,nvl(a.age_type,'未知'),nvl(a.sex_type,'未知'),round(count(*)/0.17) from (select isdn,province,from_city,split(first_time,' ')[0] as first_day,stay_day,last_traffic,age_type,sex_type from dfgx_tour_db.dfgx_gn_to_sz_ga_travel_2018gq) a group by a.province,a.from_city,a.first_day,a.stay_day,a.last_traffic,a.age_type,a.sex_type;
">./dfgx_gn_to_sz_ga_data.txt