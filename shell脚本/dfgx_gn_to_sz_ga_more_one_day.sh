#!/bin/bash

hive -e "
	select a.first_day,round(count(*)/0.17) from (select split(first_time,' ')[0] as first_day,isdn,stay_day from dfgx_tour_db.dfgx_gn_to_sz_ga_travel_2018gq) a where a.stay_day!=0 group by a.first_day order by first_day;
">./dfgx_gn_to_sz_ga_more_one_day.txt