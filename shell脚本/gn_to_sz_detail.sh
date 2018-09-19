#!/bin/bash

spark-sql --master yarn --queue root.B --num-executors 400 --executor-cores 3 --executor-memory 10G -e "
	select first_day,province,from_city,to_city,traffic_type,stay_time,age_type,nvl(sex_type,'其他'),round(count(*)/0.17) from dfgx_tour_db.sz_travel_user_info_2018gq where from_city !='深圳' group by first_day,province,from_city,to_city,traffic_type,stay_time,age_type,sex_type;
">./gn_to_sz_detail.txt