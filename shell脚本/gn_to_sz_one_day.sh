#!/bin/bash

spark-sql --master yarn --queue root.B --num-executors 400 --executor-cores 3 --executor-memory 10G -e "
	select first_day,round(count(*)/0.17) from dfgx_tour_db.sz_travel_user_info_2018gq where stay_time=0 and from_city !='深圳' group by first_day order by first_day;
">./gn_to_sz_one_day.txt