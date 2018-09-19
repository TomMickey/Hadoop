#!/bin/bash

hive -e "
	use dfgx_tour_db;
	create table dfgx_tour_db.dfgx_gn_to_sz_ga_travel_2018gq AS SELECT r.isdn,r.province,r.from_city,r.to_city,r.first_time,r.last_time,r.stay_day,r.last_traffic,nvl(r.age_type,'未知') AS age_type,nvl(r.sex_type,'未知') AS sex_type
	FROM gn_to_sz_tmp2 r GROUP BY  r.isdn,r.province,r.from_city,r.to_city,r.first_time,r.last_time,r.stay_day,r.last_traffic,r.age_type,r.sex_type;
"