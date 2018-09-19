#!/bin/bash

hive -e "
	insert overwrite table dfgx_tour_db.sz_cz_people_gq AS SELECT a11.isdn
	FROM 
		(SELECT a10.area_code,a10.isdn,count(*) AS stay_day
		FROM 
			(SELECT a9.year,a9.month,a9.day,a9.area_code,a9.isdn,sum(a9.stay_time) AS stay
			FROM 
				tmp1 a9
				GROUP BY  a9.year,a9.month,a9.day,a9.area_code,a9.isdn 
			) a10
				WHERE a10.stay>=14400
				GROUP BY  a10.area_code,a10.isdn 
		) a11
			WHERE a11.stay_day>=75;
"