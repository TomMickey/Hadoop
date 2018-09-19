#!/bin/bash

v_file_route=`sh /data2/USERS/hadoop_dfgx_A/zk/20180112-shenzhengtrip/shell/shenzhen/file.sh`
v_file=$v_file_route/sz_to_sw_data.txt

hive -e "
	set mapreduce.job.name='深圳省外游-最终结果';
	use dfgx_tour_db;
	SELECT d.first_day,d.province,d.city,d.stay_day,d.age_type,d.sex_type,count(*)
	FROM 
		(SELECT b.first_day,c.province,c.city,b.stay_day,b.age_type,b.sex_type,b.msisdn
		FROM 
			(SELECT a.first_day,a.visit_area_code,a.stay_day,a.age_type,a.sex_type,a.msisdn
			FROM 
				(SELECT split(first_date,' ')[0] AS first_day,msisdn,visit_area_code,stay_day,age_type,case
					WHEN sex LIKE '%M%' THEN
					'男'
					WHEN sex LIKE '%F%' THEN
					'女'
					ELSE '未知'
					END AS sex_type
				FROM dfgx_tour_db.dfgx_sz_to_sw_people_result_2018gq
				) a
				GROUP BY  a.first_day,a.visit_area_code,a.stay_day,a.age_type,a.sex_type,a.msisdn
			) b
				JOIN stq_location.stq_isdn_city_area_info c
					ON b.visit_area_code=c.city_code
				WHERE c.province NOT LIKE '%广东%'
				GROUP BY  b.first_day,c.province,c.city,b.stay_day,b.age_type,b.sex_type,b.msisdn
		) d
			GROUP BY  d.first_day,d.province,d.city,d.stay_day,d.age_type,d.sex_type limit 100;
">$v_file