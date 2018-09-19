#!/bin/bash


hive -e "
	use dfgx_tour_db;
	create table gn_to_sz_tmp2 as 
	SELECT o.isdn,o.province,o.from_city,o.to_city,first_time,o.last_time,o.stay_day,
			case
			WHEN o.traffic='1' THEN
			'飞机'
			WHEN o.traffic='2' THEN
			'火车'
			WHEN o.traffic='3' THEN
			'汽车'
			ELSE o.traffic
			END AS last_traffic,p.age_type,p.sex_type
		FROM 
			(SELECT m.isdn,m.province,m.from_city,m.to_city,m.type,m.first_time,m.last_time,m.stay_day,m.traffic
			FROM 
				gn_to_sz_tmp1 m
					JOIN 
						(SELECT lac,ci
						FROM dfgx_tour_db.dim_lac_ci_spot_mapping
						WHERE spot_id ='051755022'
								OR spot_id ='051755023'
								OR spot_id ='051755024'
								OR spot_id ='051755025'
								OR spot_id ='051755026'
								OR spot_id ='051755027'
								OR spot_id ='051755028'
								OR spot_id ='051755029'
						) n
							ON conv(m.lac,16,10)=n.lac
								AND conv(m.ci,16,10)=n.ci
						GROUP BY  m.isdn,m.province,m.from_city,m.to_city,m.type,m.first_time,m.last_time,m.stay_day,m.traffic
			) o
				LEFT JOIN 
					(SELECT serial_number,
				case
							WHEN sex LIKE '%M%' THEN
							'男'
							WHEN sex LIKE '%F%' THEN
							'女'
							ELSE '未知'
							END AS sex_type,case
							WHEN age <=14 THEN
							'少年'
							WHEN age>=15
								AND age<=24 THEN
							'青年'
							WHEN age>=25
								AND age<=44 THEN
							'壮盛年'
							WHEN age>=45
								AND age<=64 THEN
							'中年'
							WHEN age>=65 THEN
							'老年'
							ELSE '未知'end AS age_type
						FROM dfgx_wo_db.dfgx_user_info
					) p
				ON o.isdn=p.serial_number
				WHERE o.from_city NOT LIKE '%深圳%';
"