#!/bin/bash

hive -e "
	add jar /data2/USERS/hadoop_dfgx_A/time_interval.jar;
	create temporary function td as 'time_interval.test1';
	use dfgx_tour_db;
	create table gn_to_sz_tmp1 as 
	SELECT l.isdn,l.t_time,l.lac,l.ci,l.next_time,l.stay_date,l.province,l.from_city,l.to_city,l.type,l.traffic_type,l.first_time,l.last_time,l.stay_day,l.traffic_num,nvl(first_value(l.traffic_type) over(partition by l.isdn,l.to_city,l.type
		ORDER BY  l.traffic_num desc),'其他') AS traffic
		FROM 
			(SELECT k.isdn,k.lac,k.ci,k.t_time,k.next_time,td(k.t_time,k.next_time) AS stay_date,k.province,k.from_city,k.to_city,k.type,k.traffic_type,k.first_time,k.last_time,datediff(k.last_time,k.first_time) AS stay_day,
	case
				WHEN k.traffic_type is NOT NULL THEN
				row_number() over(partition by k.isdn,k.to_city,k.type
			ORDER BY  k.t_time desc)
				ELSE '0'
				END AS traffic_num
			FROM 
				(SELECT j.isdn,j.lac,j.ci,j.t_time,lead(j.t_time) over(partition by j.isdn,j.type
				ORDER BY  j.t_time) AS next_time,j.province,j.from_city,j.to_city,j.type,j.traffic_type,first_value(j.t_time) over(partition by j.isdn,j.type
				ORDER BY  j.t_time) AS first_time,last_value(j.t_time) over(partition by j.isdn,j.type
				ORDER BY  j.t_time rows
					BETWEEN unbounded preceding
						AND unbounded following) AS last_time
				FROM 
					(SELECT h.isdn,h.t_time,h.lac,h.ci,h.province,h.from_city,h.to_city,h.type,i.traffic_type
					FROM 
						(SELECT g.isdn,g.t_time,g.lac,g.ci,g.province,g.from_city,g.to_city,g.l_num,g.r_num,(g.l_num-g.r_num) AS type
						FROM 
							(SELECT f.isdn,f.t_time,f.lac,f.ci,f.province,f.from_city,f.to_city,f.l_num,row_number() over(partition by f.isdn,f.to_city
							ORDER BY  f.t_time,f.lac,f.ci) AS r_num
							FROM 
								(SELECT e.isdn,e.t_time,e.lac,e.ci,e.province,e.from_city,e.to_city,row_number() over(partition by e.isdn
								ORDER BY  e.t_time,e.lac,e.ci) AS l_num
								FROM 
									(SELECT a.isdn,concat(concat_ws('-',a.year,lpad(a.month,2,0),lpad(a.day,2,0)),' ',regexp_replace(a.time,'-',':')) AS t_time,a.lac,a.ci,c.province,c.city AS from_city,d.city AS to_city
									FROM dfgx_tour_db.dfgx_brd_sdtp a
									JOIN dfgx_tour_db.to_sz_travel_people_2018gq b
									JOIN stq_location.stq_isdn_city_area_info c
									JOIN dfgx_tour_db.dfgx_lacci_info d
										ON a.isdn=b.isdn
											AND c.init_isdn=substr(a.isdn,1,7)
											AND d.lacci=concat (a.lac,a.ci)
									WHERE concat (a.deal_year,lpad(a.deal_month,2,0),lpad (a.deal_day,2,0))>='20180901'
											AND concat(a.deal_year,lpad(a.deal_month,2,0),lpad(a.deal_day,2,0))<='20180905'
									) e
									GROUP BY  e.isdn,e.t_time,e.lac,e.ci,e.province,e.from_city,e.to_city
								) f
									WHERE f.to_city LIKE '%深圳%'
							)g
									ORDER BY  g.isdn,type
						) h
									LEFT JOIN dfgx_tour_db.dim_lac_ci_traffic_mapping i
										ON h.lac=conv(i.lac,10,16)
											AND h.ci=conv (i.ci,10,16)
					) j
				) k
									WHERE k.next_time is NOT null
			) l
									WHERE l.stay_date >= 18000;
"