#!/bin/bash

for i in {201806..201808}
do
	v_month=$i
	echo $v_month
	if [ $v_month -eq 201806 ]
	then
		hive -e "
			set mapreduce.job.name='节日临时表$v_momth';
			use dfgx_tour_db;
			create table gq_tmp1 as 
			SELECT a8.year,a8.month,a8.day,a8.area_code,a8.isdn,a8.max_time,a8.min_time,from_unixtime(a8.max_time,'yyyyMMdd HH:mm:ss') AS end_time, from_unixtime(a8.min_time,'yyyyMMdd HH:mm:ss') AS start_time, (a8.max_time-a8.min_time) AS stay_time
			FROM 
				(SELECT a7.year,a7.month,a7.day,a7.area_code,a7.isdn,a7.max_time,a7.min_time,a7.num,count(*)
				FROM 
					(SELECT a6.isdn,a6.year,a6.month,a6.day,a6.area_code,a6.num,max(a6.time_ws) over(partition by a6.isdn,a6.year,a6.month,a6.day,a6.area_code,a6.num) AS max_time,min(a6.time_ws) over(partition by a6.isdn,a6.year,a6.month,a6.day,a6.area_code,a6.num) AS min_time
					FROM 
						(SELECT a5.*,(cast(a5.row_num AS int) - cast(a5.rank_num AS int)) AS num
						FROM 
							(SELECT a4.*,rank() over(partition by a4.isdn,a4.year,a4.month,a4.day,a4.area_code
							ORDER BY  a4.row_num) AS rank_num
							FROM 
								(SELECT a3.year,a3.month,a3.day,a3.isdn,a3.time_ws,a3.area_code,row_number() over(partition by a3.isdn,a3.year,a3.month,a3.day
								ORDER BY  a3.time_ws) AS row_num
								FROM 
									(SELECT a1.year,a1.month,a1.day,a1.isdn,unix_timestamp(concat(a1.year,'-',a1.month,'-',a1.day,' ',split(a1.time,'-')[0],':',split(a1.time,'-')[1],':',split(a1.time,'-')[2])) AS time_ws,a2.area_code
									FROM 
										(SELECT year,month,day,time,lac,ci,isdn
										FROM dfgx_tour_db.dfgx_brd_sdtp
										WHERE concat(deal_year,deal_month)='$v_month' AND isdn <> '' ) a1
										JOIN 
											(SELECT lacci,'1' AS area_code
											FROM dfgx_tour_db.dfgx_lacci_info
											WHERE city LIKE '%深圳%') a2
												ON concat(a1.lac,a1.ci)=a2.lacci 
									) a3 
								) a4 
							) a5 
						) a6 
					)a7
						GROUP BY  a7.year,a7.month,a7.day,a7.area_code,a7.isdn,a7.max_time,a7.min_time,a7.num 
				) a8;
"	
	else
		hive -e "
		set mapreduce.job.name='节日临时表$v_momth';
		use dfgx_tour_db;
		insert into table gq_tmp1 
		SELECT a8.year,a8.month,a8.day,a8.area_code,a8.isdn,a8.max_time,a8.min_time,from_unixtime(a8.max_time,'yyyyMMdd HH:mm:ss') AS end_time, from_unixtime(a8.min_time,'yyyyMMdd HH:mm:ss') AS start_time, (a8.max_time-a8.min_time) AS stay_time
		FROM 
			(SELECT a7.year,a7.month,a7.day,a7.area_code,a7.isdn,a7.max_time,a7.min_time,a7.num,count(*)
			FROM 
				(SELECT a6.isdn,a6.year,a6.month,a6.day,a6.area_code,a6.num,max(a6.time_ws) over(partition by a6.isdn,a6.year,a6.month,a6.day,a6.area_code,a6.num) AS max_time,min(a6.time_ws) over(partition by a6.isdn,a6.year,a6.month,a6.day,a6.area_code,a6.num) AS min_time
				FROM 
					(SELECT a5.*,(cast(a5.row_num AS int) - cast(a5.rank_num AS int)) AS num
					FROM 
						(SELECT a4.*,rank() over(partition by a4.isdn,a4.year,a4.month,a4.day,a4.area_code
						ORDER BY  a4.row_num) AS rank_num
						FROM 
							(SELECT a3.year,a3.month,a3.day,a3.isdn,a3.time_ws,a3.area_code,row_number() over(partition by a3.isdn,a3.year,a3.month,a3.day
							ORDER BY  a3.time_ws) AS row_num
							FROM 
								(SELECT a1.year,a1.month,a1.day,a1.isdn,unix_timestamp(concat(a1.year,'-',a1.month,'-',a1.day,' ',split(a1.time,'-')[0],':',split(a1.time,'-')[1],':',split(a1.time,'-')[2])) AS time_ws,a2.area_code
								FROM 
									(SELECT year,month,day,time,lac,ci,isdn
									FROM dfgx_tour_db.dfgx_brd_sdtp
									WHERE concat(deal_year,deal_month)='$v_month' AND isdn <> '' ) a1
									JOIN 
										(SELECT lacci,'1' AS area_code
										FROM dfgx_tour_db.dfgx_lacci_info
										WHERE city LIKE '%深圳%') a2
											ON concat(a1.lac,a1.ci)=a2.lacci 
								) a3 
							) a4 
						) a5 
					) a6 
				)a7
					GROUP BY  a7.year,a7.month,a7.day,a7.area_code,a7.isdn,a7.max_time,a7.min_time,a7.num 
			) a8;
"	
	fi
done
