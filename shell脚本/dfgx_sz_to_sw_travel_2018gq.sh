#!/bin/bash

hive -e "
	set mapreduce.job.name='深圳省外游';
	use dfgx_tour_db;
	create table dfgx_tour_db.dfgx_sz_to_sw_travel_2018gq SELECT z.msisdn FROM (SELECT s.msisdn FROM  (SELECT msisdn FROM yx_details.tg_cdrmm_ss  WHERE roam_type= '4' AND (source_day>='20180901' AND source_day<='20180905') UNION all SELECT msisdn FROM yx_details.tg_cdrmm_gs_ss WHERE roam_type= '4'  AND (source_day>='20180901' AND source_day<='20180905') UNION all SELECT msisdn FROM yx_details.ods_tg_cdrmm_gs_23g WHERE roam_type= '4' AND ((source_day>='h20180901' AND source_day<='h20180905')  OR (source_day>='y20180901'  AND source_day<='y20180905'))  UNION all SELECT msisdn FROM yx_details.ods_tg_cdrmm_23g WHERE roam_type= '4' AND ((source_day>='h20180901'  AND source_day<='h20180905') OR (source_day>='y20180901' AND source_day<='y20180905'))) s GROUP BY  s.msisdn) z JOIN dfgx_tour_db.to_sz_travel_people_2018gq b ON z.msisdn=b.isdn;
"