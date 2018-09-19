#!/bin/bash


hive -e "
	set mapreduce.job.name='深圳本地国外游-获取出国游游客';
	use dfgx_tour_db;
	insert overwrite table dfgx_tour_db.dfgx_sz_to_foreign_travel_2018gq SELECT z.msisdn FROM (SELECT s.msisdn FROM (SELECT msisdn FROM yx_details.tg_cdrmm_ss WHERE roam_type= '6' AND (source_day>='20180901' AND source_day<='20180905') AND (visit_area_code <> '00853'  AND visit_area_code <> '00852'  AND visit_area_code <> '00886') UNION all SELECT msisdn FROM yx_details.tg_cdrmm_gs_ss WHERE roam_type= '6' AND (source_day>='20180901' AND source_day<='20180905')  AND (visit_area_code <> '00853' AND visit_area_code <> '00852'  AND visit_area_code <> '00886') UNION all SELECT msisdn FROM yx_details.ods_tg_cdrmm_gs_23g WHERE roam_type= '6'  AND ((source_day>='h20180901' AND source_day<='h20180905')  OR (source_day>='y20180901' AND source_day<='y20180905'))  AND (visit_area_code <> '00853' AND visit_area_code <> '00852'  AND visit_area_code <> '00886') UNION all SELECT msisdn  FROM yx_details.ods_tg_cdrmm_23g WHERE roam_type= '6' AND ((source_day>='h20180901' AND source_day<='h20180905') OR (source_day>='y20180901' AND source_day<='y20180905')) AND (visit_area_code <> '00853' AND visit_area_code <> '00852' AND visit_area_code <> '00886')) s GROUP BY  s.msisdn) z JOIN dfgx_tour_db.sz_cz_people_gq b  ON z.msisdn=b.isdn;
"