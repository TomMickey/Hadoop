#!/bin/bash

spark-sql --master yarn --queue root.B --num-executors 400 --executor-cores 3 --executor-memory 10G -e "
	insert overwrite table dfgx_tour_db.to_sz_travel_people_2018gq  select h.isdn from (select e.isdn from (select a.isdn,a.lac,a.ci from dfgx_tour_db.dfgx_brd_sdtp a left join dfgx_tour_db.sz_cz_people_gq b on a.isdn=b.isdn where concat (a.deal_year,lpad(a.deal_month,2,0),lpad(a.deal_day,2,0))>='20180901' and concat(a.deal_year,lpad(a.deal_month,2,0),lpad(a.deal_day,2,0))<='20180905' and a.isdn <> '' and b.isdn is null) e join dfgx_tour_db.sz_spot_mid f on e.lac=conv(f.lac,10,16) and e.ci=conv(f.ci,10,16)) h group by h.isdn;
"