select member_number, max(time_stamp) 
 
from m4m.return_feed_header 
where partner='Simply Energy' 
 
group by 1 
simply_list = spark.read.load("/user/aamreen/DMC-2314_SimplyEnergy_DQ.csv",format="csv", sep=",", inferSchema="true", header="true") 
simply_list.createOrReplaceTempView("simply_list") 
#spark.sql('''create table campaign_data.aa_dmc214_simplyenergyDQ_20210303 as select * from simply_list''').count() 
spark.sql(''' 
create table campaign_data.aa_dmc2314_simplyenergyDQ_20210303 as 
select distinct  HUB_ID, CONTRACT_SIGNED_DATE, REWARD_PLAN_CARD_ID 
from simply_list 
where REWARD_PLAN_CARD_ID is not null 
''').show(10000) 
spark.sql(''' 
select count(*), count(distinct REWARD_PLAN_CARD_ID) from simply_list s 
 
where REWARD_PLAN_CARD_ID is not null 
 
''').show(10000) 
spark.sql(''' 
select count(*), count(distinct REWARD_PLAN_CARD_ID), count(member_number) from simply_list s 
 
inner join m4m.return_feed_header m 
on m.member_number=s.REWARD_PLAN_CARD_ID 
and partner='Simply Energy' 
 
where REWARD_PLAN_CARD_ID is not null 
 
''').show(10000) 
drop table aa_dmc214_simplyenergydq_20210303 
SELECT reward_plan_card_id,count(*) from campaign_data.aa_dmc2314_simplyenergyDQ_20210303 
where reward_plan_card_id is not NULL 
AND cast(reward_plan_card_id as STRING) not like '1234%' 
GROUP BY 1 
order by 2 desc 
SELECT count(*), count(DISTINCT reward_plan_card_id) from aa_dmc2314_simplyenergydq_20210303 
where reward_plan_card_id is not NULL 
AND cast(reward_plan_card_id as STRING) not like '1234%' 
 
SELECT s.* 
-- case when cast(reward_plan_card_id as STRING) like '1234%' or cast(reward_plan_card_id as STRING) like '456%' then 'Incorrect random number' 
--           when length(cast(reward_plan_card_id as STRING))<9 then 'Less than 9' 
--           else 'unknown' end reason 
--         , count(DISTINCT s.reward_plan_card_id) 
 
from campaign_data.aa_dmc2314_simplyenergydq_20210303 s 
 
left anti join m4m.return_feed_header m 
on m.member_number=case when length(cast(reward_plan_card_id as STRING))=7 then concat(cast(s.REWARD_PLAN_CARD_ID as string),'01') else cast(s.REWARD_PLAN_CARD_ID as string) end 
and partner='Simply Energy' 
 
LEFT ANTI JOIN (select  distinct c.csn as contact_id 
        , a.row_id as asset_row_id 
        , p.prod_cd as asset_type 
        , p.name product_name 
        , a.status_cd 
from gms.s_contact as c 
     
    inner join gms.s_asset as a 
        on a.owner_con_id = c.row_id 
         
    inner join gms.s_prod_int as p 
        on a.prod_id = p.row_id 
   
 
where 1=1 
    and p.name IN ('NB_Simply_Energy_Blue_Offer_0320')) nb 
     
on nb.contact_id=cast(s.reward_plan_card_id as STRING) 
 
 
where reward_plan_card_id is not NULL 
AND cast(reward_plan_card_id as STRING) not like '1234%' 
AND cast(reward_plan_card_id as STRING) not like '456%' 
--and regexp_like(cast(reward_plan_card_id as STRING),"[1-9]*") 
and length(cast(reward_plan_card_id as STRING))<9 
 
-- GROUP BY 1 
-- ORDER BY 1 
--With offer_simply as( 
select  distinct c.row_id as contact_id 
        , c.csn 
        , a.row_id as asset_row_id 
        , p.prod_cd as asset_type 
        , p.name product_name 
        , a.status_cd 
from gms.s_contact as c 
     
    inner join gms.s_asset as a 
        on a.owner_con_id = c.row_id 
         
    inner join gms.s_prod_int as p 
        on a.prod_id = p.row_id 
   
 
where 1=1 
    and p.name IN ('NB_Simply_Energy_Blue_Offer_0320') 
    and c.csn rlike '9905866' 
    --and a.status_cd = 'Active' 
SELECT csn from gms.s_contact 
where csn rlike '49854748' 
select * from m4m.return_feed_header m 
where 1=1 
and partner='Simply Energy' 
and m.member_number='293137701' 
SELECT count(DISTINCT s.reward_plan_card_id) 
--,m.trx_header_id, m.partner, p.name  
 
from campaign_data.aa_dmc2314_simplyenergydq_20210303 s 
 
inner JOIN gms.s_contact c 
on c.csn=cast(s.reward_plan_card_id as STRING) 
 
inner join m4m.return_feed_header m 
on m.member_number=cast(s.reward_plan_card_id as STRING) 
and partner='Simply Energy' 
 
inner JOIN (select  distinct c.csn as contact_id 
        , c.csn 
        , a.row_id as asset_row_id 
        , p.prod_cd as asset_type 
        , p.name product_name 
        , a.status_cd 
        , a.start_dt 
from gms.s_contact as c 
     
    inner join gms.s_asset as a 
        on a.owner_con_id = c.row_id 
         
    inner join gms.s_prod_int as p 
        on a.prod_id = p.row_id 
   
 
where 1=1 
    and p.name IN ('NB_Simply_Energy_Blue_Offer_0320')) nb 
     
on nb.contact_id=cast(s.reward_plan_card_id as STRING) 
 
-- inner join gms.s_asset as a 
--         on a.owner_con_id = c.row_id 
         
--     inner join gms.s_prod_int as p 
--         on a.prod_id = p.row_id 
 
where 1=1 
and reward_plan_card_id is not NULL 
--AND cast(reward_plan_card_id as STRING) not like '1234%' 
--AND cast(reward_plan_card_id as STRING) not like '456%' 
--and p.name='NB' 
--and regexp_like(cast(reward_plan_card_id as STRING),"[1-9]*") 
--and length(cast(reward_plan_card_id as STRING))<9 
--length(cast(reward_plan_card_id as STRING)), 
 
-- ) select reward_plan_card_id, count(*) from main 
 
 
SELECT DISTINCT s.reward_plan_card_id 
 
from campaign_data.aa_dmc2314_simplyenergydq_20210303 s 
 
inner JOIN gms.s_contact c 
on c.csn=cast(s.reward_plan_card_id as STRING) 
 
LEFT ANTI JOIN m4m.return_feed_header m 
on m.member_number=cast(s.reward_plan_card_id as STRING) 
and partner='Simply Energy' 
 
where 1=1 
and reward_plan_card_id is not NULL 
AND cast(reward_plan_card_id as STRING) not like '1234%' 
AND cast(reward_plan_card_id as STRING) not like '456%' 
--and p.name='NB' 
--and regexp_like(cast(reward_plan_card_id as STRING),"[1-9]*") 
--and length(cast(reward_plan_card_id as STRING))<9 
--length(cast(reward_plan_card_id as STRING)), 
 
-- ) select reward_plan_card_id, count(*) from main 
 
 
SELECT to_date(cast(from_unixtime(unix_timestamp(8/05/2020,"dd-MMM-yyyy HH:mm:ss")) as timestamp))  
from gms.s_contact 
SELECT DISTINCT s.reward_plan_card_id, s.hub_id 
 
from campaign_data.aa_dmc2314_simplyenergydq_20210303 s 
 
where 1=1 
and reward_plan_card_id is not NULL 
and s.reward_plan_card_id=990756905 
--AND cast(reward_plan_card_id as STRING) not like '1234%' 
--AND cast(reward_plan_card_id as STRING) not like '456%' 
--and regexp_like(cast(reward_plan_card_id as STRING),"[1-9]*") 
--and length(cast(reward_plan_card_id as STRING))<9 
 
with main as ( 
SELECT DISTINCT s.reward_plan_card_id 
 
from campaign_data.aa_dmc2314_simplyenergydq_20210303 s 
 
left anti join gms.s_contact c 
on c.csn=cast(s.reward_plan_card_id as STRING) 
 
where 1=1 
and reward_plan_card_id is not NULL 
 
) 
select m.*, c.csn from main m 
inner join gms.s_contact c  
on c.csn=concat(cast(m.reward_plan_card_id as STRING),'02') 
 
 
 
-- inner join m4m.return_feed_header m 
-- on m.member_number=case when length(cast(reward_plan_card_id as STRING))=7 then concat(cast(s.REWARD_PLAN_CARD_ID as string),'01') else cast(s.REWARD_PLAN_CARD_ID as string) end 
-- and partner='Simply Energy' 
 
-- Inner JOIN (select  distinct c.csn as contact_id 
--         , a.row_id as asset_row_id 
--         , p.prod_cd as asset_type 
--         , p.name product_name 
--         , a.status_cd 
--         , a.start_dt 
-- from gms.s_contact as c 
     
--     inner join gms.s_asset as a 
--         on a.owner_con_id = c.row_id 
         
--     inner join gms.s_prod_int as p 
--         on a.prod_id = p.row_id 
   
 
-- where 1=1 
--     and p.name IN ('NB_Simply_Energy_Blue_Offer_0320')) nb 
     
-- on nb.contact_id=cast(s.reward_plan_card_id as STRING) 
 
 
 
--AND cast(reward_plan_card_id as STRING) not like '1234%' 
--AND cast(reward_plan_card_id as STRING) not like '456%' 
--and regexp_like(cast(reward_plan_card_id as STRING),"[1-9]*") 
--and length(cast(reward_plan_card_id as STRING))<9 
--length(cast(reward_plan_card_id as STRING)), 
 
-- ) select reward_plan_card_id, count(*) from main 
 
-- GROUP BY 1 
-- ORDER BY 1 
select  count(distinct c.csn) as contact_id 
        -- , a.row_id as asset_row_id 
        -- , p.prod_cd as asset_type 
        -- , p.name product_name 
        -- , a.status_cd 
from gms.s_contact as c 
     
    inner join gms.s_asset as a 
        on a.owner_con_id = c.row_id 
         
    inner join gms.s_prod_int as p 
        on a.prod_id = p.row_id 
     
    left anti join campaign_data.aa_dmc2314_simplyenergydq_20210303 cam 
    on cast(cam.reward_plan_card_id as string)=c.csn 
   
 
where 1=1 
    and p.name IN ('NB_Simply_Energy_Blue_Offer_0320') 
    and to_date(date_add(a.end_dt,1)) BETWEEN '2021-03-01' and '2021-06-09' 
    and a.status_cd='Active' 
select count(distinct reward_plan_card_id) from campaign_data.aa_dmc2314_simplyenergydq_20210303 