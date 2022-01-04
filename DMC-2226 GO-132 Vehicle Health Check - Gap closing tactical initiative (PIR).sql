/* 
 
Notes: 
1. s_evt_act.sra_sr_id = s_srv_req.row_id 
2. x_bus_type has "PATROL", "NULL", "AUTOELEC", "OTHER", "CARELEC", "BATTERY", "TOW" 
3. await ring patrol in "resolution_cd" means that a job was hold in CAD for 14 days 
    in case the member phoned back 
4. cad_num = "20200115002197" --> flat battery, await ring battery 
5. cad_num = "20200107002495" --> FLPVE, business members 
6. background of CAD job process 
    --> receive time, the first time when someone called IVR and asked for a job. If there are multiple jobs under one SR number, it would be the earliest one.  
    --> receipt time, sc1: the first time when someone called IVR and asked for a job; sc2: if the IVR called, and the job didn't complete, receipt time will be different from the first time called 
    --> allocation time, the time when a job is allocated to a service provider (in system) 
    --> despatch time, the time when a job and its info sent to the service provider's terminal 
    --> acknowledge time, the time when the service provider clicks the acknowledge button in the terminal 
    --> enroute time, the time when the service provider clicks the button indicating he / she is going to the destination 
    --> at scene time, the time when the service provider is on the spot 
    --> completion time, the time when the service provider completes the job 
 
*/ 
 
with job_mapping as ( 
    select distinct  
        s.x_bus_type as job_type 
        , s.sr_category_cd as bkdn_cd 
        , s.root_cause_cd 
    from gms.s_srv_req as s 
    where 
        1=1 
        and s.x_bulk_source = "CAD" 
) 
 
select distinct 
    srx.attrib_04 as cad_num 
    , s.sr_num -- service number 
    , s.x_bus_type as job_type 
    , s.sr_subtype_cd as job_subtype 
    , s.sr_category_cd as bkdn_cd 
    , s.root_cause_cd as bkdn_reason 
    , s.sr_prio_cd as job_priority 
    , s.sr_stat_id as job_status 
    , s.x_bulk_source as source 
    , s.resolution_cd -- full name of completion cd  
    , s.asset_id 
    , s.ou_addr_id 
    , s.per_addr_id 
    , s.cst_ou_id -- member's organisation id 
    , s.cst_con_id -- member contact id 
    , srx.attrib_27 as receive_dt 
    , srx.attrib_28 as receipt_dt 
    , srx.attrib_26 as ack_dt -- accurate 
    , srx.attrib_13 as enroute_dt -- accurate 
    , srx.attrib_12 as at_scene_dt -- accurate 
    , srx.attrib_30 as completion_dt -- accurate 
from gms.s_srv_req_x as srx  
    inner join gms.s_srv_req as s 
    on srx.row_id = s.row_id 
    and srx.attrib_04 is not null 
where 
    1=1 
    --and to_date(s.created) >= "2020-01-01" 
    and s.x_bulk_source = "CAD"  
    and s.x_bus_type = "PATROL"  
   -- and srx.attrib_04 = "20170109002755"  -- for testing purpose 
 
 
 
 
 
 
/* 
for comparison between cad and gms in bda 
 
*/ 
 
-- with gms as ( 
 
-- select  
--     srx.attrib_04 as cad_num 
--     , s.sr_num -- service number 
--     , s.x_bus_type as job_type 
--     , s.sr_subtype_cd as job_subtype 
--     , s.sr_category_cd as bkdn_cd 
--     , s.root_cause_cd as bkdn_reason 
--     , s.sr_prio_cd as job_priority 
--     , s.sr_stat_id as job_status 
--     , s.x_bulk_source as source 
--     , s.resolution_cd -- full name of completion cd  
--     , s.asset_id 
--     , s.ou_addr_id 
--     , s.per_addr_id 
--     , s.cst_ou_id -- member's organisation id 
--     , s.cst_con_id -- member contact id 
--     , srx.attrib_27 as receive_dt 
--     , srx.attrib_28 as receipt_dt 
--     , srx.attrib_26 as ack_dt -- accurate 
--     , srx.attrib_13 as enroute_dt -- accurate 
--     , srx.attrib_12 as at_scene_dt -- accurate 
--     , srx.attrib_30 as completion_dt -- accurate 
--     , row_number() over (partition by srx.attrib_04 order by srx.attrib_28) as index 
-- from gms.s_srv_req_x as srx  
--     inner join gms.s_srv_req as s 
--     on srx.row_id = s.row_id 
--     and srx.attrib_04 is not null 
-- where 
--     1=1 
--     --and to_date(s.created) >= "2020-01-01" 
--     and s.x_bulk_source = "CAD"  
--     -- and s.x_bus_type = "PATROL"  
--     -- and srx.attrib_04 = "20170109002755"  -- for testing purpose 
-- ), cad as ( 
     
--     select job_number 
--         ,  breakdown_code_1 
--         , recieved_time 
--         , activation_time 
--         , allocation_time 
--         , acknowledge_time 
--         , enroute_time 
--         , at_scene_time 
--         , completion_time  
--         , row_number() over (partition by job_number order by activation_time) as index 
--     from cad.cad_jobs 
--     where 
--         1=1 
--         -- and job_number = "20170109002755" 
 
-- ), comp as ( 
 
-- select gms.cad_num 
--     , gms.job_type 
--     , hours_add(cad.allocation_time, 11) as cad_alc 
--     , gms.receipt_dt as gms_rpt 
--     , unix_timestamp(hours_add(cad.allocation_time,11)) - unix_timestamp(gms.receipt_dt) as alc_diff-- down to seconds 
--     , cad.completion_time as cad_cpl 
--     , gms.completion_dt as gms_cpl 
--     , unix_timestamp(cad.completion_time) - unix_timestamp(gms.completion_dt) as cpl_diff -- down to seconds 
-- from cad 
--     inner join gms 
--     on cast(gms.cad_num as string) = cast(cad.job_number as string) 
--     and cad.index = gms.index 
-- where 
--     1=1 
--     and to_date(gms.receipt_dt) between "2019-11-01" and "2020-03-31" -- only AEST for comparison 
-- ) 
 
-- select case when abs(cpl_diff) <= 3600 then "<=1hr" else ">1hr" end as diff 
--     , comp.job_type 
--     , count(distinct cad_num) as vol 
-- from comp 
-- group by 1,2 
-- order by 2,1 
 
SELECT * from cad.job 
where 1=1 
--and jobb_number='20210113000840' 
--and jobb_health_check_requested='Y' 
--and jobb_member_number='990353369' 
-- limit 100; 
 
/* 
 
Notes: 
1. s_evt_act.sra_sr_id = s_srv_req.row_id 
2. x_bus_type has "PATROL", "NULL", "AUTOELEC", "OTHER", "CARELEC", "BATTERY", "TOW" 
3. await ring patrol in "resolution_cd" means that a job was hold in CAD for 14 days 
    in case the member phoned back 
4. cad_num = "20200115002197" --> flat battery, await ring battery 
5. cad_num = "20200107002495" --> FLPVE, business members 
6. background of CAD job process 
    --> receive time, the first time when someone called IVR and asked for a job. If there are multiple jobs under one SR number, it would be the earliest one.  
    --> receipt time, sc1: the first time when someone called IVR and asked for a job; sc2: if the IVR called, and the job didn't complete, receipt time will be different from the first time called 
    --> allocation time, the time when a job is allocated to a service provider (in system) 
    --> despatch time, the time when a job and its info sent to the service provider's terminal 
    --> acknowledge time, the time when the service provider clicks the acknowledge button in the terminal 
    --> enroute time, the time when the service provider clicks the button indicating he / she is going to the destination 
    --> at scene time, the time when the service provider is on the spot 
    --> completion time, the time when the service provider completes the job 
 
*/ 
with job_mapping as ( 
    select distinct  
        s.x_bus_type as job_type 
        , s.sr_category_cd as bkdn_cd 
        , s.root_cause_cd 
    from gms.s_srv_req as s 
    where 
        1=1 
        and s.x_bulk_source = "CAD" 
) 
 
select distinct 
    srx.attrib_04 as cad_num 
    , s.sr_num -- service number 
    , s.x_bus_type as job_type 
    , s.sr_subtype_cd as job_subtype 
    , s.sr_category_cd as bkdn_cd 
    , s.root_cause_cd as bkdn_reason 
    , s.sr_prio_cd as job_priority 
    , s.sr_stat_id as job_status 
    , s.x_bulk_source as source 
    , s.resolution_cd -- full name of completion cd  
    , s.asset_id 
    , s.ou_addr_id 
    , s.per_addr_id 
    , s.cst_ou_id as org_id -- member's organisation id 
    , s.cst_con_id as contact_id-- member contact id 
    , srx.attrib_27 as receive_dt 
    , srx.attrib_28 as receipt_dt 
    , srx.attrib_26 as ack_dt -- accurate 
    , srx.attrib_13 as enroute_dt -- accurate 
    , srx.attrib_12 as at_scene_dt -- accurate 
    , srx.attrib_30 as completion_dt -- accurate 
from gms.s_srv_req_x as srx  
    inner join gms.s_srv_req as s 
    on srx.row_id = s.row_id 
    and srx.attrib_04 is not null 
where 
    1=1 
    --and to_date(s.created) >= "2020-01-01" 
    and s.x_bulk_source = "CAD"  
    and s.x_bus_type = "PATROL"  
    and s.sr_num='1-31797537635' 
   -- and srx.attrib_04 = "20170109002755"  -- for testing purpose 
with job_mapping as ( 
    select distinct  
        s.x_bus_type as job_type 
        , s.sr_category_cd as bkdn_cd 
        , s.root_cause_cd 
    from gms.s_srv_req as s 
    where 
        1=1 
        and s.x_bulk_source = "CAD" 
) 
 
select    year(to_date(srx.attrib_27)) year_ 
        , month(to_date(srx.attrib_27)) month_  
        , count(distinct s.sr_num) 
     
from gms.s_srv_req_x as srx  
    inner join gms.s_srv_req as s 
    on srx.row_id = s.row_id 
    and srx.attrib_04 is not null 
where 
    1=1 
    and to_date(srx.attrib_27) between "2019-01-01" and '2021-03-01' 
    and s.x_bulk_source = "CAD"  
    and s.x_bus_type = "PATROL"  
    --and s.sr_num='1-31797537635' 
 
group by 1,2 
order by 1,2 
with job_mapping as ( 
    select distinct  
        s.x_bus_type as job_type 
        , s.sr_category_cd as bkdn_cd 
        , s.root_cause_cd 
    from gms.s_srv_req as s 
    where 
        1=1 
        and s.x_bulk_source = "CAD" 
) 
 
select distinct 
     s.x_bus_type as job_type 
    , s.sr_subtype_cd as job_subtype 
    , s.sr_category_cd as bkdn_cd 
    , s.root_cause_cd as bkdn_reason 
from gms.s_srv_req_x as srx  
    inner join gms.s_srv_req as s 
    on srx.row_id = s.row_id 
    and srx.attrib_04 is not null 
where 
    1=1 
    --and to_date(s.created) >= "2020-01-01" 
    and s.x_bulk_source = "CAD"  
    and s.x_bus_type = "PATROL"  
    --and s.sr_num='1-31797537635' 
   -- and srx.attrib_04 = "20170109002755"  -- for testing purpose 
with job_mapping as ( 
    select distinct  
        s.x_bus_type as job_type 
        , s.sr_category_cd as bkdn_cd 
        , s.root_cause_cd 
    from gms.s_srv_req as s 
    where 
        1=1 
        and s.x_bulk_source = "CAD" 
) 
 
select case when lower(s.root_cause_cd) rlike 'batter' then 'Battery Related' 
            when s.root_cause_cd='VEHICLE HEALTH CHECK' then 'VEHICLE HEALTH CHECK' 
            else 'Other' end Call_Out 
     
from gms.s_srv_req_x as srx  
    inner join gms.s_srv_req as s 
    on srx.row_id = s.row_id 
    and srx.attrib_04 is not null 
where 
    1=1 
    and to_date(s.created) between "2020-01-01" and '2021-03-01' 
    and s.x_bulk_source = "CAD"  
    and s.x_bus_type = "PATROL"  
    --and s.sr_category_cd='845' 
    and s.sr_subtype_cd='VEHICLE HEALTH CHECK' 
    --and s.sr_num='1-31797537635' 
 
-- group by 1,2 
-- order by 1,2 
-- with job_mapping as ( 
--     select distinct  
--         s.x_bus_type as job_type 
--         , s.sr_category_cd as bkdn_cd 
--         , s.root_cause_cd 
--     from gms.s_srv_req as s 
--     where 
--         1=1 
--         and s.x_bulk_source = "CAD" 
--         and s.x_bus_type = "PATROL"  
-- ) 
 
select distinct 
    srx.attrib_04 as cad_num 
    , s.sr_num -- service number 
    , s.x_bus_type as job_type 
    , s.sr_subtype_cd as job_subtype 
    , s.sr_category_cd as bkdn_cd 
    , s.root_cause_cd as bkdn_reason 
    , s.sr_prio_cd as job_priority 
    , s.sr_stat_id as job_status 
    , s.x_bulk_source as source 
    , s.resolution_cd -- full name of completion cd  
    , s.asset_id 
    , s.ou_addr_id 
    , s.per_addr_id 
    , s.cst_ou_id -- member's organisation id 
    , s.cst_con_id -- member contact id 
    , srx.attrib_27 as receive_dt 
    , srx.attrib_28 as receipt_dt 
    , srx.attrib_26 as ack_dt -- accurate 
    , srx.attrib_13 as enroute_dt -- accurate 
    , srx.attrib_12 as at_scene_dt -- accurate 
    , srx.attrib_30 as completion_dt -- accurate 
 
from gms.s_srv_req_x as srx  
    inner join gms.s_srv_req as s 
    on srx.row_id = s.row_id 
    and srx.attrib_04 is not null 
where 
    1=1 
    --and to_date(s.created) >= "2020-01-01" 
    and s.x_bulk_source = "CAD"  
    and s.x_bus_type = "PATROL"  
    and s.root_cause_cd='VEHICLE HEALTH CHECK' 
    --and s.sr_num='1-31797537635' 
-- with job_mapping as ( 
--     select distinct  
--         s.x_bus_type as job_type 
--         , s.sr_category_cd as bkdn_cd 
--         , s.root_cause_cd 
--     from gms.s_srv_req as s 
--     where 
--         1=1 
--         and s.x_bulk_source = "CAD" 
-- ) 
 
select    year(to_date(srx.attrib_27)) year_ 
        , month(to_date(srx.attrib_27)) month_  
        , count(distinct s.sr_num) 
     
from gms.s_srv_req_x as srx  
    inner join gms.s_srv_req as s 
    on srx.row_id = s.row_id 
    and srx.attrib_04 is not null 
where 
    1=1 
    and to_date(srx.attrib_27) between "2019-01-01" and '2021-03-01' 
    and s.x_bulk_source = "CAD"  
    and s.x_bus_type = "PATROL"  
    and s.root_cause_cd='VEHICLE HEALTH CHECK' 
    --and s.sr_num='1-31797537635' 
 
group by 1,2 
order by 1,2 
select  case when s.root_cause_cd='VEHICLE HEALTH CHECK' then 'VEHICLE HEALTH CHECK' 
            when lower(s.root_cause_cd) rlike 'batter' then 'Battery Related' 
            else 'Other' end Call_Out 
        , year(to_date(srx.attrib_27)) year_ 
        , month(to_date(srx.attrib_27)) month_  
        , count(distinct s.sr_num) 
     
from gms.s_srv_req_x as srx  
    inner join gms.s_srv_req as s 
    on srx.row_id = s.row_id 
    and srx.attrib_04 is not null 
 
inner join gms.s_contact c 
on c.row_id=s.cst_con_id 
 
-- inner join m4m.return_feed_header h 
-- on h.member_number=c.csn 
-- and h.partner='NRMA Batteries' 
 
where 
    1=1 
    and to_date(srx.attrib_27) between "2019-01-01" and '2021-03-01' 
    and s.x_bulk_source = "CAD"  
    and s.x_bus_type = "PATROL"  
    --and s.root_cause_cd='VEHICLE HEALTH CHECK' 
    --and s.sr_num='1-31797537635' 
 
group by 1,2,3 
order by 1,2,3 
select case when p.name rlike 'PP' then 'Premium Plus' 
            when p.name rlike 'PC' then 'Premium Care' 
            when p.name rlike 'CC' then 'Classic Care' 
            when p.name rlike 'NB' then 'NRMA Blue' 
            else p.name end Product 
        , count(distinct s.sr_num) 
         
from gms.s_srv_req_x as srx  
 
inner join gms.s_srv_req as s 
    on srx.row_id = s.row_id 
    and srx.attrib_04 is not null 
 
inner join gms.s_contact c 
on c.row_id=s.cst_con_id 
 
-- inner join m4m.return_feed_header h 
-- on h.member_number=c.csn 
-- and h.partner='NRMA Batteries' 
 
inner join gms.s_asset as a 
        on a.owner_con_id = c.row_id 
         
inner join gms.s_prod_int as p 
        on a.prod_id = p.row_id 
 
where 
    1=1 
    and to_date(srx.attrib_27) between "2020-11-19" and '2021-01-31' 
    and s.x_bulk_source = "CAD"  
    and s.x_bus_type = "PATROL"  
    and s.root_cause_cd='VEHICLE HEALTH CHECK' 
    and a.status_cd='Active' 
    and p.prod_cd='Promotion' 
    --and p.prod_cd='Product' 
    --1974 
    --872 
group by 1 
order by 1 
select  distinct c.csn as contact_id 
        , c.csn as member_number 
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
    and a.status_cd='Active' 
    --and p.prod_cd='Promotion' 
    and p.prod_cd='Product' 
select p.name  Product 
        , count(distinct s.sr_num) 
         
from gms.s_srv_req_x as srx  
 
inner join gms.s_srv_req as s 
    on srx.row_id = s.row_id 
    and srx.attrib_04 is not null 
 
inner join gms.s_contact c 
on c.row_id=s.cst_con_id 
 
-- inner join m4m.return_feed_header h 
-- on h.member_number=c.csn 
-- and h.partner='NRMA Batteries' 
 
inner join gms.s_asset as a 
        on a.owner_con_id = c.row_id 
         
inner join gms.s_prod_int as p 
        on a.prod_id = p.row_id 
 
where 
    1=1 
    and to_date(srx.attrib_27) between "2020-11-19" and '2021-01-31' 
    and s.x_bulk_source = "CAD"  
    and s.x_bus_type = "PATROL"  
    and s.root_cause_cd='VEHICLE HEALTH CHECK' 
    and a.status_cd='Active' 
    --and p.prod_cd='Promotion' 
    and p.prod_cd='Product' 
    --1974 
    --872 
group by 1 
order by 1 
select cx.attrib_55 
        --c.cust_value_cd 
        , count(distinct s.sr_num) 
         
from gms.s_srv_req_x as srx  
 
inner join gms.s_srv_req as s 
    on srx.row_id = s.row_id 
    and srx.attrib_04 is not null 
 
inner join gms.s_contact c 
on c.row_id=s.cst_con_id 
 
inner join gms.s_contact_x as cx  
on c.row_id = cx.par_row_id 
 
 
-- inner join m4m.return_feed_header h 
-- on h.member_number=c.csn 
-- and h.partner='NRMA Batteries' 
 
inner join gms.s_asset as a 
        on a.owner_con_id = c.row_id 
         
inner join gms.s_prod_int as p 
        on a.prod_id = p.row_id 
 
where 
    1=1 
    and to_date(srx.attrib_27) between "2020-11-19" and '2021-01-31' 
    and s.x_bulk_source = "CAD"  
    and s.x_bus_type = "PATROL"  
    and s.root_cause_cd='VEHICLE HEALTH CHECK' 
    and a.status_cd='Active' 
    --and p.prod_cd='Promotion' 
    and p.prod_cd='Product' 
    --1974 
    --872 
group by 1 
order by 1 
select CASE  
                        WHEN  
                         (ad.zipcode >= '2000' and ad.zipcode <= '2082' or 
                          ad.zipcode >= '2084' and ad.zipcode <= '2234' or  
                          ad.zipcode >= '2555' and ad.zipcode <= '2574' or  
                          ad.zipcode >= '2745' and ad.zipcode <= '2770' or  
                          ad.zipcode >= '2775' and ad.zipcode <= '2775') 
                          AND (upper(ad.country) = 'AUSTRALIA' or upper(ad.country) = 'AU') 
                          THEN 'METROPOLITAN' 
                        WHEN  
                         (ad.zipcode >= '2083' and ad.zipcode <= '2083' or  
                          ad.zipcode >= '2250' and ad.zipcode <= '2338' or  
                          ad.zipcode >= '2415' and ad.zipcode <= '2423' or  
                          ad.zipcode >= '2425' and ad.zipcode <= '2425' or  
                          ad.zipcode >= '2428' and ad.zipcode <= '2428' or  
                          ad.zipcode >= '2500' and ad.zipcode <= '2535' or  
                          ad.zipcode >= '2538' and ad.zipcode <= '2541' or  
                          ad.zipcode >= '2575' and ad.zipcode <= '2578' or  
                          ad.zipcode >= '2600' and ad.zipcode <= '2617' or  
                          ad.zipcode >= '2773' and ad.zipcode <= '2774' or  
                          ad.zipcode >= '2776' and ad.zipcode <= '2786' or  
                          ad.zipcode >= '2900' and ad.zipcode <= '2914') 
                          AND (upper(ad.country) = 'AUSTRALIA' or upper(ad.country) = 'AU')  
                          THEN  'REGIONAL' 
                        WHEN  
                          (ad.zipcode >= '2339' and ad.zipcode <= '2411' or  
                          ad.zipcode >= '2424' and ad.zipcode <= '2424' or  
                          ad.zipcode >= '2426' and ad.zipcode <= '2427' or  
                          ad.zipcode >= '2429' and ad.zipcode <= '2490' or  
                          ad.zipcode >= '2536' and ad.zipcode <= '2537' or  
                          ad.zipcode >= '2545' and ad.zipcode <= '2551' or  
                          ad.zipcode >= '2579' and ad.zipcode <= '2594' or  
                          ad.zipcode >= '2618' and ad.zipcode <= '2739' or  
                          ad.zipcode >= '2787' and ad.zipcode <= '2898' or  
                          ad.zipcode >= '6798' and ad.zipcode <= '6799') 
                          AND (upper(ad.country) = 'AUSTRALIA' or upper(ad.country) = 'AU')  
                          THEN  'RURAL' 
                        WHEN  
                          (ad.zipcode >= '0800' and ad.zipcode <= '0886' or  
                          ad.zipcode >= '3000' and ad.zipcode <= '6770' or  
                          ad.zipcode >= '6907' and ad.zipcode <= '7470' or  
                          ad.zipcode >= '7471' ) 
                          AND (upper(ad.country) = 'AUSTRALIA' or upper(ad.country) = 'AU')  
                          THEN  'INTERSTATE' 
                        ELSE 'UNKNOWN' 
                        END AS area 
        --c.cust_value_cd 
        , count(distinct s.sr_num) 
         
from gms.s_srv_req_x as srx  
 
inner join gms.s_srv_req as s 
    on srx.row_id = s.row_id 
    and srx.attrib_04 is not null 
 
inner join gms.s_contact c 
on c.row_id=s.cst_con_id 
 
inner join gms.s_contact_x as cx  
on c.row_id = cx.par_row_id 
 
inner join gms.s_addr_per as ad 
on c.pr_per_addr_id = ad.row_id 
 
 
inner join m4m.return_feed_header h 
on h.member_number=c.csn 
and h.partner='NRMA Batteries' 
 
inner join gms.s_asset as a 
        on a.owner_con_id = c.row_id 
         
inner join gms.s_prod_int as p 
        on a.prod_id = p.row_id 
 
where 
    1=1 
    and to_date(srx.attrib_27) between "2020-11-19" and '2021-01-31' 
    and s.x_bulk_source = "CAD"  
    and s.x_bus_type = "PATROL"  
    and s.root_cause_cd='VEHICLE HEALTH CHECK' 
    and a.status_cd='Active' 
    --and p.prod_cd='Promotion' 
    and p.prod_cd='Product' 
    --1974 
    --872 
group by 1 
order by 1 
-- with job_mapping as ( 
--     select distinct  
--         s.x_bus_type as job_type 
--         , s.sr_category_cd as bkdn_cd 
--         , s.root_cause_cd 
--     from gms.s_srv_req as s 
--     where 
--         1=1 
--         and s.x_bulk_source = "CAD" 
-- ) 
 
select    year(to_date(srx.attrib_27)) year_ 
        , month(to_date(srx.attrib_27)) month_  
        , count(distinct s.sr_num) 
     
from gms.s_srv_req_x as srx  
    inner join gms.s_srv_req as s 
    on srx.row_id = s.row_id 
    and srx.attrib_04 is not null 
where 
    1=1 
    and to_date(srx.attrib_27) between "2019-01-01" and '2021-03-01' 
    and s.x_bulk_source = "CAD"  
    and s.x_bus_type = "PATROL"  
    and s.root_cause_cd='VEHICLE HEALTH CHECK' 
    --and s.sr_num='1-31797537635' 
 
group by 1,2 
order by 1,2 
select   case when (p.name rlike 'PP' or p.name rlike 'PC') then 'Premium Membership' 
            -- when p.name rlike 'CC' then 'Classic Membership' 
            -- when p.name rlike 'NB' then 'NRMA Blue' 
            else 'Others' end  Product 
        , year(to_date(srx.attrib_27)) year_ 
        , month(to_date(srx.attrib_27)) month_  
        , count(distinct s.sr_num) 
         
from gms.s_srv_req_x as srx  
 
inner join gms.s_srv_req as s 
    on srx.row_id = s.row_id 
    and srx.attrib_04 is not null 
 
inner join gms.s_contact c 
on c.row_id=s.cst_con_id 
 
-- inner join m4m.return_feed_header h 
-- on h.member_number=c.csn 
-- and h.partner='NRMA Batteries' 
 
inner join gms.s_asset as a 
        on a.owner_con_id = c.row_id 
         
inner join gms.s_prod_int as p 
        on a.prod_id = p.row_id 
 
where 
    1=1 
    and to_date(srx.attrib_27) between "2019-01-01" and '2021-03-01' 
    and s.x_bulk_source = "CAD"  
    and s.x_bus_type = "PATROL"  
    and s.root_cause_cd='VEHICLE HEALTH CHECK' 
    and a.status_cd='Active' 
    and p.prod_cd='Promotion' 
    --and p.prod_cd='Product' 
    --1974 
    --872 
group by 1,2,3 
order by 1,2,3