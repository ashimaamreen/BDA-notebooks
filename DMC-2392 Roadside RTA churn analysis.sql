--with job_mapping as ( 
    select distinct  
        s.x_bus_type as job_type 
        , s.sr_category_cd as bkdn_cd 
        , s.root_cause_cd 
    from gms.s_srv_req as s 
    where 
        1=1 
        and s.x_bulk_source = "CAD" 
--) 
with all_tow as ( 
 
select distinct 
    srx.attrib_04 as cad_num 
    , s.sr_num -- service number 
    , s.x_bus_type as job_type 
    , s.sr_subtype_cd as job_subtype 
    , s.sr_category_cd as bkdn_cd 
    --, s.root_cause_cd as bkdn_reason 
    , s.sr_prio_cd as job_priority 
    , s.sr_stat_id as job_status 
    --, s.x_bulk_source as source 
    , s.resolution_cd -- full name of completion cd  
    , s.asset_id 
    , s.cst_ou_id member_org_id -- member's organisation id 
    , s.cst_con_id member_contact_id-- member contact id 
    , srx.attrib_27 as receive_dt 
    , srx.attrib_12 as at_scene_dt -- accurate 
    , srx.attrib_30 as completion_dt -- accurate 
    , (UNIX_TIMESTAMP(srx.attrib_12)-UNIX_TIMESTAMP(srx.attrib_27))/60 as tow_rta 
    , b.order_end_dt 
    , b.order_start_dt 
    , b.order_id 
    , b.renewed_order_id 
    -- , b.order_id 
    -- , b.nrp_prod_name 
    -- , case when b.renewed_order_id is null then 'Non-renewed' else 'Renewed' end renewal  
    --, min(case when s.x_bus_type = 'TOW' then (UNIX_TIMESTAMP(srx.attrib_12)-UNIX_TIMESTAMP(srx.attrib_27))/60 else NULL end )   
 
from gms.s_srv_req_x as srx  
     
inner join gms.s_srv_req as s 
on srx.row_id = s.row_id 
and srx.attrib_04 is not null 
 
inner join sandpit.renewal_base b 
on  b.membernumber = srx.attrib_03  
and b.vehicle_rego = srx.attrib_05 
--and year( srx.attrib_27)=year(b.order_end_dt) 
 
 
 
where 
    1=1 
    and s.x_bulk_source = "CAD"  
    and s.x_bus_type = "TOW"  
    and to_date(order_end_dt) between '2020-01-01' and '2020-12-31' 
    and s.sr_stat_id='COMPLETED' 
    and srx.attrib_27 between b.order_start_dt and b.order_end_dt 
    ) 
     
--99664 
select case  
        when tow_rta < 60 then "0-60" 
        when tow_rta < 120 then "61-120" 
        when tow_rta >= 120 then "120+"  
        else "No Tow"  
            END         as tow_rta_band 
        ,count(*)  
from all_tow 
             
--where tow_rta<=60 
 
group by 1 
order by 1 
 
with all_tow as ( 
 
select distinct 
    srx.attrib_04 as cad_num 
    , srx.attrib_03 as member_number 
    , srx.attrib_05 as rego 
    , s.sr_num -- service number 
    , s.x_bus_type as job_type 
    , s.sr_subtype_cd as job_subtype 
    , s.sr_category_cd as bkdn_cd 
    --, s.root_cause_cd as bkdn_reason 
    , s.sr_prio_cd as job_priority 
    , s.sr_stat_id as job_status 
    --, s.x_bulk_source as source 
    , s.resolution_cd -- full name of completion cd  
    , s.asset_id as asset_id_tow 
    , s.cst_ou_id member_org_id -- member's organisation id 
    , s.cst_con_id member_contact_id-- member contact id 
    , srx.attrib_27 as receive_dt 
    , srx.attrib_12 as at_scene_dt -- accurate 
    , srx.attrib_30 as completion_dt -- accurate 
    , (UNIX_TIMESTAMP(srx.attrib_12)-UNIX_TIMESTAMP(srx.attrib_27))/60 as tow_rta 
    , b.order_id 
    , b.asset_start_dt 
    , b.asset_end_dt 
    , b.order_start_dt 
    , b.order_end_dt 
    , b.nrp_prod_name 
    , b.renewed_order_id 
    , b.asset_id 
 
from gms.s_srv_req_x as srx  
     
inner join gms.s_srv_req as s 
on srx.row_id = s.row_id 
and srx.attrib_04 is not null 
 
inner join sandpit.renewal_base b 
on b.membernumber = srx.attrib_03  
--b.asset_id=s.asset_id 
--and b.membernumber = srx.attrib_03  
and b.vehicle_rego = srx.attrib_05 
 
where 
    1=1 
    and s.x_bulk_source = "CAD" 
    and s.x_bus_type = "TOW"  
    and to_date(b.order_end_dt) between '2020-01-01' and '2020-12-31' 
    and srx.attrib_27>'2019-01-01' 
    and s.sr_stat_id='COMPLETED' 
    and srx.attrib_27 between b.order_start_dt and b.order_end_dt 
) 
--113363 
select * from all_tow  
where cad_num='20200717000053' 
--select 
with all_tow as ( 
 
select distinct 
    srx.attrib_04 as cad_num 
    , s.sr_num -- service number 
    , s.x_bus_type as job_type 
    , s.sr_subtype_cd as job_subtype 
    , s.sr_category_cd as bkdn_cd 
    --, s.root_cause_cd as bkdn_reason 
    , s.sr_prio_cd as job_priority 
    , s.sr_stat_id as job_status 
    --, s.x_bulk_source as source 
    , s.resolution_cd -- full name of completion cd  
    , s.asset_id 
    , s.cst_ou_id member_org_id -- member's organisation id 
    , s.cst_con_id member_contact_id-- member contact id 
    , srx.attrib_27 as receive_dt 
    , srx.attrib_12 as at_scene_dt -- accurate 
    , srx.attrib_30 as completion_dt -- accurate 
    , (UNIX_TIMESTAMP(srx.attrib_12)-UNIX_TIMESTAMP(srx.attrib_27))/60 as tow_rta 
    , b.order_end_dt 
    , b.order_start_dt 
    , b.order_id 
    , b.renewed_order_id 
    -- , b.order_id 
    -- , b.nrp_prod_name 
    -- , case when b.renewed_order_id is null then 'Non-renewed' else 'Renewed' end renewal  
    --, min(case when s.x_bus_type = 'TOW' then (UNIX_TIMESTAMP(srx.attrib_12)-UNIX_TIMESTAMP(srx.attrib_27))/60 else NULL end )   
 
from gms.s_srv_req_x as srx  
     
inner join gms.s_srv_req as s 
on srx.row_id = s.row_id 
and srx.attrib_04 is not null 
 
inner join sandpit.renewal_base b 
on  b.membernumber = srx.attrib_03  
and b.vehicle_rego = srx.attrib_05 
--and year( srx.attrib_27)=year(b.order_end_dt) 
 
 
 
where 
    1=1 
    and s.x_bulk_source = "CAD"  
    and s.x_bus_type = "TOW"  
    and to_date(order_end_dt) between '2020-01-01' and '2020-12-31' 
    and s.sr_stat_id='COMPLETED' 
    and srx.attrib_27 between b.order_start_dt and b.order_end_dt 
    ) 
     
--99664 
select case  
        when tow_rta < 60 then "0-60" 
        when tow_rta < 120 then "61-120" 
        when tow_rta >= 120 then "120+"  
        else "No Tow"  
            END         as tow_rta_band, 
        case when renewed_order_id is null then 0 else 1 end renewed, 
        count(*) 
from all_tow 
             
--where tow_rta<=60 
 
group by 1,2 
order by 1,2 
 
with all_tow as ( 
 
select distinct 
    srx.attrib_04 as cad_num 
    , s.sr_num -- service number 
    , s.x_bus_type as job_type 
    , s.sr_subtype_cd as job_subtype 
    , s.sr_category_cd as bkdn_cd 
    --, s.root_cause_cd as bkdn_reason 
    , s.sr_prio_cd as job_priority 
    , s.sr_stat_id as job_status 
    --, s.x_bulk_source as source 
    , s.resolution_cd -- full name of completion cd  
    , s.asset_id 
    , s.cst_ou_id member_org_id -- member's organisation id 
    , s.cst_con_id member_contact_id-- member contact id 
    , srx.attrib_27 as receive_dt 
    , srx.attrib_12 as at_scene_dt -- accurate 
    , srx.attrib_30 as completion_dt -- accurate 
    , (UNIX_TIMESTAMP(srx.attrib_12)-UNIX_TIMESTAMP(srx.attrib_27))/60 as tow_rta 
    , b.order_end_dt 
    , b.order_start_dt 
    , b.order_id 
    , b.renewed_order_id 
    -- , b.order_id 
    -- , b.nrp_prod_name 
    -- , case when b.renewed_order_id is null then 'Non-renewed' else 'Renewed' end renewal  
    --, min(case when s.x_bus_type = 'TOW' then (UNIX_TIMESTAMP(srx.attrib_12)-UNIX_TIMESTAMP(srx.attrib_27))/60 else NULL end )   
 
from gms.s_srv_req_x as srx  
     
inner join gms.s_srv_req as s 
on srx.row_id = s.row_id 
and srx.attrib_04 is not null 
 
inner join sandpit.renewal_base b 
on  b.membernumber = srx.attrib_03  
and b.vehicle_rego = srx.attrib_05 
--and year( srx.attrib_27)=year(b.order_end_dt) 
 
 
 
where 
    1=1 
    and s.x_bulk_source = "CAD"  
    and s.x_bus_type = "TOW"  
    and to_date(order_end_dt) between '2020-01-01' and '2020-12-31' 
    and s.sr_stat_id='COMPLETED' 
    --and srx.attrib_27 between b.order_start_dt and b.order_end_dt 
    ) 
     
--99664 
select case  
        when tow_rta < 60 then "0-60" 
        when tow_rta < 120 then "61-120" 
        when tow_rta >= 120 then "120+"  
        else "No Tow"  
            END         as tow_rta_band, 
        case when renewed_order_id is null then 0 else 1 end renewed, 
        count(*) 
from all_tow 
             
--where tow_rta<=60 
 
group by 1,2 
order by 1,2 
 
spark.sql(''' 
    select cad_tow_rta_band 
            , count(*) as nr 
            , avg(sub_renewed) as sub_renewed  
    from sandpit.model_df  
    where year(order_end_dt) = '2020'  
    group by cad_tow_rta_band 
''').show(100) 
 
from  sandpit.renewal_base b  
    inner join gms.s_srv_req_x as srx  
        on  b.membernumber = srx.attrib_03  
        and b.vehicle_rego = srx.attrib_05  
    inner join gms.s_srv_req as s 
        on srx.row_id = s.row_id 
        and srx.attrib_04 is not null    
    -- DISTANCE Tagging 
    LEFT JOIN sandpit.poa_2_sa4 sa4  
        on b.owner_postcode = sa4.poa_code_2016 
    left join sandpit.distance_cad_2_sa4 cadsa4 
        ON s.x_work_area  = cadsa4.cad_region 
        AND 1 = cadsa4.sa4_rank 
    left join sandpit.distance_sa4_2_sa4 dist 
        ON  sa4.sa4_code_2016    = dist.sa4_code16_x 
        AND cadsa4.sa4_code16 = dist.sa4_code16_y 
    LEFT JOIN sandpit.util_sa4_names sa4n 
        on cadsa4.sa4_code16 = sa4n.sa4_code 
    where 
        1=1 
        and s.x_bulk_source = "CAD"  
        and s.x_bus_type in ("PATROL", "AUTOELEC", "OTHER", "CARELEC", "BATTERY", "TOW") 
        and srx.attrib_27 between b.order_start_dt and b.order_end_dt  
        and membernumber is not null 
         
    group by  
            b.vehicle_rego 
        ,   b.membernumber            
        ,   b.integration_id         
        ,   b.order_id  
) 
WITH cad_df AS ( 
    select  
            b.vehicle_rego 
        ,   b.membernumber 
        ,   b.integration_id         
        ,   b.order_id  
        ,   sr_stat_id 
        -- Distance predictors 
        ,   max(cadsa4.sa4_code16)      as cad_sa4_code 
        ,   max(cadsa4.sa4_name16)      as cad_sa4_name 
        ,   max(sa4n.sa4_region)        as cad_sa4_region 
        ,   max(sa4n.sa4_metro_cat)     as cad_sa4_metro_cat 
        ,   max(dist.distance)          as cad_sa4_distance 
        ,   min((UNIX_TIMESTAMP(srx.attrib_12)-UNIX_TIMESTAMP(srx.attrib_27))/60  )               as cad_rta 
        ,   min(case when s.x_bus_type = 'TOW' then (UNIX_TIMESTAMP(srx.attrib_12)-UNIX_TIMESTAMP(srx.attrib_27))/60 else NULL end )                   as cad_tow_rta 
        ,   min(case when s.x_bus_type = 'PATROL' then (UNIX_TIMESTAMP(srx.attrib_12)-UNIX_TIMESTAMP(srx.attrib_27))/60 else NULL end )                as cad_patrol_rta 
        ,   min(case when s.x_bus_type = 'BATTERY' then (UNIX_TIMESTAMP(srx.attrib_12)-UNIX_TIMESTAMP(srx.attrib_27))/60 else NULL end )               as cad_battery_rta 
         
        -- Job summary 
        ,   count(*)                        as cad_jobs 
        ,   concat_ws('|' , srx.attrib_04)     as cad_num 
        ,   concat_ws('|' , srx.attrib_27)     as cad_receive_dt 
        ,   concat_ws('|' , srx.attrib_12)     as cad_arrival_dt 
        ,   concat_ws('|' , srx.attrib_30)     as cad_complete_dt 
        ,   concat_ws('|' , s.root_cause_cd)   as cad_root_cause 
        ,   concat_ws('|' , s.x_work_area)     as cad_job_region 
        -- attributing the jobs  
        ,   max(case when lower(concat(s.x_bus_type,s.sr_subtype_cd,s.root_cause_cd)) like '%battery%' then 1 else 0 end )           as cad_battery 
        ,   max(case when lower(concat(s.x_bus_type,s.sr_subtype_cd,s.root_cause_cd)) like '%alternator%' then 1 else 0 end )        as cad_alternator 
        ,   max(case when lower(concat(s.x_bus_type,s.sr_subtype_cd,s.root_cause_cd)) like '%tyre%' then 1 else 0 end )              as cad_tyre 
        ,   max(case when lower(concat(s.x_bus_type,s.sr_subtype_cd,s.root_cause_cd)) like '%transmission%' then 1 else 0 end )      as cad_transmission 
        ,   max(case when lower(concat(s.x_bus_type,s.sr_subtype_cd,s.root_cause_cd)) like '%brake%' then 1 else 0 end )             as cad_brake 
        ,   max(case when lower(concat(s.x_bus_type,s.sr_subtype_cd,s.root_cause_cd)) like '%lock%' then 1 else 0 end )              as cad_lockout 
        ,   max(case when lower(concat(s.x_bus_type,s.sr_subtype_cd,s.root_cause_cd)) like '%tow%' then 1 else 0 end )               as cad_tow 
     
    from  sandpit.renewal_base b  
     
    inner join gms.s_srv_req_x as srx  
        on  b.membernumber = srx.attrib_03  
        and b.vehicle_rego = srx.attrib_05  
     
    inner join gms.s_srv_req as s 
        on srx.row_id = s.row_id 
        and srx.attrib_04 is not null    
    -- DISTANCE Tagging 
     
    LEFT JOIN sandpit.poa_2_sa4 sa4  
        on b.owner_postcode = sa4.poa_code_2016 
     
    left join sandpit.distance_cad_2_sa4 cadsa4 
        ON s.x_work_area  = cadsa4.cad_region 
        AND 1 = cadsa4.sa4_rank 
    left join sandpit.distance_sa4_2_sa4 dist 
        ON  sa4.sa4_code_2016    = dist.sa4_code16_x 
        AND cadsa4.sa4_code16 = dist.sa4_code16_y 
     
    LEFT JOIN sandpit.util_sa4_names sa4n 
        on cadsa4.sa4_code16 = sa4n.sa4_code 
    where 
        1=1 
        and s.x_bulk_source = "CAD"  
        and s.x_bus_type in ("PATROL", "AUTOELEC", "OTHER", "CARELEC", "BATTERY", "TOW") 
        and srx.attrib_27 between b.order_start_dt and b.order_end_dt  
        and membernumber is not null 
         
    group by  
            b.vehicle_rego 
        ,   b.membernumber            
        ,   b.integration_id         
        ,   b.order_id  
) 
SELECT c.* FROM cad_df c  
-- """) 
-- #  
-- # CHECK 
-- # spark.sql(""" 
-- select  count(*) as nrow 
-- -- distance mentrics  
-- ,   sum(case when cad_sa4_code is not null then 1 else 0 end )  / count(*)      as cad_sa4_code  
-- ,   sum(case when cad_sa4_name is not null then 1 else 0 end )/ count(*)        as cad_sa4_name  
-- ,   sum(case when cad_sa4_distance is not null then 1 else 0 end )/ count(*)    as cad_sa4_distance  
-- -- RTA  
-- ,   sum(case when cad_rta is not null then 1 else 0 end )  / count(*)           as cad_rta  
-- ,   sum(case when cad_tow_rta is not null then 1 else 0 end )  / count(*)       as cad_tow_rta  
-- ,   sum(case when cad_patrol_rta is not null then 1 else 0 end )  / count(*)    as cad_patrol_rta  
-- ,   sum(case when cad_battery_rta is not null then 1 else 0 end )  / count(*)   as cad_battery_rta  
-- from sandpit.model_cad_order  
-- """).show(100)# 
-- # 
-- # distance tagging post  
-- # +-------+------------------+------------------+------------------+------------------+-----------------+------------------+------------------+ 
-- # |   nrow|      cad_sa4_code|      cad_sa4_name|  cad_sa4_distance|           cad_rta|      cad_tow_rta|    cad_patrol_rta|   cad_battery_rta| 
-- # +-------+------------------+------------------+------------------+------------------+-----------------+------------------+------------------+ 
-- # |1716974|0.9319127721211853|0.9319127721211853|0.9192521261242163|0.9320368275815475|0.186495543904567|0.5826925742614624|0.4117598752223388| 
-- # +-------+------------------+------------------+------------------+------------------+-----------------+------------------+------------------+ 
-- #  
-- spark.sql(""" 
-- select  
--     case  
--         when cad_tow_rta < 60 then "0-60" 
--         when cad_tow_rta < 120 then "61-120" 
--         when cad_tow_rta >= 120 then "120+"  
--         else "No Tow"  
--     END         as cad_tow_rta_band  
-- ,   count(*) as nrow 
-- from sandpit.model_cad_order 
-- group by  
--     case  
--         when cad_tow_rta < 60 then "0-60" 
--         when cad_tow_rta < 120 then "61-120" 
--         when cad_tow_rta >= 120 then "120+"  
--         else "No Tow"  
--     END    
-- """).show(100)# 
-- #  
SELECT * from sandpit.model_cad_order 
where cad_job_region 
--327791 
SELECT max(cad_receive_dt), min(cad_receive_dt) from sandpit.model_cad_order 
where cad_tow=1 
and cad_receipt_dt>'2017-05-01' 
spark.sql(''' 
select cad_tow_rta_band 
        , count(*) as nr 
        ,avg(sub_renewed) as sub_renewed  
 
from sandpit.model_df  
where year(order_end_dt) = '2020'  
group by cad_tow_rta_band 
''').show(100)# 
 
spark.sql(''' 
select *  
 
from sandpit.model_df  
where year(order_end_dt) = '2020'  
--group by cad_tow_rta_band 
''').show(100)# 
 
select  
    case  
        when cad_tow_rta < 60 then "0-60" 
        when cad_tow_rta < 120 then "61-120" 
        when cad_tow_rta >= 120 then "120+"  
        else "No Tow"  
    END         as cad_tow_rta_band  
--,   case when order_id is null then 'Non-renewed' else 'Renewed' end renewal 
,   count(*) as nrow 
from sandpit.model_cad_order 
group by 1 
order by 1 
 
select case  
        when cad_tow_rta < 60 then "0-60" 
        when cad_tow_rta < 120 then "61-120" 
        when cad_tow_rta >= 120 then "120+"  
        else "No Tow"  
        END             as cad_tow_rta_band  
    ,   case when b.renewed_order_id is null then 'Non-renewed' else 'Renewed' end renewal 
    , count(*) 
 
from sandpit.model_cad_order tow 
 
inner join sandpit.renewal_base b 
on b.order_id=tow.order_id 
 
where tow.cad_tow=1 
 
group by 1,2 
order by 1,2 
select count(distinct tow.cad_num) 
 
from sandpit.model_cad_order tow 
 
inner join sandpit.renewal_base b 
on b.order_id=tow.order_id 
 
where tow.cad_tow=1 
 
select prod_grp_agg1,count(distinct order_id), avg(sub_renewed) 
 
from sandpit.model_df  
where year(order_end_dt) = 2020 
group by prod_grp_agg1 
 
SELECT member_loyalty_colour,count(*) from sandpit.model_df 
where cad_tow=1 
and year(order_end_dt) = 2020 
 
spark.sql(""" 
CREATE TABLE campaign_data.model_cad_order_20200513 AS  
WITH cad_df AS ( 
    select  
            b.vehicle_rego 
        ,   b.membernumber 
        ,   b.integration_id         
        ,   b.order_id  
        -- Distance predictors 
        ,   max(cadsa4.sa4_code16)      as cad_sa4_code 
        ,   max(cadsa4.sa4_name16)      as cad_sa4_name 
        ,   max(sa4n.sa4_region)        as cad_sa4_region 
        ,   max(sa4n.sa4_metro_cat)     as cad_sa4_metro_cat 
        ,   max(dist.distance)          as cad_sa4_distance 
        ,   min((UNIX_TIMESTAMP(srx.attrib_12)-UNIX_TIMESTAMP(srx.attrib_27))/60  )               as cad_rta 
        ,   min(case when s.x_bus_type = 'TOW' then (UNIX_TIMESTAMP(srx.attrib_12)-UNIX_TIMESTAMP(srx.attrib_27))/60 else NULL end )                   as cad_tow_rta 
        ,   min(case when s.x_bus_type = 'PATROL' then (UNIX_TIMESTAMP(srx.attrib_12)-UNIX_TIMESTAMP(srx.attrib_27))/60 else NULL end )                as cad_patrol_rta 
        ,   min(case when s.x_bus_type = 'BATTERY' then (UNIX_TIMESTAMP(srx.attrib_12)-UNIX_TIMESTAMP(srx.attrib_27))/60 else NULL end )               as cad_battery_rta 
         
        -- Job summary 
        ,   count(*)                        as cad_jobs 
        ,   concat_ws('|' , collect_set(srx.attrib_04))     as cad_num 
        ,   concat_ws('|' , collect_set(srx.attrib_27))     as cad_receive_dt 
        ,   concat_ws('|' , collect_set(srx.attrib_26))     as cad_acknowledge_dt 
        ,   concat_ws('|' , collect_set(srx.attrib_28))     as cad_receipt_dt 
        ,   concat_ws('|' , collect_set(srx.attrib_12))     as cad_arrival_dt 
        ,   concat_ws('|' , collect_set(srx.attrib_30))     as cad_complete_dt 
        ,   concat_ws('|' , collect_set(s.root_cause_cd))   as cad_root_cause 
        ,   concat_ws('|' , collect_set(s.x_work_area))     as cad_job_region 
        -- attributing the jobs  
        ,   max(case when lower(concat(s.x_bus_type,s.sr_subtype_cd,s.root_cause_cd)) like '%battery%' then 1 else 0 end )           as cad_battery 
        ,   max(case when lower(concat(s.x_bus_type,s.sr_subtype_cd,s.root_cause_cd)) like '%alternator%' then 1 else 0 end )        as cad_alternator 
        ,   max(case when lower(concat(s.x_bus_type,s.sr_subtype_cd,s.root_cause_cd)) like '%tyre%' then 1 else 0 end )              as cad_tyre 
        ,   max(case when lower(concat(s.x_bus_type,s.sr_subtype_cd,s.root_cause_cd)) like '%transmission%' then 1 else 0 end )      as cad_transmission 
        ,   max(case when lower(concat(s.x_bus_type,s.sr_subtype_cd,s.root_cause_cd)) like '%brake%' then 1 else 0 end )             as cad_brake 
        ,   max(case when lower(concat(s.x_bus_type,s.sr_subtype_cd,s.root_cause_cd)) like '%lock%' then 1 else 0 end )              as cad_lockout 
        ,   max(case when lower(concat(s.x_bus_type,s.sr_subtype_cd,s.root_cause_cd)) like '%tow%' then 1 else 0 end )               as cad_tow 
        ,   s.sr_stat_id as job_status 
     
    from  sandpit.renewal_base b  
    inner join gms.s_srv_req_x as srx  
        on  b.membernumber = srx.attrib_03  
        and b.vehicle_rego = srx.attrib_05  
    inner join gms.s_srv_req as s 
        on srx.row_id = s.row_id 
        and srx.attrib_04 is not null    
    -- DISTANCE Tagging 
    LEFT JOIN sandpit.poa_2_sa4 sa4  
        on b.owner_postcode = sa4.poa_code_2016 
    left join sandpit.distance_cad_2_sa4 cadsa4 
        ON s.x_work_area  = cadsa4.cad_region 
        AND 1 = cadsa4.sa4_rank 
    left join sandpit.distance_sa4_2_sa4 dist 
        ON  sa4.sa4_code_2016    = dist.sa4_code16_x 
        AND cadsa4.sa4_code16 = dist.sa4_code16_y 
    LEFT JOIN sandpit.util_sa4_names sa4n 
        on cadsa4.sa4_code16 = sa4n.sa4_code 
    where 
        1=1 
        and s.x_bulk_source = "CAD"  
        and s.x_bus_type in ("PATROL", "AUTOELEC", "OTHER", "CARELEC", "BATTERY", "TOW") 
        and srx.attrib_27 between b.order_start_dt and b.order_end_dt  
        and membernumber is not null 
         
    group by  
            b.vehicle_rego 
        ,   b.membernumber            
        ,   b.integration_id         
        ,   b.order_id  
        ,   s.sr_stat_id 
) 
SELECT c.* FROM cad_df c  
""").show(100) 
, s.x_bus_type as job_type 
    , s.sr_subtype_cd as job_subtype 
    , s.sr_category_cd as bkdn_cd 
    --, s.root_cause_cd as bkdn_reason 
    , s.sr_prio_cd as job_priority 
    , s.sr_stat_id as job_status 
    --, s.x_bulk_source as source 
    , s.resolution_cd -- full name of completion cd  
    , s.asset_id 