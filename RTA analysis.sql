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
    , s.asset_id as asset_id_cad 
    , s.asset_reg_id 
    --, srx.as 
    , s.cst_ou_id member_org_id -- member's organisation id 
    , s.cst_con_id member_contact_id-- member contact id 
    , srx.attrib_27 as receive_dt 
    , srx.attrib_12 as at_scene_dt -- accurate 
    , srx.attrib_30 as completion_dt -- accurate 
   -- , CASE WHEN ((UNIX_TIMESTAMP(srx.attrib_12)-UNIX_TIMESTAMP(srx.attrib_27)) * 1440) >= 0 AND (UNIX_TIMESTAMP(srx.attrib_12)-UNIX_TIMESTAMP(srx.attrib_27)) * 1440) < 60 THEN 1 ELSE 0 END rta60_num 
    --, (UNIX_TIMESTAMP(srx.attrib_12)-UNIX_TIMESTAMP(srx.attrib_27))/60 as tow_rta 
    --, case when srx.attrib_12 is null then 0 else 1 end RTA_denom 
    -- , b.order_id 
    -- , b.asset_start_dt 
    -- , b.asset_end_dt 
    -- , b.order_start_dt 
    -- , b.order_end_dt 
    -- , b.nrp_prod_name 
    -- , b.renewed_order_id 
 
from gms.s_srv_req_x as srx  
     
inner join gms.s_srv_req as s 
on srx.row_id = s.row_id 
and srx.attrib_04 is not null 
 
-- inner join sandpit.renewal_base b 
-- on b.asset_row_id=s.asset_id 
-- and b.membernumber = srx.attrib_03  
-- and b.vehicle_rego = srx.attrib_05 
 
where 
    1=1 
    and s.x_bulk_source = "CAD" 
    and s.x_bus_type = "TOW"  
    --and year(b.order_end_dt)=2020 
    --and s.resolution_cd not in ('OWN ARRANGEMENTS','UNATTEND VEHICLE','UNABLE TO LOCATE') 
    and s.sr_stat_id not in ('CANCELLED','ABORTED') 
    and srx.attrib_30 >'2019-01-01' 
    --and srx.attrib_27 between b.order_start_dt and b.order_end_dt 
) 
--NR:96916 
--CAD:94317 
--rta60 : 36109 
 
select count(*) from all_tow 
where receive_dt is null 
 
 
 
--where tow_rta<=60 
--where cad_num='20190508001652' 
spark.sql(''' 
SELECT max( 
from cad.cad_jobs 
 
--where  
''').show(1000,False) 
spark.sql(''' 
SELECT max(completion_time) 
from cad.cad_jobs 
 
where completion_code not in ('DALLOC', 'JOBCANCL') 
and business_type = 'TOW' 
--and to_timestamp(completion_time,'yyyy-mm-dd')>'2019-01-01' 
''').show(1000) 
SELECT distinct job_number 
        , rego 
        , recieved_time 
        , activation_time 
        , allocation_time 
        , dispatch_time 
        , enroute_time 
        , at_scene_time 
        , completion_time 
        , member_number 
        , tow_type 
        , tow_pay_all 
        , tow_kms_entitlement 
        , tow_kms_travelled 
        , case when at_scene_time is null then 0 else 1 end RTA_denom 
        , (unix_timestamp(at_scene_time) - unix_timestamp(activation_time)) as time_Diff 
        , (unix_timestamp(at_scene_time) - unix_timestamp(activation_time))/60 as rta_mins 
 
from cad.cad_jobs 
 
where completion_code not in ('DALLOC', 'JOBCANCL') 
and business_type = 'TOW' 
and date(completion_time)>'2018-12-01' 
spark.sql(''' 
SELECT distinct job_number 
        , rego 
        , recieved_time 
        , activation_time 
        , allocation_time 
        , dispatch_time 
        , enroute_time 
        , at_scene_time 
        , completion_time 
        , member_number 
        , tow_type 
        , tow_pay_all 
        , tow_kms_entitlement 
        , tow_kms_travelled 
        , case when COALESCE(at_scene_time, '') != '' then 1 else 0 end RTA_denom 
        , (unix_timestamp(at_scene_time) - unix_timestamp(activation_time)) as time_Diff 
        , (unix_timestamp(at_scene_time) - unix_timestamp(activation_time))/60 as rta_mins 
  
from cad.cad_jobs 
  
where completion_code not in ('DALLOC', 'JOBCANCL') 
and business_type = 'TOW' 
and to_timestamp(completion_time,'yyyy-mm-dd')>'2018-12-01' 
''').repartition(1).write.saveAsTable('campaign_data.aa_dmc2392_rta_churn_20210514_v3') 
spark.sql(''' 
SELECT count(distinct job_number) 
         
from cad.cad_jobs 
 
where completion_code not in ('DALLOC', 'JOBCANCL') 
and business_type = 'TOW' 
and to_timestamp(completion_time,'yyyy-mm-dd')>'2018-12-01' 
''').show(1000) 
spark.sql(''' 
SELECT min(completion_time), max(completion_time) 
from cad.cad_jobs 
 
where completion_code not in ('DALLOC', 'JOBCANCL') 
and business_type = 'TOW' 
and to_timestamp(completion_time,'yyyy-mm-dd')>'2019-01-01' 
''').show(1000) 
select a.* 
    , b.order_id 
    , b.asset_start_dt 
    , b.asset_end_dt 
    , b.order_start_dt 
    , b.order_end_dt 
    , b.nrp_prod_name 
    , b.renewed_order_id 
from campaign_data.aa_dmc2392_rta_churn_20210514_v2 a 
 
inner join sandpit.renewal_base b 
on b.membernumber = a.member_number  
and b.vehicle_rego = a.rego 
 
where activation_time between b.order_start_dt and b.order_end_dt 
--where rta_mins<60 
select a.job_number,count(*) 
from campaign_data.aa_dmc2392_rta_churn_20210514_v2 a 
 
inner join sandpit.renewal_base b 
on b.membernumber = a.member_number  
and b.vehicle_rego = a.rego 
 
where to_timestamp(activation_time,'yyyy-MM-dd HH:mm:ss') between b.order_start_dt and b.order_end_dt 
and b.order_start_dt>'2018-02-20' 
and b.order_end_dt<'2021-11-24' 
 
group by 1 
order by 2 desc 
with all_tow as ( 
select a.* 
    , b.order_id 
    , b.vehicle_rego 
    , b.asset_start_dt 
    , b.asset_end_dt 
    , b.order_start_dt 
    , b.order_end_dt 
    , b.nrp_prod_name 
    , b.renewed_order_id 
from campaign_data.aa_dmc2392_rta_churn_20210514_v2 a 
 
inner join sandpit.renewal_base b 
on b.membernumber = a.member_number  
and b.vehicle_rego = a.rego 
 
where to_timestamp(activation_time,'yyyy-MM-dd HH:mm:ss') between b.order_start_dt and b.order_end_dt 
and b.order_start_dt>'2018-02-20' 
and b.order_end_dt<'2021-11-24' 
and b.order_status_cd 
) 
select * from all_tow 
where job_number='20190829000990' 
with all_tow as ( 
select rta.* 
    , b.order_id 
    , b.vehicle_rego 
    , b.asset_start_dt 
    , b.asset_end_dt 
    , b.order_start_dt 
    , b.order_end_dt 
    , b.nrp_prod_name 
    , b.renewed_order_id 
    , b.action_type1 
    , b.asset_cancel_cd 
    , b.x_sch_pay_status 
    , b.x_renewal_status 
    , b.asset_status_cd 
    , b.prod_name 
    , b.member_loyalty_colour 
    , b.member_colour 
    , b.owner_postcode 
    , CASE 
        WHEN 
            ( 
                b.owner_postcode >= 2000 AND b.owner_postcode <= 2082 OR 
                b.owner_postcode >= 2084 AND b.owner_postcode <= 2234 OR 
                b.owner_postcode >= 2555 AND b.owner_postcode <= 2574 OR 
                b.owner_postcode >= 2745 AND b.owner_postcode <= 2770 OR 
                b.owner_postcode >= 2775 AND b.owner_postcode <= 2775 
            ) 
        THEN 
            'METROPOLITAN' 
 
        WHEN 
            ( 
                b.owner_postcode >= 2083 AND b.owner_postcode <= 2083 OR 
                b.owner_postcode >= 2250 AND b.owner_postcode <= 2338 OR 
                b.owner_postcode >= 2415 AND b.owner_postcode <= 2423 OR 
                b.owner_postcode >= 2425 AND b.owner_postcode <= 2425 OR 
                b.owner_postcode >= 2428 AND b.owner_postcode <= 2428 OR 
                b.owner_postcode >= 2500 AND b.owner_postcode <= 2535 OR 
                b.owner_postcode >= 2538 AND b.owner_postcode <= 2541 OR 
                b.owner_postcode >= 2575 AND b.owner_postcode <= 2578 OR 
                b.owner_postcode >= 2600 AND b.owner_postcode <= 2617 OR 
                b.owner_postcode >= 2773 AND b.owner_postcode <= 2774 OR 
                b.owner_postcode >= 2776 AND b.owner_postcode <= 2786 OR 
                b.owner_postcode >= 2900 AND b.owner_postcode <= 2914 
            ) 
        THEN 
            'REGIONAL' 
         
        WHEN 
            ( 
                b.owner_postcode >= 2339 AND b.owner_postcode <= 2411 OR 
                b.owner_postcode >= 2424 AND b.owner_postcode <= 2424 OR 
                b.owner_postcode >= 2426 AND b.owner_postcode <= 2427 OR 
                b.owner_postcode >= 2429 AND b.owner_postcode <= 2490 OR 
                b.owner_postcode >= 2536 AND b.owner_postcode <= 2537 OR 
                b.owner_postcode >= 2545 AND b.owner_postcode <= 2551 OR 
                b.owner_postcode >= 2579 AND b.owner_postcode <= 2594 OR 
                b.owner_postcode >= 2618 AND b.owner_postcode <= 2739 OR 
                b.owner_postcode >= 2787 AND b.owner_postcode <= 2898 OR 
                b.owner_postcode >= 6798 AND b.owner_postcode <= 6799 
            ) 
        THEN 
           ' RURAL' 
         
        WHEN 
            ( 
                b.owner_postcode >= 0800 AND b.owner_postcode <= 0886 OR 
                b.owner_postcode >= 3000 AND b.owner_postcode <= 6770 OR 
                b.owner_postcode >= 6907 AND b.owner_postcode <= 7470 OR 
                b.owner_postcode >= 7471  
            ) 
        THEN 
            'INTERSTATE' 
 
        ELSE 
            'UNKNOWN' end region 
 
    , b.contact_cd 
    , b.org_type_cd 
    , b.order_bulk_src 
    , b.renewal_yyyymm 
    , c.renewal_cd 
from campaign_data.aa_dmc2392_rta_churn_20210514_v3 rta 
 
inner join sandpit.renewal_base b 
on b.membernumber = rta.member_number  
and b.vehicle_rego = rta.rego 
 
inner join sandpit.util_renew_summ c  
on b.match_rnk = c.match_rnk  
 
left join  sandpit.util_prod_budget  a  
on b.prod_budget = a.prod_budget 
and case when b.order_payment_term = "Y" then 'MPP' else 'Annual' end = a.payment_plan 
 
where to_timestamp(activation_time,'yyyy-MM-dd HH:mm:ss') between b.order_start_dt and b.order_end_dt 
and year(b.order_end_dt) in (2019,2020) 
--between '2019-11-01' and '2020-10-31' 
and b.prod_type='RSA' 
and COALESCE(c.type_rnk, 0)  <> 1  
and COALESCE(b.member_staff, 0)  = 0  
AND a.removeID = 0  
and (date_add(b.order_end_dt,2) >= a.dt_min  OR a.dt_min IS NULL) 
) 
--, next as ( 
--select count(*) from all_tow    --137955 
--select count(distinct job_number) from all_tow    --131244 
select case when rta_mins<60 then 'RTA60' 
            when rta_mins<120 then 'RTA120' 
            when rta_mins>=120 then 'RTA120+' 
            else 'other' end RTA 
        , sum(1)                      AS renewals 
        ,   sum(renewal_cd)           as renewed 
        --, sum(rta_denom) 
from all_tow 
where 1=1 
and region='METROPOLITAN' 
-- ) 
-- select * from next 
-- where rta='other' 
-- --and rta_mins<60 
 
group by 1 
order by 1 
with all_tow as ( 
select rta.* 
    , b.order_id 
    , b.vehicle_rego 
    , b.asset_start_dt 
    , b.asset_end_dt 
    , b.order_start_dt 
    , b.order_end_dt 
    , b.nrp_prod_name 
    , b.renewed_order_id 
    , b.action_type1 
    , b.asset_cancel_cd 
    , b.x_sch_pay_status 
    , b.x_renewal_status 
    , b.asset_status_cd 
    , b.prod_name 
    , b.member_loyalty_colour 
    , b.member_colour 
    , b.contact_cd 
    , b.org_type_cd 
    , b.order_bulk_src 
    , b.renewal_yyyymm 
    , c.renewal_cd 
from campaign_data.aa_dmc2392_rta_churn_20210514_v3 rta 
 
inner join sandpit.renewal_base b 
on b.membernumber = rta.member_number  
and b.vehicle_rego = rta.rego 
 
inner join sandpit.util_renew_summ c  
on b.match_rnk = c.match_rnk  
 
left join  sandpit.util_prod_budget  a  
on b.prod_budget = a.prod_budget 
and case when b.order_payment_term = "Y" then 'MPP' else 'Annual' end = a.payment_plan 
 
where to_timestamp(activation_time,'yyyy-MM-dd HH:mm:ss') between b.order_start_dt and b.order_end_dt 
and year(b.order_end_dt) in (2019,2020) 
--between '2019-11-01' and '2020-10-31' 
and b.prod_type='RSA' 
and COALESCE(c.type_rnk, 0)  <> 1  
and COALESCE(b.member_staff, 0)  = 0  
AND a.removeID = 0  
and (date_add(b.order_end_dt,2) >= a.dt_min  OR a.dt_min IS NULL) 
) 
select sum(rta_denom) from all_tow 
with all_tow as ( 
select rta.* 
    , b.order_id 
    , b.vehicle_rego 
    , b.asset_start_dt 
    , b.asset_end_dt 
    , b.order_start_dt 
    , b.order_end_dt 
    , b.nrp_prod_name 
    , b.renewed_order_id 
    , b.action_type1 
    , b.asset_cancel_cd 
    , b.x_sch_pay_status 
    , b.x_renewal_status 
    , b.asset_status_cd 
    , b.prod_name 
    , b.member_loyalty_colour 
    , b.member_colour 
    , b.contact_cd 
    , b.org_type_cd 
    , b.order_bulk_src 
    , b.renewal_yyyymm 
    , c.renewal_cd 
from campaign_data.aa_dmc2392_rta_churn_20210514_v3 rta 
 
inner join sandpit.renewal_base b 
on b.membernumber = rta.member_number  
and b.vehicle_rego = rta.rego 
 
inner join sandpit.util_renew_summ c  
on b.match_rnk = c.match_rnk  
 
left join  sandpit.util_prod_budget  a  
on b.prod_budget = a.prod_budget 
and case when b.order_payment_term = "Y" then 'MPP' else 'Annual' end = a.payment_plan 
 
where to_timestamp(activation_time,'yyyy-MM-dd HH:mm:ss') between b.order_start_dt and b.order_end_dt 
and year(b.order_end_dt) in (2019,2020) 
and b.prod_type='RSA' 
and COALESCE(c.type_rnk, 0)  <> 1  
and COALESCE(b.member_staff, 0)  = 0  
AND a.removeID = 0  
and (date_add(b.order_end_dt,2) >= a.dt_min  OR a.dt_min IS NULL) 
) 
--, next as ( 
--select count(*) from all_tow    --137955 
--select count(distinct job_number) from all_tow    --131244 
select prod_name 
        , sum(1)                      AS renewals 
        ,   sum(renewal_cd)           as renewed 
from all_tow 
where 1=1 
 
 
group by 1 
order by 1 
with all_tow as ( 
select rta.* 
    , b.order_id 
    , b.vehicle_rego 
    , b.asset_start_dt 
    , b.asset_end_dt 
    , b.order_start_dt 
    , b.order_end_dt 
    , b.nrp_prod_name 
    , b.renewed_order_id 
    , b.action_type1 
    , b.asset_cancel_cd 
    , b.x_sch_pay_status 
    , b.x_renewal_status 
    , b.asset_status_cd 
    , b.prod_name 
    , b.member_loyalty_colour 
    , b.member_colour 
    , b.contact_cd 
    , b.org_type_cd 
    , b.order_bulk_src 
    , b.renewal_yyyymm 
    , c.renewal_cd 
from campaign_data.aa_dmc2392_rta_churn_20210514_v3 rta 
 
inner join sandpit.renewal_base b 
on b.membernumber = rta.member_number  
and b.vehicle_rego = rta.rego 
 
inner join sandpit.util_renew_summ c  
on b.match_rnk = c.match_rnk  
 
left join  sandpit.util_prod_budget  a  
on b.prod_budget = a.prod_budget 
and case when b.order_payment_term = "Y" then 'MPP' else 'Annual' end = a.payment_plan 
 
where to_timestamp(activation_time,'yyyy-MM-dd HH:mm:ss') between b.order_start_dt and b.order_end_dt 
and year(b.order_end_dt) in (2019,2020) 
--between '2019-11-01' and '2020-10-31' 
and b.prod_type='RSA' 
and COALESCE(c.type_rnk, 0)  <> 1  
and COALESCE(b.member_staff, 0)  = 0  
AND a.removeID = 0  
and (date_add(b.order_end_dt,2) >= a.dt_min  OR a.dt_min IS NULL) 
) 
select asset_cancel_cd 
        ,   sum(1)                    AS renewals 
        ,   sum(renewal_cd)           as renewed 
from all_tow 
 
group by 1 
order by 1 
with all_tow as ( 
select rta.* 
    , b.order_id 
    , b.vehicle_rego 
    , b.asset_start_dt 
    , b.asset_end_dt 
    , b.order_start_dt 
    , b.order_end_dt 
    , b.nrp_prod_name 
    , b.renewed_order_id 
    , b.action_type1 
    , b.asset_cancel_cd 
    , b.x_sch_pay_status 
    , b.x_renewal_status 
    , b.asset_status_cd 
    , b.prod_name 
    , b.member_loyalty_colour 
    , b.member_colour 
    , b.contact_cd 
    , b.org_type_cd 
    , b.order_bulk_src 
    , b.renewal_yyyymm 
    , c.renewal_cd 
from campaign_data.aa_dmc2392_rta_churn_20210514_v3 rta 
 
inner join sandpit.renewal_base b 
on b.membernumber = rta.member_number  
and b.vehicle_rego = rta.rego 
 
inner join sandpit.util_renew_summ c  
on b.match_rnk = c.match_rnk  
 
left join  sandpit.util_prod_budget  a  
on b.prod_budget = a.prod_budget 
and case when b.order_payment_term = "Y" then 'MPP' else 'Annual' end = a.payment_plan 
 
where to_timestamp(activation_time,'yyyy-MM-dd HH:mm:ss') between b.order_start_dt and b.order_end_dt 
and year(b.order_end_dt) in (2019,2020) 
--between '2019-11-01' and '2020-10-31' 
and b.prod_type='RSA' 
and COALESCE(c.type_rnk, 0)  <> 1  
and COALESCE(b.member_staff, 0)  = 0  
AND a.removeID = 0  
and (date_add(b.order_end_dt,2) >= a.dt_min  OR a.dt_min IS NULL) 
) 
select case when rta_mins<60 then 'RTA60' 
            when rta_mins<120 then 'RTA120' 
            when rta_mins>=120 then 'RTA120+' 
            else 'other' end RTA 
        ,   b.tow_satisfaction 
        ,   sum(1)                    AS renewals 
        --,   sum(renewal_cd)           as renewed 
from all_tow a 
 
inner join campaign_data.aa_dmc2392_tow_satisfaction_20210514 b 
on cast(b.membership_id as string)=a.member_number 
 
--where b.nps='Detractor' 
 
group by 2,1 
order by 2,1 
with all_tow as ( 
select rta.* 
    , b.order_id 
    , b.vehicle_rego 
    , b.asset_start_dt 
    , b.asset_end_dt 
    , b.order_start_dt 
    , b.order_end_dt 
    , b.nrp_prod_name 
    , b.renewed_order_id 
    , b.action_type1 
    , b.asset_cancel_cd 
    , b.x_sch_pay_status 
    , b.x_renewal_status 
    , b.asset_status_cd 
    , b.prod_name 
    , b.member_loyalty_colour 
    , b.member_colour 
    , b.contact_cd 
    , b.org_type_cd 
    , b.order_bulk_src 
    , b.renewal_yyyymm 
    , c.renewal_cd 
from campaign_data.aa_dmc2392_rta_churn_20210514_v3 rta 
 
inner join sandpit.renewal_base b 
on b.membernumber = rta.member_number  
and b.vehicle_rego = rta.rego 
 
inner join sandpit.util_renew_summ c  
on b.match_rnk = c.match_rnk  
 
left join  sandpit.util_prod_budget  a  
on b.prod_budget = a.prod_budget 
and case when b.order_payment_term = "Y" then 'MPP' else 'Annual' end = a.payment_plan 
 
where to_timestamp(activation_time,'yyyy-MM-dd HH:mm:ss') between b.order_start_dt and b.order_end_dt 
and year(b.order_end_dt) in (2019,2020) 
--between '2019-11-01' and '2020-10-31' 
and b.prod_type='RSA' 
and COALESCE(c.type_rnk, 0)  <> 1  
and COALESCE(b.member_staff, 0)  = 0  
AND a.removeID = 0  
and (date_add(b.order_end_dt,2) >= a.dt_min  OR a.dt_min IS NULL) 
) 
select asset_cancel_cd 
        ,case when rta_mins<60 then 'RTA60' 
            when rta_mins<120 then 'RTA120' 
            when rta_mins>=120 then 'RTA120+' 
            else 'other' end RTA 
        ,   sum(1)                    AS renewals 
        --,   sum(renewal_cd)           as renewed 
from all_tow a 
 
inner join campaign_data.aa_dmc2392_tow_satisfaction_20210514 b 
on cast(b.membership_id as string)=a.member_number 
 
--where b.nps='Detractor' 
 
group by 1,2 
order by 1,2 
with all_tow as ( 
select rta.* 
    , b.order_id 
    , b.vehicle_rego 
    , b.asset_start_dt 
    , b.asset_end_dt 
    , b.order_start_dt 
    , b.order_end_dt 
    , b.nrp_prod_name 
    , b.renewed_order_id 
    , b.action_type1 
    , b.asset_cancel_cd 
    , b.x_sch_pay_status 
    , b.x_renewal_status 
    , b.asset_status_cd 
    , b.prod_name 
    , b.member_loyalty_colour 
    , b.member_colour 
    , b.contact_cd 
    , b.org_type_cd 
    , b.order_bulk_src 
    , b.renewal_yyyymm 
    , c.renewal_cd 
from campaign_data.aa_dmc2392_rta_churn_20210514_v3 rta 
 
inner join sandpit.renewal_base b 
on b.membernumber = rta.member_number  
and b.vehicle_rego = rta.rego 
 
inner join sandpit.util_renew_summ c  
on b.match_rnk = c.match_rnk  
 
left join  sandpit.util_prod_budget  a  
on b.prod_budget = a.prod_budget 
and case when b.order_payment_term = "Y" then 'MPP' else 'Annual' end = a.payment_plan 
 
where to_timestamp(activation_time,'yyyy-MM-dd HH:mm:ss') between b.order_start_dt and b.order_end_dt 
and year(b.order_end_dt) in (2019,2020) 
--between '2019-11-01' and '2020-10-31' 
and b.prod_type='RSA' 
and COALESCE(c.type_rnk, 0)  <> 1  
and COALESCE(b.member_staff, 0)  = 0  
AND a.removeID = 0  
and (date_add(b.order_end_dt,2) >= a.dt_min  OR a.dt_min IS NULL) 
) 
select a.member_colour, sum(rta_denom) 
from all_tow a 
 
-- inner join campaign_data.aa_dmc2392_tow_satisfaction_20210514 b 
-- on cast(b.membership_id as string)=a.member_number 
 
--where b.nps='Detractor' 
 
group by 1 
order by 1 
spark.sql(''' 
    select cad_tow_rta_band, b.prod_name 
            , count(*) as nr 
            , avg(sub_renewed) as sub_renewed  
    from sandpit.model_df b 
    where year(order_end_dt) = 2020 
    and (member_postcode >= 2000 AND b.member_postcode <= 2082 OR 
                b.member_postcode >= 2084 AND b.member_postcode <= 2234 OR 
                b.member_postcode >= 2555 AND b.member_postcode <= 2574 OR 
                b.member_postcode >= 2745 AND b.member_postcode <= 2770 OR 
                b.member_postcode >= 2775 AND b.member_postcode <= 2775 
    ) 
    group by 1,2 
    order by 1,2 
''').show(100) 
 
tow_engagement = spark.read.load("/ user/ aamreen/ DMC2392_tow_satisfaction_survey.csv",format="csv", sep=",", inferSchema="true", header="true") 
tow_engagement.createOrReplaceTempView("tow_engagement") 
spark.sql (""" create table campaign_data.aa_dmc2392_tow_satisfaction_20210514 as select * from tow_engagement""").count() 
 
SELECT * FROM campaign_data.bped_agg 