SELECT * from sandpit.model_df 
SELECT sum(1) from sandpit.renewal_base 
where 1=1 
and lower(item_promo_name) like '%&%' 
and year(date_add(order_end_dt,2))=2021 
--or lower(item_promo_name) like '%add%' 
---Overall renewal rate per product 
SELECT case when lower(item_promo_name) like '%join%' then 'Join&Go' 
                when lower(item_promo_name) like '%add%' then 'Add&Go' 
                else 'others' end product_name 
        , count(DISTINCT contact_id) 
        , sum(1)                      AS renewals 
        , sum(renewal_cd)           as renewed 
        , sum(case when  b.renewed_completed_dt <= date_add(order_end_dt,2) then 1 else 0 end  ) as renewed_pot 
 
from sandpit.renewal_base b 
 
inner join sandpit.util_renew_summ c  
on b.match_rnk = c.match_rnk  
 
where lower(item_promo_name) like '%&%' 
and year(date_add(b.order_end_dt,2))=2021 
and  COALESCE(c.type_rnk, 0)  <> 1  
 
group by 1 
order by 1 
---App enagagement and redemption and renewal rate 
with ga_App as ( 
select distinct membernumber from googleanalytics.appdata 
where eventyear in ('2019','2020') 
), redemption as ( 
select distinct member_number from m4m.return_feed_header 
where partner not in ('IAG','NRMA Multi Policy Discount','NRMA MPD Insurance','NRMA Insurance') 
and time_stamp between '2019-01-01' and '2020-12-31' 
) 
SELECT case when r.member_number is not null then 1 else 0 end   as redeemer 
        , case when app.membernumber is not null then 1 else 0 end  as app_user 
        , case when lower(item_promo_name) like '%join%' then 'Join&Go' 
                when lower(item_promo_name) like '%add%' then 'Add&Go' 
                else 'others' end product_name 
        , count(DISTINCT contact_id) 
        , sum(1)                      AS renewals 
        , sum(renewal_cd)           as renewed 
        , sum(case when b.renewed_completed_dt <= date_add(order_end_dt,2) then 1 else 0 end  ) as renewed_pot 
        , floor(sum(renewal_cd)/sum(1)) 
 
from sandpit.renewal_base b 
 
inner join sandpit.util_renew_summ c  
on b.match_rnk = c.match_rnk  
 
left join ga_App app 
on b.membernumber=app.membernumber 
 
left join redemption r 
on r.member_number=b.membernumber 
 
where lower(item_promo_name) like '%&%' 
and year(date_add(b.order_end_dt,2))=2020 
and  COALESCE(c.type_rnk, 0)  <> 1  
 
group by 1,2,3 
order by 1,2,3 
#what did they redeem?? 
spark.sql(''' 
with  redemption as ( 
select distinct member_number, partner from m4m.return_feed_header 
where time_stamp between '2019-01-01' and '2020-12-31' 
) 
SELECT case when lower(item_promo_name) like '%join%' then 'Join&Go' 
                when lower(item_promo_name) like '%add%' then 'Add&Go' 
                else 'others' end product_name 
        ,case when partner in ('IAG','NRMA Multi Policy Discount','NRMA MPD Insurance','NRMA Insurance') then 'IAG'  
            when partner LIKE 'NRMA Holiday%' then 'NRMA Parks and Resorts'  
            when partner LIKE 'HP%' then 'NRMA Parks and Resorts'  
            when partner LIKE 'BIG4%' then 'NRMA Parks and Resorts'  
            when partner LIKE 'NRMA Parks and Resorts%' then 'NRMA Parks and Resorts'  
            when partner LIKE 'NRMA Bowen Beachfront Holiday Park' then 'NRMA Parks and Resorts'  
            else partner end as partner 
        , count(DISTINCT contact_id) 
        , sum(renewal_cd)/sum(1) as RR 
 
from sandpit.renewal_base b 
 
inner join sandpit.util_renew_summ c  
on b.match_rnk = c.match_rnk  
 
inner join redemption r 
on r.member_number=b.membernumber 
 
where lower(item_promo_name) like '%&%' 
and year(date_add(b.order_end_dt,2))=2020 
and  COALESCE(c.type_rnk, 0)  <> 1  
 
group by 1,2 
order by 1,2 
''').show(10000,False) 
---open clicks and renewal rate 
with opens as ( 
select distinct customer_id 
 
from omc.send_level_summary a 
 
inner join sandpit.renewal_base b 
on b.contact_id=a.customer_id 
 
where 1=1 
and year(date_add(b.order_end_dt,2))=2020 
 
and (open_event_date is not null or click_event_date is not null) 
and datediff(b.order_end_dt,a.send_event_date) between 0 and 365 
 
and channel='Email' 
and control is false 
 
), clicks as ( 
 
select distinct customer_id 
 
from omc.send_level_summary a 
 
inner join sandpit.renewal_base b 
on b.contact_id=a.customer_id 
 
where 1=1 
and year(date_add(b.order_end_dt,2))=2020 
and click_event_date is not null 
and datediff(b.order_end_dt,a.send_event_date) between 0 and 365 
and channel='Email' 
and control is false 
) 
 
SELECT case when o.customer_id is not null then 1 else 0 end opened 
        , case when cl.customer_id is not null then 1 else 0 end as clicks 
        , case when lower(item_promo_name) like '%join%' then 'Join&Go' 
                when lower(item_promo_name) like '%add%' then 'Add&Go' 
                else 'others' end product_name 
        , count(DISTINCT contact_id) 
        , sum(1)                      AS renewals 
        , sum(renewal_cd)           as renewed 
        , sum(case when b.renewed_completed_dt <= date_add(order_end_dt,2) then 1 else 0 end  ) as renewed_pot 
 
from sandpit.renewal_base b 
 
inner join sandpit.util_renew_summ c  
on b.match_rnk = c.match_rnk  
 
left join opens o 
on o.customer_id=b.contact_id 
 
left join clicks cl 
on cl.customer_id=b.contact_id 
 
 
where lower(item_promo_name) like '%&%' 
and year(date_add(b.order_end_dt,2))=2020 
and  COALESCE(c.type_rnk, 0)  <> 1  
 
group by 1,2,3 
order by 1,2,3 
select case when lower(item_promo_name) like '%join%' then 'Join&Go' 
            when lower(item_promo_name) like '%add%' then 'Add&Go' 
            else 'others' end product_name 
        ,case when lower(c.campaign_name) like '%onboarding%' then 'On-boarding' 
            when (lower(c.campaign_name) like '%enl%' or lower(c.campaign_name) like '%enews%') then 'eNews' 
            when lower(c.campaign_name) like '%solus%' then 'Solus' 
            when (lower(c.campaign_name) like '%renew%' or lower(c.campaign_name) like '%reminder%') then 'renewal' 
            else 'others' end email_type 
        , count(DISTINCT contact_id) 
        , sum(renewal_cd)/sum(1) as RR 
 
from omc.send_level_summary a 
 
inner join omc.campaign_info c 
on c.campaign_id=a.campaign_id 
 
inner join sandpit.renewal_base b 
on b.contact_id=a.customer_id 
 
inner join sandpit.util_renew_summ u  
on b.match_rnk = u.match_rnk  
 
where lower(item_promo_name) like '%&%' 
and year(date_add(b.order_end_dt,2))=2020 
and  COALESCE(u.type_rnk, 0)  <> 1  
--and a.open_event_date is null 
and click_event_date is null 
and datediff(b.order_end_dt,a.send_event_date) between 0 and 365 
and a.channel='Email' 
and control is false 
 
group by 1,2 
order by 1,2 
SELECT case when lower(item_promo_name) like '%join%' then 'Join&Go' 
            when lower(item_promo_name) like '%add%' then 'Add&Go' 
            else 'others' end product_name 
        , a.push_consent 
        , count(DISTINCT contact_id) 
        , sum(renewal_cd)/sum(1) as RR 
 
from sandpit.renewal_base b 
 
left join campaign_data.bped_base a 
on b.contact_id=a.con_id 
 
inner join sandpit.util_renew_summ u  
on b.match_rnk = u.match_rnk  
 
where lower(item_promo_name) like '%&%' 
and year(date_add(b.order_end_dt,2))=2020 
and  COALESCE(u.type_rnk, 0)  <> 1 
 
group by 1,2 
order by 1,2 
SELECT case when lower(item_promo_name) like '%join%' then 'Join&Go' 
                when lower(item_promo_name) like '%add%' then 'Add&Go' 
                else 'others' end product_name 
        , sum(case when (lower(campaign_name) like '%onboarding%') then 1 else 0 end) Onboarding 
        , sum(case when lower(campaign_name) like '%enl%'  then 1 else 0 end) as eNews 
        , sum(case when lower(campaign_name) like '%solus%' then 1 else 0 end) as Solus 
        , sum(case when lower(campaign_name) like '%renew%' then 1 else 0 end) as renewal 
        , sum(case when lower(campaign_name) like '%reminder%'then 1 else 0 end) as renewal_reminder  
        , sum(case when lower(campaign_name) like '%onboarding%' and (e.open_event_date is not null or e.click_event_date is not null) then 1 else 0 end) Onboarding_opened 
        , sum(case when lower(campaign_name) like '%enl%' and (e.open_event_date is not null or e.click_event_date is not null) then 1 else 0 end) as eNews_open 
        , sum(case when lower(campaign_name) like '%solus%' and (e.open_event_date is not null or e.click_event_date is not null) then 1 else 0 end) as Solus_open 
        , sum(case when lower(campaign_name) like '%renew%'and (e.open_event_date is not null or e.click_event_date is not null) then 1 else 0 end) as renewal_open 
        , sum(case when lower(campaign_name) like '%reminder%'and (e.open_event_date is not null or e.click_event_date is not null) then 1 else 0 end) as renewal_reminder_open 
        , sum(case when lower(campaign_name) like '%onboarding%' AND e.click_event_date is not null then 1 else 0 end) Onboarding_click 
        , sum(case when lower(campaign_name) like '%enl%' and e.click_event_date is not null   then 1 else 0 end) as eNews_click 
        , sum(case when lower(campaign_name) like '%solus%' and e.click_event_date is not null   then 1 else 0 end) as Solus_click 
        , sum(case when lower(campaign_name) like '%renew%'and e.click_event_date is not null  then 1 else 0 end) as renewal_click 
        , sum(case when lower(campaign_name) like '%reminder%'and e.click_event_date is not null  then 1 else 0 end) as renewal_reminder_click 
 
from sandpit.renewal_base b 
 
inner join sandpit.util_renew_summ c  
on b.match_rnk = c.match_rnk  
 
inner join omc.send_level_summary e     
on e.customer_id=b.contact_id 
and e.channel='Email'  
and datediff(b.order_end_dt,e.send_event_date) between 0 and 365 
and control is false 
 
inner join omc.campaign_info n 
on n.campaign_id=e.campaign_id  
and n.channel='Email' 
and n.campaign_name not like '%CMO%' 
and n.campaign_name not like '%UAT%' 
and n.campaign_name not like '%E09%' 
 
 
where lower(item_promo_name) like '%&%' 
and year(date_add(b.order_end_dt,2))=2020 
and  COALESCE(c.type_rnk, 0)  <> 1  
 
GROUP BY 1 
ORDER BY 1 
SELECT case when lower(campaign_name) like '%renew%'and (e.open_event_date is not null or e.click_event_date is not null) then 1 else 0 end 
        , case when lower(item_promo_name) like '%join%' then 'Join&Go' 
                when lower(item_promo_name) like '%add%' then 'Add&Go' 
                else 'others' end product_name 
        , count(DISTINCT contact_id) 
        , sum(renewal_cd)/sum(1) as RR 
     
from sandpit.renewal_base b 
 
inner join sandpit.util_renew_summ c  
on b.match_rnk = c.match_rnk  
 
inner join omc.send_level_summary e     
on e.customer_id=b.contact_id 
and e.channel='Email'  
and datediff(b.order_end_dt,e.send_event_date) between 0 and 365 
and control is false 
 
inner join omc.campaign_info n 
on n.campaign_id=e.campaign_id  
and n.channel='Email' 
and n.campaign_name not like '%CMO%' 
and n.campaign_name not like '%UAT%' 
and n.campaign_name not like '%E09%' 
 
 
where lower(item_promo_name) like '%&%' 
and year(date_add(b.order_end_dt,2))=2020 
and  COALESCE(c.type_rnk, 0)  <> 1  
 
 
-- , sum() Onboarding_opened 
        -- , sum(case when lower(campaign_name) like '%enl%' and (e.open_event_date is not null or e.click_event_date is not null) then 1 else 0 end) as eNews_open 
        -- , sum(case when lower(campaign_name) like '%solus%' and (e.open_event_date is not null or e.click_event_date is not null) then 1 else 0 end) as Solus_open 
        -- , sum(case when lower(campaign_name) like '%renew%'and (e.open_event_date is not null or e.click_event_date is not null) then 1 else 0 end) as renewal_open 
        -- , sum(case when lower(campaign_name) like '%reminder%'and (e.open_event_date is not null or e.click_event_date is not null) then 1 else 0 end) as renewal_reminder_open 
        -- , sum(case when lower(campaign_name) like '%onboarding%' AND e.click_event_date is not null then 1 else 0 end) Onboarding_click 
        -- , sum(case when lower(campaign_name) like '%enl%' and e.click_event_date is not null   then 1 else 0 end) as eNews_click 
        -- , sum(case when lower(campaign_name) like '%solus%' and e.click_event_date is not null   then 1 else 0 end) as Solus_click 
        -- , sum(case when lower(campaign_name) like '%renew%'and e.click_event_date is not null  then 1 else 0 end) as renewal_click 
        -- , sum(case when lower(campaign_name) like '%reminder%'and e.click_event_date is not null  then 1 else 0 end) as renewal_reminder_click 
 
 
GROUP BY 1,2 
ORDER BY 2,1 
SELECT   
        --  (case when lower(campaign_name) like '%onboarding%' and (e.open_event_date is not null or e.click_event_date is not null) then 1 else 0 end) Onboarding_opened 
        --(case when (lower(campaign_name) like '%enl%' or lower(campaign_name) like '%enews%') and (e.open_event_date is not null or e.click_event_date is not null) then 1 else 0 end) as eNews_open 
        -- (case when lower(campaign_name) like '%solus%' and (e.open_event_date is not null or e.click_event_date is not null) then 1 else 0 end) as Solus_open 
        -- (case when (lower(campaign_name) like '%renew%' or lower(campaign_name) like '%reminder%') and (e.open_event_date is not null or e.click_event_date is not null) then 1 else 0 end) as renewal_open 
         (case when lower(campaign_name) like '%onboarding%' AND e.click_event_date is not null then 1 else 0 end) Onboarding_click 
        -- (case when (lower(campaign_name) like '%enl%' or lower(campaign_name) like '%enews%') and e.click_event_date is not null   then 1 else 0 end) as eNews_click 
        --, (case when lower(campaign_name) like '%solus%' and e.click_event_date is not null   then 1 else 0 end) as Solus_click 
        --, (case when (lower(campaign_name) like '%renew%' or lower(campaign_name) like '%reminder%') and e.click_event_date is not null  then 1 else 0 end) as renewal_click 
        , case when lower(item_promo_name) like '%join%' then 'Join&Go' 
                when lower(item_promo_name) like '%add%' then 'Add&Go' 
                else 'others' end product_name 
        , count(DISTINCT contact_id) 
        , sum(renewal_cd)/sum(1) as RR 
         
from sandpit.renewal_base b 
 
inner join sandpit.util_renew_summ c  
on b.match_rnk = c.match_rnk  
 
inner join omc.send_level_summary e     
on e.customer_id=b.contact_id 
and e.channel='Email'  
and datediff(b.order_end_dt,e.send_event_date) between 0 and 365 
and control is false 
 
inner join omc.campaign_info n 
on n.campaign_id=e.campaign_id  
and n.channel='Email' 
and n.campaign_name not like '%CMO%' 
and n.campaign_name not like '%UAT%' 
and n.campaign_name not like '%E09%' 
 
 
where lower(item_promo_name) like '%&%' 
and year(date_add(b.order_end_dt,2))=2020 
and  COALESCE(c.type_rnk, 0)  <> 1  
 
 
GROUP BY 1,2 
ORDER BY 2,1 
SELECT b.member_loyalty_colour 
        ,case when lower(item_promo_name) like '%join%' then 'Join&Go' 
                when lower(item_promo_name) like '%add%' then 'Add&Go' 
                else 'others' end product_name 
        , count(DISTINCT contact_id) 
        , sum(1)                      AS renewals 
        , sum(renewal_cd)           as renewed 
        , sum(case when  b.renewed_completed_dt <= date_add(order_end_dt,2) then 1 else 0 end  ) as renewed_pot 
         
from sandpit.renewal_base b 
 
inner join sandpit.util_renew_summ c  
on b.match_rnk = c.match_rnk  
 
where lower(item_promo_name) like '%&%' 
and year(date_add(b.order_end_dt,2))=2020 
and  COALESCE(c.type_rnk, 0)  <> 1  
 
 
GROUP BY 1,2 
ORDER BY 1,2 
SELECT b.member_loyalty_colour 
        , count(DISTINCT b.account_id) 
        , sum(1)                      AS renewals 
        , sum(renewal_cd)           as renewed 
        , sum(case when  b.renewed_completed_dt <= date_add(order_end_dt,2) then 1 else 0 end  ) as renewed_pot 
         
from sandpit.renewal_base b 
 
inner join sandpit.util_renew_summ c  
on b.match_rnk = c.match_rnk  
 
where lower(item_promo_name) like '%&%' 
and year(date_add(b.order_end_dt,2))=2020 
and  COALESCE(c.type_rnk, 0)  <> 1  
and lower(item_promo_name) like '%add%' 
 
 
GROUP BY 1 
ORDER BY 1 
SELECT b.member_colour 
        ,case when lower(item_promo_name) like '%join%' then 'Join&Go' 
                when lower(item_promo_name) like '%add%' then 'Add&Go' 
                else 'others' end product_name 
        , count(DISTINCT contact_id) 
        , sum(1)                      AS renewals 
        , sum(renewal_cd)           as renewed 
        , sum(case when  b.renewed_completed_dt <= date_add(order_end_dt,2) then 1 else 0 end  ) as renewed_pot 
         
from sandpit.renewal_base b 
 
inner join sandpit.util_renew_summ c  
on b.match_rnk = c.match_rnk  
 
where lower(item_promo_name) like '%&%' 
and year(date_add(b.order_end_dt,2))=2020 
and  COALESCE(c.type_rnk, 0)  <> 1  
 
 
GROUP BY 1,2 
ORDER BY 1,2 
SELECT b.renewal_yyyymm 
        , case when lower(item_promo_name) like '%join%' then 'Join&Go' 
                when lower(item_promo_name) like '%add%' then 'Add&Go' 
                else 'others' end product_name 
        , count(DISTINCT contact_id) 
        , sum(1)                      AS renewals 
        , sum(renewal_cd)           as renewed 
        , sum(case when  b.renewed_completed_dt <= date_add(order_end_dt,2) then 1 else 0 end  ) as renewed_pot 
 
from sandpit.renewal_base b 
 
inner join sandpit.util_renew_summ c  
on b.match_rnk = c.match_rnk  
 
where lower(item_promo_name) like '%&%' 
and year(date_add(b.order_end_dt,2))=2021 
and  COALESCE(c.type_rnk, 0)  <> 1  
 
group by 1,2 
order by 1,2 
SELECT case when lower(item_promo_name) like '%join%' then 'Join&Go' 
                when lower(item_promo_name) like '%add%' then 'Add&Go' 
                else 'others' end product_name 
        , count(DISTINCT contact_id) 
        , sum(1)                      AS renewals 
        , sum(renewal_cd)           as renewed 
        , sum(case when  b.renewed_completed_dt <= date_add(order_end_dt,2) then 1 else 0 end  ) as renewed_pot 
         
from sandpit.renewal_base b 
 
inner join sandpit.util_renew_summ c  
on b.match_rnk = c.match_rnk  
 
where lower(item_promo_name) like '%&%' 
and year(date_add(b.order_end_dt,2))=2020 
and  COALESCE(c.type_rnk, 0)  <> 1  
 
 
GROUP BY 1 
ORDER BY 1 
SELECT * 
from sandpit.renewal_base_old b 
where  1=1  
and year(date_add(b.order_end_dt,2))=2020 
AND lower(item_promo_name) like '%&%' 
 
limit 1000; 
SELECT SUBSTR(prod_tenure_split, INSTR(prod_tenure_split,'-') + 1) from sandpit.renewal_base 
SELECT SUBSTR(prod_tenure_split, INSTR(prod_tenure_split,'-') + 1) as tenure_band 
        , case when lower(item_promo_name) like '%join%' then 'Join&Go' 
                when lower(item_promo_name) like '%add%' then 'Add&Go' 
                else 'others' end product_name 
        , count(DISTINCT contact_id) 
        , sum(1)                      AS renewals 
        , sum(renewal_cd)           as renewed 
        , sum(case when  b.renewed_completed_dt <= date_add(order_end_dt,2) then 1 else 0 end  ) as renewed_pot 
 
from sandpit.renewal_base b 
 
inner join sandpit.util_renew_summ c  
on b.match_rnk = c.match_rnk  
 
where lower(item_promo_name) like '%&%' 
and year(date_add(b.order_end_dt,2))=2020 
and  COALESCE(c.type_rnk, 0)  <> 1  
 
GROUP BY 1,2 
ORDER BY 2,1 
SELECT b.prod_name 
        , case when lower(item_promo_name) like '%join%' then 'Join&Go' 
                when lower(item_promo_name) like '%add%' then 'Add&Go' 
                else 'others' end product_name 
        , count(DISTINCT contact_id) 
        , sum(1)                      AS renewals 
        , sum(renewal_cd)           as renewed 
        , sum(case when  b.renewed_completed_dt <= date_add(order_end_dt,2) then 1 else 0 end  ) as renewed_pot 
 
from sandpit.renewal_base b 
 
inner join sandpit.util_renew_summ c  
on b.match_rnk = c.match_rnk  
 
where lower(item_promo_name) like '%&%' 
and year(date_add(b.order_end_dt,2))=2020 
and  COALESCE(c.type_rnk, 0)  <> 1  
 
GROUP BY 1,2 
ORDER BY 2,1 
SELECT  case when cad.order_id is null then 'N' else 'Y' end call_out 
        , case when lower(item_promo_name) like '%join%' then 'Join&Go' 
                when lower(item_promo_name) like '%add%' then 'Add&Go' 
                else 'others' end product_name 
        , count(DISTINCT contact_id) 
        , sum(1)                      AS renewals 
        , sum(renewal_cd)           as renewed 
        , sum(case when  b.renewed_completed_dt <= date_add(order_end_dt,2) then 1 else 0 end  ) as renewed_pot 
 
from sandpit.renewal_base b 
 
inner join sandpit.util_renew_summ c  
on b.match_rnk = c.match_rnk  
 
left join sandpit.model_cad_order cad 
on cad.membernumber=b.membernumber 
and cad.integration_id=b.integration_id 
and cad.order_id=b.order_id 
 
where  1=1  
and year(date_add(b.order_end_dt,2))=2020 
AND lower(item_promo_name) like '%&%' 
and  COALESCE(c.type_rnk, 0)  <> 1  
--and cad.cad_complete_dt>b.member_join_dt 
 
GROUP BY 1,2 
ORDER BY 2,1 
with ga_App as ( 
select distinct membernumber from googleanalytics.appdata 
where eventyear in ('2019','2020') 
) 
 
SELECT  case when app.membernumber is null then 'N' else 'Y' end app_user 
        , case when lower(item_promo_name) like '%join%' then 'Join&Go' 
                when lower(item_promo_name) like '%add%' then 'Add&Go' 
                else 'others' end product_name 
        , count(DISTINCT contact_id) 
        , sum(1)                      AS renewals 
        , sum(renewal_cd)           as renewed 
        , sum(case when  b.renewed_completed_dt <= date_add(order_end_dt,2) then 1 else 0 end  ) as renewed_pot 
         
from sandpit.renewal_base b 
 
inner join sandpit.util_renew_summ c  
on b.match_rnk = c.match_rnk  
 
left join ga_App app 
on app.membernumber=b.membernumber 
 
 
where  1=1  
and year(date_add(b.order_end_dt,2))=2020 
AND lower(item_promo_name) like '%&%' 
and  COALESCE(c.type_rnk, 0)  <> 1  
--and cad.cad_complete_dt>b.member_join_dt 
 
GROUP BY 1,2 
ORDER BY 2,1 
with redemption as ( 
select distinct member_number, time_stamp from m4m.return_feed_header 
where partner not in ('IAG','NRMA Multi Policy Discount','NRMA MPD Insurance','NRMA Insurance') 
and time_stamp between '2019-01-01' and '2020-12-31' 
) 
 
SELECT  case when red.member_number is null then 'N' else 'Y' end redeemer 
        , case when lower(item_promo_name) like '%join%' then 'Join&Go' 
                when lower(item_promo_name) like '%add%' then 'Add&Go' 
                else 'others' end product_name 
        , count(DISTINCT contact_id) 
        , sum(1)                      AS renewals 
        , sum(renewal_cd)           as renewed 
        , sum(case when  b.renewed_completed_dt <= date_add(order_end_dt,2) then 1 else 0 end  ) as renewed_pot 
         
from sandpit.renewal_base b 
 
inner join sandpit.util_renew_summ c  
on b.match_rnk = c.match_rnk 
 
left join redemption red 
on red.member_number=b.membernumber 
 
 
where  1=1  
and year(date_add(b.order_end_dt,2))=2020 
AND lower(item_promo_name) like '%&%' 
and  COALESCE(c.type_rnk, 0)  <> 1  
--and cad.cad_complete_dt>b.member_join_dt 
 
GROUP BY 1,2 
ORDER BY 2,1 
--checks opens and clicks 
--then profile which partner 
--loyalty colour and usual stuff 
with opens as ( 
select distinct customer_id, send_event_date, open_event_date, click_event_date from omc.send_level_summary 
where send_event_date>'2018-12-31' 
) 
 
SELECT   case when lower(item_promo_name) like '%join%' then 'Join&Go' 
                when lower(item_promo_name) like '%add%' then 'Add&Go' 
                else 'others' end product_name 
        , count(DISTINCT contact_id) 
        , sum(1)                      AS renewals 
        , sum(renewal_cd)           as renewed 
        , sum(case when  b.renewed_completed_dt <= date_add(order_end_dt,2) then 1 else 0 end  ) as renewed_pot 
         
from sandpit.renewal_base b 
 
inner join sandpit.util_renew_summ c  
on b.match_rnk = c.match_rnk 
 
left join opens red 
on red.customer_id=b.contact_id 
 
 
where  1=1  
and year(date_add(b.order_end_dt,2))=2020 
AND lower(item_promo_name) like '%&%' 
and  COALESCE(c.type_rnk, 0)  <> 1  
and datediff(b.order_start_dt,red.send_event_date)>=365 
--and cad.cad_complete_dt>b.member_join_dt 
 
GROUP BY 1 
ORDER BY 1 
with opens as ( 
select distinct customer_id, send_event_date, open_event_date, click_event_date from omc.send_level_summary 
where open_event_date is not null 
and send_event_date>'2018-12-31' 
) 
 
SELECT  case when red.member_number is null then 'N' else 'Y' end redeemer 
        , case when lower(item_promo_name) like '%join%' then 'Join&Go' 
                when lower(item_promo_name) like '%add%' then 'Add&Go' 
                else 'others' end product_name 
        , sum(1) as renewals 
        , sum(case when b.renewed_order_id is null then 0 else 1 end) as renewed 
        , sum(case when  b.renewed_completed_dt <= date_add(order_end_dt,2) then 1 else 0 end  ) as renewed_pot 
 
from sandpit.renewal_base_old b 
 
left join opens red 
on red.customer_id=b.contact_id 
 
 
where  1=1  
and year(date_add(b.order_end_dt,2))=2020 
and datediff(b.order_start_dt,red.send_event_date)=365 
AND lower(item_promo_name) like '%&%' 
--and cad.cad_complete_dt>b.member_join_dt 
 
GROUP BY 1,2 
ORDER BY 2,1 
select case when renewed_order_id is null then 'Non-renewed' else 'Renewed' end renewal 
        , e.score 
    --   , case when r.item_net_price=0 then 'Free_year'  
    --             when r.item_net_price=48 then 'Paid_year1' 
    --             when r.item_net_price=96 or r.item_net_price=144 then 'Paid_year2' 
    --             --when item_gross_amt=144 then 'Paid_2years' 
    --             else 'others' end f2go_type 
        ,count(distinct r.contact_id) 
 
from sandpit.renewal_base r 
 
left join sandpit.engagement_score_staging e 
on e.membernumber=r.membernumber 
 
where prod_name='Free2go' 
and to_date(date_add(r.order_end_dt,2)) between '2019-01-01' and '2019-12-31'  
 
group by 1,2 
order by 1,2 
select case when r.renewed_order_id is null then 0 else 1 end  as renewed 
       , case when r.item_net_price=0 then 'Free_year'  
                when r.item_net_price=48 then 'Paid_year1' 
                when r.item_net_price=96 or r.item_net_price=144 then 'Paid_year2' 
                else 'others' end f2go_type 
        ,count(distinct r.contact_id) 
 
from sandpit.renewal_base r 
 
inner join sandpit.model_cad_order cad 
on cad.membernumber=r.membernumber 
and cad.integration_id=r.integration_id 
and cad.order_id=r.order_id 
 
where prod_name='Free2go' 
and to_date(date_add(r.order_end_dt,2)) between '2019-01-01' and '2019-12-31'  
 
group by case when r.renewed_order_id is null then 0 else 1 end 
       , case when r.item_net_price=0 then 'Free_year'  
                when r.item_net_price=48 then 'Paid_year1' 
                when r.item_net_price=96 or r.item_net_price=144 then 'Paid_year2' 
                --when item_gross_amt=144 then 'Paid_2years' 
                else 'others' end 
order by renewed 
       , f2go_type 
SELECT * FROM googleanalytics.appdata ga 
--select distinct  x_user_id from gms.s_src_payment 
-- SELECT   
--             order_id  
--         ,   src.created 
--         ,   received_dt 
--         ,   row_number() over (partition by order_id order by received_dt ) as first_txn 
--         ,   mss_map.order_channel 
--         ,   pay_type_map.pay_type_clean 
--         ,   x_iag_loc_desc 
--         ,   x_iag_loc_name 
--         ,   users.login 
--         FROM gms.s_src_payment src 
--         INNER JOIN gms.s_user users 
--             ON src.created_by = users.row_id 
--         -- Payment Method 
--         LEFT JOIN cvm.pay_type_map pay_type_map  
--             ON src.pay_type_cd = pay_type_map.pay_type_cd 
--         -- identify MSS channel 
--         LEFT JOIN cvm.payment_channel_map mss_map 
--             ON  coalesce(src.x_nrma_source,'NULL') = mss_map.x_nrma_source 
--             AND coalesce(pay_type_map.pay_type_clean,'NULL') = mss_map.pay_type_clean 
--             AND COALESCE( 
--                     case when src.x_nrma_source in ('GMS')  
--                         and x_user_id in ('EAI_USER','BPAY_USER','IVR_USER','APO_USER')  
--                         then x_user_id else 'NULL'  
--                     end,'NULL') = mss_map.summ_user_id 
        WHERE src.pay_stat_cd in ('Reconciled','Payment Taken') 
        AND order_id is not null 
select * from googleanalytics.appdata limit 10; 
SELECT  case when renewed_order_id is null then 0 else 1 end  as renewed 
       -- , case when  b.renewed_completed_dt <= date_add(order_end_dt,2) then 1 else 0 end  as renewed_pot 
        , count(DISTINCT b.contact_id) 
     --   , count(ga.membernumber) 
 
from sandpit.renewal_base b 
 
INNER join googleanalytics.appdata ga 
on b.membernumber=ga.membernumber 
 
where prod_name='Free2go' 
and to_date(date_add(order_end_dt,2)) between '2019-01-01' and '2019-12-31'  
and ga.eventyear='2019' 
--and item_base_price=96 
 
group by 1 
order by 1 
select  case when renewed_order_id is null then 0 else 1 end  as renewed 
       -- , case when  b.renewed_completed_dt <= date_add(order_end_dt,2) then 1 else 0 end  as renewed_pot 
        , count(DISTINCT r.contact_id) 
 
from sandpit.renewal_base r 
 
inner join m4m.return_feed_header p 
on r.membernumber=p.member_number 
 
where r.prod_name='Free2go' 
--and asset_status_cd='Active' 
and to_date(date_add(r.order_end_dt,2)) between '2019-01-01' and '2019-12-31'  
and to_date(date_add(p.time_stamp,2)) between '2019-01-01' and '2019-12-31'  
 
group by 1 
order by 1 
select  case when renewed_order_id is null then 0 else 1 end  as renewed 
       , case when p.partner  in ('IAG','NRMA Multi Policy Discount','NRMA MPD Insurance','NRMA Insurance','NRMA Insurance ControlPro','NRMA Insurance Branch') then 'IAG' 
            when (p.partner like 'NRMA Parks and Resorts%' or p.partner like '%Holiday Park') then 'NRMA Parks and Resorts' 
            else p.partner end member_benefit 
        , count(DISTINCT r.contact_id) 
 
from sandpit.renewal_base r 
 
inner join m4m.return_feed_header p 
on r.membernumber=p.member_number 
 
where r.prod_name='Free2go' 
--and asset_status_cd='Active' 
and to_date(date_add(r.order_end_dt,2)) between '2019-01-01' and '2019-12-31'  
and to_date(date_add(p.time_stamp,2)) between '2019-01-01' and '2019-12-31'  
 
group by 1,2 
order by 1,2 
 
spark.sql(''' 
select  p.member_number, r.contact_id, count(distinct p.trx_header_id) as freq, count(distinct p.partner) as partner_count 
 
from sandpit.renewal_base r 
 
inner join m4m.return_feed_header p 
on r.membernumber=p.member_number 
 
where r.prod_name='Free2go' 
--and asset_status_cd='Active' 
and to_date(date_add(r.order_end_dt,2)) between '2019-01-01' and '2019-12-31'  
and to_date(date_add(p.time_stamp,2)) between '2019-01-01' and '2019-12-31'  
and renewed_order_id is null 
 
group by 1,2 
''').show(100000,False) 
spark.sql(''' 
select  p.member_number, r.contact_id, count(distinct p.trx_header_id) as freq, count(distinct p.partner) as partner_count 
 
from sandpit.renewal_base r 
 
inner join m4m.return_feed_header p 
on r.membernumber=p.member_number 
 
where r.prod_name='Free2go' 
--and asset_status_cd='Active' 
and to_date(date_add(r.order_end_dt,2)) between '2019-01-01' and '2019-12-31'  
and to_date(date_add(p.time_stamp,2)) between '2019-01-01' and '2019-12-31'  
and renewed_order_id is not null 
 
group by 1,2 
''').show(100000,False) 
select count(distinct r.contact_id) 
--,r.order_id, r.contact_cd, r.membernumber, r.member_colour, r.member_gender, r.member_join_dt, r.member_loyalty_colour, r.member_type, r.tenure_member 
         
 
from sandpit.renewal_base r     --50322 
 
-- inner join gms.s_contact c      --40193 
-- on c.row_id=r.contact_id 
-- and case when trim(c.x_inv_email_1) = 'Y' or c.email_addr is null then 'N' else 'Y' end = 'Y' 
 
-- inner join gms.s_contact_x cx   --38652 
-- on cx.par_row_id=c.row_id 
-- and (cx.attrib_36 ='Yes' or Upper(cx.attrib_36) ='NULL')  
 
-- inner join omc.send_level_summary e     --38526 
-- on e.customer_id=c.row_id 
-- and e.channel='Email'               --38332 
-- -- and (e.open_event_date is not null or e.click_event_date is not null)   --34011 
-- and e.click_event_date is not null          --13400 
 
 
where r.prod_name='Free2go' 
and to_date(date_add(r.order_end_dt,2)) between '2019-01-01' and '2019-12-31'  
select distinct campaign_name, to_date(e.send_event_date) from omc.campaign_info c 
 
inner join omc.send_level_summary e     --38526 
on e.campaign_id=c.campaign_id 
and e.channel='Email'       
 
where lower(campaign_name) like '%onboarding%' 
--and channel='Email' 
and e.customer_id='1-AS9QWV1' 
order by 2 
select case when renewed_order_id is null then 'Non-renewed' else 'Renewed' end renewal 
        , count(distinct r.contact_id) 
--,r.order_id, r.contact_cd, r.membernumber, r.member_colour, r.member_gender, r.member_join_dt, r.member_loyalty_colour, r.member_type, r.tenure_member 
         
 
from sandpit.renewal_base r     --50322 
 
inner join omc.send_level_summary e     --38526 
on e.customer_id=r.contact_id 
and e.channel='Email'               --38332 
--and (e.open_event_date is not null or e.click_event_date is not null)   --34011 
--and e.click_event_date is not null          --13400 
 
inner join omc.campaign_info n 
on n.campaign_id=e.campaign_id  
and n.channel='Email' 
and n.campaign_name not like '%CMO%' 
and n.campaign_name not like '%UAT%' 
and n.campaign_name not like '%E09%' 
 
where r.prod_name='Free2go' 
and to_date(date_add(r.order_end_dt,2)) between '2019-01-01' and '2019-12-31'  
and e.unsubbed_event_date is not null 
--and (lower(n.campaign_name) like '%enl%' or lower(n.campaign_name) like '%enews%') 
and lower(n.campaign_name) like '%onboarding%' 
--and lower(n.campaign_name) like '%solus%' 
--and (lower(n.campaign_name) like '%renew%' or lower(n.campaign_name) like '%reminder%') 
 
group by 1 
-- with score as ( 
-- select e.membernumber, case when max(e.score in (0.5,0.95)) then 'clicked' 
--                                     when max(e.score in (0.4,0.85)) then 'opened' 
--                                     else 'not opened' end email_enagegment 
-- from sandpit.engagement_score_staging e 
-- group by e.membernumber)  
select   sum(case when (lower(n.campaign_name) like '%onboarding%') then 1 else 0 end) Onboarding 
        , sum(case when lower(n.campaign_name) like '%enl%'  then 1 else 0 end) as eNews 
        , sum(case when lower(n.campaign_name) like '%solus%' then 1 else 0 end) as Solus 
        , sum(case when lower(n.campaign_name) like '%renew%' then 1 else 0 end) as renewal 
        , sum(case when lower(n.campaign_name) like '%reminder%'then 1 else 0 end) as renewal_reminder  
        , sum(case when lower(n.campaign_name) like '%onboarding%' and (e.open_event_date is not null or e.click_event_date is not null) then 1 else 0 end) Onboarding_opened 
        , sum(case when lower(n.campaign_name) like '%enl%' and (e.open_event_date is not null or e.click_event_date is not null) then 1 else 0 end) as eNews_open 
        , sum(case when lower(n.campaign_name) like '%solus%' and (e.open_event_date is not null or e.click_event_date is not null) then 1 else 0 end) as Solus_open 
        , sum(case when lower(n.campaign_name) like '%renew%'and (e.open_event_date is not null or e.click_event_date is not null) then 1 else 0 end) as renewal_open 
        , sum(case when lower(n.campaign_name) like '%reminder%'and (e.open_event_date is not null or e.click_event_date is not null) then 1 else 0 end) as renewal_reminder_open 
        , sum(case when lower(n.campaign_name) like '%onboarding%' AND e.click_event_date is not null then 1 else 0 end) Onboarding_click 
        , sum(case when lower(n.campaign_name) like '%enl%' and e.click_event_date is not null   then 1 else 0 end) as eNews_click 
        , sum(case when lower(n.campaign_name) like '%solus%' and e.click_event_date is not null   then 1 else 0 end) as Solus_click 
        , sum(case when lower(n.campaign_name) like '%renew%'and e.click_event_date is not null  then 1 else 0 end) as renewal_click 
        , sum(case when lower(n.campaign_name) like '%reminder%'and e.click_event_date is not null  then 1 else 0 end) as renewal_reminder_click 
 
from sandpit.renewal_base r     --50322 
 
inner join omc.send_level_summary e     
on e.customer_id=r.contact_id 
and e.channel='Email'  
 
inner join omc.campaign_info n 
on n.campaign_id=e.campaign_id  
and n.channel='Email' 
and n.campaign_name not like '%CMO%' 
and n.campaign_name not like '%UAT%' 
and n.campaign_name not like '%E09%' 
 
where r.prod_name='Free2go' 
and to_date(date_add(r.order_end_dt,2)) between '2019-01-01' and '2019-12-31'  
and to_date(e.send_event_date) between '2019-01-01' and '2019-12-31'  
with emails as( 
select distinct c.campaign_name 
        , e.send_event_date  
        , e.customer_id 
        , e.member_number 
        , e.open_event_date 
        , e.bounce_event_date 
        , e.click_event_date 
        , e.unsubbed_event_date 
        , case when lower(c.campaign_name) like '%onboarding%' then 'On-boarding' 
                when (lower(c.campaign_name) like '%enl%' or lower(c.campaign_name) like '%enews%') then 'eNews' 
                when lower(c.campaign_name) like '%solus%' then 'Solus' 
                when (lower(c.campaign_name) like '%renew%' or lower(c.campaign_name) like '%reminder%') then 'renewal' 
                else 'others' end email_type 
  
 
from omc.send_level_summary e 
 
inner join omc.campaign_info c 
on c.campaign_id=e.campaign_id  
 
-- inner join sandpit.renewal_base r 
-- on r.contact_id=e.customer_id 
-- and r.prod_name='Free2go' 
-- and r.order_completed_dt between '2019-01-01' and '2019-12-31' 
 
where 1=1 
--e.customer_id='1-DN1AP75' 
and c.channel='Email' 
and c.campaign_name not like '%CMO%' 
and c.campaign_name not like '%UAT%' 
and c.campaign_name not like '%E09%' 
) 
select * from emails 
select case when renewed_order_id is null then 'Non-renewed' else 'Renewed' end renewal_status 
    ,count(distinct contact_id) 
--prod_f2g_split,renewed_prod_f2g_split,*  
from sandpit.renewal_base r 
 
-- inner join m4m.return_feed_header p 
-- on r.membernumber=p.member_number 
 
-- inner join googleanalytics.appdata app 
-- on app.membernumber=r.membernumber 
-- and app.eventyear='2019' 
 
where r.prod_name='Free2go' 
--and asset_status_cd='Active' 
and to_date(date_add(r.order_end_dt,2)) between '2019-01-01' and '2019-12-31'  
--and to_date(date_add(p.time_stamp,2)) between '2019-01-01' and '2019-12-31'  
 
and r.nrp_prod_name='CC' 
--and case when renewed_order_id is null then 'Non-renewed' else 'Renewed' end='Renewed' 
 
 
group by 1 
order by 1 
select case when renewed_order_id is null then 0 else 1 end  as renewed 
    --   , case when p.partner  in ('IAG','NRMA Multi Policy Discount','NRMA MPD Insurance','NRMA Insurance','NRMA Insurance ControlPro','NRMA Insurance Branch') then 'IAG' 
    --         when (p.partner like 'NRMA Parks and Resorts%' or p.partner like '%Holiday Park') then 'NRMA Parks and Resorts' 
    --         else p.partner end member_benefit 
        , count(DISTINCT r.contact_id) 
 
from sandpit.renewal_base r 
 
inner join m4m.return_feed_header p 
on r.membernumber=p.member_number 
and to_date(date_add(p.time_stamp,2)) between '2019-01-01' and '2019-12-31' 
 
where r.prod_name='Free2go' 
--and asset_status_cd='Active' 
and to_date(date_add(r.order_end_dt,2)) between '2019-01-01' and '2019-12-31'  
--and prod_f2g_split='Free2go NRP_CC' 
 
group by 1 
order by 1 
select distinct prod_name from sandpit.renewal_base 
order by 1 
select  case when p.partner  in ('IAG','NRMA Multi Policy Discount','NRMA MPD Insurance','NRMA Insurance','NRMA Insurance ControlPro','NRMA Insurance Branch') then 'IAG' 
            when (p.partner like 'NRMA Parks and Resorts%' or p.partner like '%Holiday Park') then 'NRMA Parks and Resorts' 
            else p.partner end member_benefit 
        , count(distinct r.contact_id) 
 
from sandpit.renewal_base r 
 
-- inner join googleanalytics.appdata app 
-- on app.membernumber=r.membernumber 
-- and app.eventyear='2019' 
 
inner join m4m.return_feed_header p 
on r.membernumber=p.member_number 
and to_date(date_add(p.time_stamp,2)) between '2019-01-01' and '2019-12-31'  
 
where r.prod_name like '%Classic Care' 
and to_date(date_add(r.order_end_dt,2)) between '2019-01-01' and '2019-12-31'  
and renewed_order_id is not null 
 
 
group by 1 
order by 2 desc 
select case when r.renewed_item_net_price>0 then 'free2paid' 
            when r.renewed_order_id is not null then 'free2free' 
            else 'not-renewed' end  renewat_status 
      , case when p.partner  in ('IAG','NRMA Multi Policy Discount','NRMA MPD Insurance','NRMA Insurance','NRMA Insurance ControlPro','NRMA Insurance Branch') then 'IAG' 
            when (p.partner like 'NRMA Parks and Resorts%' or p.partner like '%Holiday Park') then 'NRMA Parks and Resorts' 
            else p.partner end member_benefit 
        , count(DISTINCT r.contact_id) 
 
from sandpit.renewal_base r 
 
-- inner join googleanalytics.appdata app 
-- on app.membernumber=r.membernumber 
 
inner join m4m.return_feed_header p 
on r.membernumber=p.member_number 
and to_date(date_add(p.time_stamp,2)) between '2019-01-01' and '2019-12-31'  
 
where r.prod_name='Free2go' 
and to_date(date_add(r.order_end_dt,2)) between '2019-01-01' and '2019-12-31'  
and r.item_net_price=0 
-- and r.ren 
--and app.eventyear='2019' 
 
group by 1,2 
order by 1,2 
select  case when renewed_order_id is null then 0 else 1 end  as renewed 
        , count(distinct r.contact_id) 
 
from sandpit.renewal_base r 
 
-- inner join googleanalytics.appdata app 
-- on app.membernumber=r.membernumber 
-- and app.eventyear='2019' 
 
inner join m4m.return_feed_header p 
on r.membernumber=p.member_number 
and to_date(date_add(p.time_stamp,2)) between '2019-01-01' and '2019-12-31'  
 
where r.prod_name='Free2go' 
and to_date(date_add(r.order_end_dt,2)) between '2019-01-01' and '2019-12-31'  
and r.item_net_price=48 
-- -- 
 
group by 1 
order by 1 
select  upper(app.tab) 
        , count(distinct r.contact_id) 
 
from sandpit.renewal_base r 
 
inner join googleanalytics.appdata app 
on app.membernumber=r.membernumber 
and app.eventyear='2019' 
 
-- inner join m4m.return_feed_header p 
-- on r.membernumber=p.member_number 
-- and to_date(date_add(p.time_stamp,2)) between '2019-01-01' and '2019-12-31'  
 
where r.prod_name='Free2go' 
and to_date(date_add(r.order_end_dt,2)) between '2019-01-01' and '2019-12-31'  
and renewed_order_id is not null 
and eventname='main_tab_click' 
 
group by 1 
order by 1 
 
 
 
-- SELECT upper(appcategory), count(membernumber) as before_count from googleanalytics.appdata  
-- where eventname = 'map_detail_view'  
-- and eventtime BETWEEN '2020-01-27' and '2020-03-21' 
select  count(distinct r.contact_id) 
 
from sandpit.renewal_base r 
 
inner join googleanalytics.appdata app 
on app.membernumber=r.membernumber 
and app.eventyear='2019' 
 
-- inner join m4m.return_feed_header p 
-- on r.membernumber=p.member_number 
-- and to_date(date_add(p.time_stamp,2)) between '2019-01-01' and '2019-12-31'  
 
where r.prod_name like '%Classic Care' 
and to_date(date_add(r.order_end_dt,2)) between '2019-01-01' and '2019-12-31'  
and renewed_order_id is not null 
--and eventname='main_tab_click' 
--and upper(app.tab)='MAP' 
 
-- group by 1 
-- order by 1 
 
 
 
-- SELECT upper(appcategory), count(membernumber) as before_count from googleanalytics.appdata  
-- where eventname = 'map_detail_view'  
-- and eventtime BETWEEN '2020-01-27' and '2020-03-21' 
select  prod_F2G_split 
         
        , count(distinct contact_id) 
                             
--, item_net_price  
 
from sandpit.renewal_base 
where prod_name='Free2go' 
--and item_base_price=96 
and to_date(date_add(order_end_dt,2)) between '2019-01-01' and '2019-12-31' 
 
group by 1 
order by 1 
select distinct item_net_price 
--, item_gross_amt,item_net_price,renewed_item_base_price, renewed_item_net_price 
       -- ,*  
from sandpit.renewal_base 
where prod_name='Free2go' 
--and contact_id='1-AOF62Y8' 
with f2g as ( 
select  prod_F2G_split 
        , case when item_net_price=0 then 'Free_year'  
                when item_net_price=48 then 'Paid_year1' 
                when item_net_price=96 or item_net_price=144 then 'Paid_year2' 
                --when item_gross_amt=144 then 'Paid_2years' 
                else 'others' end f2go_type 
        , contact_id 
        , item_base_price 
, item_gross_amt,item_net_price,renewed_item_base_price, renewed_item_net_price 
                             
--, item_net_price  
 
from sandpit.renewal_base 
where prod_name='Free2go' 
--and item_base_price=96 
and to_date(date_add(order_end_dt,2)) between '2019-01-01' and '2019-12-31' 
) 
select * from f2g 
where 1=1 
--prod_F2G_split='Free2go Year 1.0' 
and f2go_type='Paid_year1' 
select case when renewed_order_id is null then 0 else 1 end  as renewed 
        ,count(distinct r.contact_id) 
--,r.order_id, r.contact_cd, r.membernumber, r.member_colour, r.member_gender, r.member_join_dt, r.member_loyalty_colour, r.member_type, r.tenure_member 
         
 
from sandpit.renewal_base r     --50322 
 
-- inner join googleanalytics.appdata app 
-- on app.membernumber=r.membernumber 
-- and app.eventyear='2019' 
 
inner join m4m.return_feed_header p 
on r.membernumber=p.member_number 
and to_date(date_add(p.time_stamp,2)) between '2019-01-01' and '2019-12-31'  
 
 
where r.prod_name='Free2go' 
and to_date(date_add(r.order_end_dt,2)) between '2019-01-01' and '2019-12-31'  
 
group by 1 
order by 1 
select case when renewed_order_id is null then 0 else 1 end  as renewed 
            , count(distinct r.contact_id) 
        -- , r.prod_f2g_split 
        -- , r.renewed_prod_f2g_split 
        -- , r.item_net_price 
        -- , r.renewed_item_net_price 
        -- , r.order_id 
        -- , r.contact_id 
        -- , r.prod_name 
        -- , r.nrp_prod_name 
        -- , r.prod_tenure_split 
        -- , r.tenure_member 
        -- , r.renewed_tenure_member 
        -- , r.member_join_dt  
 
from sandpit.renewal_base r 
 
where r.tenure_member=0 
and r.prod_name='Free2go' 
and to_date(r.member_join_dt) between '2018-01-01' and '2018-12-31'  
 
group by 1 
order by 1 
 
-- group by case when r.item_net_price=0 and r.renewed_item_net_price=0 then 'Free2free' 
--             when r.item_net_price=0 and r.renewed_item_net_price=48 then 'Free2paid' 
--             when r.item_net_price=48 and r.renewed_item_net_price=96 then 'Paid2paid' 
--             when r.nrp_prod_name='CC' then 'Paid2CC' 
--             when r.item_net_price=0 then 'Free_year'  
--             when r.item_net_price=48 then 'Paid_year1' 
--             when r.item_net_price=96 or r.item_net_price=144 then 'Paid_year2' 
--             else 'Other' end 
--         , case when r.renewed_order_id is null then 0 else 1 end 
-- order by prod_type, renewed 
-- with eligible as( 
-- select * from sandpit.renewal_base 
-- where prod_name='Free2go' 
-- and to_date(date_add(order_end_dt,2)) between '2019-01-01' and '2019-12-31'  
-- ) 
select  
        case when r.item_net_price=0 and r.renewed_item_net_price=0 then 'Free2free' 
            when r.item_net_price=0 and r.renewed_item_net_price=48 then 'Free2paid' 
            when r.item_net_price=48 and r.renewed_item_net_price=96 then 'Paid2paid' 
            when r.nrp_prod_name='CC' then 'Paid2CC' 
            when r.item_net_price=0 then 'Free_year'  
            when r.item_net_price=48 then 'Paid_year1' 
            when r.item_net_price=96 or r.item_net_price=144 then 'Paid_year2' 
            else 'Other' end prod_type 
        , case when r.renewed_order_id is null then 0 else 1 end  as renewed 
        , count(distinct r.contact_id) 
        -- , r.prod_f2g_split 
        -- , r.renewed_prod_f2g_split 
        -- , r.item_net_price 
        -- , r.renewed_item_net_price 
        -- , r.order_id 
        -- , r.contact_id 
        -- , r.prod_name 
        -- , r.nrp_prod_name 
        -- , r.prod_tenure_split 
        -- , r.tenure_member 
        -- , r.renewed_tenure_member 
        -- , r.member_join_dt  
 
from sandpit.renewal_base r 
 
inner join eligible e 
on e.contact_id=r.contact_id 
 
where r.tenure_member=0 
 
group by case when r.item_net_price=0 and r.renewed_item_net_price=0 then 'Free2free' 
            when r.item_net_price=0 and r.renewed_item_net_price=48 then 'Free2paid' 
            when r.item_net_price=48 and r.renewed_item_net_price=96 then 'Paid2paid' 
            when r.nrp_prod_name='CC' then 'Paid2CC' 
            when r.item_net_price=0 then 'Free_year'  
            when r.item_net_price=48 then 'Paid_year1' 
            when r.item_net_price=96 or r.item_net_price=144 then 'Paid_year2' 
            else 'Other' end 
        , case when r.renewed_order_id is null then 0 else 1 end 
order by prod_type, renewed 
case when r.item_net_price=0 and r.renewed_item_net_price=0 then 'Free2free' 
            when r.item_net_price=0 and r.renewed_item_net_price=48 then 'Free2paid' 
            when r.item_net_price=48 and r.renewed_item_net_price=96 then 'Paid2paid' 
            when r.nrp_prod_name='CC' then 'Paid2CC' 
            when r.item_net_price=0 then 'Free_year'  
            when r.item_net_price=48 then 'Paid_year1' 
            when r.item_net_price=96 or r.item_net_price=144 then 'Paid_year2' 
            else 'Other' end 
             
            case when r.renewed_item_net_price=0 then 'Free2Free' else 'Paid' end type  
        , case when renewed_order_id is null then 0 else 1 end  as renewed,count(distinct r.contact_id) 
         
        -- inner join gms.s_contact_x cx   --38652 
-- on cx.par_row_id=c.row_id 
-- and (cx.attrib_36 ='Yes' or Upper(cx.attrib_36) ='NULL')  
 
-- inner join omc.send_level_summary e     --38526 
-- on e.customer_id=c.row_id 
-- and e.channel='Email'               --38332 
-- --and (e.open_event_date is not null or e.click_event_date is not null)   --34011 
-- and e.click_event_date is not null          --13400 
 
-- inner join googleanalytics.appdata app 
-- on app.membernumber=r.membernumber 
-- and app.eventyear='2019' 
 
-- inner join m4m.return_feed_header p 
-- on r.membernumber=p.member_number 
-- and to_date(date_add(p.time_stamp,2)) between '2019-01-01' and '2019-12-31'  
 
inner join gms.s_contact c      --40193 
on c.row_id=r.contact_id 
and case when trim(c.x_inv_email_1) = 'Y' or c.email_addr is null then 'N' else 'Y' end = 'Y' 
 
select case when r.renewed_item_net_price=0 then 'Free2Free' else 'Paid' end type 
    ,case when renewed_order_id is null then 0 else 1 end  as renewed 
    ,count(distinct r.contact_id) 
--,r.order_id, r.contact_cd, r.membernumber, r.member_colour, r.member_gender, r.member_join_dt, r.member_loyalty_colour, r.member_type, r.tenure_member 
         
 
from sandpit.renewal_base r     --50322 
 
-- inner join gms.s_contact c      --40193 
-- on c.row_id=r.contact_id 
-- and case when trim(c.x_inv_email_1) = 'Y' or c.email_addr is null then 'N' else 'Y' end = 'Y' 
 
-- inner join gms.s_contact_x cx   --38652 
-- on cx.par_row_id=c.row_id 
-- and (cx.attrib_36 ='Yes' or Upper(cx.attrib_36) ='NULL')  
 
-- inner join omc.send_level_summary e     --38526 
-- on e.customer_id=c.row_id 
-- and e.channel='Email'               --38332 
-- --and (e.open_event_date is not null or e.click_event_date is not null)   --34011 
-- and e.click_event_date is not null          --13400 
 
inner join googleanalytics.appdata app 
on app.membernumber=r.membernumber 
and app.eventyear='2019' 
 
-- inner join m4m.return_feed_header p 
-- on r.membernumber=p.member_number 
-- and to_date(date_add(p.time_stamp,2)) between '2019-01-01' and '2019-12-31'  
 
where r.prod_name='Free2go' 
and to_date(date_add(r.order_end_dt,2)) between '2019-01-01' and '2019-12-31'  
and r.item_net_price=0 
--and r.nrp_prod_name ='CC' 
 
group by 1,2 
order by 1,2 
select case when r.nrp_prod_name='CC' then 'NRP_CC' 
            when r.item_net_price>0 then 'Paid2paid' 
            when r.item_net_price=0  then 'Free2paid' 
            else 'Other' end prod_type 
        , case when r.renewed_order_id is null then 0 else 1 end  as renewed 
        , count(distinct r.contact_id) 
 
from sandpit.renewal_base r 
 
left join sandpit.engagement_score_staging es 
on es.membernumber=r.membernumber 
 
where r.prod_name='Free2go' 
and to_date(date_add(r.order_end_dt,2)) between '2019-01-01' and '2019-12-31'  
--and r.item_net_price=0 
 
group by 1,2 
order by 1,2 
 
 select r.prod_f2g_split, count(distinct r.contact_id) 
 --r.renewed_item_net_price,r.nrp_prod_name, count(distinct r.contact_id) 
 
from sandpit.renewal_base r 
 
left join sandpit.engagement_score_staging es 
on es.membernumber=r.membernumber 
 
where r.prod_name='Free2go' 
and to_date(date_add(r.order_end_dt,2)) between '2019-01-01' and '2019-12-31'  
--and r.item_net_price=0 
and r.nrp_prod_name!='CC' 
and r.item_net_price=48 
and r.prod_f2g_split='Free2go NRP_CC' 
group by 1 
with emails as( 
select distinct c.campaign_name 
        , e.send_event_date  
        , e.customer_id 
        , e.member_number 
        , e.open_event_date 
        , e.bounce_event_date 
        , e.click_event_date 
        , e.unsubbed_event_date 
        , case when lower(c.campaign_name) like '%onboarding%' then 'On-boarding' 
                when (lower(c.campaign_name) like '%enl%' or lower(c.campaign_name) like '%enews%') then 'eNews' 
                when lower(c.campaign_name) like '%solus%' then 'Solus' 
                when (lower(c.campaign_name) like '%renew%' or lower(c.campaign_name) like '%reminder%') then 'renewal' 
                else 'others' end email_type 
  
 
from omc.send_level_summary e 
 
inner join omc.campaign_info c 
on c.campaign_id=e.campaign_id  
 
inner join sandpit.renewal_base r 
on r.contact_id=e.customer_id 
and r.prod_name='Free2go' 
and r.order_completed_dt between '2019-01-01' and '2019-12-31' 
 
where 1=1 
--e.customer_id='1-DN1AP75' 
and c.channel='Email' 
and c.campaign_name not like '%CMO%' 
and c.campaign_name not like '%UAT%' 
and c.campaign_name not like '%E09%' 
) 
select * from emails 
select case when renewed_order_id is null then 'Non-renewed' else 'Renewed' end renewal 
        -- , case when item_net_price=0 then 'Free_year'  
        --         when item_net_price=48 then 'Paid_year1' 
        --         when item_net_price=96 or item_net_price=144 then 'Paid_year2' 
        --         --when item_gross_amt=144 then 'Paid_2years' 
        --         else 'others' end prod_type 
        --, case when (e.open_event_date is not null or e.click_event_date is not null) then 'Open' else 'Non-Open' end type 
        , count(distinct r.contact_id) 
         
from sandpit.renewal_base r     --50322 
 
left anti join omc.send_level_summary e     --38526 
on e.customer_id=r.contact_id 
and e.channel='Email'               --38332 
and (e.open_event_date is not null or e.click_event_date is not null) 
 
 
-- inner join omc.campaign_info n 
-- on n.campaign_id=e.campaign_id  
-- and n.channel='Email' 
-- and n.campaign_name not like '%CMO%' 
-- and n.campaign_name not like '%UAT%' 
-- and n.campaign_name not like '%E09%' 
 
where r.prod_name='Free2go' 
and to_date(date_add(r.order_end_dt,2)) between '2019-01-01' and '2019-12-31'  
--and e.unsubbed_event_date is not null 
--and (lower(n.campaign_name) like '%enl%' or lower(n.campaign_name) like '%enews%') 
--and lower(n.campaign_name) like '%onboarding%' 
--and lower(n.campaign_name) like '%solus%' 
--and (lower(n.campaign_name) like '%renew%' or lower(n.campaign_name) like '%reminder%') 
 
group by 1 
select case when r.nrp_prod_name='CC' then 'NRP_CC' 
            when r.item_net_price>0 then 'Paid2paid' 
            when r.item_net_price=0  then 'Free2paid' 
            else 'Other' end prod_type 
select case when renewed_order_id is null then 'Non-renewed' else 'Renewed' end renewal_status 
        , case when app.membernumber is not null then 'Y' else 'N' end app_user 
    ,count(distinct contact_id) 
     
from sandpit.renewal_base r 
 
-- inner join m4m.return_feed_header p 
-- on r.membernumber=p.member_number 
 
left join googleanalytics.appdata app 
on app.membernumber=r.membernumber 
and app.eventyear='2019' 
 
where r.prod_name='Free2go' 
and to_date(date_add(r.order_end_dt,2)) between '2019-01-01' and '2019-12-31' 
 
group by 1,2 
order by 1,2 