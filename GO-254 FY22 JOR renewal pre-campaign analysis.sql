SELECT b.item_promo_name 
        , count(DISTINCT contact_id) 
        , count(case when b.renewed_order_id is not null then contact_id else Null end) renewed_contacts 
        , sum(1)                      AS renewals 
        , sum(renewal_cd)           as renewed 
        , sum(case when b.renewed_completed_dt <= date_add(order_end_dt,2) then 1 else 0 end  ) as renewed_pot 
        , sum(renewal_cd)/sum(1) as renewal_rate 
 
from sandpit.renewal_base b 
 
inner join sandpit.util_renew_summ c  
on b.match_rnk = c.match_rnk  
 
where b.prod_name in ('Classic Care','Premium Care','Premium Plus') 
--lower(item_promo_name) like '%&%' 
--and lower(item_promo_name) like '%add%' 
and year(date_add(b.order_end_dt,2))=2020 
and  COALESCE(c.type_rnk, 0)  <> 1  
 
group by 1 
order by 1 
SELECT count(DISTINCT contact_id) 
        -- , sum(1)                      AS renewals 
        -- , sum(renewal_cd)           as renewed 
        -- , sum(case when b.renewed_completed_dt <= date_add(order_end_dt,2) then 1 else 0 end  ) as renewed_pot 
        -- , sum(renewal_cd)/sum(1) as renewal_rate 
 
from sandpit.renewal_base b 
 
inner join sandpit.util_renew_summ c  
on b.match_rnk = c.match_rnk  
 
where b.prod_type='RSA' 
and b.contact_cd<>'Business Contact' 
--and b.prod_name in ('Classic Care','Premium Care','Premium Plus') 
--and lower(item_promo_name) not like '%join%' 
and year(date_add(b.order_end_dt,2))=2020 
and  COALESCE(c.type_rnk, 0)  <> 1  
 
SELECT case when lower(item_promo_name) like '%add%' then 'Add&Go' 
            when b.prod_name in ('Classic Care','Premium Care','Premium Plus') then 'RSA' 
            else 'others' end type 
        , count(DISTINCT contact_id) 
        , sum(1)                      AS renewals 
 
from sandpit.renewal_base b 
 
inner join sandpit.util_renew_summ c  
on b.match_rnk = c.match_rnk  
 
where b.prod_type='RSA' 
and b.prod_name in ('Classic Care','Premium Care','Premium Plus') 
and lower(item_promo_name) not like '%join%' 
and date_add(b.order_end_dt,2) between '2021-07-01' and '2022-06-30' 
and COALESCE(c.type_rnk, 0)  <> 1  
 
group by 1 
order by 1 
SELECT case when lower(item_promo_name) like '%add%' then 'Add&Go' 
            when lower(item_promo_name) like '%join%' then 'Join&Go' 
            when b.prod_name in ('Classic Care','Premium Care','Premium Plus') then 'RSA' 
            else 'others' end type 
        , sum(1)                      AS renewals 
 
from sandpit.renewal_base b 
 
inner join sandpit.util_renew_summ c  
on b.match_rnk = c.match_rnk  
 
where b.prod_type='RSA' 
and b.prod_name in ('Classic Care','Premium Care','Premium Plus') 
and date_add(b.order_end_dt,2) between '2021-07-01' and '2022-06-30' 
and COALESCE(c.type_rnk, 0)  <> 1  
 
group by 1 
order by 1 
SELECT b.renewal_yyyymm 
        , sum(1)                      AS renewals 
 
from sandpit.renewal_base b 
 
inner join sandpit.util_renew_summ c  
on b.match_rnk = c.match_rnk  
 
where lower(item_promo_name) like '%add%' 
and date_add(b.order_end_dt,2) between '2021-07-01' and '2022-06-30' 
and COALESCE(c.type_rnk, 0)  <> 1  
 
group by 1 
order by 1 
SELECT case when lower(item_promo_name) like '%add%' then 'Add&Go' 
            when b.prod_name in ('Classic Care','Premium Care','Premium Plus') then 'RSA' 
            else 'others' end type 
        , count(DISTINCT contact_id) 
        , sum(1)                      AS renewals 
        , sum(renewal_cd)           as renewed 
        , sum(case when b.renewed_completed_dt <= date_add(order_end_dt,2) then 1 else 0 end  ) as renewed_pot 
        , sum(renewal_cd)/sum(1) as renewal_rate 
 
from sandpit.renewal_base b 
 
inner join sandpit.util_renew_summ c  
on b.match_rnk = c.match_rnk  
 
where b.prod_type='RSA' 
and b.prod_name in ('Classic Care','Premium Care','Premium Plus') 
and lower(item_promo_name) not like '%join%' 
and year(date_add(b.order_end_dt,2))=2020 
and  COALESCE(c.type_rnk, 0)  <> 1  
 
group by 1 
order by 1 
SELECT case when lower(item_promo_name) like '%add%' then 'Add&Go' 
            when b.prod_name in ('Classic Care','Premium Care','Premium Plus') then 'RSA' 
            else 'others' end type 
        , count(DISTINCT contact_id) 
        , sum(1)                      AS renewals 
        , sum(renewal_cd)           as renewed 
        , sum(case when b.renewed_completed_dt <= date_add(order_end_dt,2) then 1 else 0 end  ) as renewed_pot 
        , sum(renewal_cd)/sum(1) as renewal_rate 
 
from sandpit.renewal_base b 
 
inner join sandpit.util_renew_summ c  
on b.match_rnk = c.match_rnk  
 
where b.prod_type='RSA' 
and b.prod_name in ('Classic Care','Premium Care','Premium Plus') 
and lower(item_promo_name) not like '%join%' 
and year(date_add(b.order_end_dt,2))=2020 
and  COALESCE(c.type_rnk, 0)  <> 1  
 
group by 1 
order by 1 
SELECT b.prod_name 
        , case when lower(item_promo_name) like '%add%' then 'Add&Go' 
            when b.prod_name in ('Classic Care','Premium Care','Premium Plus') then 'RSA' 
            else 'others' end type 
        , count(DISTINCT contact_id) 
        , sum(1)                      AS renewals 
        , sum(renewal_cd)           as renewed 
        , sum(case when b.renewed_completed_dt <= date_add(order_end_dt,2) then 1 else 0 end  ) as renewed_pot 
        , sum(renewal_cd)/sum(1) as renewal_rate 
 
from sandpit.renewal_base b 
 
inner join sandpit.util_renew_summ c  
on b.match_rnk = c.match_rnk  
 
where b.prod_type='RSA' 
and b.prod_name in ('Classic Care','Premium Care','Premium Plus') 
and lower(item_promo_name) not like '%join%' 
and year(date_add(b.order_end_dt,2))=2020 
and  COALESCE(c.type_rnk, 0)  <> 1  
 
group by 1,2 
order by 2,1 
SELECT  case when lower(item_promo_name) like '%add%' then 'Add&Go' 
            when b.prod_name in ('Classic Care','Premium Care','Premium Plus') then 'RSA' 
            else 'others' end type 
        , CASE 
        WHEN 
            ( 
                b.owner_postcode >= 1215 AND b.owner_postcode <= 2082 OR 
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
            'RURAL' 
         
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
            'UNKNOWN' 
    END AS region 
    , count(DISTINCT b.contact_id) 
    , sum(renewal_cd)/sum(1) as renewal_rate 
 
from sandpit.renewal_base b 
 
inner join sandpit.util_renew_summ c  
on b.match_rnk = c.match_rnk  
 
where b.prod_type='RSA' 
and b.prod_name in ('Classic Care','Premium Care','Premium Plus') 
and lower(item_promo_name) not like '%join%' 
and year(date_add(b.order_end_dt,2))=2020 
and  COALESCE(c.type_rnk, 0)  <> 1  
 
group by 1,2 
order by 1,2 
SELECT case when b.tenure_member<0 then 'error' 
            when b.tenure_member<6 then '0-5' 
            when b.tenure_member<11 then '6-10' 
            when b.tenure_member<20 then '11-19' 
            when b.tenure_member>=20 then '20+' 
            else 'others' end tenure 
        , case when lower(item_promo_name) like '%add%' then 'Add&Go' 
            when b.prod_name in ('Classic Care','Premium Care','Premium Plus') then 'RSA' 
            else 'others' end type 
        , count(DISTINCT contact_id) 
        , sum(1)                      AS renewals 
        , sum(renewal_cd)           as renewed 
        , sum(case when b.renewed_completed_dt <= date_add(order_end_dt,2) then 1 else 0 end  ) as renewed_pot 
        , sum(renewal_cd)/sum(1) as renewal_rate 
 
from sandpit.renewal_base b 
 
inner join sandpit.util_renew_summ c  
on b.match_rnk = c.match_rnk  
 
where b.prod_type='RSA' 
and b.prod_name in ('Classic Care','Premium Care','Premium Plus') 
and lower(item_promo_name) not like '%join%' 
and year(date_add(b.order_end_dt,2))=2020 
and  COALESCE(c.type_rnk, 0)  <> 1  
 
group by 1,2 
order by 2,1 
select case when lower(item_promo_name) like '%add%' then 'Add&Go' 
            when b.prod_name in ('Classic Care','Premium Care','Premium Plus') then 'RSA' 
            else 'others' end type 
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
 
where b.prod_type='RSA' 
and b.prod_name in ('Classic Care','Premium Care','Premium Plus') 
and lower(item_promo_name) not like '%join%' 
and year(date_add(b.order_end_dt,2))=2020 
and  COALESCE(u.type_rnk, 0)  <> 1  
and (a.open_event_date is not null or click_event_date is not null) 
and datediff(b.order_end_dt,a.send_event_date) between 0 and 365 
and a.channel='Email' 
and control is false 
 
group by 1,2 
order by 1,2 
select case when lower(item_promo_name) like '%add%' then 'Add&Go' 
            when b.prod_name in ('Classic Care','Premium Care','Premium Plus') then 'RSA' 
            else 'others' end type 
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
 
where b.prod_type='RSA' 
and b.prod_name in ('Classic Care','Premium Care','Premium Plus') 
and lower(item_promo_name) not like '%join%' 
and year(date_add(b.order_end_dt,2))=2020 
and  COALESCE(u.type_rnk, 0)  <> 1  
and click_event_date is not null 
and datediff(b.order_end_dt,a.send_event_date) between 0 and 365 
and a.channel='Email' 
and control is false 
 
group by 1,2 
order by 1,2 
---App enagagement and redemption and renewal rate 
with ga_App as ( 
select distinct membernumber from googleanalytics.appdata 
where eventyear in ('2019','2020') 
), redemption as ( 
select distinct member_number from m4m.return_feed_header 
where partner not in ('IAG','NRMA Multi Policy Discount','NRMA MPD Insurance','NRMA Insurance') 
and time_stamp between '2019-01-01' and '2020-12-31' 
), opens as ( 
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
SELECT case when lower(item_promo_name) like '%add%' then 'Add&Go' 
            when b.prod_name in ('Classic Care','Premium Care','Premium Plus') then 'RSA' 
            else 'others' end type 
        , count(DISTINCT contact_id) 
        , sum(renewal_cd)/sum(1) as RR 
        , sum(case when o.customer_id is not null then 1 else 0 end) opens 
        , sum(case when cl.customer_id is not null then 1 else 0 end) clicks 
        --, sum(case when r.member_number is not null then 1 else 0 end)  redeemer 
        --, sum(case when app.membernumber is not null then 1 else 0 end) app_user 
 
from sandpit.renewal_base b 
 
inner join sandpit.util_renew_summ c  
on b.match_rnk = c.match_rnk  
 
-- left join ga_App app 
-- on b.membernumber=app.membernumber 
 
-- left join redemption r 
-- on r.member_number=b.membernumber 
 
left join opens o 
on o.customer_id=b.contact_id 
 
left join clicks cl 
on cl.customer_id=b.contact_id 
 
 
where b.prod_type='RSA' 
and b.prod_name in ('Classic Care','Premium Care','Premium Plus') 
and lower(item_promo_name) not like '%join%' 
and year(date_add(b.order_end_dt,2))=2020 
and  COALESCE(c.type_rnk, 0)  <> 1  
 
group by 1 
order by 1 
SELECT b.member_colour 
        , case when lower(item_promo_name) like '%add%' then 'Add&Go' 
            when b.prod_name in ('Classic Care','Premium Care','Premium Plus') then 'RSA' 
            else 'others' end type 
        , count(DISTINCT contact_id) 
        , sum(1)                      AS renewals 
        , sum(renewal_cd)           as renewed 
        , sum(case when b.renewed_completed_dt <= date_add(order_end_dt,2) then 1 else 0 end  ) as renewed_pot 
        , sum(renewal_cd)/sum(1) as renewal_rate 
 
from sandpit.renewal_base b 
 
inner join sandpit.util_renew_summ c  
on b.match_rnk = c.match_rnk  
 
where b.prod_type='RSA' 
and b.prod_name in ('Classic Care','Premium Care','Premium Plus') 
and lower(item_promo_name) not like '%join%' 
and year(date_add(b.order_end_dt,2))=2020 
and  COALESCE(c.type_rnk, 0)  <> 1  
 
group by 1,2 
order by 2,1 
SELECT b.member_loyalty_colour 
        , case when lower(item_promo_name) like '%add%' then 'Add&Go' 
            when b.prod_name in ('Classic Care','Premium Care','Premium Plus') then 'RSA' 
            else 'others' end type 
        , count(DISTINCT contact_id) 
        , sum(1)                      AS renewals 
        , sum(renewal_cd)           as renewed 
        , sum(case when b.renewed_completed_dt <= date_add(order_end_dt,2) then 1 else 0 end  ) as renewed_pot 
        , sum(renewal_cd)/sum(1) as renewal_rate 
 
from sandpit.renewal_base b 
 
inner join sandpit.util_renew_summ c  
on b.match_rnk = c.match_rnk  
 
where b.prod_type='RSA' 
and b.prod_name in ('Classic Care','Premium Care','Premium Plus') 
and lower(item_promo_name) not like '%join%' 
and year(date_add(b.order_end_dt,2))=2020 
and  COALESCE(c.type_rnk, 0)  <> 1  
 
group by 1,2 
order by 2,1 
JOR_NPS = spark.read.load("/ user/ aamreen/ NPS_JOR_210705.csv",format="csv", sep=",", inferSchema="true", header="true") 
JOR_NPS.createOrReplaceTempView("JOR_NPS") 
spark.sql (""" create table campaign_data.aa_dmc2402_NPS_JOR_210705_20210713 as select * from JOR_NPS""").count() 
 
with NPS as( 
SELECT distinct c.member_number 
        , a.* 
 
from cad.cad_jobs c 
 
INNER JOIN campaign_data.aa_dmc2402_NPS_JOR_210705_20210713 a 
on cast(a.job_number as string)=cast(c.job_number as STRING) 
) 
SELECT n.nps 
        , case when lower(item_promo_name) like '%add%' then 'Add&Go' 
            when b.prod_name in ('Classic Care','Premium Care','Premium Plus') then 'RSA' 
            else 'others' end type 
        , count(DISTINCT contact_id) 
        , sum(1)                      AS renewals 
        , sum(renewal_cd)           as renewed 
        , sum(case when b.renewed_completed_dt <= date_add(order_end_dt,2) then 1 else 0 end  ) as renewed_pot 
        , sum(renewal_cd)/sum(1) as renewal_rate 
 
from sandpit.renewal_base b 
 
inner join sandpit.util_renew_summ c  
on b.match_rnk = c.match_rnk  
 
left join nps n 
on n.member_number=b.membernumber 
 
where b.prod_type='RSA' 
and b.prod_name in ('Classic Care','Premium Care','Premium Plus') 
and lower(item_promo_name) not like '%join%' 
and year(date_add(b.order_end_dt,2))=2020 
and  COALESCE(c.type_rnk, 0)  <> 1  
 
group by 1,2 
order by 2,1 
-- aamreen/ NPS_JOR_210705.csv 
SELECT  sum(1)                      AS renewals 
 
from sandpit.renewal_base b 
 
inner join sandpit.util_renew_summ c  
on b.match_rnk = c.match_rnk  
 
where item_promo_name like '%Join & Go%' 
and date_add(b.order_end_dt,2) between '2021-07-01' and '2022-06-30' 
and COALESCE(c.type_rnk, 0)  <> 1  
 
-- group by 1 
-- order by 1 
select case when item_promo_name like '%Join & Go%' then 'Join&Go' 
            when b.prod_name in ('Classic Care','Premium Care','Premium Plus','Free2go') then 'RSA' 
            else 'others' end type 
        , count(DISTINCT contact_id) 
        , sum(1)                      AS renewals 
        , sum(renewal_cd)           as renewed 
        , sum(case when b.renewed_completed_dt <= date_add(order_end_dt,2) then 1 else 0 end  ) as renewed_pot 
        , sum(renewal_cd)/sum(1) as renewal_rate 
         
 
from sandpit.renewal_base b 
 
inner join sandpit.util_renew_summ c  
on b.match_rnk = c.match_rnk  
 
where 1=1 
and b.prod_type='RSA' 
and b.prod_name in ('Classic Care','Premium Care','Premium Plus','Free2go') 
and lower(item_promo_name) not like '%add%' 
and b.member_type not like '%Honorary%' 
and b.contact_cd<>'Business Contact' 
and (b.prod_tenure_split like '%y_0' or b.prod_tenure_split is null) 
and year(date_add(b.order_end_dt,2))=2020 
and COALESCE(c.type_rnk, 0)  <> 1  
 
group by 1 
order by 1  
select case when item_promo_name like '%Join & Go%' then 'Join&Go' 
            when b.prod_name in ('Classic Care','Premium Care','Premium Plus','Free2go') then 'RSA' 
            else 'others' end type 
        , b.prod_name 
        , count(DISTINCT contact_id) 
        , sum(renewal_cd)/sum(1) as renewal_rate 
         
 
from sandpit.renewal_base b 
 
inner join sandpit.util_renew_summ c  
on b.match_rnk = c.match_rnk  
 
where 1=1 
and b.prod_type='RSA' 
and b.prod_name in ('Classic Care','Premium Care','Premium Plus','Free2go') 
and lower(item_promo_name) not like '%add%' 
and b.member_type not like '%Honorary%' 
and b.contact_cd<>'Business Contact' 
and (b.prod_tenure_split like '%y_0' or b.prod_tenure_split is null) 
and year(date_add(b.order_end_dt,2))=2020 
and COALESCE(c.type_rnk, 0)  <> 1  
 
group by 1,2 
order by 1 ,2 
select case when item_promo_name like '%Join & Go%' then 'Join&Go' 
            when b.prod_name in ('Classic Care','Premium Care','Premium Plus','Free2go') then 'RSA' 
            else 'others' end type 
        , b.member_colour 
        , count(DISTINCT contact_id) 
        , sum(renewal_cd)/sum(1) as renewal_rate 
         
 
from sandpit.renewal_base b 
 
inner join sandpit.util_renew_summ c  
on b.match_rnk = c.match_rnk  
 
where 1=1 
and b.prod_type='RSA' 
and b.prod_name in ('Classic Care','Premium Care','Premium Plus','Free2go') 
and lower(item_promo_name) not like '%add%' 
and b.member_type not like '%Honorary%' 
and b.contact_cd<>'Business Contact' 
and (b.prod_tenure_split like '%y_0' or b.prod_tenure_split is null) 
and year(date_add(b.order_end_dt,2))=2020 
and COALESCE(c.type_rnk, 0)  <> 1  
 
group by 1,2 
order by 1 ,2 
SELECT  b.member_colour, count(DISTINCT b.contact_id) 
 
from sandpit.renewal_base b 
 
inner join sandpit.util_renew_summ c  
on b.match_rnk = c.match_rnk  
 
where item_promo_name like '%Join & Go%' 
and date_add(b.order_end_dt,2) between '2021-07-01' and '2022-06-30' 
and COALESCE(c.type_rnk, 0)  <> 1  
 
group by 1 
order by 1 
SELECT  case when item_promo_name like '%Join & Go%' then 'Join&Go' 
            when b.prod_name in ('Classic Care','Premium Care','Premium Plus','Free2go') then 'RSA' 
            else 'others' end type 
        , CASE 
        WHEN 
            ( 
                b.owner_postcode >= 1215 AND b.owner_postcode <= 2082 OR 
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
            'RURAL' 
         
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
            'UNKNOWN' 
    END AS region 
    , count(DISTINCT b.contact_id) 
    , sum(renewal_cd)/sum(1) as renewal_rate 
 
from sandpit.renewal_base b 
 
inner join sandpit.util_renew_summ c  
on b.match_rnk = c.match_rnk  
 
where 1=1 
and b.prod_type='RSA' 
and b.prod_name in ('Classic Care','Premium Care','Premium Plus','Free2go') 
and lower(item_promo_name) not like '%add%' 
and b.member_type not like '%Honorary%' 
and b.contact_cd<>'Business Contact' 
and (b.prod_tenure_split like '%y_0' or b.prod_tenure_split is null) 
and year(date_add(b.order_end_dt,2))=2020 
and COALESCE(c.type_rnk, 0)  <> 1  
 
group by 1,2 
order by 1,2 
---App enagagement and redemption and renewal rate 
with ga_App as ( 
select distinct membernumber from googleanalytics.appdata 
where eventyear in ('2019','2020') 
), redemption as ( 
select distinct member_number from m4m.return_feed_header 
where partner not in ('IAG','NRMA Multi Policy Discount','NRMA MPD Insurance','NRMA Insurance') 
and time_stamp between '2019-01-01' and '2020-12-31' 
), opens as ( 
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
SELECT case when item_promo_name like '%Join & Go%' then 'Join&Go' 
            when b.prod_name in ('Classic Care','Premium Care','Premium Plus','Free2go') then 'RSA' 
            else 'others' end type 
        , b.prod_name 
        , count(DISTINCT contact_id) 
        , sum(renewal_cd)/sum(1) as renewal_rate 
        , sum(case when o.customer_id is not null then 1 else 0 end) opens 
        , sum(case when cl.customer_id is not null then 1 else 0 end) clicks 
        , sum(case when r.member_number is not null then 1 else 0 end)  redeemer 
        , sum(case when app.membernumber is not null then 1 else 0 end) app_user 
 
from sandpit.renewal_base b 
 
inner join sandpit.util_renew_summ c  
on b.match_rnk = c.match_rnk  
 
left join ga_App app 
on b.membernumber=app.membernumber 
 
left join redemption r 
on r.member_number=b.membernumber 
 
left join opens o 
on o.customer_id=b.contact_id 
 
left join clicks cl 
on cl.customer_id=b.contact_id 
 
 
where 1=1 
and b.prod_type='RSA' 
and b.prod_name in ('Classic Care','Premium Care','Premium Plus','Free2go') 
and lower(item_promo_name) not like '%add%' 
and b.member_type not like '%Honorary%' 
and b.contact_cd<>'Business Contact' 
and (b.prod_tenure_split like '%y_0' or b.prod_tenure_split is null) 
and year(date_add(b.order_end_dt,2))=2020 
and COALESCE(c.type_rnk, 0)  <> 1  
 
group by 1,2 
order by 1,2 
---App enagagement and redemption and renewal rate 
with ga_App as ( 
select distinct membernumber from googleanalytics.appdata 
where eventyear in ('2019','2020') 
), redemption as ( 
select distinct member_number from m4m.return_feed_header 
where partner not in ('IAG','NRMA Multi Policy Discount','NRMA MPD Insurance','NRMA Insurance') 
and time_stamp between '2019-01-01' and '2020-12-31' 
), opens as ( 
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
SELECT case when app.membernumber is not null then 1 else 0 end opens 
        , case when item_promo_name like '%Join & Go%' then 'Join&Go' 
            when b.prod_name in ('Classic Care','Premium Care','Premium Plus','Free2go') then 'RSA' 
            else 'others' end type 
        , sum(renewal_cd)/sum(1) as renewal_rate 
        -- , sum(case when o.customer_id is not null then 1 else 0 end) opens 
        -- , sum(case when cl.customer_id is not null then 1 else 0 end) clicks 
        -- , sum(case when r.member_number is not null then 1 else 0 end)  redeemer 
        -- , sum(case when app.membernumber is not null then 1 else 0 end) app_user 
 
from sandpit.renewal_base b 
 
inner join sandpit.util_renew_summ c  
on b.match_rnk = c.match_rnk  
 
left join ga_App app 
on b.membernumber=app.membernumber 
 
-- left join redemption r 
-- on r.member_number=b.membernumber 
 
-- left join opens o 
-- on o.customer_id=b.contact_id 
 
-- left join clicks cl 
-- on cl.customer_id=b.contact_id 
 
 
where 1=1 
and b.prod_type='RSA' 
and b.prod_name in ('Classic Care','Premium Care','Premium Plus','Free2go') 
and lower(item_promo_name) not like '%add%' 
and b.member_type not like '%Honorary%' 
and b.contact_cd<>'Business Contact' 
and (b.prod_tenure_split like '%y_0' or b.prod_tenure_split is null) 
and year(date_add(b.order_end_dt,2))=2020 
and COALESCE(c.type_rnk, 0)  <> 1  
 
group by 1,2 
order by 2,1 
with NPS as( 
SELECT distinct c.member_number 
        , a.* 
 
from cad.cad_jobs c 
 
INNER JOIN campaign_data.aa_dmc2402_NPS_JOR_210705_20210713 a 
on cast(a.job_number as string)=cast(c.job_number as STRING) 
) 
SELECT n.nps 
        , case when item_promo_name like '%Join & Go%' then 'Join&Go' 
            when b.prod_name in ('Classic Care','Premium Care','Premium Plus','Free2go') then 'RSA' 
            else 'others' end type 
        , sum(renewal_cd)/sum(1) as renewal_rate 
        , count(DISTINCT contact_id) 
 
from sandpit.renewal_base b 
 
inner join sandpit.util_renew_summ c  
on b.match_rnk = c.match_rnk  
 
left join nps n 
on n.member_number=b.membernumber 
 
where 1=1 
and b.prod_type='RSA' 
and b.prod_name in ('Classic Care','Premium Care','Premium Plus','Free2go') 
and lower(item_promo_name) not like '%add%' 
and b.member_type not like '%Honorary%' 
and b.contact_cd<>'Business Contact' 
--and (b.prod_tenure_split like '%y_0' or b.prod_tenure_split is null) 
--and year(date_add(b.order_end_dt,2))=2020 
--and n.member_number='990979528' 
and COALESCE(c.type_rnk, 0)  <> 1  
 
group by 1,2 
order by 2,1 
SELECT distinct c.member_number 
        , a.* 
 
from cad.cad_jobs c 
 
INNER JOIN campaign_data.aa_dmc2402_NPS_JOR_210705_20210713 a 
on cast(a.job_number as string)=cast(c.job_number as STRING) 
with v_age as ( 
 
select distinct vehicle_yr, year(now())- cast(vehicle_yr as int) as vehcile_age from sandpit.renewal_base 
) 
 
select case when item_promo_name like '%Join & Go%' then 'Join&Go' 
            when b.prod_name in ('Classic Care','Premium Care','Premium Plus','Free2go') then 'RSA' 
            else 'others' end type 
        , case when v.vehcile_age<0 then 'error' 
            when v.vehcile_age<6 then '0-5' 
            when v.vehcile_age<11 then '6-10' 
            when v.vehcile_age<20 then '11-19' 
            when v.vehcile_age>=20 then '20+' 
            else 'others' end v_age 
        , count(DISTINCT contact_id) 
        , sum(renewal_cd)/sum(1) as renewal_rate 
         
 
from sandpit.renewal_base b 
 
left join v_age v 
on v.vehicle_yr=b.vehicle_yr 
 
inner join sandpit.util_renew_summ c  
on b.match_rnk = c.match_rnk  
 
where 1=1 
and b.prod_type='RSA' 
and b.prod_name in ('Classic Care','Premium Care','Premium Plus','Free2go') 
and lower(item_promo_name) not like '%add%' 
and b.member_type not like '%Honorary%' 
and b.contact_cd<>'Business Contact' 
and (b.prod_tenure_split like '%y_0' or b.prod_tenure_split is null) 
and year(date_add(b.order_end_dt,2))=2020 
and COALESCE(c.type_rnk, 0)  <> 1  
 
group by 1,2 
order by 1 ,2 
SELECT membernumber, contact_id, item_base_price, item_net_price, renewed_item_base_price, renewed_item_net_price, asset_num 
from sandpit.renewal_base 
where member_loyalty_colour='Gold Life' 
and lower(item_promo_name) like '%add%' 
and year(date_add(order_end_dt,2))=2020 
select case when lower(item_promo_name) like '%add%' then 'Add&Go' 
            when b.prod_name in ('Classic Care','Premium Care','Premium Plus') then 'RSA' 
            else 'others' end type 
        ,case when lower(c.campaign_name) like '%onboarding%' then 'On-boarding' 
            when (lower(c.campaign_name) like '%enl%' or lower(c.campaign_name) like '%enews%') then 'eNews' 
            when lower(c.campaign_name) like '%solus%' then 'Solus' 
            when (lower(c.campaign_name) like '%renew%' or lower(c.campaign_name) like '%reminder%') then 'renewal' 
            else 'others' end email_type 
---App enagagement and redemption and renewal rate 
with clicks as ( 
 
select distinct customer_id 
 
from omc.send_level_summary a 
 
inner join omc.campaign_info c 
on c.campaign_id=a.campaign_id 
 
inner join sandpit.renewal_base b 
on b.contact_id=a.customer_id 
 
where 1=1 
and year(date_add(b.order_end_dt,2))=2020 
and click_event_date is not null 
and datediff(b.order_end_dt,a.send_event_date) between 0 and 365 
and a.channel='Email' 
and control is false 
and (lower(c.campaign_name) like '%renew%' or lower(c.campaign_name) like '%reminder%') 
) 
SELECT case when cl.customer_id is not null then 1 else 0 end clicks 
        , case when item_promo_name like '%Join & Go%' then 'Join&Go' 
            when b.prod_name in ('Classic Care','Premium Care','Premium Plus','Free2go') then 'RSA' 
            else 'others' end type 
        , sum(renewal_cd)/sum(1) as renewal_rate 
        , count(distinct b.contact_id) 
         
from sandpit.renewal_base b 
 
inner join sandpit.util_renew_summ c  
on b.match_rnk = c.match_rnk  
 
left join clicks cl 
on cl.customer_id=b.contact_id 
 
 
where 1=1 
and b.prod_type='RSA' 
and b.prod_name in ('Classic Care','Premium Care','Premium Plus','Free2go') 
and lower(item_promo_name) not like '%add%' 
and b.member_type not like '%Honorary%' 
and b.contact_cd<>'Business Contact' 
and (b.prod_tenure_split like '%y_0' or b.prod_tenure_split is null) 
and year(date_add(b.order_end_dt,2))=2020 
and COALESCE(c.type_rnk, 0)  <> 1  
 
group by 1,2 
order by 2,1 
SELECT  case when cad.order_id is null then 'N'  
            when to_date(cad.cad_receipt_dt)>to_date(b.member_join_dt) then 'Y_after_join'  
            else 'Y' end call_out 
        , case when lower(item_promo_name) like '%join & go%' then 'Join&Go' 
                when lower(item_promo_name) like '%add%' then 'Add&Go' 
                else 'others' end product_name 
        , count(DISTINCT contact_id) 
        , sum(renewal_cd)/sum(1) as renewal_rate 
 
 
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
 
GROUP BY 1,2 
ORDER BY 2,1 
SELECT sum(cad.cad_battery) 
        , sum(cad.cad_alternator) 
        , sum(cad.cad_tyre) 
        , sum(cad.cad_transmission) 
        , sum(cad.cad_brake) 
        , sum(cad.cad_lockout) 
        , sum(cad.cad_tow) 
        , count(DISTINCT cad.order_id) 
 
 
 
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
and lower(item_promo_name) like '%add%' 
and cad.order_id is not null 
 
-- GROUP BY 1 
-- ORDER BY 1 
SELECT  cad.cad_tow, sum(renewal_cd)/sum(1) as renewal_rate 
 
 
 
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
and lower(item_promo_name) like '%join & go%' 
and cad.order_id is not null 
 
GROUP BY 1 
ORDER BY 1 
  
CASE 
        WHEN 
            ( 
                address.zipcode >= '1215' AND address.zipcode <= '2082' OR 
                address.zipcode >= '2084' AND address.zipcode <= '2234' OR 
                address.zipcode >= '2555' AND address.zipcode <= '2574' OR 
                address.zipcode >= '2745' AND address.zipcode <= '2770' OR 
                address.zipcode >= '2775' AND address.zipcode <= '2775' 
            ) 
            AND 
            ( 
                UPPER(address.country) = 'AUSTRALIA' OR 
                UPPER(address.country) = 'AU' 
            ) 
        THEN 
            'METROPOLITAN' 
 
        WHEN 
            ( 
                address.zipcode >= '2083' AND address.zipcode <= '2083' OR 
                address.zipcode >= '2250' AND address.zipcode <= '2338' OR 
                address.zipcode >= '2415' AND address.zipcode <= '2423' OR 
                address.zipcode >= '2425' AND address.zipcode <= '2425' OR 
                address.zipcode >= '2428' AND address.zipcode <= '2428' OR 
                address.zipcode >= '2500' AND address.zipcode <= '2535' OR 
                address.zipcode >= '2538' AND address.zipcode <= '2541' OR 
                address.zipcode >= '2575' AND address.zipcode <= '2578' OR 
                address.zipcode >= '2600' AND address.zipcode <= '2617' OR 
                address.zipcode >= '2773' AND address.zipcode <= '2774' OR 
                address.zipcode >= '2776' AND address.zipcode <= '2786' OR 
                address.zipcode >= '2900' AND address.zipcode <= '2914' 
            ) 
            AND 
            ( 
                UPPER(address.country) = 'AUSTRALIA' OR 
                UPPER(address.country) = 'AU' 
            ) 
        THEN 
            'REGIONAL' 
         
        WHEN 
            ( 
                address.zipcode >= '2339' AND address.zipcode <= '2411' OR 
                address.zipcode >= '2424' AND address.zipcode <= '2424' OR 
                address.zipcode >= '2426' AND address.zipcode <= '2427' OR 
                address.zipcode >= '2429' AND address.zipcode <= '2490' OR 
                address.zipcode >= '2536' AND address.zipcode <= '2537' OR 
                address.zipcode >= '2545' AND address.zipcode <= '2551' OR 
                address.zipcode >= '2579' AND address.zipcode <= '2594' OR 
                address.zipcode >= '2618' AND address.zipcode <= '2739' OR 
                address.zipcode >= '2787' AND address.zipcode <= '2898' OR 
                address.zipcode >= '6798' AND address.zipcode <= '6799' 
            ) 
            AND 
            ( 
                UPPER(address.country) = 'AUSTRALIA' OR 
                UPPER(address.country) = 'AU' 
            ) 
        THEN 
            'RURAL' 
         
        WHEN 
            ( 
                address.zipcode >= '0800' AND address.zipcode <= '0886' OR 
                address.zipcode >= '3000' AND address.zipcode <= '6770' OR 
                address.zipcode >= '6907' AND address.zipcode <= '7470' OR 
                address.zipcode >= '7471'  
            ) 
            AND 
            ( 
                UPPER(address.country) = 'AUSTRALIA' OR 
                UPPER(address.country) = 'AU' 
            ) 
        THEN 
            'INTERSTATE' 
 
        ELSE 
            'UNKNOWN' 
    END AS region,