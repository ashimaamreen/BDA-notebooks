SELECT case when renewed_order_id is null then 'Non-renewed' else 'Renewed' end renewal 
        --, sum(case when  b.renewed_completed_dt <= date_add(order_end_dt,2) then 1 else 0 end  ) as renewed_pot 
        , count(DISTINCT contact_id) 
from sandpit.renewal_base b 
 
where prod_name='Free2go' 
and to_date(date_add(order_end_dt,2)) between '2019-01-01' and '2019-12-31'  
--and item_base_price=96 
 
group by 1 
order by 1 
select  
   sum(1)                      AS renewals 
,   sum(c.renewal_cd)           as renewed 
,   sum(case when  b.renewed_completed_dt <= date_add(order_end_dt,2) then 1 else 0 end  ) as renewed_pot 
from sandpit.renewal_base b 
 
 
inner join sandpit.util_renew_summ c  
    on b.match_rnk = c.match_rnk  
where    
        to_date(date_add(b.order_end_dt,2)) between '2019-01-01' and '2019-12-31'  
    and  COALESCE(c.type_rnk, 0)  <> 1  
    and COALESCE(b.member_staff, 0)  = 0  
        -- May wanna modify the next two lines 
    AND b.contact_cd IN ('Ordinary Member', 'Affiliate Member', 'Customer') 
    AND b.prod_name='Free2go' 
-- group by  
-- b.renewal_yyyymm 
-- order by 
-- b.renewal_yyyymm 
select   
         nrp_prod_name 
        , item_net_price 
        , renewed_item_net_price 
        , count(distinct contact_id) 
                             
--, item_net_price  
 
from sandpit.renewal_base 
where prod_name='Free2go' 
and (item_net_price=96 or item_net_price=144) 
and renewed_order_id is null 
--and item_base_price=96 
and to_date(date_add(order_end_dt,2)) between '2019-01-01' and '2019-12-31' 
--case when renewed_order_id is null then 'Non-renewed' else 'Renewed' end renewal 
 
group by 1,2,3 
order by 1,2,3 
select   
case when renewed_order_id is null then 'Non-renewed' else 'Renewed' end renewal 
        , case when renewed_item_net_price>0 then 'free2paid' 
            when renewed_order_id is not null then 'free2free' 
            else 'not-renewed' end  renewat_status 
        ,  count(distinct contact_id) 
                             
--, item_net_price  
 
from sandpit.renewal_base 
 
where prod_name='Free2go' 
and item_net_price=0 
and to_date(date_add(order_end_dt,2)) between '2019-01-01' and '2019-12-31' 
 
group by 1,2 
order by 1,2 
SELECT * FROM sandpit.model_cad_order LIMIT 100; 
select  
        -- sum(cad.cad_battery) 
        -- , sum(cad.cad_alternator) 
        -- , sum(cad.cad_brake) 
        -- , sum(cad.cad_tyre) 
        -- , sum(cad.cad_transmission) 
        -- , sum(cad.cad_lockout) 
        -- , sum(cad.cad_tow) 
    --     case when r.renewed_order_id is null then 0 else 1 end  as renewed 
    --   , case when r.item_net_price=0 then 'Free_year'  
    --             when r.item_net_price=48 then 'Paid_year1' 
    --             when r.item_net_price=96 or r.item_net_price=144 then 'Paid_year2' 
    --             --when item_gross_amt=144 then 'Paid_2years' 
    --             else 'others' end f2go_type 
        count(distinct r.contact_id) 
 
from sandpit.renewal_base r 
 
inner join sandpit.model_cad_order cad 
on cad.membernumber=r.membernumber 
and cad.integration_id=r.integration_id 
and cad.order_id=r.order_id 
 
where prod_name='Free2go' 
and to_date(date_add(r.order_end_dt,2)) between '2019-01-01' and '2019-12-31' 
--and to_date(r.x_nrma_join_dt)=to_date(cad.cad_complete_dt) 
 
-- group by case when r.renewed_order_id is null then 0 else 1 end 
--       , case when r.item_net_price=0 then 'Free_year'  
--                 when r.item_net_price=48 then 'Paid_year1' 
--                 when r.item_net_price=96 or r.item_net_price=144 then 'Paid_year2' 
--                 --when item_gross_amt=144 then 'Paid_2years' 
--                 else 'others' end 
-- order by renewed 
--       , f2go_type 
select renewal_yyyymm from sandpit.renewal_base 
with score as ( 
select membernumber, max(score) as eng_score from sandpit.engagement_score_staging 
group by 1 
) 
 
select e.eng_score 
        , case when renewed_order_id is null then 'Non-renewed' else 'Renewed' end renewal 
        , case when r.item_net_price=0 then 'Free_year'  
                when r.item_net_price=48 then 'Paid_year1' 
                when r.item_net_price=96 or r.item_net_price=144 then 'Paid_year2' 
                --when item_gross_amt=144 then 'Paid_2years' 
                else 'others' end f2go_type 
        ,count(distinct r.contact_id) 
 
from sandpit.renewal_base r 
 
left join score e 
on e.membernumber=r.membernumber 
 
where prod_name='Free2go' 
and to_date(date_add(r.order_end_dt,2)) between '2019-01-01' and '2019-12-31'  
 
group by 1,2,3 
order by 1,2,3 
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
-- with payment_channel as ( 
-- select x_nrma_source as first_txn_channel 
--         , x_user_id 
--         , row_number() over (partition by order_id order by received_dt ) as first_txn 
--         , src.order_id 
-- from gms.s_src_payment src 
 
-- WHERE src.pay_stat_cd in ('Reconciled','Payment Taken') 
-- AND src.order_id is not null 
-- ) 
 
-- select r.prod_f2g_split 
--         , r.order_channel 
--         , p.first_txn_channel 
--         , count(distinct r.contact_id) 
 
-- from sandpit.renewal_base r 
 
-- inner join payment_channel p 
-- on p.order_id=r.order_id 
 
-- where prod_name='Free2go' 
-- and order_completed_dt between '2019-01-01' and '2019-12-31' 
 
-- group by 1,2,3 
-- order by 1,2,3 
-- select x_nrma_source 
--         , x_user_id 
--         , row_number() over (partition by order_id order by received_dt ) as first_txn 
--         , src.order_id 
-- from gms.s_src_payment src 
 
-- WHERE src.pay_stat_cd in ('Reconciled','Payment Taken') 
-- AND src.order_id is not null 
-- --where x_nrma_source='GFS' 
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