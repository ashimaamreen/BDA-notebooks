spark.sql(''' 
SELECT 
    NULL AS event_uuid, 
    NULL AS customer_id, 
    riid, 
    TO_TIMESTAMP(event_captured_dt, 'dd-MMM-yyyy HH:mm:ss') AS send_event_date, 
    NULL AS skip_reason, 
    TRUE AS control, 
    'Push' AS channel, 
    '55367702' AS campaign_id, 
    NULL AS launch_id, 
    program_id, 
    NULL AS signature_id, 
    NULL AS campaign_version_name, 
    NULL AS campaign_version_content, 
    NULL AS segment, 
    nrma_account_id, 
    member_number, 
    dynamic_sales_id AS order_id, 
    NULL AS mobile_number, 
    NULL AS mobile_channel, 
    NULL AS app_id, 
    NULL AS platform_type, 
    NULL AS delivered_event_date, 
    NULL AS bounce_event_date, 
    NULL AS open_event_date, 
    NULL AS click_event_date, 
    NULL AS unsubbed_event_date, 
    NULL AS clicked_elements 
   
FROM 
    omc.audit_hold_out 
     
WHERE 
    program_id = '37198062' 
    AND program_version = '43' 
''').createOrReplaceTempView('send_level_summary_aux') 
spark.sql(''' 
SELECT * FROM send_level_summary_aux 
UNION ALL 
SELECT * FROM omc.send_level_summary 
''').createOrReplaceTempView('send_level_summary') 
select distinct program_id from omc.send_level_summary 
where campaign_id in ('33091842')maz transaction date 15/01/ 
SELECT distinct b.holdout_id 
 
FROM omc.audit_hold_out b 
--on a.contact_riid=b.riid 
 
where b.program_id='33095142' 
select partner, max(time_stamp) from m4m.return_feed_header 
where lower(partner) rlike 'attr' 
group by 1 
select control,a.segment,count(distinct hm.customer_id)  
 
from omc.send_level_summary a 
 
inner join campaign_data.aa_dmc2299_hack_map_20200128 hm 
on hm.riid=a.riid 
 
inner join gms.s_contact c 
on c.row_id=hm.customer_id 
 
inner join gms.s_contact_x cx 
on c.row_id=cx.par_row_id 
 
inner join gms.s_contact_fnx fn 
on fn.par_row_id=c.row_id 
 
-- inner join m4m.return_feed_header h 
-- on h.member_number=c.csn 
-- and partner not in ('IAG','NRMA Multi Policy Discount','NRMA MPD Insurance','NRMA Insurance','Caltex') 
-- and datediff(h.time_stamp,a.send_event_date) between 0 and 30 
-- and h.partner='Frequent Values' 
 
inner join sandpit.ga_appsession app 
on app.membernumber=c.csn 
and datediff(to_date(app.event_dt),to_date(a.send_event_date)) between 0 and 30 
 
where campaign_id in ('33091842') 
and to_date(send_event_date)>'2020-10-12' 
 
--and (a.open_event_date is not null or a.click_event_date is not null) 
--and a.click_event_date is not null 
 
group by 1,2 
order by 1,2 
select count(distinct hm.customer_id)  
--concat_ws(">>", eng01, eng02, eng03, eng04, eng05) as flow, count(1) 
--,count(distinct hm.customer_id)  
 
from omc.send_level_summary a 
 
inner join campaign_data.aa_dmc2299_hack_map_20200128 hm 
on hm.riid=a.riid 
 
inner join gms.s_contact c 
on c.row_id=hm.customer_id 
 
inner join gms.s_contact_x cx 
on c.row_id=cx.par_row_id 
 
inner join gms.s_contact_fnx fn 
on fn.par_row_id=c.row_id 
 
inner join sandpit.ga_appsession app 
on app.membernumber=c.csn 
and datediff(to_date(app.event_dt),to_date(a.send_event_date)) between 0 and 30 
 
where campaign_id in ('33091842') 
and to_date(send_event_date)>'2020-10-12' 
and a.control is false 
and app.redeemed=1 
 
-- group by 1 
-- order by 2 desc 
select * from 
 
sandpit.ga_util_appevent  
where redemption = 1 
 
 
select control,a.segment,count(distinct hm.customer_id)  
 
from omc.send_level_summary a 
 
inner join campaign_data.aa_dmc2299_hack_map_20200128 hm 
on hm.riid=a.riid 
 
inner join gms.s_contact c 
on c.row_id=hm.customer_id 
 
inner join gms.s_contact_x cx 
on c.row_id=cx.par_row_id 
 
inner join gms.s_contact_fnx fn 
on fn.par_row_id=c.row_id 
 
-- inner join m4m.return_feed_header h 
-- on h.member_number=c.csn 
-- and partner not in ('IAG','NRMA Multi Policy Discount','NRMA MPD Insurance','NRMA Insurance','Repco') 
-- and datediff(h.time_stamp,a.send_event_date) between 0 and 30 
--and h.partner= 
 
inner join sandpit.ga_appsession app 
on app.membernumber=c.csn 
and datediff(to_date(app.event_dt),to_date(a.send_event_date)) between 0 and 30 
 
where campaign_id in ('2754562') 
and to_date(send_event_date)>'2020-10-12' 
--and (a.open_event_date is not null or a.click_event_date is not null) 
--and a.click_event_date is not null 
 
group by 1,2 
order by 1,2 
select a.segment,count(distinct hm.customer_id)  
--concat_ws(">>", eng01, eng02, eng03, eng04, eng05) as flow, count(1) 
--,count(distinct hm.customer_id)  
 
from omc.send_level_summary a 
 
inner join campaign_data.aa_dmc2299_hack_map_20200128 hm 
on hm.riid=a.riid 
 
inner join gms.s_contact c 
on c.row_id=hm.customer_id 
 
inner join gms.s_contact_x cx 
on c.row_id=cx.par_row_id 
 
inner join gms.s_contact_fnx fn 
on fn.par_row_id=c.row_id 
 
inner join sandpit.ga_appsession app 
on app.membernumber=c.csn 
and datediff(to_date(app.event_dt),to_date(a.send_event_date)) between 0 and 30 
 
where campaign_id in ('2754562') 
and to_date(send_event_date)>'2020-10-12' 
and a.control is false 
and app.engaged_fuel=1 
 
group by 1 
order by 1 
select c.cust_value_cd,count(distinct hm.customer_id)  
 
from omc.send_level_summary a 
 
inner join campaign_data.aa_dmc2299_hack_map_20200128 hm 
on hm.riid=a.riid 
 
inner join gms.s_contact c 
on c.row_id=hm.customer_id 
 
inner join gms.s_contact_x cx 
on c.row_id=cx.par_row_id 
 
inner join gms.s_contact_fnx fn 
on fn.par_row_id=c.row_id 
 
inner join m4m.return_feed_header h 
on h.member_number=c.csn 
and partner not in ('IAG','NRMA Multi Policy Discount','NRMA MPD Insurance','NRMA Insurance') 
and datediff(h.time_stamp,a.send_event_date) between 0 and 30 
and h.partner in ('Frequent Values','Caltex','experiences and attractions tickets') 
 
-- inner join sandpit.ga_appsession app 
-- on app.membernumber=c.csn 
-- and datediff(to_date(app.event_dt),to_date(a.send_event_date)) between 0 and 30 
 
where campaign_id in ('33091842','2754562') 
and to_date(send_event_date)>'2020-10-12' 
 
--and (a.open_event_date is not null or a.click_event_date is not null) 
--and a.click_event_date is not null 
 
group by 1 
order by 1 
select cx.attrib_55,count(distinct hm.customer_id)  
 
from omc.send_level_summary a 
 
inner join campaign_data.aa_dmc2299_hack_map_20200128 hm 
on hm.riid=a.riid 
 
inner join gms.s_contact c 
on c.row_id=hm.customer_id 
 
inner join gms.s_contact_x cx 
on c.row_id=cx.par_row_id 
 
inner join gms.s_contact_fnx fn 
on fn.par_row_id=c.row_id 
 
inner join m4m.return_feed_header h 
on h.member_number=c.csn 
and partner not in ('IAG','NRMA Multi Policy Discount','NRMA MPD Insurance','NRMA Insurance') 
and datediff(h.time_stamp,a.send_event_date) between 0 and 30 
and h.partner in ('Frequent Values','Caltex','experiences and attractions tickets') 
 
 
-- inner join sandpit.ga_appsession app 
-- on app.membernumber=c.csn 
-- and datediff(to_date(app.event_dt),to_date(a.send_event_date)) between 0 and 30 
 
where campaign_id in ('33091842','2754562') 
and to_date(send_event_date)>'2020-10-12' 
 
--and (a.open_event_date is not null or a.click_event_date is not null) 
--and a.click_event_date is not null 
 
group by 1 
order by 1 
select distinct partner from m4m.return_feed_header 
where lower(partner) rlike 'attractions'