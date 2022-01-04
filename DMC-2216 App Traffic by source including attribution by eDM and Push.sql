-- with goal as ( 
  
--     select * 
--         , from_unixtime(unix_timestamp(substr(event_dt, 1, 19),  "yyyy-MM-dd HH:mm:ss") - 60*10) as lookback_10m 
--         , from_unixtime(unix_timestamp(substr(event_dt, 1, 19),  "yyyy-MM-dd HH:mm:ss") - 60*60) as lookback_1h 
--         , from_unixtime(unix_timestamp(substr(event_dt, 1, 19),  "yyyy-MM-dd HH:mm:ss") - 60*60*24) as lookback_1d 
--     from sandpit.digital_engagement_base 
--     where 
--         datasource = "app" 
--         and event_yyyymm >= "2020-04" 
  
-- ), 10m_lbw as ( 
  
--     select g.session_id 
--         , g.event_yyyymm 
--         , coalesce(b.datasource, "organic") as datasource 
--         , coalesce(b.medium, "organic") as medium 
--         , coalesce(b.source, "organic") as source 
--         , coalesce(b.campaign, "organic") as campaign 
--         , coalesce(b.event_dt, g.event_dt) as event_dt 
--         , row_number() over (partition by g.session_id order by coalesce(b.event_dt, g.event_dt) desc) as rid 
         
--     from goal as g 
--         left join sandpit.digital_engagement_base as b 
--         on b.membernumber = g.membernumber 
--         and substr(b.event_dt,1,19) between substr(g.event_dt, 1, 19) and g.lookback_10m 
--         and b.event_yyyymm >= "2020-03" 
  
-- ) 
  
-- select * 
-- from 10m_lbw 
-- where 
--     rid = 1 
select source, count(distinct membernumber) from sandpit.ga_appsession 
where to_date(event_dt) between '2020-10-19' and '2020-10-25' 
group by 1 
order by 1 
SELECT  count(DISTINCT membernumber) from googleanalytics.appdata 
where to_date(to_timestamp(eventdate, "yyyyMMdd"))='2020-10-01'  
--and source='(direct)' 
 
-- group by 1 
-- order by 1 
with app_users as ( 
SELECT  DISTINCT membernumber,source from googleanalytics.appdata 
where to_date(to_timestamp(eventdate, "yyyyMMdd"))='2020-10-01' 
--and source='(direct)' 
) 
select membernumber, count(distinct source) from app_users 
group by 1 
order by 2 desc 
SELECT DISTINCT medium from googleanalytics.appdata 
spark.sql(''' 
SELECT source,medium, eventmonth, count(*) from googleanalytics.appdata 
where eventmonth in ('4','6','5','8','7','9','10') 
and eventyear='2020' 
and eventname='session_start' 
 
GROUP BY 1,2,3 
ORDER BY 1,2,3 
''').show(100000,False) 
#--event session start 
SELECT eventmonth, count(*) from googleanalytics.appdata 
where eventmonth in ('4','6','5','8','7','9') 
and eventyear='2020' 
and eventname='session_start' 
 
GROUP BY 1 
ORDER BY 1 
--event session start 
SELECT eventmonth 
        , case WHEN medium_param like '%push%' or medium_param='message_center' or medium_param='sms' then 'Push' 
            WHEN medium_param='inappcta' then 'Organic' 
            ELSE 'Email' end channel  
        , count(*)  
         
from googleanalytics.appdata 
 
where eventmonth in ('4','6','5','8','7','9') 
and eventyear='2020' 
and eventname='deeplink_click' 
--notifications_click 
 
 
GROUP BY 1,2 
ORDER BY 1,2 
--event session start 
SELECT eventmonth  
        , count(*)  
         
from googleanalytics.appdata 
 
where eventmonth in ('4','6','5','8','7','9') 
and eventyear='2020' 
and eventname='notifications_click' 
 
 
GROUP BY 1 
ORDER BY 1 
--event session start 
SELECT eventmonth 
        , case WHEN medium_param like '%push%' or medium_param='message_center' or medium_param='sms' then 'Push' 
            WHEN medium_param='inappcta' then 'Organic' 
            ELSE 'Email' end channel  
        , count(*)  
         
from googleanalytics.appdata 
 
where eventmonth in ('4','6','5','8','7','9') 
and eventyear='2020' 
and eventname='deeplink_click' 
 
GROUP BY 1,2 
ORDER BY 1,2 
--event session start 
SELECT eventmonth, count(DISTINCT membernumber) from googleanalytics.appdata 
where eventmonth in ('4','6','5','8','7','9') 
and eventyear='2020' 
and eventname='session_start' 
 
GROUP BY 1 
ORDER BY 1 
--event session start 
SELECT eventmonth 
        , case WHEN medium_param like '%push%' or medium_param='message_center' or medium_param='sms' then 'Push' 
            WHEN medium_param='inappcta' then 'Organic' 
            ELSE 'Email' end channel  
        , count(DISTINCT membernumber)  
         
from googleanalytics.appdata 
 
where eventmonth in ('4','6','5','8','7','9') 
and eventyear='2020' 
and eventname='deeplink_click' 
 
GROUP BY 1,2 
ORDER BY 1,2 
--event session start 
SELECT eventmonth  
        , count(DISTINCT membernumber)  
         
from googleanalytics.appdata 
 
where eventmonth in ('4','6','5','8','7','9') 
and eventyear='2020' 
and eventname='notifications_click' 
 
 
GROUP BY 1 
ORDER BY 1 
--event session start 
SELECT  DISTINCT membernumber,source,eventmonth, to_timestamp(eventtime, 'yyyy-MM-dd HH:mm:ss') as app_event from googleanalytics.appdata 
where eventmonth='9' 
and eventyear='2020' 
--to_date(to_timestamp(eventtime, 'yyyy-MM-dd HH:mm:ss'))='2020-10-01' 
--and source='(direct)' 
--and source='omc' 
with app_users as ( 
SELECT  DISTINCT membernumber,source,eventmonth, to_timestamp(eventtime, 'yyyy-MM-dd HH:mm:ss') as app_event from googleanalytics.appdata 
where eventmonth='9' 
and eventyear='2020' 
) 
select  
--distinct a.membernumber, a.source, e.channel, e.mobile_channel, e.mobile_number, a.app_event, e.send_event_date,datediff(app_event,e.send_event_date) 
case when a.source like '%google%' then 'Google'  
            when a.source='(direct)' then 'Organic' 
            when a.source='omc' then e.channel 
            else 'unknown' end Lead 
        , case when e.channel='Email' and (e.open_event_date is not null or e.click_event_date is not null) then 'Email_Opened' 
                when e.channel='Email' then 'Organic' 
                when e.channel='Push' and (e.open_event_date is not null or e.click_event_date is not null) then 'Push_opened' 
                when e.channel='Push' then 'Organic' 
         else e.channel end Actual_lead 
        ,count(distinct e.customer_id)  
 
from app_users a  
 
inner join gms.s_contact c 
on c.csn=a.membernumber 
 
 
left join omc.send_level_summary e 
on e.customer_id=c.row_id 
and datediff(app_event,e.send_event_date) between 0 and 15 
 
group by 1,2 
order by 1,2 
with app_users as ( 
SELECT  DISTINCT membernumber,source,eventmonth, to_timestamp(eventtime, 'yyyy-MM-dd HH:mm:ss') as app_event from googleanalytics.appdata 
where eventmonth in ('4','6','5','8','7','9') 
and eventyear='2020' 
) 
select eventmonth 
--distinct a.membernumber, a.source, e.channel, e.mobile_channel, e.mobile_number, a.app_event, e.send_event_date,datediff(app_event,e.send_event_date) 
    ,  case when a.source like '%google%' then 'Google'  
            when a.source='(direct)' and e.channel='Email' and (e.open_event_date is not null or e.click_event_date is not null) then 'Email_Opened' 
            when a.source='(direct)' and e.channel='Push' and (e.open_event_date is not null or e.click_event_date is not null) then 'Push_opened' 
            when a.source='(direct)' then 'Organic' 
            when a.source='omc' then e.channel 
            else 'unknown' end Lead 
        -- , case when e.channel='Email' and (e.open_event_date is not null or e.click_event_date is not null) then 'Email_Opened' 
        --         when e.channel='Email' then 'Organic' 
        --         when e.channel='Push' and (e.open_event_date is not null or e.click_event_date is not null) then 'Push_opened' 
        --         when e.channel='Push' then 'Organic' 
        --  else e.channel end Actual_lead 
        , count(distinct a.membernumber)  
 
from app_users a  
 
inner join gms.s_contact c 
on c.csn=a.membernumber 
 
 
left join omc.send_level_summary e 
on e.customer_id=c.row_id 
and datediff(app_event,e.send_event_date) between 0 and 7 
 
group by 1,2 
order by 1,2 
with app_users as ( 
SELECT  DISTINCT membernumber,source,eventmonth, to_timestamp(eventtime, 'yyyy-MM-dd HH:mm:ss') as app_event from googleanalytics.appdata 
where eventmonth in ('4','6','5','8','7','9') 
and eventyear='2020' 
) 
select a.eventmonth, 
--distinct a.membernumber, a.source, e.channel, e.mobile_channel, e.mobile_number, a.app_event, e.send_event_date,datediff(app_event,e.send_event_date) 
case when a.source like '%google%' then 'Google'  
            when a.source='(direct)' and e.channel='Email' and (e.open_event_date is not null or e.click_event_date is not null) then 'Email_Opened' 
            when a.source='(direct)' and e.channel='Push' and (e.open_event_date is not null or e.click_event_date is not null) then 'Push_opened' 
            when a.source='(direct)' then 'Organic' 
            when a.source='omc' then e.channel 
            else 'unknown' end Lead 
        -- , case when e.channel='Email' and (e.open_event_date is not null or e.click_event_date is not null) then 'Email_Opened' 
        --         when e.channel='Email' then 'Organic' 
        --         when e.channel='Push' and (e.open_event_date is not null or e.click_event_date is not null) then 'Push_opened' 
        --         when e.channel='Push' then 'Organic' 
        --  else e.channel end Actual_lead 
        , count(distinct a.membernumber)  
 
from app_users a  
 
inner join gms.s_contact c 
on c.csn=a.membernumber 
 
left join omc.send_level_summary e 
on e.customer_id=c.row_id 
and datediff(app_event,e.send_event_date) between 0 and 2 
 
group by 1,2 
order by 1,2 
SELECT eventmonth, count(DISTINCT membernumber) 
--,source,eventmonth, to_timestamp(eventtime, 'yyyy-MM-dd HH:mm:ss') as app_event  
from googleanalytics.appdata 
where eventmonth in ('4','6','5','8','7','9') 
and eventyear='2020' 
GROUP BY 1 
ORDER BY 1 
select count(distinct customer_id) 
--distinct open_event_date,click_event_date  
--,send_event_date  
from omc.send_level_summary 
where channel='Push' 
and to_date(send_event_date)='2020-06-04' 
and open_event_date is not null 
--328199 
--11157 
create table aa_dmc2216_temptable2 as  
with app_users as ( 
SELECT  DISTINCT membernumber,source,eventmonth, to_timestamp(eventtime, 'yyyy-MM-dd HH:mm:ss') as app_event from googleanalytics.appdata 
where eventmonth in ('4','6','5','8','7','9') 
and eventyear='2020' 
), all_users as ( 
select distinct a.membernumber, a.source, e.channel, e.mobile_channel, e.mobile_number, a.app_event, e.send_event_date,datediff(app_event,e.send_event_date) as datedifference, 
case when a.source like '%google%' then 'Google'  
            when a.source='(direct)' and e.channel='Email' and (e.open_event_date is not null or e.click_event_date is not null) then 'Email_Opened' 
            when a.source='(direct)' and e.channel='Push' and (e.open_event_date is not null or e.click_event_date is not null) then 'Push_opened' 
            when a.source='(direct)' then 'Organic' 
            when a.source='omc' then e.channel 
            else 'unknown' end Lead 
         
 
from app_users a  
 
inner join gms.s_contact c 
on c.csn=a.membernumber 
 
left join omc.send_level_summary e 
on e.customer_id=c.row_id 
and datediff(app_event,e.send_event_date) between 0 and 7 
) 
select * from all_users 
select datedifference,lead, count(distinct membernumber) from aa_dmc2216_temptable2 
where datedifference is not null 
group by 1,2 
order by 1,2 
select month(app_event),lead, count(distinct membernumber) from aa_dmc2216_temptable2 
where datedifference is not null 
group by 1,2 
order by 1,2 
select month(app_event), lead,count(distinct app_event) from aa_dmc2216_temptable2 
where app_event>send_event_date 
and (datedifference is null or datedifference=0) 
group by 1,2 
order by 1,2 
with freq as ( 
select membernumber, count(distinct to_date(app_event)) as frequency from aa_dmc2216_temptable2 
where datedifference=7 
or datedifference is null 
group by 1 
order by 1 
) 
select a.lead,avg(f.frequency) from aa_dmc2216_temptable2 a 
inner join freq f 
on f.membernumber=a.membernumber 
--and a.lead in ('Email','Email_Opened') 
 
group by 1 
order by 1 
with freq as ( 
select membernumber, count(distinct app_event) as frequency from aa_dmc2216_temptable2 
where month(app_event)=8 
group by 1 
order by 1 
) 
select avg(f.frequency) from aa_dmc2216_temptable2 a 
inner join freq f 
on f.membernumber=a.membernumber 
and a.lead in ('Organic') 
SELECT  
case WHEN medium_param like '%push%' or medium_param='message_center' or medium_param='sms' then 'Push' 
            WHEN medium_param='inappcta' then 'Organic' 
            ELSE 'Email' end channel  
        ,  
        count(DISTINCT membernumber)  
         
from googleanalytics.appdata 
 
where eventweek='37' 
and eventyear='2020' 
and eventname='deeplink_click' 
--'deeplink_click' --notifications_click 
 
GROUP BY 1 
ORDER BY 1 
--event session start 
SELECT * from googleanalytics.appdata 
where eventmonth in ('9') 
and eventyear='2020' 
and eventname='session_start' 
-- with freq as ( 
-- select membernumber, count(*) as frequency from googleanalytics.appdata 
 
-- where eventweek='36' 
-- and eventyear='2020' 
-- and eventname='session_start' 
-- group by 1 
-- order by 1 
-- ) 
 
SELECT case WHEN medium_param like '%push%' or medium_param='message_center' or medium_param='sms' then 'Push' 
            WHEN medium_param='inappcta' then 'Organic' 
            ELSE 'Email' end channel  
        ,cx.attrib_55 
        ,count(DISTINCT membernumber)  
         
from googleanalytics.appdata a                      --108536 
 
inner JOIN gms.s_contact c                          --108537 
on c.csn=a.membernumber 
 
inner join gms.s_contact_x as cx  
on c.row_id = cx.par_row_id 
 
 
where eventmonth in ('4','6','5','8','7','9') 
and eventyear='2020' 
and eventname='deeplink_click' 
 
GROUP BY 1,2 
ORDER BY 1,2 
--event session start 
SELECT CUST_VALUE_CD as loyaltycolor 
        ,count(DISTINCT membernumber)  
         
from googleanalytics.appdata a                      --108536 
 
inner JOIN gms.s_contact c                          --108537 
on c.csn=a.membernumber 
 
inner join gms.s_contact_x as cx  
on c.row_id = cx.par_row_id 
 
 
where eventmonth in ('4','6','5','8','7','9') 
and eventyear='2020' 
and eventname='session_start' 
 
GROUP BY 1 
ORDER BY 1 
--event session start 
SELECT case WHEN medium_param like '%push%' or medium_param='message_center' or medium_param='sms' then 'Push' 
            WHEN medium_param='inappcta' then 'Organic' 
            ELSE 'Email' end channel  
        ,cx.attrib_55 
        ,count(DISTINCT membernumber)  
         
from googleanalytics.appdata a                      --108536 
 
inner JOIN gms.s_contact c                          --108537 
on c.csn=a.membernumber 
 
inner join gms.s_contact_x as cx  
on c.row_id = cx.par_row_id 
 
 
where eventmonth in ('4','6','5','8','7','9') 
and eventyear='2020' 
and eventname='deeplink_click' 
 
GROUP BY 1,2 
ORDER BY 1,2 
--event session start 
SELECT case WHEN medium_param like '%push%' or medium_param='message_center' or medium_param='sms' then 'Push' 
            WHEN medium_param='inappcta' then 'Organic' 
            ELSE 'Email' end channel  
        ,CUST_VALUE_CD as loyaltycolor 
        ,count(DISTINCT membernumber)  
         
from googleanalytics.appdata a                      --108536 
 
inner JOIN gms.s_contact c                          --108537 
on c.csn=a.membernumber 
 
where eventmonth in ('4','6','5','8','7','9') 
and eventyear='2020' 
and eventname='deeplink_click' 
 
GROUP BY 1,2 
ORDER BY 1,2 