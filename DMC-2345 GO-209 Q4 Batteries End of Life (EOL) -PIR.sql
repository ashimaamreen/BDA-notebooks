SELECT * from ek_dmc2341_battery_campaign_dmedm_20210428_adhoc 
SELECT segment, count(DISTINCT csn) from ek_dmc2341_battery_campaign_dmedm_20210428_adhoc 
GROUP BY 1 
ORDER BY 1 
SELECT DISTINCT partner from m4m.return_feed_header 
where lower(partner) like '%batter%' 
SELECT DISTINCT s.segment 
from omc.send_level_summary s 
 
INNER JOIN omc.campaign_info n 
on n.campaign_id=s.campaign_id 
 
INNER JOIN gms.s_contact c 
on c.row_id=s.customer_id 
 
where n.campaign_id in ('57438262','57438982') 
and s.bounce_event_date is null 
SELECT DISTINCT to_date(s.send_event_date) 
from omc.send_level_summary s 
 
INNER JOIN omc.campaign_info n 
on n.campaign_id=s.campaign_id 
 
INNER JOIN gms.s_contact c 
on c.row_id=s.customer_id 
 
-- INNER JOIN m4m.return_feed_header m 
-- on m.member_number=c.csn 
-- and m.time_stamp BETWEEN '2021-05-10' and '2021-06-30' 
-- and m.partner='NRMA Batteries' 
 
-- INNER JOIN ek_dmc2341_battery_campaign_dmedm_20210428_adhoc ek 
-- on ek.csn=c.csn 
 
where n.campaign_id in ('57438262','57438982') 
and s.bounce_event_date is null 
--and (s.open_event_date is not null or s.click_event_date is not null) 
--and s.click_event_date is not null 
 
-- GROUP BY 1,2,3 
-- ORDER BY 1,2,3 
SELECT control, s.campaign_id,n.campaign_name, count(DISTINCT s.customer_id) 
from omc.send_level_summary s 
 
INNER JOIN omc.campaign_info n 
on n.campaign_id=s.campaign_id 
 
INNER JOIN gms.s_contact c 
on c.row_id=s.customer_id 
 
-- INNER JOIN m4m.return_feed_header m 
-- on m.member_number=c.csn 
-- and m.time_stamp BETWEEN '2021-05-10' and '2021-06-30' 
-- and m.partner='NRMA Batteries' 
 
INNER JOIN ek_dmc2341_battery_campaign_dmedm_20210428_adhoc ek 
on ek.csn=c.csn 
 
where n.campaign_id in ('57438262','57438982') 
and s.bounce_event_date is null 
--and (s.open_event_date is not null or s.click_event_date is not null) 
--and s.click_event_date is not null 
 
GROUP BY 1,2,3 
ORDER BY 1,2,3 
SELECT control, s.campaign_id,n.campaign_name,ek.segment, count(DISTINCT s.customer_id) 
from omc.send_level_summary s 
 
INNER JOIN omc.campaign_info n 
on n.campaign_id=s.campaign_id 
 
INNER JOIN gms.s_contact c 
on c.row_id=s.customer_id 
 
INNER JOIN m4m.return_feed_header m 
on m.member_number=c.csn 
and m.time_stamp BETWEEN '2021-05-10' and '2021-06-30' 
and m.partner='NRMA Batteries' 
 
INNER JOIN ek_dmc2341_battery_campaign_dmedm_20210428_adhoc ek 
on ek.csn=c.csn 
 
where n.campaign_id in ('57438262','57438982') 
and s.bounce_event_date is null 
--and (s.open_event_date is not null or s.click_event_date is not null) 
--and s.click_event_date is not null 
 
GROUP BY 1,2,3,4 
ORDER BY 1,2,3,4 
SELECT count(DISTINCT csn)  
 
from campaign_data.ek_dmc2341_battery_campaign_dmedm_20210428_adhoc ek 
 
INNER JOIN m4m.return_feed_header m 
on m.member_number=ek.csn 
and m.time_stamp BETWEEN '2021-05-10' and '2021-06-30' 
and m.partner='NRMA Batteries' 
 
--GROUP BY 1 
--ORDER BY 1 
SELECT control, campaign_id, count(DISTINCT riid) 
from omc.send_level_summary 
where program_id='51381562'