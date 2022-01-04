SELECT * from campaign_data.aa_dmc2389_iagpirprep_campaign_edm_20210524 
SELECT control 
        ---, member_type 
        , count(DISTINCT h.customer_id)  
 
from omc.send_level_summary a 
 
LEFT JOIN campaign_data.rw_aprjun_hackmap_20210831 h 
on h.riid=a.riid 
 
LEFT JOIN campaign_data.aa_dmc2389_iagpirprep_campaign_edm_20210524 b 
on b.con_id=h.customer_id 
 
where program_id='61381682' 
and campaign_id in ('61372162','61372422') 
 
GROUP BY 1 
ORDER BY 1 
 
SELECT campaign_id 
        , control 
        , count(DISTINCT h.customer_id)  
 
from omc.send_level_summary a 
 
LEFT JOIN campaign_data.rw_aprjun_hackmap_20210831 h 
on h.riid=a.riid 
 
LEFT JOIN campaign_data.aa_dmc2389_iagpirprep_campaign_edm_20210524 b 
on b.con_id=h.customer_id 
 
where program_id='61381682' 
and campaign_id in ('61372162','61372422') 
 
GROUP BY 1,2 
ORDER BY 1,2 
 
SELECT campaign_id 
        , a.clicked_elements 
        , count(DISTINCT h.customer_id)  
 
from omc.send_level_summary a 
 
LEFT JOIN campaign_data.rw_aprjun_hackmap_20210831 h 
on h.riid=a.riid 
 
LEFT JOIN campaign_data.aa_dmc2389_iagpirprep_campaign_edm_20210524 b 
on b.con_id=h.customer_id 
 
where program_id='61381682' 
and campaign_id in ('61372162','61372422') 
--and (a.open_event_date is not null or a.click_event_date is not null) 
and a.click_event_date is not null 
 
GROUP BY 1,2 
ORDER BY 1,2 
 
SELECT campaign_id 
        , control 
        , count(DISTINCT h.customer_id)  
 
from omc.send_level_summary a 
 
LEFT JOIN campaign_data.rw_aprjun_hackmap_20210831 h 
on h.riid=a.riid 
 
LEFT JOIN campaign_data.aa_dmc2389_iagpirprep_campaign_edm_20210524 b 
on b.con_id=h.customer_id 
 
where program_id='61381682' 
and campaign_id in ('61372162','61372422') 
--and (a.open_event_date is not null or a.click_event_date is not null) 
and a.click_event_date is not null 
 
GROUP BY 1,2 
ORDER BY 1,2 
 
SELECT campaign_id 
        , control 
        , count(DISTINCT h.customer_id)  
 
from omc.send_level_summary a 
 
LEFT JOIN campaign_data.rw_aprjun_hackmap_20210831 h 
on h.riid=a.riid 
 
LEFT JOIN campaign_data.aa_dmc2389_iagpirprep_campaign_edm_20210524 b 
on b.con_id=h.customer_id 
 
INNER JOIN gms.s_contact c 
on c.row_id=a.customer_id 
 
INNER JOIN m4m.return_feed_header m 
on m.member_number=c.csn 
and m.partner in ('IAG','NRMA Multi Policy Discount','NRMA MPD Insurance','NRMA Insurance') 
and datediff(m.time_stamp,a.send_event_date) between 0 and 30 
 
where program_id='61381682' 
and campaign_id in ('61372162','61372422') 
--and (a.open_event_date is not null or a.click_event_date is not null) 
--and a.click_event_date is not null 
 
GROUP BY 1,2 
ORDER BY 1,2 
 
SELECT  member_type 
       -- , control 
        , count(DISTINCT h.customer_id)  
 
from omc.send_level_summary a 
 
LEFT JOIN campaign_data.rw_aprjun_hackmap_20210831 h 
on h.riid=a.riid 
 
LEFT JOIN campaign_data.aa_dmc2389_iagpirprep_campaign_edm_20210524 b 
on b.con_id=h.customer_id 
 
INNER JOIN gms.s_contact c 
on c.row_id=a.customer_id 
 
-- INNER JOIN m4m.return_feed_header m 
-- on m.member_number=c.csn 
-- and m.partner in ('IAG','NRMA Multi Policy Discount','NRMA MPD Insurance','NRMA Insurance') 
-- and datediff(m.time_stamp,a.send_event_date) between 0 and 30 
 
where program_id='61381682' 
and campaign_id in ('61372162','61372422') 
and a.control is false 
--and (a.open_event_date is not null or a.click_event_date is not null) 
--and a.click_event_date is not null 
 
GROUP BY 1 
ORDER BY 1 
 
SELECT campaign_id 
        --, member_type 
        , control 
        , count(DISTINCT h.customer_id)  
 
from omc.send_level_summary a 
 
LEFT JOIN campaign_data.rw_aprjun_hackmap_20210831 h 
on h.riid=a.riid 
 
LEFT JOIN campaign_data.aa_dmc2389_iagpirprep_campaign_edm_20210524 b 
on b.con_id=h.customer_id 
 
INNER JOIN gms.s_contact c 
on c.row_id=a.customer_id 
 
INNER JOIN m4m.return_feed_header m 
on m.member_number=c.csn 
and m.partner in ('IAG','NRMA Multi Policy Discount','NRMA MPD Insurance','NRMA Insurance') 
and datediff(m.time_stamp,a.send_event_date) BETWEEN 0 and 30 
 
where program_id='61381682' 
and campaign_id in ('61372162','61372422') 
--and (a.open_event_date is not null or a.click_event_date is not null) 
--and a.click_event_date is not null 
 
GROUP BY 1,2 
ORDER BY 1,2 
 
SELECT campaign_id 
        --, member_type 
        , control 
        , count(DISTINCT h.customer_id)  
 
from omc.send_level_summary a 
 
LEFT JOIN campaign_data.rw_aprjun_hackmap_20210831 h 
on h.riid=a.riid 
 
LEFT JOIN campaign_data.aa_dmc2389_iagpirprep_campaign_edm_20210524 b 
on b.con_id=h.customer_id 
 
INNER JOIN gms.s_contact c 
on c.row_id=a.customer_id 
 
-- INNER JOIN m4m.return_feed_header m 
-- on m.member_number=c.csn 
-- and m.partner in ('IAG','NRMA Multi Policy Discount','NRMA MPD Insurance','NRMA Insurance') 
-- and datediff(m.time_stamp,a.send_event_date) BETWEEN 0 and 30 
 
where 1=1 
--and program_id='63361042' 
and campaign_id in ('61402982','61403042','61403102') 
and (a.open_event_date is not null or a.click_event_date is not null) 
--and a.click_event_date is not null 
 
GROUP BY 1,2 
ORDER BY 1,2 
 
SELECT DISTINCT a.segment 
--a.signature_id, count(DISTINCT h.customer_id) 
 
from omc.send_level_summary a 
 
LEFT JOIN campaign_data.rw_aprjun_hackmap_20210831 h 
on h.riid=a.riid 
 
LEFT JOIN campaign_data.aa_dmc2389_iagpirprep_campaign_edm_20210524 b 
on b.con_id=h.customer_id 
 
INNER JOIN gms.s_contact c 
on c.row_id=a.customer_id 
 
-- INNER JOIN m4m.return_feed_header m 
-- on m.member_number=c.csn 
-- and m.partner in ('IAG','NRMA Multi Policy Discount','NRMA MPD Insurance','NRMA Insurance') 
-- and datediff(m.time_stamp,a.send_event_date) BETWEEN 0 and 30 
 
where 1=1 
--and (a.segment rlike 'NSW' or a.segment rlike 'ACT') 
--and program_id='63361042' 
and campaign_id in ('61402982','61403042','61403102') 
--and (a.open_event_date is not null or a.click_event_date is not null) 
--and a.click_event_date is not null 
 
-- GROUP BY 1 
-- ORDER BY 1 
 
SELECT cx.attrib_55 ,count(DISTINCT h.customer_id)  
 
from omc.send_level_summary a 
 
LEFT JOIN campaign_data.rw_aprjun_hackmap_20210831 h 
on h.riid=a.riid 
 
LEFT JOIN campaign_data.aa_dmc2389_iagpirprep_campaign_edm_20210524 b 
on b.con_id=h.customer_id 
 
INNER JOIN gms.s_contact c 
on c.row_id=a.customer_id 
 
INNER JOIN gms.s_contact_x cx 
on cx.par_row_id=c.row_id 
 
-- INNER JOIN m4m.return_feed_header m 
-- on m.member_number=c.csn 
-- and m.partner in ('IAG','NRMA Multi Policy Discount','NRMA MPD Insurance','NRMA Insurance') 
-- and datediff(m.time_stamp,a.send_event_date) BETWEEN 0 and 30 
 
where 1=1 
--and program_id='63361042' 
and campaign_id in ('61402982','61403042','61403102') 
--and (a.open_event_date is not null or a.click_event_date is not null) 
and a.click_event_date is not null 
--and a.clicked_elements rlike 'Loyalty_should_be_rewarded' 
 
GROUP BY 1 
ORDER BY 1 