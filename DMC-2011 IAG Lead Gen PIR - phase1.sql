· Analysis period: To be the month Lead Gen email was sent + the next month (to incorporate 21day cooling off period) 
 
o In this case for Feb/March send the analysis period would be March and April 
 
  
 
· The conversion will be based on Total Members who opened the eDM and signed up for IAG divided by the total number of Members who were sent the eDM 
 
o The only way to sign up to IAG from the email would be the link taking the member to the IAG landing page. 
 
o We are still using opens and not clicks to cover all bases in case the Member does not click on the link and finds another way to join IAG 
 
  
 
· Analysis Results: To provide conversions per month in order to understand whether there is any merit in analysing results per month 
 
o In this case we will look at conversion in March and conversions in April –this will help dictate future direction 
 
  
 
· Thus I have created a PIR JIRA for early May to analyse the results from the first send and use these results to design the future PIR expectations - DMC-2011 
 
o Such as whether we do it every month, or use a cumulative approach, should we create an automated template or should be build this manually. 
SELECT * from omc.archived_emails 
where 1=1 
and campaign_id='41334962' 
SELECT count(DISTINCT riid) 
 
from omc.member_campaign_summary_table 
where 1=1 
and campaign_id='41334962' 
and to_date(sendtime)='2020-03-09' 
--AND sendtime<'2020-03-13' 
and deliverystatus='Delivered' 
--and opentoclick like 'Opened%' 
--campaign_name like '%IAG%' 
SELECT to_date(sendtime) 
        , count(DISTINCT riid) 
        , NDV(contact_id) 
        --, NDV(case when opentoclick like 'Opened%' then riid else NULL end) opens 
 
from omc.member_campaign_summary_table 
where 1=1 
and campaign_id='41334962' 
AND sendtime<'2020-03-13' 
--and to_date(sendtime)='2020-02-28' 
and deliverystatus='Delivered' 
--and opentoclick like 'Opened%' 
--campaign_name like '%IAG%' 
GROUP BY 1 
order by 1 
SELECT to_date(sendtime) 
        , count(DISTINCT riid) 
        , NDV(contact_id) 
        --, NDV(case when opentoclick like 'Opened%' then riid else NULL end) opens 
 
from omc.member_campaign_summary_table 
where 1=1 
and campaign_id='41334962' 
AND sendtime<'2020-03-13' 
and deliverystatus='Delivered' 
and opentoclick like 'Opened%' 
GROUP BY 1 
order by 1 
SELECT to_date(sendtime) 
        , count(DISTINCT riid) 
        , NDV(contact_id) 
        --, NDV(case when opentoclick like 'Opened%' then riid else NULL end) opens 
 
from omc.member_campaign_summary_table 
where 1=1 
and campaign_id='41334962' 
AND sendtime<'2020-03-13' 
and deliverystatus='Delivered' 
and opentoclick like '%- Clicked' 
GROUP BY 1 
order by 1 
SELECT to_date(sendtime) 
        , count(DISTINCT riid) 
        , NDV(contact_id) 
        --, NDV(case when opentoclick like 'Opened%' then riid else NULL end) opens 
 
from omc.member_campaign_summary_table o 
 
inner join gms.s_contact c 
on c.row_id=o.contact_id 
 
INNER join m4m.return_feed_header m 
on m.member_number=c.csn 
and partner in ('IAG','NRMA Multi Policy Discount','NRMA MPD Insurance','NRMA Insurance') 
and time_stamp >o.sendtime 
 
 
where 1=1 
and campaign_id='41334962' 
AND sendtime<'2020-03-13' 
and deliverystatus='Delivered' 
and opentoclick like 'Opened%' 
GROUP BY 1 
order by 1 
spark.sql(''' 
SELECT to_date(sendtime) 
        , count(DISTINCT riid) 
        --, NDV(contact_id) 
        --, NDV(case when opentoclick like 'Opened%' then riid else NULL end) opens 
 
from omc.member_campaign_summary_table o 
 
inner join gms.s_contact c 
on c.row_id=o.contact_id 
 
--INNER join m4m.return_feed_header m 
--on m.member_number=c.csn 
--and partner in ('IAG','NRMA Multi Policy Discount','NRMA MPD Insurance','NRMA Insurance') 
--and time_stamp >o.sendtime 
 
left anti join omc.audit_form f 
on f.customer_id=c.row_id 
and iag_lead_product='CTP Green Slip' 
and iag_lead_follow_up in ('February 2020','March 2020') 
 
 
where 1=1 
and campaign_id='41334962' 
AND sendtime<'2020-03-13' 
and deliverystatus='Delivered' 
and opentoclick like 'Opened%' 
GROUP BY 1 
order by 1 
''').show(1000) 
spark.sql(''' 
SELECT count(distinct ctp.riid)  
 
from omc.audit_form ctp 
 
inner join omc.member_campaign_summary_table o 
on o.riid=ctp.riid 
and o.campaign_id='41334962' 
AND o.sendtime<'2020-03-13' 
and o.deliverystatus='Delivered' 
--and o.opentoclick like 'Opened%' 
 
where 1=1 
and iag_lead_product='CTP Green Slip' 
and iag_lead_follow_up in ('February 2020') 
''').show(1000) 
spark.sql(''' 
SELECT count(distinct ctp.riid)  
 
from omc.audit_form ctp 
 
inner join omc.member_campaign_summary_table o 
on o.riid=ctp.riid 
and o.campaign_id='41334962' 
AND o.sendtime<'2020-03-13' 
and o.deliverystatus='Delivered' 
--and o.opentoclick like 'Opened%' 
 
where 1=1 
and iag_lead_product='CTP Green Slip' 
and iag_lead_follow_up in ('March 2020') 
''').show(1000) 
SELECT min(m.time_stamp),max(m.time_stamp) 
 
from omc.member_campaign_summary_table o 
 
inner join gms.s_contact c 
on c.row_id=o.contact_id 
 
INNER join m4m.return_feed_header m 
on m.member_number=c.csn 
and partner in ('IAG','NRMA Multi Policy Discount','NRMA MPD Insurance','NRMA Insurance') 
and time_stamp >o.sendtime 
 
 
where 1=1 
and campaign_id='41334962' 
AND sendtime<'2020-03-13' 
and deliverystatus='Delivered' 
SELECT month(sendtime), month(m.time_stamp) 
        , count(DISTINCT riid) 
 
from omc.member_campaign_summary_table o 
 
inner join gms.s_contact c 
on c.row_id=o.contact_id 
 
INNER join m4m.return_feed_header m 
on m.member_number=c.csn 
and partner in ('IAG','NRMA Multi Policy Discount','NRMA MPD Insurance','NRMA Insurance') 
and time_stamp >o.sendtime 
 
 
where 1=1 
and campaign_id='41334962' 
AND sendtime<'2020-03-13' 
and deliverystatus='Delivered' 
and opentoclick like 'Opened%' 
GROUP BY 1,2 
 
having(count(m.trx_header_id)<2) 
order by 1,2 
SELECT o.riid, count(m.trx_header_id) 
 
from omc.member_campaign_summary_table o 
 
inner join gms.s_contact c 
on c.row_id=o.contact_id 
 
INNER join m4m.return_feed_header m 
on m.member_number=c.csn 
and partner in ('IAG','NRMA Multi Policy Discount','NRMA MPD Insurance','NRMA Insurance') 
and time_stamp >o.sendtime 
 
 
where 1=1 
and campaign_id='41334962' 
AND sendtime<'2020-03-13' 
and deliverystatus='Delivered' 
and opentoclick like 'Opened%' 
 
GROUP BY 1 
having(count(m.trx_header_id)<2) 
order by 2 DESC 
SELECT cx.attrib_55,count(DISTINCT a.contact_id) 
        --, NDV(a.contact_id) 
        --, NDV(case when opentoclick like 'Opened%' then riid else NULL end) opens 
 
from omc.member_campaign_summary_table a 
inner join gms.s_contact c 
on c.row_id=a.contact_id 
 
INNER JOIN gms.s_contact_x cx 
on cx.par_row_id= c.row_id 
 
 
where 1=1 
and campaign_id='41334962' 
and sendtime>'2020-02-29' 
AND sendtime<'2020-03-13' 
and deliverystatus='Delivered' 
--and opentoclick like '%- Clicked' 
GROUP BY 1 
-- order by 1 
 
SELECT cx.attrib_55,count(DISTINCT a.contact_id) 
        --, NDV(a.contact_id) 
        --, NDV(case when opentoclick like 'Opened%' then riid else NULL end) opens 
 
from omc.member_campaign_summary_table a 
inner join gms.s_contact c 
on c.row_id=a.contact_id 
 
INNER JOIN gms.s_contact_x cx 
on cx.par_row_id= c.row_id 
 
 
where 1=1 
and campaign_id='41334962' 
and sendtime<'2020-02-29' 
--AND sendtime<'2020-03-13' 
and deliverystatus='Delivered' 
--and opentoclick like '%- Clicked' 
GROUP BY 1 