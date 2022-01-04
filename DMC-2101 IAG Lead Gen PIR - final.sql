· Analysis period: To be the month Lead Gen email was sent + the next month (to incorporate 21day cooling off period) 
 
o In this case for Feb/March send the analysis period would be March and April 
 
  
 
· The conversion will be based on Total Members who opened the eDM and signed up for IAG divided by the total number of Members who were sent the eDM 
 
o The only way to sign up to IAG from the email would be the link taking the member to the IAG landing page. 
 
o We are still using opens and not clicks to cover all bases in case the Member does not click on the link and finds another way to join IAG 
 
  
 
· Analysis Results: To provide conversions per month in order to understand whether there is any merit in analysing results per month 
 
o In this case we will look at conversion in March and conversions in April –this will help dictate future direction 
 
  
 
· Thus I have created a PIR JIRA for early May to analyse the results from the first send and use these results to design the future PIR expectations - DMC-2011 
 
o Such as whether we do it every month, or use a cumulative approach, should we create an automated template or should be build this manually. 
SELECT * FROM omc.temp_contingency LIMIT 100; 
SELECT * from omc.send_level_summary 
where 1=1 
and campaign_id='41334962' 
and bounce_event_date is null 
and open_event_date is not null 
and click_event_date is not null 
SELECT count(DISTINCT riid) from omc.send_level_summary 
where 1=1 
and campaign_id='41334962' 
and bounce_event_date is null 
and to_date(send_event_date)>'2020-02-01' 
and to_date(send_event_date)<'2020-06-24' 
SELECT count(DISTINCT riid) 
 
from omc.member_campaign_summary_table 
where 1=1 
and campaign_id='41334962' 
and to_date(sendtime)>'2020-02-02' 
--AND sendtime<'2020-03-13' 
and deliverystatus='Delivered' 
--and opentoclick like 'Opened%' 
--campaign_name like '%IAG%' 
select distinct sendtime from omc.member_campaign_summary_table 
where riid='510057082' 
and campaign_id='41334962' 
SELECT count(DISTINCT o.riid) from omc.member_campaign_summary_table o --1106 
 
INNER JOIN omc.send_level_summary s 
on s.riid=o.riid 
 
 
where 1=1 
and o.campaign_id='41334962' 
and sendtime>'2020-05-31' 
and sendtime<'2020-06-24' 
select to_date(sendtime), count(distinct riid)  
 
from omc.member_campaign_summary_table 
where 1=1 
and campaign_id='41334962' 
and sendtime>'2020-05-01' 
and sendtime<'2020-05-31' 
group by 1 
order by 1  
SELECT to_date(from_utc_timestamp(send_event_date,'AEST')), count(DISTINCT customer_id) 
 
from omc.send_level_summary 
where 1=1 
and campaign_id='41334962' 
and bounce_event_date is null 
and to_date(send_event_date)>'2020-05-01' 
and to_date(send_event_date)<'2020-05-31' 
 
group by 1 
order by 1  
spark.sql(''' 
SELECT   l.channel 
        ,month(to_date(from_utc_timestamp(send_event_date,'AEST'))) as send_month 
        , count(DISTINCT riid) total_sent 
        , count(distinct customer_id) total_contacts 
        --, NDV(case when opentoclick like 'Opened%' then riid else NULL end) opens 
 
from omc.send_level_summary o 
  
left join campaign_data.aa_dmc2101_iagleadgen l 
on l.riid=o.riid 
where 1=1 
and campaign_id='41334962' 
and to_date(send_event_date)>'2020-02-02' 
and to_date(send_event_date)<'2020-06-25' 
and bounce_event_date is null 
--and open_event_date is not null 
--and click_event_date is not null 
GROUP BY 1,2 
order by 1,2 
''').show(1000,False) 
spark.sql(''' 
SELECT   l.channel 
        ,month(to_date(from_utc_timestamp(o.send_event_date,'AEST'))) as send_month 
        , count(DISTINCT o.riid) total_sent 
        , count(distinct o.customer_id) total_contacts 
        --, NDV(case when opentoclick like 'Opened%' then riid else NULL end) opens 
 
from omc.send_level_summary o 
  
left join campaign_data.aa_dmc2101_iagleadgen l 
on l.riid=o.riid 
 
where 1=1 
and campaign_id='41334962' 
and to_date(o.send_event_date)>'2020-02-02' 
and to_date(o.send_event_date)<'2020-06-25' 
and bounce_event_date is null 
--and open_event_date is not null 
--and click_event_date is not null 
GROUP BY 1,2 
order by 1,2 
''').show(1000,False) 
spark.sql(''' 
SELECT   month(to_date(from_utc_timestamp(send_event_date,'AEST'))) as send_month 
        , count(DISTINCT riid) total_sent 
        , count(distinct customer_id) total_contacts 
        --, NDV(case when opentoclick like 'Opened%' then riid else NULL end) opens 
 
from omc.send_level_summary 
where 1=1 
and campaign_id='41334962' 
and to_date(send_event_date)>'2020-02-02' 
and to_date(send_event_date)<'2020-06-25' 
and bounce_event_date is null 
--and open_event_date is not null 
--and click_event_date is not null 
GROUP BY 1 
order by 1 
''').show(1000,False) 
SELECT   month(to_date(from_utc_timestamp(send_event_date,'AEST'))) as send_month 
        , count(DISTINCT riid) total_sent 
        --, count(distinct customer_id) total_contacts 
        --, NDV(case when opentoclick like 'Opened%' then riid else NULL end) opens 
 
from omc.send_level_summary 
where 1=1 
and campaign_id='41334962' 
and to_date(send_event_date)>'2020-02-02' 
and to_date(send_event_date)<'2020-06-25' 
and bounce_event_date is null 
and open_event_date is not null 
AND click_event_date is not null 
GROUP BY 1 
order by 1 
SELECT   case when segment is null then 'Default' else segment end segment 
        ,month(to_date(from_utc_timestamp(send_event_date,'AEST'))) as send_month 
        , count(DISTINCT riid) total_sent 
        , NDV(customer_id) total_contacts 
        --, NDV(case when opentoclick like 'Opened%' then riid else NULL end) opens 
 
from omc.send_level_summary 
where 1=1 
and campaign_id='41334962' 
and to_date(send_event_date)>'2020-02-02' 
and to_date(send_event_date)<'2020-06-25' 
and bounce_event_date is null 
--and open_event_date is not null 
--and click_event_date is not null 
GROUP BY 1,2 
order by 1,2 
SELECT case when segment is null then 'Default' else segment end segment 
        , month(to_date(from_utc_timestamp(send_event_date,'AEST'))) as send_month 
        , count(DISTINCT riid) total_sent 
        , NDV(customer_id) total_contacts 
        --, NDV(case when opentoclick like 'Opened%' then riid else NULL end) opens 
 
from omc.send_level_summary 
where 1=1 
and campaign_id='41334962' 
and to_date(send_event_date)>'2020-02-02' 
and to_date(send_event_date)<'2020-06-25' 
and bounce_event_date is null 
and (open_event_date is not null OR click_event_date is not null) 
GROUP BY 1,2 
order by 1,2 
SELECT case when segment is null then 'Default' else segment end segment 
        , month(to_date(from_utc_timestamp(send_event_date,'AEST'))) as send_month 
        , count(DISTINCT riid) total_sent 
        , NDV(customer_id) total_contacts 
 
from omc.send_level_summary 
where 1=1 
and campaign_id='41334962' 
and to_date(send_event_date)>'2020-02-02' 
and to_date(send_event_date)<'2020-06-25' 
and bounce_event_date is null 
--and open_event_date is not null 
and click_event_date is not null 
GROUP BY 1,2 
order by 1,2 
SELECT  
--case when segment is null then 'Default' else segment end segment 
         month(to_date(from_utc_timestamp(send_event_date,'AEST'))) as send_month 
        , count(DISTINCT o.riid) as converted 
 
from omc.send_level_summary o 
 
inner join gms.s_contact c 
on c.row_id=o.customer_id 
 
-- INNER join m4m.return_feed_header m 
-- on m.member_number=c.csn 
-- and partner in ('IAG','NRMA Multi Policy Discount','NRMA MPD Insurance','NRMA Insurance') 
-- and time_stamp >o.send_event_date 
 
 
where 1=1 
and campaign_id='41334962' 
and to_date(send_event_date)>'2020-02-02' 
and to_date(send_event_date)<'2020-06-25' 
and bounce_event_date is null 
and (open_event_date is not null OR click_event_date is not null) 
--and o.segment!='CTP' 
GROUP BY 1 
order by 1 
SELECT month(to_date(from_utc_timestamp(send_event_date,'AEST'))) as send_month 
        , count(DISTINCT o.riid) as converted 
 
from omc.send_level_summary o 
 
inner join gms.s_contact c 
on c.row_id=o.customer_id 
 
INNER join m4m.return_feed_header m 
on m.member_number=c.csn 
and partner in ('IAG','NRMA Multi Policy Discount','NRMA MPD Insurance','NRMA Insurance') 
and time_stamp >o.send_event_date 
 
 
where 1=1 
and campaign_id='41334962' 
and to_date(send_event_date)>'2020-02-02' 
and to_date(send_event_date)<'2020-06-25' 
and bounce_event_date is null 
and (open_event_date is not null OR click_event_date is not null) 
--and o.segment!='CTP' 
GROUP BY 1 
order by 1 
spark.sql(''' 
select iag_lead_follow_up,case when lower(lead_modified_by_user)='digital' then 'Digital' Else 'Call Center' end channel, count(distinct riid) 
 from omc.audit_form 
 where 1=1 
 and iag_lead_follow_up in ('February 2020','March 2020','April 2020','May 2020','June 2020') 
 group by 1,2 
 order by 1,2 
''').show(100) 
spark.sql(''' 
select iag_lead_follow_up,case when lower(lead_modified_by_user)='digital' then 'Digital' Else 'Call Center' end channel, count(distinct a.riid) 
 
from omc.audit_form a 
 
inner join omc.send_level_summary o 
on o.riid=a.riid 
 
where 1=1 
and a.iag_lead_follow_up in ('February 2020','March 2020','April 2020','May 2020','June 2020') 
and o.campaign_id='41334962' 
and to_date(o.send_event_date)>'2020-02-02' 
and to_date(o.send_event_date)<'2020-06-25' 
and o.bounce_event_date is null 
and (o.open_event_date is not null OR o.click_event_date is not null) 
group by 1,2 
order by 1,2 
''').show(100) 
spark.sql(''' 
select iag_lead_follow_up,case when lower(lead_modified_by_user)='digital' then 'Digital' Else 'Call Center' end channel, count(distinct a.riid) 
 
from omc.audit_form a 
 
inner join omc.send_level_summary o 
on o.riid=a.riid 
 
inner join gms.s_contact c 
on c.row_id=o.customer_id 
 
INNER join m4m.return_feed_header m 
on m.member_number=c.csn 
and partner in ('IAG','NRMA Multi Policy Discount','NRMA MPD Insurance','NRMA Insurance') 
and time_stamp >o.send_event_date 
 
where 1=1 
and a.iag_lead_follow_up in ('February 2020','March 2020','April 2020','May 2020','June 2020') 
and o.campaign_id='41334962' 
and to_date(o.send_event_date)>'2020-02-02' 
and to_date(o.send_event_date)<'2020-06-25' 
and o.bounce_event_date is null 
and (o.open_event_date is not null OR o.click_event_date is not null) 
group by 1,2 
order by 1,2 
''').show(100) 
select h.member_number 
        , case when d.sub_category is null then d.item_description 
                else concat(d.item_description,' - ',d.sub_category) end product_code 
        , d.item_description 
        , d.sub_category 
        , d.product_id  
from m4m.return_feed_header h 
inner join m4m.return_feed_detail d 
on h.trx_header_id=d.trx_header_id 
where partner in ('IAG','NRMA Multi Policy Discount','NRMA MPD Insurance','NRMA Insurance') 
--and d.sub_category is not null 
--When sub_category = NULL then item_description;  
--Else combine item_description with sub_category. 
 
with product as ( 
select distinct case when d.sub_category is null then d.item_description 
                else concat(d.item_description,' - ',d.sub_category) end product_code 
        , d.trx_header_id 
        , h.member_number 
        , h.time_stamp 
from m4m.return_feed_header h 
inner join m4m.return_feed_detail d 
on h.trx_header_id=d.trx_header_id 
where partner in ('IAG','NRMA Multi Policy Discount','NRMA MPD Insurance','NRMA Insurance') 
and h.time_stamp between '2018-06-26' and '2020-06-25' 
) 
select case When product_code='BOT' then 'Boat Insurance' 
            when product_code='CVT' then 'Caravan Insurance– On site, Touring' 
            When product_code='HOM - BLDG' then 'Home Insurance - Building' 
            when product_code='HOM - CONT' then 'Home Insurance – Content' 
            When product_code='HOM - HPAC' then 'Home Insurance - Combined Building and Contents' 
            When product_code='HOM - LAND' then 'Home Insurance – Landlord' 
            when product_code='MOT - BKCP' then 'Bike Insurance - Comprehensive' 
            When product_code='MOT - BKTP' then 'Bike Insurance - Third Party' 
            when product_code='MOT - CRCP' then 'Car Insurance - Comprehensive' 
            When product_code='MOT - CRFT' then 'Car Insurance - Fire & Theft' 
            when product_code='MOT - CRTP' then 'Car Insurance – Third Party Property Damage' 
                        else 'Other' end product_name 
        --, product_code 
        , count(distinct member_number) 
 
from product 
 
group by 1 
order by 1 
with product as ( 
select distinct case when d.sub_category is null then d.item_description 
                else concat(d.item_description,' - ',d.sub_category) end product_code 
        , d.trx_header_id 
        , h.member_number 
        , h.time_stamp 
from m4m.return_feed_header h 
inner join m4m.return_feed_detail d 
on h.trx_header_id=d.trx_header_id 
where partner in ('IAG','NRMA Multi Policy Discount','NRMA MPD Insurance','NRMA Insurance') 
) 
select case When product_code='BOT' then 'Boat Insurance' 
            when product_code='CVT' then 'Caravan Insurance– On site, Touring' 
            When product_code='HOM - BLDG' then 'Home Insurance - Building' 
            when product_code='HOM - CONT' then 'Home Insurance – Content' 
            When product_code='HOM - HPAC' then 'Home Insurance - Combined Building and Contents' 
            When product_code='HOM - LAND' then 'Home Insurance – Landlord' 
            when product_code='MOT - BKCP' then 'Bike Insurance - Comprehensive' 
            When product_code='MOT - BKTP' then 'Bike Insurance - Third Party' 
            when product_code='MOT - CRCP' then 'Car Insurance - Comprehensive' 
            When product_code='MOT - CRFT' then 'Car Insurance - Fire & Theft' 
            when product_code='MOT - CRTP' then 'Car Insurance – Third Party Property Damage' 
                        else 'Other' end product_name 
        --, product_code 
        , count(distinct o.customer_id) 
 
from omc.send_level_summary o 
 
inner join gms.s_contact c 
on c.row_id=o.customer_id 
 
INNER join product m 
on m.member_number=c.csn 
and time_stamp >o.send_event_date 
 
 
where 1=1 
and campaign_id='41334962' 
and to_date(send_event_date)>'2020-02-02' 
and to_date(send_event_date)<'2020-06-25' 
and bounce_event_date is null 
and (open_event_date is not null OR click_event_date is not null) 
 
group by 1 
order by 1 
SELECT c.cust_value_cd as loyalty_colour 
        , count(DISTINCT o.riid) as converted 
 
from omc.send_level_summary o 
 
inner join gms.s_contact c 
on c.row_id=o.customer_id 
 
INNER join m4m.return_feed_header m 
on m.member_number=c.csn 
and partner in ('IAG','NRMA Multi Policy Discount','NRMA MPD Insurance','NRMA Insurance') 
and time_stamp >o.send_event_date 
 
 
where 1=1 
and campaign_id='41334962' 
and to_date(send_event_date)>'2020-02-02' 
and to_date(send_event_date)<'2020-06-25' 
and bounce_event_date is null 
and (open_event_date is not null OR click_event_date is not null) 
--and o.segment!='CTP' 
GROUP BY 1 
order by 1 
SELECT c.cust_value_cd as loyalty_colour 
        , count(DISTINCT o.riid) as converted 
 
from omc.send_level_summary o 
 
inner join gms.s_contact c 
on c.row_id=o.customer_id 
 
-- INNER join m4m.return_feed_header m 
-- on m.member_number=c.csn 
-- and partner in ('IAG','NRMA Multi Policy Discount','NRMA MPD Insurance','NRMA Insurance') 
-- and time_stamp >o.send_event_date 
 
 
where 1=1 
and campaign_id='41334962' 
and to_date(send_event_date)>'2020-02-02' 
and to_date(send_event_date)<'2020-06-25' 
and bounce_event_date is null 
and (open_event_date is not null OR click_event_date is not null) 
--and o.segment!='CTP' 
GROUP BY 1 
order by 1 
with first_trans as ( 
select min(time_stamp) as first_trans, member_number  
from m4m.return_feed_header 
where partner in ('IAG','NRMA Multi Policy Discount','NRMA MPD Insurance','NRMA Insurance') 
group by 2 
) 
 
SELECT  cx.attrib_55 as colour_plus 
        , count(DISTINCT f.member_number) 
 
from gms.s_contact c 
 
INNER join first_trans f 
on f.member_number=c.csn 
 
INNER JOIN gms.s_contact_x cx 
on cx.par_row_id= c.row_id 
 
 
where 1=1 
and f.first_trans between '2019-06-25' and '2020-06-24' 
GROUP BY 1 
order by 1 
SELECT cx.attrib_55 as colour_plus 
        , count(DISTINCT o.riid) as converted 
 
from omc.send_level_summary o 
 
inner join gms.s_contact c 
on c.row_id=o.customer_id 
 
INNER JOIN gms.s_contact_x cx 
on cx.par_row_id= c.row_id 
 
INNER join m4m.return_feed_header m 
on m.member_number=c.csn 
and partner in ('IAG','NRMA Multi Policy Discount','NRMA MPD Insurance','NRMA Insurance') 
and time_stamp >o.send_event_date 
 
 
where 1=1 
and campaign_id='41334962' 
and to_date(send_event_date)>'2020-02-02' 
and to_date(send_event_date)<'2020-06-25' 
and bounce_event_date is null 
and (open_event_date is not null OR click_event_date is not null) 
--and o.segment!='CTP' 
GROUP BY 1 
order by 1 
SELECT cx.attrib_55 as colour_plus 
        , count(DISTINCT o.riid) as converted 
 
from omc.send_level_summary o 
 
inner join gms.s_contact c 
on c.row_id=o.customer_id 
 
INNER JOIN gms.s_contact_x cx 
on cx.par_row_id= c.row_id 
 
-- INNER join m4m.return_feed_header m 
-- on m.member_number=c.csn 
-- and partner in ('IAG','NRMA Multi Policy Discount','NRMA MPD Insurance','NRMA Insurance') 
-- and time_stamp >o.send_event_date 
 
 
where 1=1 
and campaign_id='41334962' 
and to_date(send_event_date)>'2020-02-02' 
and to_date(send_event_date)<'2020-06-25' 
and bounce_event_date is null 
and (open_event_date is not null OR click_event_date is not null) 
--and o.segment!='CTP' 
GROUP BY 1 
order by 1 
iag first time acquired between month of feb and june 
Leag Gen in general and eDM performance 
spark.sql(''' 
select riid 
        , customer_id 
        ,event_captured_dt 
        , form_id 
        , form_name 
        , iag_lead_product 
        , case when lower(lead_modified_by_user)='digital' then 'Digital' Else 'Call Center' end channel 
        , iag_lead_follow_up 
        , send_email 
        , lead_type 
--iag_lead_follow_up,, count(distinct riid) 
 from omc.audit_form 
 where form_id like 'IAG_FORM%' 
-- and iag_lead_follow_up in ('February 2020','March 2020','April 2020','May 2020','June 2020') 
-- group by 1,2 
-- order by 1,2 
''').show(100) 
spark.sql(''' 
create table campaign_data.aa_dmc2101_iagleadGen as 
select riid 
        , customer_id 
        , event_captured_dt 
        , substring(event_captured_dt,1,11) as capture_Date 
        , substring(substring(event_captured_dt,1,11),4,3) as Lead_month 
        , form_id 
        , form_name 
        , iag_lead_product 
        , case when lower(lead_modified_by_user)='digital' then 'Digital' Else 'Call Center' end channel 
        , iag_lead_follow_up 
        , send_email 
        , lead_type 
 from omc.audit_form 
 where 1=1 
 and form_id like 'IAG_FORM%' 
''').show(100) 
spark.sql(''' 
select count(distinct riid) total_leads 
        , case when lower(lead_modified_by_user)='digital' then 'Digital' Else 'Call Center' end channel 
        , substring(substring(event_captured_dt,1,11),4,3) as Lead_month 
 from omc.audit_form 
 where form_id like 'IAG_FORM%' 
group by 2,3 
order by 2,3 
''').show(10000) 
spark.sql(''' 
select count(distinct riid) total_leads 
       -- , case when lower(lead_modified_by_user)='digital' then 'Digital' Else 'Call Center' end channel 
       -- , substring(substring(event_captured_dt,1,11),4,3) as Lead_month 
 from omc.audit_form 
 where where form_id like 'IAG_FORM%' 
-- and send_email='Y' 
-- and iag_lead_follow_up in ('February 2020','March 2020','April 2020','May 2020','June 2020') 
--group by 2,3 
--order by 2,3 
''').show(10000) 
spark.sql(''' 
select count(distinct riid) total_leads 
       -- , case when lower(lead_modified_by_user)='digital' then 'Digital' Else 'Call Center' end channel 
       -- , substring(substring(event_captured_dt,1,11),4,3) as Lead_month 
 from omc.audit_form 
 where where form_id like 'IAG_FORM%' 
 and send_email='Y' 
 and iag_lead_follow_up in ('February 2020','March 2020','April 2020','May 2020','June 2020','Immediately') 
--group by 2,3 
--order by 2,3 
''').show(10000) 
select substring(substring('24-JUN-2020 09:24:39',1,11),4,3) from gms.s_contact 
SELECT c.cust_value_cd as loyalty_colour 
--cx.attrib_55 as colour_plus 
        , count(DISTINCT o.riid) as converted 
 
from campaign_data.aa_dmc2101_iagleadgen o 
 
inner join gms.s_contact c 
on c.row_id=o.customer_id 
 
INNER JOIN gms.s_contact_x cx 
on cx.par_row_id= c.row_id 
 
-- INNER join m4m.return_feed_header m 
-- on m.member_number=c.csn 
-- and partner in ('IAG','NRMA Multi Policy Discount','NRMA MPD Insurance','NRMA Insurance') 
-- and time_stamp >o.send_event_date 
 
 
where 1=1 
-- and campaign_id='41334962' 
-- and to_date(send_event_date)>'2020-02-02' 
-- and to_date(send_event_date)<'2020-06-25' 
-- and bounce_event_date is null 
-- and (open_event_date is not null OR click_event_date is not null) 
-- --and o.segment!='CTP' 
GROUP BY 1 
order by 1 
SELECT iag_lead_product, lead_month, count(distinct riid) FROM campaign_data.aa_dmc2101_iagleadgen  
group by 1,2 
order by 1,2 
--08/12 - 30/06 event captures dates 
SELECT count(DISTINCT event_captured_dt) FROM campaign_data.aa_dmc2101_iagleadgen  
where lead_month!='NOV' 
 
SELECT count(DISTINCT riid) FROM campaign_data.aa_dmc2101_iagleadgen 
--where send_email='Y' 
--and iag_lead_follow_up in ('February 2020','March 2020','April 2020','May 2020','June 2020','Immediately') 
SELECT l.channel 
        , month(to_date(from_utc_timestamp(send_event_date,'AEST'))) as send_month 
        , count(DISTINCT o.riid) as target 
 
from omc.send_level_summary o 
 
inner join gms.s_contact c 
on c.row_id=o.customer_id 
 
-- INNER join m4m.return_feed_header m 
-- on m.member_number=c.csn 
-- and partner in ('IAG','NRMA Multi Policy Discount','NRMA MPD Insurance','NRMA Insurance') 
-- and time_stamp >o.send_event_date 
 
inner join campaign_data.aa_dmc2101_iagleadgen l 
on l.riid=o.riid 
 
 
where 1=1 
and campaign_id='41334962' 
and to_date(send_event_date)>'2020-02-02' 
and to_date(send_event_date)<'2020-06-25' 
and bounce_event_date is null 
and (open_event_date is not null OR click_event_date is not null) 
--and o.segment!='CTP' 
GROUP BY 1,2 
order by 1,2 
SELECT case when segment is null then 'Default' else segment end segment 
        , month(to_date(from_utc_timestamp(send_event_date,'AEST'))) as send_month 
        , count(DISTINCT o.riid) as target 
 
from omc.send_level_summary o 
 
inner join gms.s_contact c 
on c.row_id=o.customer_id 
 
-- INNER join m4m.return_feed_header m 
-- on m.member_number=c.csn 
-- and partner in ('IAG','NRMA Multi Policy Discount','NRMA MPD Insurance','NRMA Insurance') 
-- and time_stamp >o.send_event_date 
 
left join campaign_data.aa_dmc2101_iagleadgen l 
on l.riid=o.riid 
 
 
where 1=1 
and campaign_id='41334962' 
and to_date(send_event_date)>'2020-02-02' 
and to_date(send_event_date)<'2020-06-25' 
and bounce_event_date is null 
and (open_event_date is not null OR click_event_date is not null) 
--and o.segment!='CTP' 
GROUP BY 1,2 
order by 1,2 
SELECT  month(to_date(from_utc_timestamp(send_event_date,'AEST'))) as send_month 
        , month(to_date(from_utc_timestamp(m.time_stamp,'AEST'))) as converted_month 
        , count(DISTINCT o.riid) as converted 
 
from omc.send_level_summary o 
 
inner join gms.s_contact c 
on c.row_id=o.customer_id 
 
INNER join m4m.return_feed_header m 
on m.member_number=c.csn 
and partner in ('IAG','NRMA Multi Policy Discount','NRMA MPD Insurance','NRMA Insurance') 
and time_stamp >o.send_event_date 
 
 
where 1=1 
and campaign_id='41334962' 
and to_date(send_event_date)>'2020-02-02' 
and to_date(send_event_date)<'2020-06-25' 
and bounce_event_date is null 
and (open_event_date is not null OR click_event_date is not null) 
--and o.segment!='CTP' 
GROUP BY 1,2 
order by 1,2 
SELECT  month(to_date(from_utc_timestamp(send_event_date,'AEST'))) as send_month 
        , month(to_date(from_utc_timestamp(m.time_stamp,'AEST'))) as converted_month 
        , count(DISTINCT o.riid) as converted 
 
from omc.send_level_summary o 
 
inner join gms.s_contact c 
on c.row_id=o.customer_id 
 
INNER join m4m.return_feed_header m 
on m.member_number=c.csn 
and partner in ('IAG','NRMA Multi Policy Discount','NRMA MPD Insurance','NRMA Insurance') 
and time_stamp >o.send_event_date 
 
 
where 1=1 
and campaign_id='41334962' 
and to_date(send_event_date)>'2020-02-02' 
and to_date(send_event_date)<'2020-06-25' 
and bounce_event_date is null 
and (open_event_date is not null OR click_event_date is not null) 
and datediff(m.time_stamp,o.send_event_date)<65 
--and o.segment!='CTP' 
GROUP BY 1,2 
order by 1,2 
SELECT  datediff(m.time_stamp,o.send_event_date) as days_since 
        , count(DISTINCT o.riid) as converted 
 
from omc.send_level_summary o 
 
inner join gms.s_contact c 
on c.row_id=o.customer_id 
 
INNER join m4m.return_feed_header m 
on m.member_number=c.csn 
and partner in ('IAG','NRMA Multi Policy Discount','NRMA MPD Insurance','NRMA Insurance') 
and time_stamp >o.send_event_date 
 
 
where 1=1 
and campaign_id='41334962' 
and to_date(send_event_date)>'2020-02-02' 
and to_date(send_event_date)<'2020-04-25' 
and bounce_event_date is null 
and (open_event_date is not null OR click_event_date is not null) 
--and o.segment!='CTP' 
GROUP BY 1 
order by 1 
Select count(distinct af.riid)  
--15764 
--15199 
-- 
from campaign_data.aa_dmc2101_iagleadGen af 
 
where af.event_captured_dt is not null  
and af.event_captured_dt != 'EVENT_CAPTURED_DT' 
--and af.form_id ='IAG_FORM_41248142'  
and to_date(cast(from_unixtime(unix_timestamp(af.event_captured_dt,"dd-MMM-yyyy HH:mm:ss")) as timestamp)) between '2019-12-08' and '2020-06-30'  
Select form_id, count(DISTINCT riid) 
--15764 
--15199 
-- 
from campaign_data.aa_dmc2101_iagleadGen af 
 
where af.event_captured_dt is not null  
and af.event_captured_dt != 'EVENT_CAPTURED_DT' 
--and af.form_id ='IAG_FORM_41248142'  
and to_date(cast(from_unixtime(unix_timestamp(af.event_captured_dt,"dd-MMM-yyyy HH:mm:ss")) as timestamp)) between '2019-12-08' and '2020-06-30'  
group by 1 