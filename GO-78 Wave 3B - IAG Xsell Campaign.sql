select p.clicked_elements, count(distinct p.customer_id)  
--p.control 
 
from omc.send_level_summary p 
 
inner join omc.campaign_info c 
on c.campaign_id=p.campaign_id 
 
where p.campaign_id='51252622' 
and p.bounce_event_date is null 
--and (p.open_event_date is not null or p.click_event_date is not null)   --34011 
and p.click_event_date is not null  
 
 
group by 1 
order by 2 desc 
select p.control, count(distinct p.customer_id)  
-- 
-- 
 
from omc.send_level_summary p 
 
inner join omc.campaign_info c 
on c.campaign_id=p.campaign_id 
 
where p.campaign_id='51252622' 
and p.bounce_event_date is null 
--and (p.open_event_date is not null or p.click_event_date is not null)    
and p.click_event_date is not null  
and p.unsubbed_event_date is null 
 
 
group by 1 
order by 2 desc 
select e.control, count(distinct e.customer_id)  
 
from omc.send_level_summary e 
 
-- inner join omc.campaign_info c 
-- on c.campaign_id=e.campaign_id 
 
inner join gms.s_contact c 
on c.row_id=e.customer_id 
 
inner join m4m.return_feed_header m 
on m.member_number=c.csn 
and partner in ('IAG','NRMA Multi Policy Discount','NRMA MPD Insurance','NRMA Insurance') 
and to_date(m.time_stamp) between '2020-07-21' and '2020-08-28' 
  
 
where e.campaign_id='51252622' 
and e.bounce_event_date is null 
--and (p.open_event_date is not null or p.click_event_date is not null)   --34011 
--and p.click_event_date is not null  
 
 
group by 1 
order by 1 
 
-- inner join googleanalytics.appdata app 
-- on app.membernumber=r.membernumber 
-- and app.eventyear='2019' 
select iag.band, count(distinct e.customer_id)  
 
from omc.send_level_summary e 
 
inner join gms.s_contact c 
on c.row_id=e.customer_id 
 
inner join campaign_data.aa_dmc2097_iagwave3_campaign_edm_20200720_adhoc iag 
on iag.contact_id=e.customer_id 
 
inner join m4m.return_feed_header m 
on m.member_number=c.csn 
and partner in ('IAG','NRMA Multi Policy Discount','NRMA MPD Insurance','NRMA Insurance') 
and to_date(m.time_stamp) between '2020-07-21' and '2020-08-28' 
  
 
where e.campaign_id='51252622' 
and e.bounce_event_date is null 
and e.control is false 
--and (p.open_event_date is not null or p.click_event_date is not null)   --34011 
--and p.click_event_date is not null  
 
 
group by 1 
order by 1 
 
-- inner join googleanalytics.appdata app 
-- on app.membernumber=r.membernumber 
-- and app.eventyear='2019' 
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
and to_date(h.time_stamp) between '2020-07-21' and '2020-08-28' 
) 
 
select e.control,case When product_code='BOT' then 'Boat Insurance' 
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
        , count(distinct p.member_number) 
 
from omc.send_level_summary e 
 
inner join gms.s_contact c 
on c.row_id=e.customer_id 
 
inner join product p 
on p.member_number=c.csn 
 
where e.campaign_id='51252622' 
and e.bounce_event_date is null 
--and (p.open_event_date is not null or p.click_event_date is not null)   --34011 
--and p.click_event_date is not null  
 
 
group by 1,2 
order by 1 ,2 
 
select o.control 
        --,iag_lead_follow_up 
       -- ,case when lower(lead_modified_by_user)='digital' then 'Digital' Else 'Call Center' end channel 
        , count(distinct a.riid) 
 
from omc.audit_form a 
 
inner join omc.send_level_summary o 
on o.riid=a.riid 
 
where 1=1 
and o.campaign_id='51252622' 
and o.bounce_event_date is null 
--and (o.open_event_date is not null OR o.click_event_date is not null) 
and to_date(cast(from_unixtime(unix_timestamp(event_captured_dt,"dd-MMM-yyyy HH:mm:ss")) as timestamp)) between '2020-07-21' and '2020-08-28' 
 
group by control 
order by control 
select datediff(m.time_stamp,e.send_event_date) as days_btw 
        , count(distinct e.customer_id)  
 
from omc.send_level_summary e 
 
-- inner join omc.campaign_info c 
-- on c.campaign_id=e.campaign_id 
 
inner join gms.s_contact c 
on c.row_id=e.customer_id 
 
inner join m4m.return_feed_header m 
on m.member_number=c.csn 
and partner in ('IAG','NRMA Multi Policy Discount','NRMA MPD Insurance','NRMA Insurance') 
and to_date(m.time_stamp) between '2020-07-21' and '2020-08-28' 
  
 
where e.campaign_id='51252622' 
and e.bounce_event_date is null 
and e.control is false 
and (e.open_event_date is not null or e.click_event_date is not null)   --34011 
--and p.click_event_date is not null  
 
 
group by 1 
order by 1 
 
-- inner join googleanalytics.appdata app 
-- on app.membernumber=r.membernumber 
-- and app.eventyear='2019' 
select CUST_VALUE_CD as loyaltycolor 
        , count(distinct e.customer_id)  
 
from omc.send_level_summary e 
 
-- inner join omc.campaign_info c 
-- on c.campaign_id=e.campaign_id 
 
inner join gms.s_contact c 
on c.row_id=e.customer_id 
 
-- inner join m4m.return_feed_header m 
-- on m.member_number=c.csn 
-- and partner in ('IAG','NRMA Multi Policy Discount','NRMA MPD Insurance','NRMA Insurance') 
-- and to_date(m.time_stamp) between '2020-07-21' and '2020-08-28' 
  
 
where e.campaign_id='51252622' 
and e.bounce_event_date is null 
and e.control is false 
--and (e.open_event_date is not null or e.click_event_date is not null)   --34011 
--and p.click_event_date is not null  
 
 
group by 1 
order by 1 
 
-- inner join googleanalytics.appdata app 
-- on app.membernumber=r.membernumber 
-- and app.eventyear='2019' 
select cx.attrib_55  AS Colour_Plus 
        , count(distinct e.customer_id)  
 
from omc.send_level_summary e 
 
-- inner join omc.campaign_info c 
-- on c.campaign_id=e.campaign_id 
 
inner join gms.s_contact c 
on c.row_id=e.customer_id 
 
inner join gms.s_contact_x as cx  
on c.row_id = cx.par_row_id 
 
-- inner join m4m.return_feed_header m 
-- on m.member_number=c.csn 
-- and partner in ('IAG','NRMA Multi Policy Discount','NRMA MPD Insurance','NRMA Insurance') 
-- and to_date(m.time_stamp) between '2020-07-21' and '2020-08-28' 
  
 
where e.campaign_id='51252622' 
and e.bounce_event_date is null 
and e.control is false  
 
 
group by 1 
order by 1 