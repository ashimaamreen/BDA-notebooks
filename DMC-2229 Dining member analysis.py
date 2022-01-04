select count(distinct h.member_number) 
--,month(time_stamp) 
                 
from m4m.return_feed_header h 
 
inner join gms.s_contact c 
on c.csn=h.member_number 
 
inner join gms.s_addr_per ad 
on ad.row_id=c.pr_per_addr_id 
 
inner join m4m.return_feed_detail d 
on h.trx_header_id=d.trx_header_id 
 
where h.partner='Frequent Values' 
and h.time_stamp<'2021-01-27' 
select year(time_stamp),count(distinct h.trx_header_id) 
--,month(time_stamp) 
                 
from m4m.return_feed_header h 
 
inner join gms.s_contact c 
on c.csn=h.member_number 
 
inner join gms.s_addr_per ad 
on ad.row_id=c.pr_per_addr_id 
 
inner join m4m.return_feed_detail d 
on h.trx_header_id=d.trx_header_id 
 
where h.partner='Frequent Values' 
and h.time_stamp<'2021-01-27' 
 
group by 1 
order by 1 
-- order by 3 desc 
spark.sql(''' 
select upper(ad.city) as suburb 
        , ad.zipcode 
        , count(distinct member_number) 
                 
from m4m.return_feed_header h 
 
inner join gms.s_contact c 
on c.csn=h.member_number 
 
inner join gms.s_addr_per ad 
on ad.row_id=c.pr_per_addr_id 
 
inner join m4m.return_feed_detail d 
on h.trx_header_id=d.trx_header_id 
 
where h.partner='Frequent Values' 
and h.time_stamp<'2021-01-27' 
 
group by 1,2 
order by 3 desc 
''').show(100000,False) 
spark.sql(''' 
select upper(ad.city) as suburb,ad.zipcode, count(distinct h.trx_header_id) 
                 
from m4m.return_feed_header h 
 
inner join gms.s_contact c 
on c.csn=h.member_number 
 
inner join gms.s_addr_per ad 
on ad.row_id=c.pr_per_addr_id 
 
inner join m4m.return_feed_detail d 
on h.trx_header_id=d.trx_header_id 
 
where h.partner='Frequent Values' 
and h.time_stamp<'2021-01-27' 
 
group by 1,2 
order by 3 desc 
''').show(10000,False) 
select CASE  
                        WHEN  
                         (ad.zipcode >= '2000' and ad.zipcode <= '2082' or 
                          ad.zipcode >= '2084' and ad.zipcode <= '2234' or  
                          ad.zipcode >= '2555' and ad.zipcode <= '2574' or  
                          ad.zipcode >= '2745' and ad.zipcode <= '2770' or  
                          ad.zipcode >= '2775' and ad.zipcode <= '2775') 
                          AND (upper(ad.country) = 'AUSTRALIA' or upper(ad.country) = 'AU') 
                          THEN 'METROPOLITAN' 
                        WHEN  
                         (ad.zipcode >= '2083' and ad.zipcode <= '2083' or  
                          ad.zipcode >= '2250' and ad.zipcode <= '2338' or  
                          ad.zipcode >= '2415' and ad.zipcode <= '2423' or  
                          ad.zipcode >= '2425' and ad.zipcode <= '2425' or  
                          ad.zipcode >= '2428' and ad.zipcode <= '2428' or  
                          ad.zipcode >= '2500' and ad.zipcode <= '2535' or  
                          ad.zipcode >= '2538' and ad.zipcode <= '2541' or  
                          ad.zipcode >= '2575' and ad.zipcode <= '2578' or  
                          ad.zipcode >= '2600' and ad.zipcode <= '2617' or  
                          ad.zipcode >= '2773' and ad.zipcode <= '2774' or  
                          ad.zipcode >= '2776' and ad.zipcode <= '2786' or  
                          ad.zipcode >= '2900' and ad.zipcode <= '2914') 
                          AND (upper(ad.country) = 'AUSTRALIA' or upper(ad.country) = 'AU')  
                          THEN  'REGIONAL' 
                        WHEN  
                          (ad.zipcode >= '2339' and ad.zipcode <= '2411' or  
                          ad.zipcode >= '2424' and ad.zipcode <= '2424' or  
                          ad.zipcode >= '2426' and ad.zipcode <= '2427' or  
                          ad.zipcode >= '2429' and ad.zipcode <= '2490' or  
                          ad.zipcode >= '2536' and ad.zipcode <= '2537' or  
                          ad.zipcode >= '2545' and ad.zipcode <= '2551' or  
                          ad.zipcode >= '2579' and ad.zipcode <= '2594' or  
                          ad.zipcode >= '2618' and ad.zipcode <= '2739' or  
                          ad.zipcode >= '2787' and ad.zipcode <= '2898' or  
                          ad.zipcode >= '6798' and ad.zipcode <= '6799') 
                          AND (upper(ad.country) = 'AUSTRALIA' or upper(ad.country) = 'AU')  
                          THEN  'RURAL' 
                        WHEN  
                          (ad.zipcode >= '0800' and ad.zipcode <= '0886' or  
                          ad.zipcode >= '3000' and ad.zipcode <= '6770' or  
                          ad.zipcode >= '6907' and ad.zipcode <= '7470' or  
                          ad.zipcode >= '7471' ) 
                          AND (upper(ad.country) = 'AUSTRALIA' or upper(ad.country) = 'AU')  
                          THEN  'INTERSTATE' 
                        ELSE 'UNKNOWN' 
                        END AS Region_Name 
                , count(distinct h.trx_header_id) 
                 
from m4m.return_feed_header h 
 
inner join gms.s_contact c 
on c.csn=h.member_number 
 
inner join gms.s_addr_per ad 
on ad.row_id=c.pr_per_addr_id 
 
inner join m4m.return_feed_detail d 
on h.trx_header_id=d.trx_header_id 
 
where h.partner='Frequent Values' 
and h.time_stamp<'2021-01-27' 
 
group by 1 
order by 2 desc 
select  d.sub_category 
            , count(h.trx_header_id) 
            , count(distinct h.member_number) 
                 
from m4m.return_feed_header h 
 
inner join m4m.return_feed_detail d 
on h.trx_header_id=d.trx_header_id 
 
where h.partner='Frequent Values' 
and h.time_stamp<'2021-01-27' 
-- and d.product_id rlike 'KFC' 
-- case when d.product_id rlike 'McCafe' then 'McDonalds'  
--                 when d.product_id rlike 'KFC' then 'KFC' 
--                 else d.product_id end Category 
 
group by 1 
order by 2 desc 
select distinct h.member_number 
                , h.trx_header_id 
                , d.trx_detail_id 
                , d.category 
                , d.sub_category 
                , h.time_stamp 
                , product_id 
                , item_description 
                -- , short_description 
                --, d.location 
                 
from m4m.return_feed_header h 
 
inner join m4m.return_feed_detail d 
on h.trx_header_id=d.trx_header_id 
 
where h.partner='Frequent Values' 
and h.time_stamp<'2021-01-27' 
select max(h.time_stamp), min(h.time_Stamp) 
--product_id, count(distinct h.member_number) 
                 
from m4m.return_feed_header h 
 
inner join m4m.return_feed_detail d 
on h.trx_header_id=d.trx_header_id 
 
where h.partner='Frequent Values' 
and product_id='McCafe' 
--and d.sub_category='TAKEAWAY' 
--and h.time_stamp>'2020-01-27' 
--and h.time_stamp<'2021-01-27' 
--group by 1 
--order by 2 desc 
--count(distinct member_number) 
spark.sql(''' 
create table campaign_data.aa_dmc2299_hack_map_20200128 as 
SELECT DISTINCT 
    riid, 
    customer_id 
     
FROM 
    ( 
        SELECT riid, customer_id FROM omc.email_sent 
        UNION ALL 
        SELECT riid, customer_id FROM omc.email_skipped 
        UNION ALL 
        SELECT riid, customer_id FROM omc.sms_sent 
        UNION ALL 
        SELECT riid, customer_id FROM omc.sms_skipped 
        UNION ALL 
        SELECT riid, customer_id FROM omc.push_sent 
        UNION ALL 
        SELECT riid, customer_id FROM omc.push_skipped 
        UNION ALL 
        SELECT 
            SUBSTRING(contact_riid, 2, LENGTH(contact_riid) - 2) AS riid, 
            SUBSTRING(customer_id_, 2, LENGTH(customer_id_) - 2) AS customer_id 
        FROM omc.riid_mapping 
         
    ) AS aleph 
WHERE 
    customer_id != '' 
''').createOrReplaceTempView('hack_map') 
SELECT DISTINCT hm.customer_id as contact_id from omc.send_level_summary p 
 
INNER JOIN campaign_data.aa_dmc2299_hack_map_20200128 hm 
on p.riid=hm.riid 
 
inner join omc.campaign_info c 
on c.campaign_id=p.campaign_id 
 
where p.channel='Push' 
and to_date(p.send_event_date) between '2020-01-01' and '2021-01-01' 
and c.dispatchable_type ='PushIOCampaign' 
and upper(c.campaign_name) not like '%UAT%' 
and upper(c.campaign_name) not like '%TEST%' 
and upper(c.campaign_name) not like '%ICX%'  
select case when trim(c.x_inv_email_1) = 'Y' or c.email_addr is null or cx.attrib_36='No' then 'N'  else 'Y' end email_able 
        , case when trim(ad.premise_flg) = 'Y' or ad.addr_name is null then 'N' else 'Y' end dm_able 
        , case when p.contact_id is null then 'N' else 'Y' end push_sent 
        , count(distinct h.member_number) 
from m4m.return_feed_header h 
 
inner join m4m.return_feed_detail d 
on h.trx_header_id=d.trx_header_id 
 
inner join gms.s_contact c 
on c.csn=h.member_number 
 
inner join gms.s_contact_x cx 
on cx.row_id=c.par_row_id 
 
INNER JOIN gms.s_contact_fnx fn 
on fn.par_row_id=c.row_id 
 
inner join gms.s_addr_per as ad         --813620 
on c.pr_per_addr_id = ad.row_id 
 
left join (SELECT DISTINCT hm.customer_id as contact_id from omc.send_level_summary p 
 
INNER JOIN campaign_data.aa_dmc2299_hack_map_20200128 hm 
on p.riid=hm.riid 
 
inner join omc.campaign_info c 
on c.campaign_id=p.campaign_id 
 
where p.channel='Push' 
and to_date(p.send_event_date) between '2020-01-01' and '2021-01-01' 
and c.dispatchable_type ='PushIOCampaign' 
and upper(c.campaign_name) not like '%UAT%' 
and upper(c.campaign_name) not like '%TEST%' 
and upper(c.campaign_name) not like '%ICX%' ) p 
on p.contact_id=c.row_id 
 
where h.partner='Frequent Values' 
and h.time_stamp<'2021-01-27' 
 
 
group by 1,2,3 
select case when trim(c.x_inv_email_1) = 'Y' or c.email_addr is null or cx.attrib_36='No' then 'N'  else 'Y' end email_able 
        , case when trim(ad.premise_flg) = 'Y' or ad.addr_name is null then 'N' else 'Y' end dm_able 
        , case when p.contact_id is null then 'N' else 'Y' end push_sent 
        , count(distinct h.trx_header_id) 
from m4m.return_feed_header h 
 
inner join m4m.return_feed_detail d 
on h.trx_header_id=d.trx_header_id 
 
inner join gms.s_contact c 
on c.csn=h.member_number 
 
inner join gms.s_contact_x cx 
on cx.row_id=c.par_row_id 
 
INNER JOIN gms.s_contact_fnx fn 
on fn.par_row_id=c.row_id 
 
inner join gms.s_addr_per as ad         --813620 
on c.pr_per_addr_id = ad.row_id 
 
left join (SELECT DISTINCT hm.customer_id as contact_id from omc.send_level_summary p 
 
INNER JOIN campaign_data.aa_dmc2299_hack_map_20200128 hm 
on p.riid=hm.riid 
 
inner join omc.campaign_info c 
on c.campaign_id=p.campaign_id 
 
where p.channel='Push' 
and to_date(p.send_event_date) between '2020-01-01' and '2021-01-01' 
and c.dispatchable_type ='PushIOCampaign' 
and upper(c.campaign_name) not like '%UAT%' 
and upper(c.campaign_name) not like '%TEST%' 
and upper(c.campaign_name) not like '%ICX%' ) p 
on p.contact_id=c.row_id 
 
where h.partner='Frequent Values' 
and h.time_stamp<'2021-01-27' 
 
 
group by 1,2,3 
with first_red as ( 
select h.member_number, to_date(min(time_stamp)) first_redemption 
 
from m4m.return_feed_header h 
 
inner join m4m.return_feed_detail d 
on h.trx_header_id=d.trx_header_id 
 
inner join gms.s_contact c 
on c.csn=h.member_number 
 
inner join gms.s_contact_x cx 
on cx.row_id=c.par_row_id 
 
INNER JOIN gms.s_contact_fnx fn 
on fn.par_row_id=c.row_id 
 
inner join gms.s_addr_per as ad         --813620 
on c.pr_per_addr_id = ad.row_id 
 
left join (SELECT DISTINCT hm.customer_id as contact_id from omc.send_level_summary p 
 
INNER JOIN campaign_data.aa_dmc2299_hack_map_20200128 hm 
on p.riid=hm.riid 
 
inner join omc.campaign_info c 
on c.campaign_id=p.campaign_id 
 
where p.channel='Push' 
and to_date(p.send_event_date) between '2020-01-01' and '2021-01-01' 
and c.dispatchable_type ='PushIOCampaign' 
and upper(c.campaign_name) not like '%UAT%' 
and upper(c.campaign_name) not like '%TEST%' 
and upper(c.campaign_name) not like '%ICX%' ) p 
on p.contact_id=c.row_id 
 
where h.partner='Frequent Values' 
and h.time_stamp<'2021-01-27' 
 
group by 1 
) 
 
select month(first_redemption), year(first_redemption) 
        , count(distinct member_number) from first_red 
group by 2,1 
order by 2,1 
spark.sql(''' 
select h.member_number, DATE_FORMAT(time_stamp,'yyyy-MM'), count(distinct h.trx_header_id) 
 
from m4m.return_feed_header h 
 
inner join m4m.return_feed_detail d 
on h.trx_header_id=d.trx_header_id 
 
inner join gms.s_contact c 
on c.csn=h.member_number 
 
inner join gms.s_contact_x cx 
on cx.row_id=c.par_row_id 
 
INNER JOIN gms.s_contact_fnx fn 
on fn.par_row_id=c.row_id 
 
inner join gms.s_addr_per as ad         --813620 
on c.pr_per_addr_id = ad.row_id 
 
left join (SELECT DISTINCT hm.customer_id as contact_id from omc.send_level_summary p 
 
INNER JOIN campaign_data.aa_dmc2299_hack_map_20200128 hm 
on p.riid=hm.riid 
 
inner join omc.campaign_info c 
on c.campaign_id=p.campaign_id 
 
where p.channel='Push' 
and to_date(p.send_event_date) between '2020-01-01' and '2021-01-01' 
and c.dispatchable_type ='PushIOCampaign' 
and upper(c.campaign_name) not like '%UAT%' 
and upper(c.campaign_name) not like '%TEST%' 
and upper(c.campaign_name) not like '%ICX%' ) p 
on p.contact_id=c.row_id 
 
where h.partner='Frequent Values' 
and h.time_stamp<'2021-01-27' 
 
group by 1,2 
order by 1,2 
''').show(100000,False) 
select  distinct h.trx_header_id 
                , d.trx_detail_id 
                , d.category 
                , d.sub_category 
                , h.time_stamp 
                , product_id 
                , item_description 
                , d.`location` 
                 
from m4m.return_feed_header h 
 
inner join m4m.return_feed_detail d 
on h.trx_header_id=d.trx_header_id 
 
where member_number='296804401' 
and h.partner='Frequent Values' 
and h.time_stamp<'2021-01-27' 
 
 
order by h.time_stamp 
select h.member_number, DATE_FORMAT(time_stamp,'yyyy-MM'), count(distinct h.trx_header_id) 
 
from m4m.return_feed_header h 
 
inner join m4m.return_feed_detail d 
on h.trx_header_id=d.trx_header_id 
 
inner join gms.s_contact c 
on c.csn=h.member_number 
 
inner join gms.s_contact_x cx 
on cx.row_id=c.par_row_id 
 
INNER JOIN gms.s_contact_fnx fn 
on fn.par_row_id=c.row_id 
 
inner join gms.s_addr_per as ad         --813620 
on c.pr_per_addr_id = ad.row_id 
 
left join (SELECT DISTINCT hm.customer_id as contact_id from omc.send_level_summary p 
 
INNER JOIN campaign_data.aa_dmc2299_hack_map_20200128 hm 
on p.riid=hm.riid 
 
inner join omc.campaign_info c 
on c.campaign_id=p.campaign_id 
 
where p.channel='Push' 
and to_date(p.send_event_date) between '2020-01-01' and '2021-01-01' 
and c.dispatchable_type ='PushIOCampaign' 
and upper(c.campaign_name) not like '%UAT%' 
and upper(c.campaign_name) not like '%TEST%' 
and upper(c.campaign_name) not like '%ICX%' ) p 
on p.contact_id=c.row_id 
 
where h.partner='Frequent Values' 
and h.time_stamp<'2021-01-27' 
 
group by 1,2 
order by 1,2 
select h.trx_header_id 
                , d.trx_detail_id 
                , d.category 
                , d.sub_category 
                , h.time_stamp 
                , product_id 
                , item_description 
                , d.`location` 
                 
from m4m.return_feed_header h 
 
inner join gms.s_contact c 
on c.csn=h.member_number 
 
inner join gms.s_addr_per ad 
on ad.row_id=c.pr_per_addr_id 
 
inner join m4m.return_feed_detail d 
on h.trx_header_id=d.trx_header_id 
 
where h.partner='Frequent Values' 
and h.time_stamp<'2021-01-27' 
--and upper(ad.city)='GLASGOW' 
 
-- group by 1,2 
-- order by 3 desc 
select  case when cx.attrib_55 in ('CYAN','GREEN','RED') then 'Young' 
                when cx.attrib_55 in ('BROWN','ORANGE','PURPLE','YELLOW') then 'Family' 
                when cx.attrib_55 in ('GREY','KHAKI','LILAC') then 'Mature' else 'None' end Colour_plus 
        , d.category 
        , count(distinct c.csn) 
                 
from m4m.return_feed_header h 
 
inner join gms.s_contact c 
on c.csn=h.member_number 
 
inner join gms.s_contact_x cx 
on cx.par_row_id=c.row_id 
 
inner join gms.s_addr_per ad 
on ad.row_id=c.pr_per_addr_id 
 
inner join m4m.return_feed_detail d 
on h.trx_header_id=d.trx_header_id 
 
-- inner join googleanalytics.appdata app 
-- on app.membernumber=c.csn 
 
where h.partner='Frequent Values' 
and h.time_stamp<'2021-01-27' 
and cx.attrib_55 is not null 
--and d.product_id='Gelatissimo' 
--and d.category='CAFE AND BISTRO DINING' 
 
group by 1,2 
order by 1,2 
select c.cust_value_cd, count(distinct c.csn) 
                 
from m4m.return_feed_header h 
 
inner join gms.s_contact c 
on c.csn=h.member_number 
 
inner join gms.s_contact_x cx 
on cx.par_row_id=c.row_id 
 
inner join gms.s_addr_per ad 
on ad.row_id=c.pr_per_addr_id 
 
inner join m4m.return_feed_detail d 
on h.trx_header_id=d.trx_header_id 
 
where h.partner='Frequent Values' 
and h.time_stamp<'2021-01-27' 
 
group by 1 
order by 1 
select cx.attrib_55, count(distinct c.csn) 
-- 
                 
from googleanalytics.appdata app 
 
inner join gms.s_contact c 
on c.csn=app.membernumber 
 
inner join gms.s_contact_x cx 
on cx.par_row_id=c.row_id 
 
 
where to_date(to_timestamp(eventdate, "yyyyMMdd")) between '2019-02-01' and '2021-01-27' 
 
group by 1 
order by 1 
with first_red as ( 
select h.member_number, to_date(min(time_stamp)) first_redemption 
 
from m4m.return_feed_header h 
 
inner join m4m.return_feed_detail d 
on h.trx_header_id=d.trx_header_id 
 
inner join gms.s_contact c 
on c.csn=h.member_number 
 
inner join gms.s_contact_x cx 
on cx.row_id=c.par_row_id 
 
INNER JOIN gms.s_contact_fnx fn 
on fn.par_row_id=c.row_id 
 
inner join gms.s_addr_per as ad         --813620 
on c.pr_per_addr_id = ad.row_id 
 
left join (SELECT DISTINCT hm.customer_id as contact_id from omc.send_level_summary p 
 
INNER JOIN campaign_data.aa_dmc2299_hack_map_20200128 hm 
on p.riid=hm.riid 
 
inner join omc.campaign_info c 
on c.campaign_id=p.campaign_id 
 
where p.channel='Push' 
and to_date(p.send_event_date) between '2020-01-01' and '2021-01-01' 
and c.dispatchable_type ='PushIOCampaign' 
and upper(c.campaign_name) not like '%UAT%' 
and upper(c.campaign_name) not like '%TEST%' 
and upper(c.campaign_name) not like '%ICX%' ) p 
on p.contact_id=c.row_id 
 
where h.partner='Frequent Values' 
and h.time_stamp<'2021-01-27' 
 
group by 1 
) 
 
select month(first_redemption), year(first_redemption) 
        , count(distinct member_number) from first_red 
group by 2,1 
order by 2,1 
dining_partner_locations = spark.read.load("/user/aamreen/20210203_Dining/20210203_dining_partner_location.csv",format="csv", sep=",", inferSchema="true", header="true") 
dining_partner_locations.createOrReplaceTempView("dining_partner_locations") 
spark.sql (""" create table campaign_data.aa_dmc2229_Dining_partners as select * from dining_partner_locations""").count() 
SELECT * FROM campaign_data.aa_dmc2229_dining_partners LIMIT 100; 
select  count(distinct h.trx_header_id) 
                -- , d.trx_detail_id 
                -- , d.category 
                -- -- , p.category 
                -- , d.sub_category 
                -- -- , p.cuisine 
                -- , h.time_stamp 
                -- , product_id 
                -- -- , p.merchantname 
                -- -- , item_description 
                -- -- , d.`location` 
                -- , p.venueuuid 
                -- -- , ad.addr 
                -- , upper(ad.city) as cust_suburb 
                -- , ad.zipcode as cust_postcode 
                -- -- , p.address_street 
                -- , upper(p.address_suburb) as merchant_suburb 
                -- , p.address_postcode as merchant_postcode 
                 
                 
from m4m.return_feed_header h 
 
inner join gms.s_contact c 
on c.csn=h.member_number 
 
inner join gms.s_contact_x cx 
on cx.par_row_id=c.row_id 
 
inner join gms.s_addr_per ad 
on ad.row_id=c.pr_per_addr_id 
 
inner join m4m.return_feed_detail d 
on h.trx_header_id=d.trx_header_id 
 
inner join campaign_data.aa_dmc2229_dining_partners p 
on p.venueuuid=d.`location` 
 
-- inner join googleanalytics.appdata app 
-- on app.membernumber=c.csn 
 
where h.partner='Frequent Values' 
and h.time_stamp<'2021-01-27' 
spark.sql(''' 
select  upper(p.address_suburb) as merchant_suburb 
                , p.address_postcode as merchant_postcode 
                , count(distinct h.trx_header_id) 
                 
                 
from m4m.return_feed_header h 
 
inner join gms.s_contact c 
on c.csn=h.member_number 
 
inner join gms.s_contact_x cx 
on cx.par_row_id=c.row_id 
 
inner join gms.s_addr_per ad 
on ad.row_id=c.pr_per_addr_id 
 
inner join m4m.return_feed_detail d 
on h.trx_header_id=d.trx_header_id 
 
inner join campaign_data.aa_dmc2229_dining_partners p 
on p.venueuuid=d.`location` 
 
-- inner join googleanalytics.appdata app 
-- on app.membernumber=c.csn 
 
where h.partner='Frequent Values' 
and h.time_stamp<'2021-01-27' 
 
group by 1,2 
order by 1,2 
''').show(100000,False) 
#postcodes where most redeeming members live 
spark.sql(''' 
select  upper(p.address_suburb) as merchant_suburb 
                , p.address_postcode as merchant_postcode 
                , count(distinct csn) 
                 
                 
from m4m.return_feed_header h 
 
inner join gms.s_contact c 
on c.csn=h.member_number 
 
inner join gms.s_addr_per ad 
on ad.row_id=c.pr_per_addr_id 
 
inner join campaign_data.aa_dmc2229_dining_partners p 
on p.address_postcode=ad.zipcode 
 
where h.partner='Frequent Values' 
and h.time_stamp<'2021-01-27' 
 
group by 1,2 
order by 1,2 
''').show(100000,False) 
#postocdes where most local members are redeeming - most local redemptions 
spark.sql(''' 
select  upper(p.address_suburb) as merchant_suburb 
                , p.address_postcode as merchant_postcode 
                , count(distinct csn) 
                 
                 
from m4m.return_feed_header h 
 
inner join gms.s_contact c 
on c.csn=h.member_number 
 
inner join gms.s_addr_per ad 
on ad.row_id=c.pr_per_addr_id 
 
inner join m4m.return_feed_detail d 
on h.trx_header_id=d.trx_header_id 
 
inner join campaign_data.aa_dmc2229_dining_partners p 
on p.venueuuid=d.`location` 
 
where h.partner='Frequent Values' 
and h.time_stamp<'2021-01-27' 
and p.address_postcode=ad.zipcode 
 
group by 1,2 
order by 1,2 
''').show(100000,False) 
#postocdes with most members and most partners  
spark.sql(''' 
select  upper(p.address_suburb) as merchant_suburb 
                , p.address_postcode as merchant_postcode 
                , count(distinct csn) 
                 
                 
from m4m.return_feed_header h 
 
inner join gms.s_contact c 
on c.csn=h.member_number 
 
inner join gms.s_addr_per ad 
on ad.row_id=c.pr_per_addr_id 
 
inner join m4m.return_feed_detail d 
on h.trx_header_id=d.trx_header_id 
 
inner join campaign_data.aa_dmc2229_dining_partners p 
on p.venueuuid=d.`location` 
 
where h.partner='Frequent Values' 
and h.time_stamp<'2021-01-27' 
and p.address_postcode=ad.zipcode 
 
group by 1,2 
order by 1,2 
''').show(100000,False) 
select  h.*,p.*, ad.zipcode 
from m4m.return_feed_header h 
 
inner join gms.s_contact c 
on c.csn=h.member_number 
 
inner join gms.s_addr_per ad 
on ad.row_id=c.pr_per_addr_id 
 
inner join campaign_data.aa_dmc2229_dining_partners p 
on cast(p.address_postcode as string)=ad.zipcode 
 
where h.partner='Frequent Values' 
and h.time_stamp<'2021-01-27' 
 
-- group by 1,2 
-- order by 1,2 
select count(distinct membernumber)  
from googleanalytics.appdata 
where to_date(to_timestamp(eventdate, "yyyyMMdd")) between '2019-02-01' and '2021-01-27' 
and lower(eventname) rlike 'fuel' 
--count(*), sum(case when eventname rlike 'foodanddrink' then 1 else 0 end ) dining_events  
select count(distinct membernumber) 
        --, concat_ws(">>", eng01, eng02, eng03, eng04, eng05, eng07, eng08, eng09, eng10) as flow   
from sandpit.ga_appsession 
where to_date(event_dt) between '2019-02-01' and '2021-01-27' 
and engaged_cad=1 
 
with all_dining as( 
select distinct membernumber  
        , concat_ws(">>", eng01, eng02, eng03, eng04, eng05, eng07, eng08, eng09, eng10) as flow   
from sandpit.ga_appsession 
where to_date(event_dt) between '2019-02-01' and '2021-01-27' 
and engaged_dining=1 
 
-- group by 1 
-- order by 2 desc 
) 
select count(distinct membernumber) from all_dining 
where upper(flow) rlike 'FOOD' 
 
--and eng01 rlike 'FOOD_AND_DRINK' 
select   
-- (case when partner in ('IAG','NRMA Multi Policy Discount','NRMA MPD Insurance','NRMA Insurance') then 'IAG'  
-- when partner LIKE 'NRMA Holiday%' then 'NRMA Parks and Resorts'  
-- when partner LIKE 'HP%' then 'NRMA Parks and Resorts'  
-- when partner LIKE 'BIG4%' then 'NRMA Parks and Resorts'  
-- when partner LIKE 'NRMA Parks and Resorts%' then 'NRMA Parks and Resorts'  
-- when partner LIKE 'NRMA Bowen Beachfront Holiday Park' then 'NRMA Parks and Resorts'  
-- else partner end) as partner 
-- ,  
count(distinct h.member_number) 
                 
                 
from m4m.return_feed_header h 
 
where time_stamp between '2018-05-01' and '2021-01-27' 
--group by 1 
--order by 2 desc 
select c.campaign_name 
        , c.campaign_id 
        --, to_date(p.send_event_date) as send_date 
        , count(distinct hm.customer_id)  
        --, sum(case when p.open_event_date is not null or p.click_event_date is not null then 1 else 0 end ) opens 
        --, sum(case when p.click_event_date is not null then 1 else 0 end ) clicks 
 
from omc.send_level_summary p 
 
INNER JOIN campaign_data.aa_dmc2299_hack_map_20200128 hm 
on p.riid=hm.riid 
 
inner join omc.campaign_info c 
on c.campaign_id=p.campaign_id 
 
where 1=1 
and c.campaign_id in ('55385322','55385382','51274702','51276002','55384502','55384602') 
--and (p.open_event_date is not null or p.click_event_date is not null) 
--and p.click_event_date is not null 
and lower(p.clicked_elements) rlike 'dining' 
group by 1,2 
having count(distinct hm.customer_id) >100 
order by 1,2 
select p.* from omc.send_level_summary p 
 
INNER JOIN campaign_data.aa_dmc2299_hack_map_20200128 hm 
on p.riid=hm.riid 
 
inner join omc.campaign_info c 
on c.campaign_id=p.campaign_id 
 
where c.campaign_id in ('55384502','55384602') 
and p.click_event_date is not null 
with annual_Freq as( 
select h.member_number, count(distinct month(h.time_stamp)) as freq 
     
                 
from m4m.return_feed_header h 
 
inner join gms.s_contact c 
on c.csn=h.member_number 
 
inner join gms.s_contact_x cx 
on cx.par_row_id=c.row_id 
 
inner join gms.s_addr_per ad 
on ad.row_id=c.pr_per_addr_id 
 
inner join m4m.return_feed_detail d 
on h.trx_header_id=d.trx_header_id 
 
-- inner join campaign_data.aa_dmc2229_dining_partners p 
-- on p.venueuuid=d.`location` 
 
-- inner join googleanalytics.appdata app 
-- on app.membernumber=c.csn 
 
where h.partner='Frequent Values' 
and h.time_stamp between '2019-02-01' and '2020-01-31' 
--and h.member_number='744086401' 
group by 1 
) 
select case when freq=0 then 0 
            when freq<=1 then 1 
            when freq<=2 then 2 
            when freq<=3 then 3 
            when freq<=4 then 4 
            when freq<=5 then 5 
            when freq<=6 then 6 
            when freq<=7 then 7 
            when freq<=8 then 8 
            when freq<=9 then 9 
            when freq<=10 then 10 
            when freq<=11 then 11 
            when freq<=12 then 12 
            else 99 end annual_frequency 
        , count(distinct member_number) 
from annual_Freq 
group by 1 
with redemption as( 
select h.member_number, count(distinct h.trx_header_id) as freq 
     
                 
from m4m.return_feed_header h 
 
inner join gms.s_contact c 
on c.csn=h.member_number 
 
inner join gms.s_contact_x cx 
on cx.par_row_id=c.row_id 
 
inner join gms.s_addr_per ad 
on ad.row_id=c.pr_per_addr_id 
 
inner join m4m.return_feed_detail d 
on h.trx_header_id=d.trx_header_id 
 
-- inner join campaign_data.aa_dmc2229_dining_partners p 
-- on p.venueuuid=d.`location` 
 
-- inner join googleanalytics.appdata app 
-- on app.membernumber=c.csn 
 
where h.partner='Frequent Values' 
and h.time_stamp between '2019-02-01' and '2020-01-31' 
--and h.member_number='744086401' 
group by 1 
) 
select case when freq=0 then 0 
            when freq<=1 then 1 
            when freq<=2 then 2 
            when freq<=3 then 3 
            when freq<=4 then 4 
            when freq<=5 then 5 
            when freq<=10 then 10 
            when freq<=15 then 15 
            when freq<=20 then 20 
            --when freq<=25 then 25 
            when freq<=30 then 30 
            --when freq<=35 then 35 
            --when freq<=40 then 40 
            else 30 end annual_frequency 
        , count(distinct member_number) 
from redemption 
group by 1 
order by 1 
with redemption as( 
select h.member_number, count(distinct h.trx_header_id) as freq 
     
                 
from m4m.return_feed_header h 
 
inner join gms.s_contact c 
on c.csn=h.member_number 
 
inner join m4m.return_feed_detail d 
on h.trx_header_id=d.trx_header_id 
 
where h.partner='Frequent Values' 
and h.time_stamp< '2021-01-27' 
--and h.member_number='744086401' 
group by 1 
) 
select case when freq>=2 then 'More than twice' 
            else 'Once' end annual_frequency 
        , count(distinct member_number) 
from redemption 
group by 1 
order by 1 
With all_push_Members as ( 
select distinct o.campaign_id 
        , o.customer_id 
        , o.riid 
        , o.send_event_date 
        , case when open_event_date is not null then 1 else 0 end open_flag 
from omc.send_level_summary o 
     
inner join gms.s_contact con 
on o.customer_id = con.row_id 
      
inner join m4m.return_feed_header h 
on con.csn=h.member_number 
 
inner join m4m.return_feed_detail d 
on h.trx_header_id=d.trx_header_id 
 
where h.partner='Frequent Values' 
and h.time_stamp< '2021-01-27' 
and o.channel='Email' 
) 
, next_loop as( 
select customer_id, sum(open_flag)/count(campaign_id) as open_rate 
from all_push_Members 
GROUP BY 1 
) 
select case when open_rate = 0 then '0' 
            when open_rate>0 and open_rate<=0.1 then '10%' 
            when open_rate<=0.2 then '20%' 
            when open_rate<=0.3 then '30%' 
            when open_rate<=0.4 then '40%' 
            when open_rate<=0.5 then '50%' 
            when open_rate<=0.6 then '60%' 
            when open_rate<=0.7 then '70%' 
            when open_rate<=0.8 then '80%' 
            when open_rate<=0.9 then '90%' 
            when open_rate<=1 then '100%'  else '0' end member_split 
            , count(DISTINCT customer_id) 
FROM next_loop 
 
 
GROUP BY 1 
order by 1,2 