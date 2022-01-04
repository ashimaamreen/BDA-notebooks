SELECT min(eventtime), max(eventtime) FROM googleanalytics.appdata LIMIT 100; 
SELECT eventdate,eventtime, eventname, start_time, end_time ,id FROM googleanalytics.appdata  
where membernumber='990459469' 
and eventdate='20190803' 
ORDER BY 1 
select distinct eventname FROM googleanalytics.appdata  
where start_time is not null 
--and membernumber='990459469' 
-- and eventdate='20190803' 
-- ORDER BY 1 
spark.sql(''' 
SELECT `date`, count(DISTINCT membernumber) from googleanalytics.appdata_history 
--where `date`>'2019-07-06' 
GROUP BY 1 
ORDER BY 1 
''').show(10000) 
SELECT eventyear, eventmonth, count(DISTINCT membernumber) from googleanalytics.appdata  
where eventtime>'2019-07-06' 
GROUP BY 1,2 
ORDER BY 1,2 
-- 22/03/2020 - 15/05/2020 was my 'After' 
-- 29/01/2020 - 21/03/2020 was my 'Before' 
 
SELECT * from googleanalytics.appdata  
where eventtime BETWEEN '2020-01-27' and '2020-03-21' 
and membernumber='898736501' 
and start_time is not null 
-- GROUP BY 1 
-- order by 2 DESC 
-- 22/03/2020 - 15/05/2020 was my 'After' 
-- 29/01/2020 - 21/03/2020 was my 'Before' 
with freq as ( 
SELECT membernumber, count(DISTINCT eventdate) as frequency from googleanalytics.appdata  
where eventtime BETWEEN '2020-01-27' and '2020-03-21' 
GROUP BY 1 
) 
 
select frequency,count(distinct membernumber) from freq 
group by 1 
order by 1 
SELECT eventdate as before_count from googleanalytics.appdata  
where eventtime BETWEEN '2020-01-27' and '2020-03-21' 
--GROUP BY 1 
order by 1 DESC 
-- 22/03/2020 - 15/05/2020 was my 'After' 
-- 29/01/2020 - 21/03/2020 was my 'Before' 
with freq as ( 
SELECT membernumber, count(DISTINCT eventdate) as frequency from googleanalytics.appdata  
where eventtime  BETWEEN '2020-03-22' and '2020-05-15' 
GROUP BY 1 
) 
 
select frequency,count(distinct membernumber) as after_count from freq 
group by 1 
order by 1 
SELECT count(DISTINCT membernumber) as after_count from googleanalytics.appdata  
where eventtime BETWEEN '2020-03-22' and '2020-05-15' 
spark.sql(''' 
select distinct eventname, count(DISTINCT membernumber) FROM googleanalytics.appdata  
where start_time is not null 
and eventtime BETWEEN '2020-03-22' and '2020-05-15' 
group by 1 
''').show(1000,False) 
spark.sql(''' 
select cx.attrib_17 MembershipTenure,count(distinct a.membernumber) as after_count FROM googleanalytics.appdata a         --#130321 
 
inner join gms.s_contact c                  -- #130319 
on c.csn = a.membernumber 
 
inner join gms.s_contact_x as cx  
on c.row_id = cx.par_row_id 
 
-- left join m4m.return_feed_header m 
-- on m.member_number=a.membernumber 
 
where eventtime BETWEEN '2020-03-22' and '2020-05-15' 
group by 1 
order by 1 
''').show(1000) 
 
spark.sql(''' 
select cx.attrib_17 MembershipTenure,count(distinct a.membernumber) as before_count FROM googleanalytics.appdata a         --#130321 
 
inner join gms.s_contact c                  -- #130319 
on c.csn = a.membernumber 
 
inner join gms.s_contact_x as cx  
on c.row_id = cx.par_row_id 
 
-- left join m4m.return_feed_header m 
-- on m.member_number=a.membernumber 
 
where eventtime BETWEEN '2020-01-27' and '2020-03-21' 
group by 1 
order by 1 
''').show(1000) 
 
select distinct cx.attrib_17 MembershipTenure, a.membernumber, c.csn, c.row_id FROM googleanalytics.appdata a         --#130321 
 
inner join gms.s_contact c                  -- #130319 
on c.csn = a.membernumber 
 
inner join gms.s_contact_x as cx  
on c.row_id = cx.par_row_id 
 
-- left join m4m.return_feed_header m 
-- on m.member_number=a.membernumber 
 
where eventtime BETWEEN '2020-01-27' and '2020-03-21' 
and cx.attrib_17 in (1966.0,119.0) 
distinct 
         c.csn as Member_Number 
        ,c.fst_name AS First_Name 
        ,c.last_name AS Last_Name 
        ,c.email_addr AS Email_Address  
        ,cx.attrib_55  AS Colour_Plus 
        ,cx.attrib_17 MembershipTenure 
        ,(year(current_date) - year(c.birth_dt)) AS Age 
        ,c.sex_mf as Gender 
        ,ad.city as Suburb 
        ,ad.zipcode as Postcode 
        ,cx.attrib_22 as Geotribe_Segment 
        ,cal.Caltex_Redemption 
        ,m4m.Redemption_Flag 
        ,m4mIAG.Redemption_nonIAG_Flag 
        ,case when (nvl(cx.attrib_36,'Yes') !='No') then 'Y' else 'N' end subscription 
        ,e.last_engaged_date 
        ,e.email_engaged 
        ,CASE  
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
 
from gms.s_contact as c 
 
inner join Blue_Eligible as b  
on c.row_id = b.contact_id 
 
inner join gms.s_contact_x as cx  
on c.row_id = cx.par_row_id 
 
inner join gms.s_addr_per as ad 
on c.pr_per_addr_id = ad.row_id 
 
left join M4M_Caltex as cal 
on c.csn=cal.member_number 
 
left join M4M_Redemption as m4m 
on c.csn=m4m.member_number 
 
left join M4M_nonIAG_Redemption as m4mIAG 
on c.csn=m4mIAG.member_number 
 
left join Engagement_status as e 
on e.contact_id=c.row_id 
 
select distinct cx.attrib_17 MembershipTenure, a.membernumber, c.csn, c.row_id FROM googleanalytics.appdata a         --#130321 
 
inner join gms.s_contact c                  -- #130319 
on c.csn = a.membernumber 
 
inner join gms.s_contact_x as cx  
on c.row_id = cx.par_row_id 
 
inner join m4m.return_feed_header m 
on m.member_number=a.membernumber 
 
where eventtime BETWEEN '2020-01-27' and '2020-03-21' 
and cx.attrib_17 in (1966.0,119.0) 
with mapping as ( 
     
    -- mapping google analytics idm table with AWS cognito table 
    -- on the uuid 
    -- so that we can find for all uuid captured in GA, how many members have a member id mapped 
     
    select  
        ga.clientId 
        , ga.user_uuid 
        , nvl(idm.username, "") as member_number 
        , count(distinct ga.clientid) as devices 
    from googleanalytics.idm_ga_user as ga 
        left join cognito.idm_users as idm 
        on cast(idm.sub as string) = cast(ga.user_uuid as string) 
    group by 1,2,3 
    having lower(trim(user_uuid)) <> ""  
    order by 1,2,3 
     
),  
 
membership as ( 
     
    select  
        sContact.csn as memberNumber 
        , sContact.row_id as contactId 
        , to_date(sContact.x_nrma_join_dt) as memberJoinDate 
        , extract(to_date(sContact.x_nrma_join_dt), "year") as memberJoinYear 
        , extract(to_date(sContact.x_nrma_join_dt), "month") as memberJoinMonth 
        , round(datediff(now(), to_date(sContact.x_nrma_join_dt))) as memberJoinLength 
        , extract(now(), "year") - extract(to_date(sContact.birth_dt), "year") as memberAge 
        , sContactX.attrib_55 as memberColourSegment 
        , sContactX.attrib_17 as memberTenure 
        , sContactX.attrib_19 as memberAccruedTenure 
        , sContact.con_cd as memberType 
        , sum(if(lower(sProdInt.name) like "%blue%", TRUE, FALSE)) as hasBlue 
        , sum(if(lower(sProdInt.name) like "%care%" or  
            lower(sProdInt.name) like "plus" or lower(sProdInt.name) like "%free%", TRUE, FALSE)) as hasRSA 
        , sum(if(lower(sProdInt.name) like "%autoclub%", TRUE, FALSE)) as hasCMO 
        , count(sContact.csn) as recordNum -- which should associated with assets num 
 
    from gms.s_contact as sContact 
 
        inner join gms.s_contact_x as sContactX 
        on sContact.row_id = sContactX.par_row_id 
         
        inner join gms.s_asset as sAsset 
        on sAsset.owner_con_id = sContact.row_id 
         
        inner join gms.s_prod_int as sProdInt 
        on sProdInt.row_id = sAsset.prod_id 
        and lower(sProdInt.sub_type_cd) in ("rsa", "non-rsa") 
        and sProdInt.prod_cd = "Product" 
     
    group by 1,2,3,4,5,6,7,8,9,10,11 
 
) 
 
 
select distinct 
    session.`date` as gaDate 
    , session.clientId as device_id 
    , nvl(mapping.member_number, "") as memberNumber 
    , nvl(membership.memberJoinDate, "") as memberJoinDate 
    , nvl(membership.memberColourSegment, "") as memberColourSegment 
    , session.visitId as gaVisitId 
    , session.channelgrouping as gaChannelGrouping 
    , session.campaign as gaCampaign 
    , pageview.hitnumber as gaPageviewHitNumber 
    , pageview.pagepath as gaPagepath 
    --,count(distinct concat(clientId,cast(visitId as string))) as sessions 
     
from googleanalytics.sessions as session 
     
    inner join googleanalytics.pageview as pageview 
    on session.`date` = pageview.`date` 
    and session.clientId = pageview.clientId 
    and session.visitId = pageview.visitId 
     
    left join mapping  
    on mapping.clientId = session.clientId 
     
    left join membership 
    on membership.memberNumber = nvl(mapping.member_number, "")  
 
where session.channelGrouping = "MyNRMA App"     
order by 1,2,3,4,5,6,7,8,9,10 
select upper(tab), count(distinct membernumber) as before_count from googleanalytics.appdata 
where eventname='main_tab_click' 
and eventtime BETWEEN '2020-01-27' and '2020-03-21' 
 
group by 1 
order by 1 
--and eventname='map_detail_view' 
select upper(tab), count(distinct membernumber) as after_count from googleanalytics.appdata 
where eventname='main_tab_click' 
and eventtime BETWEEN '2020-03-22' and '2020-05-15' 
 
group by 1 
order by 1 
--and eventname='map_detail_view' 
SELECT upper(appcategory), count(membernumber) as after_count from googleanalytics.appdata  
where eventname = 'map_detail_view'  
and eventtime BETWEEN '2020-03-22' and '2020-05-15' 
GROUP BY 1 
order by 1 
SELECT upper(appcategory), count(membernumber) as before_count from googleanalytics.appdata  
where eventname = 'map_detail_view'  
and eventtime BETWEEN '2020-01-27' and '2020-03-21' 
GROUP BY 1 
order by 1 
select * from googleanalytics.appdata 
where eventname='main_tab_click' 
and eventtime BETWEEN '2020-03-22' and '2020-05-15' 
and upper(tab)='CAR' 
--group by 1 
--order by 1 
--and eventname='map_detail_view' 
-- check frequency 
-- first time users profile comparison 
select distinct membernumber,`date` as eventdate from googleanalytics.appdata_history 
with all_app_users as ( 
select distinct membernumber, eventdate from googleanalytics.appdata 
union  
select distinct membernumber,`date` as eventdate from googleanalytics.appdata_history 
), final as( 
select membernumber, min(eventdate) as first_redemption from all_app_users 
group by 1 
) 
select count(distinct membernumber) as  from final 
where first_redemption BETWEEN '20200127' and '20200321' 
with all_app_users as ( 
select distinct membernumber, eventdate from googleanalytics.appdata 
union  
select distinct membernumber,`date` as eventdate from googleanalytics.appdata_history 
), final as( 
select membernumber, min(eventdate) as first_redemption from all_app_users 
group by 1 
) 
select count(distinct membernumber) as first_usage_after from final 
where first_redemption BETWEEN '20200322' and '20200515' 
--22/03/2020 - 15/05/2020 
SELECT * FROM gms.s_entlmnt LIMIT 100; 
end of march battery purchase is turned off 
select eventname, count(distinct membernumber) as after_count from googleanalytics.appdata 
where lower(eventname) like 'car%' 
and eventname !='car_page_view' 
and eventtime BETWEEN '2020-03-22' and '2020-05-15' 
--and upper(tab)='CAR' 
group by 1 
order by 1 
--and eventname='map_detail_view' 
select eventname, count(distinct membernumber) as before_count from googleanalytics.appdata 
where lower(eventname) like 'car%' 
and eventname !='car_page_view' 
and eventtime BETWEEN '2020-01-27' and '2020-03-21' 
--and upper(tab)='CAR' 
group by 1 
order by 1 
--and eventname='map_detail_view' 
select count(distinct membernumber) as before_count from googleanalytics.appdata 
where eventname like 'reward%' 
and eventtime BETWEEN '2020-01-27' and '2020-03-21' 
--and upper(tab)='CAR' 
--group by 1 
--order by 1 
 
select count(distinct membernumber) as before_count from googleanalytics.appdata 
where eventname like 'login%' 
and eventtime BETWEEN '2020-01-27' and '2020-03-21' 
--and upper(tab)='CAR' 
--group by 1 
--order by 1 
 
select count(distinct membernumber) as after_count from googleanalytics.appdata 
where eventname like 'login%' 
and eventtime BETWEEN '2020-03-22' and '2020-05-15' 
select count(distinct membernumber) as before_count from googleanalytics.appdata 
where eventname like 'setting%' 
and eventtime BETWEEN '2020-01-27' and '2020-03-21' 
--and upper(tab)='CAR' 
--group by 1 
--order by 1 
 
select count(distinct membernumber) as after_count from googleanalytics.appdata 
where eventname like 'setting%' 
and eventtime BETWEEN '2020-03-22' and '2020-05-15' 
with freq as( 
select membernumber, count(eventdate) as before_count from googleanalytics.appdata 
--where eventname like 'setting%' 
where eventtime BETWEEN '2020-01-27' and '2020-03-21' 
group by 1) 
select before_count, count(distinct membernumber) from freq 
group by 1 
order by 1 
--and eventtime BETWEEN '2020-03-22' and '2020-05-15'  
with freq as ( 
select membernumber, count(distinct eventdate) as before_count from googleanalytics.appdata 
--where eventname like 'setting%' 
where eventtime BETWEEN '2020-01-27' and '2020-03-21' 
group by 1) 
select before_count, count(membernumber) from freq 
group by 1 
with freq as ( 
select membernumber, count(distinct eventdate) as before_count from googleanalytics.appdata 
--where eventname like 'setting%' 
where eventtime BETWEEN '2020-03-22' and '2020-05-15'  
group by 1) 
select before_count, count(membernumber) from freq 
group by 1 
select count(membernumber), count(eventdate) as before_count from googleanalytics.appdata 