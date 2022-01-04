select  
distinct 
    p.customer_id 
    ,con.csn 
    , p.send_event_date 
    , p.control 
    , p.campaign_id 
    , p.delivered_event_date 
    , p.bounce_event_date 
    , p.open_event_date 
    , p.click_event_date 
    , c.campaign_name 
    , h.partner 
    , h.time_stamp 
    , h.number_of_items 
    , h.total_amount 
    , h.discount 
    , CASE 
        when app.engaged_events is not null 
        then 'True' 
        else 'False' 
        end as app_engaged 
 
from omc.send_level_summary p 
 
inner join omc.campaign_info c 
on c.campaign_id=p.campaign_id 
 
inner join gms.s_contact con 
on con.row_id=p.customer_id 
 
inner join m4m.return_feed_header h 
on con.csn = h.member_number 
 
left join sandpit.ga_appsession app 
on app.membernumber=con.csn 
and datediff(app.event_dt,p.send_event_date) between 0 and 7 
 
where p.campaign_id= 
    '57393102' --Caltex/Ampol 
    --'57434422' -- Repco not purchased 12 months 
    --'57434522' -- Repco not purchased ever 
    --'57434362' -- Mothers Day 
--and p.open_event_date is not null 
--and p.control = true 
and h.partner LIKE  
    'Caltex' 
    --'%NRMATix%' 
    --'Repco' 
    --'%Mothers%' 
and h.time_stamp between p.send_event_date and '2021-04-18' 
select  
    count (distinct hack.customer_id) 
    , p.control 
    , CASE  
    when p.open_event_date is not null then 'True' 
    else 'False' 
    end as open_event 
 
from omc.send_level_summary p 
 
inner join omc.campaign_info c 
on c.campaign_id=p.campaign_id 
 
left join campaign_data.rw_aprjun_hackmap_20210831 hack 
on hack.riid = p.riid 
 
where p.campaign_id= 
--April 
    --'57393102' -- Caltex/Ampol 
    --'57402142' -- App update 
    --'57403242' -- Easter Holidays 
    --'57409002' -- Dining Paramatta (In Paramatta) 
    --'57409062' -- Dining Paramatta (Around Paramatta) 
    --'57427822' -- Frozen 
--May 
    '57434422' -- Repco not purchased 12 months 
    --'57434522' -- Repco not purchased ever 
    --'57434362' -- Mothers Day 
    --'57444842' -- Specsavers 
    --'57446602' -- Motorserve 
    --'61359962' -- Parks and Resorts 
    --'61368422' -- Simply Energy 
--June 
    --'61372222' -- Dining Comp 
    --'61376462' -- Within Gosford/Warner 
    --'61376402' -- Outside Gosford/Warner 
    --'61386442' -- Double Demerits 
    --'61393722' -- Woolworths 
 
group by p.control, open_event 
select  
    count(distinct hack.customer_id) as engaged_count 
    , p.control 
 
from omc.send_level_summary p 
 
left join campaign_data.rw_aprjun_hackmap_20210831 hack 
on hack.riid = p.riid 
 
inner join omc.campaign_info c 
on c.campaign_id=p.campaign_id 
 
inner join gms.s_contact con 
on con.row_id=hack.customer_id 
 
inner join sandpit.ga_appsession app 
on app.membernumber=con.csn 
and datediff(app.event_dt,p.send_event_date) between 0 and 7 
 
where p.campaign_id= 
--April 
    '57393102' -- Caltex/Ampol 
    --'57402142' -- App update 
    --'57403242' -- Easter Holidays 
    --'57409002' -- Dining Paramatta (In Paramatta) 
    --'57409062' -- Dining Paramatta (Around Paramatta) 
    --'57427822' -- Frozen 
--May 
    --'57434422' -- Repco not purchased 12 months 
    --'57434522' -- Repco not purchased ever 
    --'57434362' -- Mothers Day 
    --'57444842' -- Specsavers 
    --'57446602' -- Motorserve 
    --'61359962' -- Parks and Resorts 
    --'61368422' -- Simply Energy 
--June 
    --'61372222' -- Dining Comp 
    --'61376462' -- Within Gosford/Warner 
    --'61376402' -- Outside Gosford/Warner 
    --'61386442' -- Double Demerits 
    --'61393722' -- Woolworths 
     
group by p.control 
 
select  
    count( distinct hack.customer_id) 
    ,p.control 
 
from omc.send_level_summary p 
 
left join campaign_data.rw_aprjun_hackmap_20210831 hack 
on hack.riid = p.riid 
 
inner join omc.campaign_info c 
on c.campaign_id=p.campaign_id 
 
inner join gms.s_contact con 
on con.row_id=hack.customer_id 
 
--conversions 
inner join m4m.return_feed_header h 
on con.csn = h.member_number 
 
where p.campaign_id= 
--April 
    '57393102' --Caltex/Ampol 
    --'57402142' -- App update 
    --'57403242' -- Easter Holidays 
    --'57409002' -- Dining Paramatta (In Paramatta) 
    --'57409062' -- Dining Paramatta (Around Paramatta) 
    --'57427822' -- Frozen 
--May 
    --'57434422' -- Repco not purchased 12 months 
    --'57434522' -- Repco not purchased ever 
    --'57434362' -- Mothers Day 
    --'57444842' -- Specsavers 
    --'57446602' -- Motorserve 
    --'61359962' -- Parks and Resorts 
    --'61368422' -- Simply Energy 
--June 
    --'61372222' -- Dining Comp 
    --'61376462' -- Within Gosford/Warner 
    --'61376402' -- Outside Gosford/Warner 
    --'61393722' -- Woolworths 
and h.partner 
--April 
    LIKE 'Caltex' 
    --in ('NRMA Parks and Resorts - Sydney Lakeside', 'experiences and attractions tickets') --Easter 
    --LIKE 'Frequent Values' --Dining 
--May 
    --LIKE 'Repco' 
    --in ('Petals', 'experiences and attractions tickets', 'Woolworths') -- Mothers Day 
    --LIKE 'Spec%' 
    --LIKE '%Parks and Resorts%' 
   -- LIKE 'Simply Energy' 
--June 
    --LIKE 'Frequent Values' -- Dining comp 
    --LIKE 'Caltex' -- Ampol within/out Gosford/Warner 
    --LIKE 'Woolworths' 
and h.time_stamp between p.send_event_date and '2021-04-18' 
 
group by p.control 
select  
    count( distinct hack.customer_id) as target_members 
    ,conx.attrib_55  AS Colour_Plus 
 
from omc.send_level_summary p 
 
left join campaign_data.rw_aprjun_hackmap_20210831 hack 
on hack.riid = p.riid 
 
inner join omc.campaign_info c 
on c.campaign_id=p.campaign_id 
 
inner join gms.s_contact con 
on con.row_id=hack.customer_id 
 
INNER JOIN gms.s_contact_x AS conx  
ON conx.par_row_id = con.row_id  
 
where p.campaign_id= 
--April 
    --'57393102' --Caltex/Ampol 
    --'57402142' -- App update 
    --'57403242' -- Easter Holidays 
    --'57409002' -- Dining Paramatta (In Paramatta) 
    --'57409062' -- Dining Paramatta (Around Paramatta) 
    --'57427822' -- Frozen 
--May 
    --'57434422' -- Repco not purchased 12 months 
    '57434522' -- Repco not purchased ever 
    --'57434362' -- Mothers Day 
    --'57444842' -- Specsavers 
    --'57446602' -- Motorserve 
    --'61359962' -- Parks and Resorts 
    --'61368422' -- Simply Energy 
--June 
    --'61372222' -- Dining Comp 
    --'61376462' -- Within Gosford/Warner 
    --'61376402' -- Outside Gosford/Warner 
    --'61393722' -- Woolworths 
and p.control = false 
     
group by Colour_Plus 
order by Colour_Plus 
 
select  
    count( distinct hack.customer_id) as members_engaged 
    ,conx.attrib_55  AS Colour_Plus 
 
from omc.send_level_summary p 
 
left join campaign_data.rw_aprjun_hackmap_20210831 hack 
on hack.riid = p.riid 
 
inner join gms.s_contact con 
on con.row_id=hack.customer_id 
 
INNER JOIN gms.s_contact_x AS conx  
ON conx.par_row_id = con.row_id  
 
inner join sandpit.ga_appsession app 
on app.membernumber=con.csn 
and datediff(app.event_dt,p.send_event_date) between 0 and 7 
 
where p.campaign_id= 
--April 
    --'57393102' --Caltex/Ampol 
    --'57402142' -- App update 
    --'57403242' -- Easter Holidays 
    --'57409002' -- Dining Paramatta (In Paramatta) 
    --'57409062' -- Dining Paramatta (Around Paramatta) 
    --'57427822' -- Frozen 
--May 
    --'57434422' -- Repco not purchased 12 months 
    '57434522' -- Repco not purchased ever 
    --'57434362' -- Mothers Day 
    --'57444842' -- Specsavers 
    --'57446602' -- Motorserve 
    --'61359962' -- Parks and Resorts 
    --'61368422' -- Simply Energy 
--June 
    --'61372222' -- Dining Comp 
    --'61376462' -- Within Gosford/Warner 
    --'61376402' -- Outside Gosford/Warner 
    --'61393722' -- Woolworths 
and p.control = false 
 
group by Colour_Plus 
order by Colour_Plus 
 
 
 
select  
    count( distinct hack.customer_id) as members_converted 
    ,conx.attrib_55  AS Colour_Plus 
 
from omc.send_level_summary p 
 
left join campaign_data.rw_aprjun_hackmap_20210831 hack 
on hack.riid = p.riid 
 
inner join omc.campaign_info c 
on c.campaign_id=p.campaign_id 
 
inner join gms.s_contact con 
on con.row_id=hack.customer_id 
 
INNER JOIN gms.s_contact_x AS conx  
ON conx.par_row_id = con.row_id  
 
--conversions 
inner join m4m.return_feed_header h 
on con.csn = h.member_number 
 
where p.campaign_id= 
--April 
    --'57393102' --Caltex/Ampol 
    --'57402142' -- App update 
    --'57403242' -- Easter Holidays 
    --'57409002' -- Dining Paramatta (In Paramatta) 
    --'57409062' -- Dining Paramatta (Around Paramatta) 
    --'57427822' -- Frozen 
--May 
    --'57434422' -- Repco not purchased 12 months 
    '57434522' -- Repco not purchased ever 
    --'57434362' -- Mothers Day 
    --'57444842' -- Specsavers 
    --'57446602' -- Motorserve 
    --'61359962' -- Parks and Resorts 
    --'61368422' -- Simply Energy 
--June 
    --'61372222' -- Dining Comp 
    --'61376462' -- Within Gosford/Warner 
    --'61376402' -- Outside Gosford/Warner 
    --'61393722' -- Woolworths 
and h.partner 
--April 
    --LIKE 'Caltex' 
    --in ('NRMA Parks and Resorts - Sydney Lakeside', 'Event Cinemas NRMA') --Easter 
    --LIKE 'Frequent Values' --Dining 
--May 
    LIKE 'Repco' 
    --in ('Petals', 'experiences and attractions tickets', 'Event Cinemas NRMA') 
    --LIKE 'Spec%' 
    --LIKE '%Parks and Resorts%' 
    --LIKE 'Simply Energy' 
--June 
    --LIKE 'Caltex' -- Ampol within/out Gosford/Warner 
    --LIKE 'Woolworths' 
and h.time_stamp between p.send_event_date and '2021-05-31' 
 
group by  Colour_Plus 
order by Colour_Plus 
 
 
spark.sql(""" 
create table campaign_data.rw_aprjun_hackmap_20210831 as  
 
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
 
""") 