select * from omc.member_campaign_summary_table limit 100; 
 
select * from campaign_data.ek_dmc1793_member_campaign_summary_table 
select * from omc.riid_mapping limit 100 
 
 
 
create table campaign_data.ek_dmc1793_member_campaign_summary_table as 
select * from omc.member_campaign_summary_table where campaign_id = '41224162'; 
 
-- 88244 9862 
SELECT holdoutgroup, count(distinct riid) FROM campaign_data.ek_dmc1793_member_campaign_summary_table group by holdoutgroup; 
select  * from campaign_data.ek_dmc1793_member_campaign_summary_table where holdoutgroup = 'Y'; 
select from_unixtime(unix_timestamp(sendtime, '"dd-MMM-yyyy HH:mm:ss"'))  from campaign_data.ek_dmc1793_member_campaign_summary_table where holdoutgroup = 'Y'; 
 
select from_timestamp(TO_TIMESTAMP(sendtime, 'dd-MMM-yyyy'),'yyyy-MM-dd') sendtime from campaign_data.ek_dmc1793_member_campaign_summary_table where holdoutgroup = 'Y'; 
 
-- check date included - ok 
select  (from_timestamp(sendtime,'yyyy-MM-dd')), count(distinct riid) 
from  
campaign_data.ek_dmc1793_member_campaign_summary_table mem group by 1 
order by 1; 
 
select * from omc.email_sent where campaign_id = '"41224162"'; 
select count(distinct riid) from omc.email_sent where campaign_id = '"41224162"'; -- 86143 
select count(distinct customer_id) from omc.email_sent where campaign_id = '"41224162"'; -- 86144 
select count(distinct member_number) from omc.email_sent where campaign_id = '"41224162"'; -- 86138 
select distinct sendtime from omc.member_campaign_summary_table where campaign_id = '41224162' and holdoutgroup = 'N' limit 200 
 
 
--select distinct sendtime from campaign_data.ek_dmc1793_member_campaign_summary_table where holdoutgroup = 'N'  
select  
     
    a.con_cd, count(distinct a.csn) 
from omc.email_sent as e 
    inner join gms.s_contact as a 
    on regexp_replace(e.customer_id, '"', "") = a.row_id 
where  
    1=1 
    and campaign_id = '"32866542"' 
   -- and a.con_cd in ("Affiliate Member", "Ordinary Member") 
    and lower(e.email) not like "%innovacx%" 
group by a.con_cd 
Blue_Eligible = spark.sql(""" 
    select distinct 
         c.csn as member_number 
        ,c.row_id as contact_id 
        ,o.row_id as account_id 
        ,c.cust_stat_cd as cust_stat 
        ,c.con_cd as contact_type 
	    ,c.x_nrma_household_flg as hh 
    	,nvl(o.pr_con_id,'N') = 'Y' as primary  
 
    from gms.s_contact as c  
     
	inner join gms.s_org_ext as o  
	on c.pr_dept_ou_id = o.row_id 
 
	inner join gms.s_asset as a  
	on a.owner_accnt_id = o.par_row_id 
 
	inner join gms.s_prod_int as p 
       on a.prod_id = p.row_id 
        
    inner join gms.s_contact_fnx as fn  
       on fn.par_row_id = c.row_id 
     
    where a.status_cd = 'Active' 
    and p.type = 'Membership' 
    and p.prod_cd = 'Promotion' 
    and NVL(fn.deceased_flg,'N') = 'N' 
    and c.csn is not null 
    """) 
     
Blue_Eligible.createOrReplaceTempView("Blue_Eligible") 
spark.sql('select * from Blue_Eligible').count() 
M4M_Redemption = spark.sql(""" 
SELECT    distinct trx_header_id as TransID 
        , partner as Partner 
        , member_number as MemberNumber 
        , time_stamp as TransDate 
        , round(discount) as Savings 
from m4m.return_feed_header 
where 
    DATE(time_stamp) >= DATE('2020-02-01') 
""") 
M4M_Redemption.createOrReplaceTempView("M4M_Redemption") 
spark.sql('select * from M4M_Redemption order by 4').show(100) 
 
#and partner not in ('IAG','NRMA Multi Policy Discount','NRMA MPD Insurance','NRMA Insurance')  
#IAG redemptions 
Redeemed_IAG = spark.sql(""" 
SELECT  distinct member_number  
 
from m4m.return_feed_header m 
inner join 
    campaign c 
on  
    c.csn = m.Member_Number 
    and to_date(m.time_stamp,'yyyy-MM-dd') < to_date(c.ecd,'yyyy-MM-dd') 
    and partner in ('IAG','NRMA Multi Policy Discount','NRMA MPD Insurance','NRMA Insurance')  
     
 
""") 
Redeemed_IAG.createOrReplaceTempView("Redeemed_IAG") 
spark.sql('select * from Redeemed_IAG').show(100) 
Postcampaign_redemption = spark.sql(""" 
select m.* 
from  
    M4M_Redemption m 
inner join 
    campaign c 
on  
    c.csn = m.MemberNumber 
    and to_date(c.ecd,'yyyy-MM-dd') <= to_date(m.TransDate,'yyyy-MM-dd') 
 
""") 
Postcampaign_redemption.createOrReplaceTempView("Postcampaign_redemption") 
spark.sql('select * from Postcampaign_redemption').show(100) 
campaign = spark.sql(""" 
SELECT 
    distinct  
    e.holdoutgroup,  
    a.csn,  
    a.cust_value_cd loyal_colour,  
    conx.attrib_55 AS colour_plus, 
    e.contact_id, 
    (case when holdoutgroup = 'N' then to_date(sendtime,'yyyy-MM-dd') else to_date(TO_TIMESTAMP(sendtime, 'dd-MMM-yyyy'),'yyyy-MM-dd') end) as ecd 
FROM campaign_data.cc_dmc1793_reonboarding_postrenewal as e 
     
inner join gms.s_contact a 
on e.contact_id = a.row_id 
 
inner join 
    gms.s_contact_x AS conx 
ON conx.par_row_id = a.row_id 
 
 
 
""") 
campaign.createOrReplaceTempView("campaign") 
spark.sql('select * from campaign').show(100) 
campaign.count() 
#NOT USED 
App_logged = spark.sql(""" 
SELECT  distinct csn  
 
from (SELECT membernumber, to_timestamp(eventdate, "yyyyMMdd") login_succeed  
    FROM googleanalytics.appdata where eventname = 'login_succeed'  ) as b 
 
inner join 
    campaign c 
on  
    c.csn = b.membernumber 
    and b.login_succeed < to_date(c.ecd,'yyyy-MM-dd') 
 
""") 
App_logged.createOrReplaceTempView("App_logged") 
#spark.sql('select * from App_logged').show(100) 
spark.sql(""" 
select * from m4m.return_feed_header where member_number ='824736601' 
""").show(100,False) 
 
 #, to_date(c.ecd,'yyyy-MM-dd') sentdate 
results = spark.sql(""" 
SELECT 
    c.csn 
    , c.holdoutgroup 
    , c.loyal_colour 
    , c.colour_plus 
    , cast(c.ecd as string) sentdate 
    , (i.member_number is not null) as redeemed_iag 
    , count(distinct m.TransID) total_redemptions 
    , count(distinct m.partner) total_redeemed_partners 
    , count(distinct cal.TransID) total_caltex 
    , count(distinct din.TransID) total_din 
    , count(distinct iag.TransID) total_iag 
 
FROM 
    campaign c 
left join 
    Postcampaign_redemption as m 
    on c.csn = m.membernumber 
left join  
    (select * from Postcampaign_redemption where lower(partner) like '%caltex%') cal 
    on c.csn = cal.membernumber 
left join  
    (select * from Postcampaign_redemption where partner LIKE '%Frequent Values%' ) din 
    on c.csn = din.membernumber 
left join  
    (select * from Postcampaign_redemption where partner in ('IAG','NRMA Multi Policy Discount','NRMA MPD Insurance','NRMA Insurance') ) iag 
    on c.csn = iag.membernumber 
left join 
    Redeemed_IAG as i 
    on c.csn = i.member_number 
     
group by 1,2,3,4,5,6 
 
""") 
results.createOrReplaceTempView("results") 
spark.sql('select * from results').show(200) 
 
select  a.*, 
        min(b.login_succeed) login_afteredm 
     
from campaign_data.ek_dmc1793_redemption_results a 
left join  
    (SELECT membernumber, to_timestamp(eventdate, "yyyyMMdd") login_succeed  
    FROM googleanalytics.appdata where eventname = 'login_succeed'  ) as b 
 
left join 
    (SELECT  distinct csn  
    from (  SELECT membernumber, to_timestamp(eventdate, "yyyyMMdd") login_succeed  
            FROM googleanalytics.appdata where eventname = 'login_succeed'  ) as b 
    inner join 
        campaign c 
    on  
        c.csn = b.membernumber 
        and b.login_succeed < to_date(c.ecd,'yyyy-MM-dd') ) logged 
     
 
on a.csn = b.membernumber 
and b.login_succeed>= a.senttime  
 
 
group by 1,2,3,4,5,6,7,8,9,10,11; 
 
results.repartition(1).write.saveAsTable('campaign_data.ek_dmc1793_redemption_results')  
drop table campaign_data.ek_dmc1793_redemption_results_app purge 
 
--select * from campaign_data.ek_dmc1793_redemption_results; 
drop table campaign_data.ek_dmc1793_redemption_results_app purge; 
Create table campaign_data.ek_dmc1793_redemption_results_app as 
SELECT  
    c.csn, 
    c.holdoutgroup, 
    c.loyal_colour, 
    c.colour_plus, 
    c.sentdate, 
    c.redeemed_iag, 
    c.total_redemptions, 
    c.total_redeemed_partners, 
    c.total_caltex, 
    c.total_din, 
    c.total_iag, 
    sum(case when b.login_succeed < to_timestamp(c.sentdate,'yyyy-MM-dd') then 1 else 0 end)  LoggedBefore, 
    sum(case when b.login_succeed >= to_timestamp(c.sentdate,'yyyy-MM-dd') then 1 else 0 end)  LoggedAfter 
From  
        campaign_data.ek_dmc1793_redemption_results c 
     
left join 
(SELECT membernumber, to_timestamp(eventdate, "yyyyMMdd") login_succeed FROM googleanalytics.appdata where eventname = 'login_succeed'  ) as b 
on  
    c.csn = b.membernumber 
 
where to_timestamp(sentdate,'yyyy-MM-dd') >= '2020-02-24' 
     
group by 1,2,3,4,5,6,7,8,9,10,11 
--drop table campaign_data.ek_dmc1793_redemption_results_app purge 
select * from campaign_data.ek_dmc1793_redemption_results_app order by sentdate; 
 
select holdoutgroup, count(*) from campaign_data.ek_dmc1793_redemption_results_app group by holdoutgroup; 
 
select holdoutgroup, count(*) from campaign_data.ek_dmc1793_redemption_results_app where total_caltex> 0 group by holdoutgroup; 
 
select holdoutgroup, count(*) from campaign_data.ek_dmc1793_redemption_results_app where total_din> 0 group by holdoutgroup; 
 
select holdoutgroup, count(*) from campaign_data.ek_dmc1793_redemption_results_app where total_redemptions> 0 group by holdoutgroup; 
 
select holdoutgroup, avg(total_redeemed_partners) from campaign_data.ek_dmc1793_redemption_results_app where total_redemptions> 0 group by holdoutgroup; 
 
 
select holdoutgroup, count(*) from campaign_data.ek_dmc1793_redemption_results_app where redeemed_iag is false and total_iag> 0 group by holdoutgroup; 
 
select holdoutgroup, count(*) from campaign_data.ek_dmc1793_redemption_results_app where loggedbefore = 0 and loggedafter > 0 group by holdoutgroup; 
 
--Profiling 
 
select colour_plus, count(*) from campaign_data.ek_dmc1793_redemption_results_app where holdoutgroup = 'N' and total_redemptions> 0 group by colour_plus; 
select loyal_colour, count(*) from campaign_data.ek_dmc1793_redemption_results_app where holdoutgroup = 'N' and total_redemptions> 0 group by loyal_colour; 
 
 
select distinct sentdate from campaign_data.ek_dmc1793_redemption_results_app order by sentdate desc; 
 
select * from campaign_data.ek_dmc1793_redemption_results_app; 
 
 
select redeemed_cal, count(*) from campaign_data.ek_dmc1793_redemption_results_app where total_caltex> 0 and holdoutgroup = 'N' group by redeemed_cal; 
select redeemed_cal, count(*) from campaign_data.ek_dmc1793_redemption_results_app where holdoutgroup = 'N' group by redeemed_cal; 
 
 
select redeemed_din, count(*) from campaign_data.ek_dmc1793_redemption_results_app where total_redemptions> 0 and holdoutgroup = 'N' group by redeemed_din; 
select redeemed_din, count(*) from campaign_data.ek_dmc1793_redemption_results_app where holdoutgroup = 'N' group by redeemed_din; 
 
spark.sql(""" 
select * from App_logged 
""").show(100) 
with new_app as ( 
 
    select membernumber, min(eventdate) as updated_dt 
    from googleanalytics.appdata 
    where 
        1=1 
        and nvl(membernumber, "") <> "" 
        and appversion >= "7.9.0" 
    group by 1 
    order by 1 
 
) 
 
 
    select new_app.updated_dt 
        , max(t1.appversion) as latest_app 
    --    , max(t1.eventdate) as last_time_used 
    , count(distinct t1.membernumber) 
    from new_app 
        left join googleanalytics.appdata as t1 
        on t1.membernumber = new_app.membernumber 
    where 
        nvl(t1.membernumber, "") <> "" 
       -- and t1.appversion < "7.9.0" 
    group by 1 
control = spark.sql(""" 
SELECT 
    distinct a.csn, e.dt 
FROM 
    (select distinct riid, to_timestamp(regexp_replace(event_captured_dt,'"',""),'dd-MMM-yyyy HH:mm:ss') dt 
    from omc.audit_hold_out where compare_to_campaign_id = '"41224162"') as e  
INNER JOIN 
    omc.riid_mapping as r 
    on e.riid=r.contact_riid 
INNER JOIN 
    gms.s_contact as a 
    on regexp_replace(r.customer_id_, '"', "") = a.row_id 
""") 
control.createOrReplaceTempView("control") 
spark.sql('select * from control').show(1000) 
select distinct riid, event_captured_dt 
from omc.audit_hold_out; 
--where compare_to_campaign_id = '"41224162"' 
select * from omc.member_campaign_summary_table where campaign_id = '33126462' 
select * from omc.audit_hold_out limit 100 
 
= '33126462' 
test = spark.sql(""" 
select distinct rule_name from omc.email_dynamic_content 
""") 
test.createOrReplaceTempView("test") 
test.show(200) 
drop table campaign_data.ek_dmc1793_redemption_results purge 
select * from gms.s_contact limit 100 
select * from omc.riid_mapping limit 100 
test = spark.sql(""" 
select * from omc.member_campaign_summary_table where campaign_id = '41224162' 
""") 
test.createOrReplaceTempView("test") 
test.show(200) 
test.repartition(1).write.saveAsTable("campaign_data.cc_dmc1793_reonboarding_postrenewal")   
select * from campaign_data.cc_dmc1793_reonboarding_postrenewal limit 100 
spark.sql(""" 
select sentdate, redeemed_iag, total_redemptions, total_redeemed_partners, total_caltex, total_din, total_iag, loggedbefore, loggedafter, holdoutgroup, count(*)  
from campaign_data.ek_dmc1793_redemption_results_app 
group by sentdate, redeemed_iag, total_redemptions, total_redeemed_partners, total_caltex, total_din, total_iag, loggedbefore, loggedafter, holdoutgroup 
""").show(2000)  