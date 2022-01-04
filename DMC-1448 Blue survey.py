Contents
s_contact = spark.table("gms.s_contact") 
s_contact.createOrReplaceTempView("s_contact") 
 
s_contact_x = spark.table("gms.s_contact_x") 
s_contact_x.createOrReplaceTempView("s_contact_x") 
 
s_org_ext = spark.table("gms.s_org_ext") 
s_org_ext.createOrReplaceTempView("s_org_ext") 
 
s_asset = spark.table("gms.s_asset") 
s_asset.createOrReplaceTempView("s_asset") 
 
s_prod_int = spark.table("gms.s_prod_int") 
s_prod_int.createOrReplaceTempView("s_prod_int") 
 
s_contact_fnx = spark.table("gms.s_contact_fnx") 
s_contact_fnx.createOrReplaceTempView("s_contact_fnx") 
 
s_addr_per=spark.table("gms.s_addr_per") 
s_addr_per.createOrReplaceTempView("s_addr_per") 
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
select   
        member_number  
        ,case when  
sum(case when (cast(time_stamp as date)) between cast ('2018-05-22' as date) and current_date then 1 else 0 end) > 0 then 'Y' else 'N' end Redemption_Flag 
from m4m.return_feed_header group by member_number 
""") 
 
M4M_Redemption.createOrReplaceTempView("M4M_Redemption") 
spark.sql('select * from M4M_Redemption').show(5) 
M4M_Caltex = spark.sql(""" 
select  member_number  
        ,count (distinct trx_header_id) as Caltex_Redemption 
from m4m.return_feed_header  
where partner like 'Caltex'  
group by member_number 
 
""") 
 
M4M_Caltex.createOrReplaceTempView("M4M_Caltex") 
spark.sql('select * from M4M_Caltex').show(5) 
Engagement_status = spark.sql(""" 
select  contact_id 
        ,max(responsetime) as last_engaged_Date 
        ,case when responsetime is not NULL then 'Y' else 'N' end email_engaged 
from sandpit.member_campaign_summary_table 
group by contact_id, case when responsetime is not NULL then 'Y' else 'N' end 
""") 
 
Engagement_status.createOrReplaceTempView("Engagement_status") 
spark.sql('select * from Engagement_status').show(1000) 
Working_table=spark.sql (""" 
select  distinct 
         c.csn as Member_Number 
        ,c.fst_name AS First_Name 
        ,c.last_name AS Last_Name 
        ,c.email_addr AS Email_Address  
        ,cx.attrib_55  AS Colour_Plus 
        ,(year(current_date) - year(c.birth_dt)) AS Age 
        ,c.sex_mf as Gender 
        ,ad.city as Suburb 
        ,ad.zipcode as Postcode 
        ,cx.attrib_22 as Geotribe_Segment 
        ,cal.Caltex_Redemption 
        ,m4m.Redemption_Flag 
        ,case when (nvl(cx.attrib_36,'Yes') !='No') then 'Y' else 'N' end subscription 
        ,e.last_engaged_date 
        ,e.email_engaged 
        ,CASE  
                        WHEN  
                         (ad.zipcode >= '2000' and ad.zipcode <= '2082' or  
                          ad.zipcode >= '2000' and ad.zipcode <= '2082' or  
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
 
left join Engagement_status as e 
on e.contact_id=c.row_id 
 
where c.cust_stat_cd = 'Active' 
        and c.con_cd in ('Ordinary Member' , 'Affiliate Member') 
        and NVL(c.x_nrma_title,'no title') != 'Estate Of The Late' 
        and case when trim(c.x_inv_email_1) = 'Y' or c.email_addr is null then 'N' else 'Y' end = 'Y' 
""") 
Working_table.createOrReplaceTempView("Working_table") 
spark.sql('select * from Working_table').count() 
Segment1=Working_table.sample(False,0.009255938262) 
Segment1.createOrReplaceTempView("Segment1") 
 
spark.sql('select * from Segment1').count() 
spark.sql("""create table campaign_data.aa_20190211_o_dmc1448_blueSurvey1 as select * from Segment1""") 
working_table2=spark.sql(""" 
select * from Working_table 
where Caltex_Redemption is not null 
""") 
working_table2.createOrReplaceTempView("working_table2") 
spark.sql('select * from working_table2').count() 
Segment2=working_table2.sample(False,0.0979) 
Segment2.createOrReplaceTempView("Segment2") 
 
spark.sql(""" 
select * from Segment2""").count() 
spark.sql("""create table campaign_data.aa_20190211_o_dmc1448_blueSurvey2 as select * from Segment2""") 
SELECT * FROM campaign_data.aa_20190211_o_dmc1448_bluesurvey1 LIMIT 100; 
SELECT * FROM campaign_data.aa_20190211_o_dmc1448_bluesurvey2 LIMIT 100;