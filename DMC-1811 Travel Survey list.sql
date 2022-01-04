drop table campaign_data.aa_dmc1811_travelSurvey1_research_others_20191119_adhoc 
select distinct ATTRIB_36 from gms.s_contact_x 
select email_Perm,travel_Perm ,count(distinct Member_Number) from 
( 
select  distinct 
         c.csn as Member_Number 
        , c.row_id as contact_id 
        , fn.hobby_cd as travel_preference 
        , case when cx.ATTRIB_36='No' then False else True end email_Perm 
        , case when a.contact_id is Null then False else true end as travel_Perm 
 
from gms.s_contact as c 
 
	inner join gms.s_contact_fnx as fn  
    on fn.par_row_id = c.row_id 
     
    inner join gms.s_contact_x as cx  
    on c.row_id = cx.par_row_id 
     
    left join omc.member_campaign_summary_table a 
    on c.row_id = a.contact_id 
    and upper(campaign_name) like '%TRAVEL%' 
     
     
where c.cust_stat_cd = 'Active' 
and NVL(fn.deceased_flg,'N') = 'N' 
and c.csn is not null 
and NVL(c.x_nrma_title,'no title') != 'Estate Of The Late' 
and case when trim(c.x_inv_email_1) = 'Y' or c.email_addr is null then 'N' else 'Y' end = 'Y' 
and c.con_cd in ('Ordinary Member' , 'Affiliate Member') 
) aa 
 
group by email_Perm,travel_Perm 
--where rng<0.01088 
select email_Perm,travel_Perm ,count(distinct Member_Number) from 
( 
select distinct 
         c.csn as Member_Number 
        , c.row_id as contact_id 
        , fn.hobby_cd as travel_preference 
        , case when cx.ATTRIB_36='No' then False else True end email_Perm 
        , case when t.contact_id is Null then False else true end as travel_Perm 
 
    from gms.s_contact as c  
     
	--inner join gms.s_org_ext as o  
	--on c.pr_dept_ou_id = o.row_id 
 
	inner join gms.s_asset as a  
	on a.owner_accnt_id = c.pr_dept_ou_id 
 
	inner join gms.s_prod_int as p 
       on a.prod_id = p.row_id 
        
    inner join gms.s_contact_fnx as fn  
       on fn.par_row_id = c.row_id  
     
    inner join gms.s_contact_x as cx  
    on c.row_id = cx.par_row_id 
     
    left join omc.member_campaign_summary_table t 
    on c.row_id = t.contact_id 
    and upper(campaign_name) like '%TRAVEL%' 
     
    where a.status_cd = 'Active' 
    and p.type = 'Membership' 
    and p.prod_cd = 'Promotion' 
    and NVL(fn.deceased_flg,'N') = 'N' 
    and c.csn is not null 
    and c.cust_stat_cd = 'Active' 
    and NVL(c.x_nrma_title,'no title') != 'Estate Of The Late' 
    and case when trim(c.x_inv_email_1) = 'Y' or c.email_addr is null then 'N' else 'Y' end = 'Y' 
    and c.con_cd in ('Ordinary Member' , 'Affiliate Member') 
    ) aa 
     
    group by email_Perm,travel_Perm 
Respondents = spark.read.load("/user/aamreen/List of repondents enew and random sample.csv",format="csv", sep=",", inferSchema="true", header="true") 
Respondents.createOrReplaceTempView("Respondents") 
spark.sql('select * from Respondents').count() 
spark.sql(""" 
create table campaign_data.aa_dmc1811_travelsurvey_respondants as  
select `member number` as member_number from Respondents""").show(100) 
select email_Perm,travel_Perm ,count(distinct Member_Number) from 
( 
select distinct 
         c.csn as Member_Number 
        , c.row_id as contact_id 
        , fn.hobby_cd as travel_preference 
        , case when cx.ATTRIB_36='No' then False else True end email_Perm 
        , case when t.contact_id is Null then False else true end as travel_Perm 
 
    from gms.s_contact as c  
     
	--inner join gms.s_org_ext as o  
	--on c.pr_dept_ou_id = o.row_id 
 
	inner join gms.s_asset as a  
	on a.owner_accnt_id = c.pr_dept_ou_id 
 
	inner join gms.s_prod_int as p 
       on a.prod_id = p.row_id 
        
    inner join gms.s_contact_fnx as fn  
       on fn.par_row_id = c.row_id  
     
    inner join gms.s_contact_x as cx  
    on c.row_id = cx.par_row_id 
     
    left join omc.member_campaign_summary_table t 
    on c.row_id = t.contact_id 
    and upper(campaign_name) like '%TRAVEL%' 
     
    inner join campaign_data.aa_dmc1811_travelsurvey_respondants r 
    on cast(r.member_number as string) = c.csn 
     
    where a.status_cd = 'Active' 
    and p.type = 'Membership' 
    and p.prod_cd = 'Promotion' 
    and NVL(fn.deceased_flg,'N') = 'N' 
    and c.csn is not null 
    and c.cust_stat_cd = 'Active' 
    and NVL(c.x_nrma_title,'no title') != 'Estate Of The Late' 
    and case when trim(c.x_inv_email_1) = 'Y' or c.email_addr is null then 'N' else 'Y' end = 'Y' 
    and c.con_cd in ('Ordinary Member' , 'Affiliate Member') 
    ) aa 
     
    group by email_Perm,travel_Perm 
create table campaign_data.aa_dmc1811_testtable1 as 
select * from 
( 
select  distinct 
         c.csn as Member_Number 
        ,c.row_id as contact_id 
        ,c.csn as member_id 
        ,0 as control 
        ,c.fst_name AS First_Name 
        ,c.last_name AS Last_Name 
        ,c.email_addr AS Email_Address 
        ,(year(utc_timestamp()) - year(c.birth_dt)) AS Age 
        ,c.sex_mf as Gender 
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
                        END AS area 
        , ad.zipcode as Postcode 
        , c.con_cd as Member_status 
        , fn.hobby_cd as travel_preference  
        , cx.attrib_55  AS Colour_Plus 
        , rand(0) as rng 
 
from gms.s_contact as c 
 
	inner join gms.s_contact_fnx as fn  
    on fn.par_row_id = c.row_id 
     
    inner join gms.s_contact_x as cx  
    on c.row_id = cx.par_row_id 
     
    inner join gms.s_addr_per as ad 
    on c.pr_per_addr_id = ad.row_id 
     
     
where c.cust_stat_cd = 'Active' 
and NVL(fn.deceased_flg,'N') = 'N' 
and c.csn is not null 
and NVL(c.x_nrma_title,'no title') != 'Estate Of The Late' 
and case when trim(c.x_inv_email_1) = 'Y' or c.email_addr is null then 'N' else 'Y' end = 'Y' 
--and fn.hobby_cd='Email' 
--and t.member_id is NULL 
and c.con_cd in ('Ordinary Member' , 'Affiliate Member') 
) aa 
--where rng<0.01088 
create table campaign_data.aa_dmc1811_travelSurvey1_research_others_20191120_adhoc as 
select * from 
( 
select  distinct 
         c.csn as Member_Number 
        ,c.row_id as contact_id 
        ,c.csn as member_id 
        ,0 as control 
        ,c.fst_name AS First_Name 
        ,c.last_name AS Last_Name 
        ,c.email_addr AS Email_Address 
        ,(year(utc_timestamp()) - year(c.birth_dt)) AS Age 
        ,c.sex_mf as Gender 
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
                        END AS area 
        , ad.zipcode as Postcode 
        , c.con_cd as Member_status 
        , fn.hobby_cd as travel_preference  
        , cx.attrib_55  AS Colour_Plus 
        , rand(0) as rng 
 
from gms.s_contact as c 
 
	inner join gms.s_contact_fnx as fn  
    on fn.par_row_id = c.row_id 
     
    inner join gms.s_contact_x as cx  
    on c.row_id = cx.par_row_id 
     
    inner join gms.s_addr_per as ad 
    on c.pr_per_addr_id = ad.row_id 
     
     
where c.cust_stat_cd = 'Active' 
and NVL(fn.deceased_flg,'N') = 'N' 
and c.csn is not null 
and NVL(c.x_nrma_title,'no title') != 'Estate Of The Late' 
and case when trim(c.x_inv_email_1) = 'Y' or c.email_addr is null then 'N' else 'Y' end = 'Y' 
--and fn.hobby_cd='Email' 
--and t.member_id is NULL 
and c.con_cd in ('Ordinary Member' , 'Affiliate Member') 
) aa 
where rng<0.01088 
create table campaign_data.aa_dmc1811_testtable2 as 
select * from 
( 
select  distinct 
         c.csn as Member_Number 
        ,c.row_id as contact_id 
        ,c.csn as member_id 
        ,0 as control 
        ,c.fst_name AS First_Name 
        ,c.last_name AS Last_Name 
        ,c.email_addr AS Email_Address 
        ,(year(utc_timestamp()) - year(c.birth_dt)) AS Age 
        ,c.sex_mf as Gender 
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
                        END AS area 
        , ad.zipcode as Postcode 
        , c.con_cd as Member_status 
        , fn.hobby_cd as travel_preference  
        , cx.attrib_55  AS Colour_Plus 
        , rand(0) as rng 
 
from gms.s_contact as c 
 
	inner join gms.s_contact_fnx as fn  
    on fn.par_row_id = c.row_id 
     
    inner join gms.s_contact_x as cx  
    on c.row_id = cx.par_row_id 
     
    inner join gms.s_addr_per as ad 
    on c.pr_per_addr_id = ad.row_id 
     
    left join campaign_data.aa_dmc1811_travelsurvey1_research_others_20191120_adhoc t 
    on t.member_id=c.csn 
     
     
where c.cust_stat_cd = 'Active' 
and NVL(fn.deceased_flg,'N') = 'N' 
and c.csn is not null 
and NVL(c.x_nrma_title,'no title') != 'Estate Of The Late' 
and case when trim(c.x_inv_email_1) = 'Y' or c.email_addr is null then 'N' else 'Y' end = 'Y' 
and fn.hobby_cd='Email' 
and t.member_id is NULL 
and c.con_cd in ('Ordinary Member' , 'Affiliate Member') 
) aa 
--where rng<0.01088 
create table campaign_data.aa_dmc1811_travelsurvey2_research_others_20191120_adhoc as   
select * from 
( 
select  distinct 
         c.csn as Member_Number 
        ,c.row_id as contact_id 
        ,c.csn as member_id 
        ,0 as control 
        ,c.fst_name AS First_Name 
        ,c.last_name AS Last_Name 
        ,c.email_addr AS Email_Address 
        ,(year(utc_timestamp()) - year(c.birth_dt)) AS Age 
        ,c.sex_mf as Gender 
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
                        END AS area 
        , ad.zipcode as Postcode 
        , c.con_cd as Member_status 
        , fn.hobby_cd as travel_preference  
        , cx.attrib_55  AS Colour_Plus 
        , rand(0) as rng 
 
from gms.s_contact as c 
 
	inner join gms.s_contact_fnx as fn  
    on fn.par_row_id = c.row_id 
     
    inner join gms.s_contact_x as cx  
    on c.row_id = cx.par_row_id 
     
    inner join gms.s_addr_per as ad 
    on c.pr_per_addr_id = ad.row_id 
     
    left join campaign_data.aa_dmc1811_travelSurvey1_research_others_20191120_adhoc t 
    on t.member_id=c.csn 
     
     
where c.cust_stat_cd = 'Active' 
and NVL(fn.deceased_flg,'N') = 'N' 
and c.csn is not null 
and NVL(c.x_nrma_title,'no title') != 'Estate Of The Late' 
and case when trim(c.x_inv_email_1) = 'Y' or c.email_addr is null then 'N' else 'Y' end = 'Y' 
and fn.hobby_cd='Email' 
and t.member_id is NULL 
and c.con_cd in ('Ordinary Member' , 'Affiliate Member') 
) aa 
where rng<0.0234 
select * from campaign_data.aa_dmc1811_travelsurvey1_research_others_20191120_adhoc a 
inner join campaign_data.aa_dmc1811_travelsurvey2_research_others_20191120_adhoc b 
on a.member_id=b.member_id 
DROP TABLE campaign_data.aa_dmc1811_travelsurvey2_research_others_20191119_adhoc; 
select gender, count(distinct member_id)  
from aa_dmc1811_testtable2  
group by gender 
 
select gender, count(distinct member_id)  
from aa_dmc1811_travelsurvey2_research_others_20191120_adhoc  
group by gender 
 
select colour_plus, count(distinct member_id)  
from aa_dmc1811_testtable2  
group by colour_plus 
 
select colour_plus, count(distinct member_id)  
from aa_dmc1811_travelsurvey2_research_others_20191120_adhoc  
group by colour_plus 
 
select travel_preference, count(distinct member_id)  
from aa_dmc1811_testtable1  
group by travel_preference 
 
select travel_preference, count(distinct member_id)  
from aa_dmc1811_travelsurvey1_research_others_20191120_adhoc  
group by travel_preference 
 
DROP TABLE campaign_data.aa_dmc1811_testtable1; 
DROP TABLE campaign_data.aa_dmc1811_testtable2; 
SELECT member_number, first_name, last_name, email_address, age, gender, area, postcode, member_status, travel_preference, colour_plus  
FROM campaign_data.aa_dmc1811_travelsurvey1_research_others_20191120_adhoc 
SELECT member_number, first_name, last_name, email_address, age, gender, area, postcode, member_status, travel_preference, colour_plus  
FROM campaign_data.aa_dmc1811_travelsurvey2_research_others_20191120_adhoc 
Investigation = spark.read.load("/user/aamreen/Investigation_MemberNumber.csv",format="csv", sep=",", inferSchema="true", header="true") 
Investigation.createOrReplaceTempView("Investigation") 
spark.sql('create table campaign_data.aa_dmc1811_travelsurvey2_investigation_20200124 as select * from Investigation').show(100) 
select  distinct 
         c.csn as Member_Number 
        ,c.row_id as contact_id 
      --  ,c.csn as member_id 
        ,fn.hobby_cd as travel_preference 
 
from gms.s_contact as c 
 
	inner join gms.s_contact_fnx as fn  
    on fn.par_row_id = c.row_id 
     
    inner join gms.s_contact_x as cx  
    on c.row_id = cx.par_row_id 
     
    inner join gms.s_addr_per as ad 
    on c.pr_per_addr_id = ad.row_id 
     
    inner join campaign_data.aa_dmc1811_travelsurvey2_investigation_20200124 iv 
    on c.csn = cast (iv.member_number as string) 
SELECT DISTINCT a.contact_id ,fn.hobby_cd as travel_preference 
FROM omc.member_campaign_summary_table a 
--a.contact_id, a.campaign_name, a.channel, a.opentoclick, a.sendtime 
 
inner join gms.s_contact c 
on c.row_id= a.contact_id 
 
-- inner join campaign_data.aa_dmc1811_travelsurvey2_investigation_20200124 iv 
-- on c.csn = cast (iv.member_number as string) 
 
inner join gms.s_contact_fnx as fn  
on fn.par_row_id = c.row_id 
 
where a.campaign_name like 'PROD_Travel_eDM%' 
select DISTINCT a.campaign_name FROM omc.member_campaign_summary_table a 
where UPPER(a.campaign_name) like '%TRAVEL%' 
and a.campaign_id='41285602' 