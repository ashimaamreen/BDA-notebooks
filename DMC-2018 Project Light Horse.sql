how many with email what percentage 
no email - demograpics age bucket into 
over 80, 70, 60 
over 50 
membership type 
 
, area, colour plus, 
 
put dishonour 
 
drop table campaign_data.aa_dmc2018_ProjectLightHorse_research_others_20200406  
--Total #1693918 
--Email-able #1277086 
--920287 
create table campaign_data.aa_dmc2018_ProjectLightHorse_research_others_20200406 as  
with CMO_F2G as ( 
select p.name as product,prod_cd,c.row_id as contact_id,c.csn as member_number,c.con_cd as member_type from gms.s_contact as c 
     
    inner join gms.s_asset as a 
        on a.owner_con_id = c.row_id 
         
    inner join gms.s_prod_int as p 
        on a.prod_id = p.row_id 
 
where 
    (p.name like 'Autoclub%' or p.name='F2G') 
    and a.status_cd = 'Active' 
 
), dishonour as ( 
 
select distinct a.owner_con_id as a_contact_ID from gms.s_asset_x as ax 
 
inner join gms.s_asset as a 
on a.row_id = ax.par_row_id 
where lower(ax.ATTRIB_04) like '%dishonour%' or ax.attrib_04 is not NULL 
) 
 
SELECT distinct 
         c.csn as Member_Number 
        ,c.fst_name AS First_Name 
        ,c.last_name AS Last_Name 
        ,c.email_addr AS Email_Address 
        ,c.cell_ph_num AS Mobile 
        ,c.home_ph_num AS Home_Phone 
        ,c.con_cd AS Membership_type 
        ,c.CUST_VALUE_CD AS Loyalty_Colour  
        ,cx.attrib_17 AS Tenure 
        ,c.sex_mf as Gender 
        ,(year(now()) - year(c.birth_dt)) AS Member_Age 
        ,cx.attrib_55  AS Colour_Plus 
        ,ad.city as Suburb 
        ,cx.attrib_22 as Geotribe_Segment 
        ,cx.ATTRIB_24 as SES_flag 
        ,case when d.a_contact_ID is null then 'N' else 'Y' end Dishonour 
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
        ,c.x_inv_email_1 
        ,c.hard_to_reach 
        ,c.veteran_flg 
 
 
    from gms.s_contact as c                      
     
    inner join gms.s_contact_x cx 
	on cx.par_row_id=c.row_id 
	 
	inner join gms.s_contact_fnx as fn  
    on fn.par_row_id = c.row_id 
     
    inner join gms.s_addr_per as ad 
    on c.pr_per_addr_id = ad.row_id 
     
    left anti join CMO_F2G ex 
    on ex.member_number=c.csn 
     
    left join dishonour d 
    on d.a_contact_ID=c.row_id 
     
    where 1=1 
    and NVL(fn.deceased_flg,'N') = 'N'  
    and NVL(c.x_nrma_title,'no title') != 'Estate Of The Late' 
    and c.csn is not null 
    and c.cust_stat_cd = 'Active' 
    and c.con_cd ='Ordinary Member' 
    and (year(now()) - year(c.birth_dt))>=18  
    and COALESCE(cx.attrib_44, '') not like 'Honorary%'  
     
    -- and case when trim(c.x_inv_email_1) = 'Y' or c.email_addr is null then 'N' else 'Y' end = 'N' 
     
    -- and (case when (c.cell_ph_num is not null and nvl(c.veteran_flg,'N')<>'Y' and regexp_replace(c.cell_ph_num, "[^0-9]+", "") not like '0_0000000_' and regexp_replace(c.cell_ph_num, "[^0-9]+", "") not like '041111111_') then 'Y' else 'N' end ='Y' 
    --         OR case when (c.home_ph_num is not null and nvl(c.hard_to_reach,'N')<>'Y') then 'Y' else 'N' end ='Y') 
    --314836 
    --and COALESCE(fn.brloc_attrib13, '') != 'Y' -- 
    --and COALESCE(c.x_nrma_bad_debt, 'N') ='N' --314643 
    --and iag.rank in (1,2) 
--920287 
 
with CMO_F2G as ( 
select p.name as product,prod_cd,c.row_id as contact_id,c.csn as member_number,c.con_cd as member_type from gms.s_contact as c 
     
    inner join gms.s_asset as a 
        on a.owner_con_id = c.row_id 
         
    inner join gms.s_prod_int as p 
        on a.prod_id = p.row_id 
 
where 
    (p.name like 'Autoclub%' or p.name='F2G') 
    and a.status_cd = 'Active' 
 
), dishonour as ( 
 
select distinct a.owner_con_id as a_contact_ID from gms.s_asset_x as ax 
 
inner join gms.s_asset as a 
on a.row_id = ax.par_row_id 
where lower(ax.ATTRIB_04) like '%dishonour%' or ax.attrib_04 is not NULL 
) 
 
SELECT distinct 
         c.csn as Member_Number 
        ,c.fst_name AS First_Name 
        ,c.last_name AS Last_Name 
        ,c.email_addr AS Email_Address 
        ,c.cell_ph_num AS Mobile 
        ,c.home_ph_num AS Home_Phone 
        ,c.con_cd AS Membership_type 
        ,cx.attrib_17 AS Tenure 
        ,c.sex_mf as Gender 
        ,(year(now()) - year(c.birth_dt)) AS Member_Age 
        ,cx.attrib_55  AS Colour_Plus 
        ,ad.city as Suburb 
        ,cx.attrib_22 as Geotribe_Segment 
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
 
    from gms.s_contact as c                     --1335486 
     
    inner join gms.s_contact_x cx 
	on cx.par_row_id=c.row_id 
	 
	inner join gms.s_contact_fnx as fn  
    on fn.par_row_id = c.row_id 
     
    inner join gms.s_addr_per as ad 
    on c.pr_per_addr_id = ad.row_id 
     
    left anti join CMO_F2G ex 
    on ex.member_number=c.csn 
     
    left anti join dishonour d 
    on d.a_contact_ID=c.row_id 
     
    where 1=1 
    and NVL(fn.deceased_flg,'N') = 'N'  
    and c.csn is not null 
    and c.cust_stat_cd = 'Active' 
    and c.con_cd ='Ordinary Member' 
    --, 'Affiliate Member') 
    and NVL(c.x_nrma_title,'no title') != 'Estate Of The Late' 
    and case when trim(c.x_inv_email_1) = 'Y' or c.email_addr is null then 'N' else 'Y' end = 'Y' 
    and (year(now()) - year(c.birth_dt))>=18 --314848 
    --and COALESCE(cx.attrib_44, '') not like 'Honorary%' --314836 
    --and COALESCE(fn.brloc_attrib13, '') != 'Y' -- 
    --and COALESCE(c.x_nrma_bad_debt, 'N') ='N' --314643 
    --and iag.rank in (1,2) 
--and case when trim(c.x_inv_email_1) = 'Y' or c.email_addr is null then 'N' else 'Y' end = 'Y' 
 
SELECT count(distinct 
         c.csn) as Member_Number 
        -- ,c.row_id as contact_id  
        -- ,c.email_addr AS Email_Address  
        -- ,cx.attrib_55  AS Colour_Plus 
        -- ,cx.attrib_17 MembershipTenure 
        -- ,(year(now()) - year(c.birth_dt)) AS Age 
        -- ,c.sex_mf as Gender 
        -- ,CASE  
        --                 WHEN  
        --                  (ad.zipcode >= '2000' and ad.zipcode <= '2082' or 
        --                   ad.zipcode >= '2084' and ad.zipcode <= '2234' or  
        --                   ad.zipcode >= '2555' and ad.zipcode <= '2574' or  
        --                   ad.zipcode >= '2745' and ad.zipcode <= '2770' or  
        --                   ad.zipcode >= '2775' and ad.zipcode <= '2775') 
        --                   AND (upper(ad.country) = 'AUSTRALIA' or upper(ad.country) = 'AU') 
        --                   THEN 'METROPOLITAN' 
        --                 WHEN  
        --                  (ad.zipcode >= '2083' and ad.zipcode <= '2083' or  
        --                   ad.zipcode >= '2250' and ad.zipcode <= '2338' or  
        --                   ad.zipcode >= '2415' and ad.zipcode <= '2423' or  
        --                   ad.zipcode >= '2425' and ad.zipcode <= '2425' or  
        --                   ad.zipcode >= '2428' and ad.zipcode <= '2428' or  
        --                   ad.zipcode >= '2500' and ad.zipcode <= '2535' or  
        --                   ad.zipcode >= '2538' and ad.zipcode <= '2541' or  
        --                   ad.zipcode >= '2575' and ad.zipcode <= '2578' or  
        --                   ad.zipcode >= '2600' and ad.zipcode <= '2617' or  
        --                   ad.zipcode >= '2773' and ad.zipcode <= '2774' or  
        --                   ad.zipcode >= '2776' and ad.zipcode <= '2786' or  
        --                   ad.zipcode >= '2900' and ad.zipcode <= '2914') 
        --                   AND (upper(ad.country) = 'AUSTRALIA' or upper(ad.country) = 'AU')  
        --                   THEN  'REGIONAL' 
        --                 WHEN  
        --                   (ad.zipcode >= '2339' and ad.zipcode <= '2411' or  
        --                   ad.zipcode >= '2424' and ad.zipcode <= '2424' or  
        --                   ad.zipcode >= '2426' and ad.zipcode <= '2427' or  
        --                   ad.zipcode >= '2429' and ad.zipcode <= '2490' or  
        --                   ad.zipcode >= '2536' and ad.zipcode <= '2537' or  
        --                   ad.zipcode >= '2545' and ad.zipcode <= '2551' or  
        --                   ad.zipcode >= '2579' and ad.zipcode <= '2594' or  
        --                   ad.zipcode >= '2618' and ad.zipcode <= '2739' or  
        --                   ad.zipcode >= '2787' and ad.zipcode <= '2898' or  
        --                   ad.zipcode >= '6798' and ad.zipcode <= '6799') 
        --                   AND (upper(ad.country) = 'AUSTRALIA' or upper(ad.country) = 'AU')  
        --                   THEN  'RURAL' 
        --                 WHEN  
        --                   (ad.zipcode >= '0800' and ad.zipcode <= '0886' or  
        --                   ad.zipcode >= '3000' and ad.zipcode <= '6770' or  
        --                   ad.zipcode >= '6907' and ad.zipcode <= '7470' or  
        --                   ad.zipcode >= '7471' ) 
        --                   AND (upper(ad.country) = 'AUSTRALIA' or upper(ad.country) = 'AU')  
        --                   THEN  'INTERSTATE' 
        --                 ELSE 'UNKNOWN' 
        --                 END AS Region_Name 
 
    from gms.s_contact as c                     --1335486 
     
    inner join gms.s_contact_x cx 
	on cx.par_row_id=c.row_id 
	 
	inner join gms.s_contact_fnx as fn  
    on fn.par_row_id = c.row_id 
     
    inner join gms.s_addr_per as ad 
    on c.pr_per_addr_id = ad.row_id 
     
    -- left anti join CMO_F2G ex 
    -- on ex.member_number=c.csn 
     
    -- left anti join dishonour d 
    -- on d.a_contact_ID=c.row_id 
     
    where 1=1 
    and NVL(fn.deceased_flg,'N') = 'N'  
    and c.csn is not null 
    and c.cust_stat_cd = 'Active' 
    and c.con_cd ='Ordinary Member' 
    --, 'Affiliate Member') 
    and NVL(c.x_nrma_title,'no title') != 'Estate Of The Late' 
    and case when trim(c.x_inv_email_1) = 'Y' or c.email_addr is null then 'N' else 'Y' end = 'N' 
    and (year(now()) - year(c.birth_dt))>=18  
    and (case when (c.cell_ph_num is not null and nvl(c.veteran_flg,'N')<>'Y' and regexp_replace(c.cell_ph_num, "[^0-9]+", "") not like '0_0000000_' and regexp_replace(c.cell_ph_num, "[^0-9]+", "") not like '041111111_') then 'Y' else 'N' end ='Y' 
            OR case when (c.home_ph_num is not null and nvl(c.hard_to_reach,'N')<>'Y') then 'Y' else 'N' end ='Y') 
select * 
 
from campaign_data.aa_dmc2018_ProjectLightHorse_research_others_20200406 
limit 10; 
select region_name, count(distinct member_number) 
--colour_plus,count(distinct member_number) 
--loyalty_colour,count(distinct member_number) 
 
-- CASE 
--   WHEN member_age BETWEEN 18 AND 29 THEN '18-29' 
--   WHEN member_age BETWEEN 30 AND 39 THEN '30-39' 
--   WHEN member_age BETWEEN 40 AND 49 THEN '40-49' 
--   WHEN member_age BETWEEN 50 AND 59 THEN '50-59' 
--   WHEN member_age BETWEEN 60 AND 69 THEN '60-69' 
--   WHEN member_age BETWEEN 70 AND 79 THEN '70-79' 
--   WHEN member_age BETWEEN 80 AND 89 THEN '80-89' 
--   WHEN member_age>= 90 THEN '90+' 
--   ELSE '0' 
-- END AS Age_band,   
 
from campaign_data.aa_dmc2018_ProjectLightHorse_research_others_20200406 
group by 1 
order by 1 
select region_name, count(distinct member_number) 
--colour_plus,count(distinct member_number) 
--loyalty_colour,count(distinct member_number) 
-- CASE 
--   WHEN member_age BETWEEN 18 AND 29 THEN '18-29' 
--   WHEN member_age BETWEEN 30 AND 39 THEN '30-39' 
--   WHEN member_age BETWEEN 40 AND 49 THEN '40-49' 
--   WHEN member_age BETWEEN 50 AND 59 THEN '50-59' 
--   WHEN member_age BETWEEN 60 AND 69 THEN '60-69' 
--   WHEN member_age BETWEEN 70 AND 79 THEN '70-79' 
--   WHEN member_age BETWEEN 80 AND 89 THEN '80-89' 
--   WHEN member_age>= 90 THEN '90+' 
--   ELSE '0' 
-- END AS Age_band, count(distinct member_number)  
 
from campaign_data.aa_dmc2018_ProjectLightHorse_research_others_20200406 
where case when trim(x_inv_email_1) = 'Y' or email_address is null then 'N' else 'Y' end = 'Y' 
group by 1 
order by 1 
 
select region_name, count(distinct member_number) 
--colour_plus,count(distinct member_number) 
--loyalty_colour,count(distinct member_number) 
-- CASE 
--   WHEN member_age BETWEEN 18 AND 29 THEN '18-29' 
--   WHEN member_age BETWEEN 30 AND 39 THEN '30-39' 
--   WHEN member_age BETWEEN 40 AND 49 THEN '40-49' 
--   WHEN member_age BETWEEN 50 AND 59 THEN '50-59' 
--   WHEN member_age BETWEEN 60 AND 69 THEN '60-69' 
--   WHEN member_age BETWEEN 70 AND 79 THEN '70-79' 
--   WHEN member_age BETWEEN 80 AND 89 THEN '80-89' 
--   WHEN member_age>= 90 THEN '90+' 
--   ELSE '0' 
-- END AS Age_band, count(distinct member_number) i 
from campaign_data.aa_dmc2018_ProjectLightHorse_research_others_20200406 
where case when trim(x_inv_email_1) = 'Y' or email_address is null then 'N' else 'Y' end = 'N' 
and (case when (mobile is not null and nvl(veteran_flg,'N')<>'Y' and regexp_replace(mobile, "[^0-9]+", "") not like '0_0000000_' and regexp_replace(mobile, "[^0-9]+", "") not like '041111111_') then 'Y' else 'N' end ='Y' 
        OR case when (home_phone is not null and nvl(hard_to_reach,'N')<>'Y') then 'Y' else 'N' end ='Y') 
 
group by 1 
order by 1 
 
 
select *  
from campaign_data.aa_dmc2018_ProjectLightHorse_research_others_20200406 
limit 10; 
create table campaign_data.aa_dmc2018_ProjectLightHorseEmail_research_others_20200407 as 
With email as ( 
select *, rand() as rng 
 
from campaign_data.aa_dmc2018_ProjectLightHorse_research_others_20200406 
where case when trim(x_inv_email_1) = 'Y' or email_address is null then 'N' else 'Y' end = 'Y' 
) 
select * FROM email 
where rng<0.01895 
 
select   member_number 
        ,first_name 
        ,last_name 
        ,email_address 
        ,mobile 
        ,home_phone 
        ,loyalty_colour 
        ,tenure 
        ,gender 
        ,member_age 
        ,colour_plus 
        ,ses_flag 
        ,suburb 
        ,region_name 
from aa_dmc2018_projectlighthorseemail_research_others_20200407 
create table campaign_data.aa_dmc2018_ProjectLightHorsePhone_research_others_20200407 as 
With phone as ( 
select *, rand() as rng 
 
from campaign_data.aa_dmc2018_ProjectLightHorse_research_others_20200406 
where case when trim(x_inv_email_1) = 'Y' or email_address is null then 'N' else 'Y' end = 'N' 
and (case when (mobile is not null and nvl(veteran_flg,'N')<>'Y' and regexp_replace(mobile, "[^0-9]+", "") not like '0_0000000_' and regexp_replace(mobile, "[^0-9]+", "") not like '041111111_') then 'Y' else 'N' end ='Y' 
        OR case when (home_phone is not null and nvl(hard_to_reach,'N')<>'Y') then 'Y' else 'N' end ='Y') 
) 
select * FROM phone 
where rng<0.0155 
select   member_number 
        ,first_name 
        ,last_name 
        ,email_address 
        ,mobile 
        ,home_phone 
        ,loyalty_colour 
        ,tenure 
        ,gender 
        ,member_age 
        ,colour_plus 
        ,ses_flag 
        ,suburb 
        ,region_name 
from aa_dmc2018_projectlighthorsephone_research_others_20200407 
create table campaign_data.aa_dmc2018_projectlighthorse_research_others_20200407_adhoc as 
select *, 'email' as segment from campaign_data.aa_dmc2018_projectlighthorseemail_research_others_20200407 
union 
select *, 'phone' as segment from campaign_data.aa_dmc2018_projectlighthorsephone_research_others_20200407