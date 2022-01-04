with Main as ( 
SELECT  asset.owner_accnt_id 
                --, v.lcns_num  
                , group_concat(prod.name," | ") as products 
                , group_concat(nrp.name," | ") as nrp_products 
                -- , con.csn 
                -- , asset.row_id 
                -- , prod.name 
                -- , nrp.name as next_renewable_prod 
 
FROM 
    gms.s_asset AS asset 
     
inner join gms.s_Asset v  
on v.row_id=asset.service_point_id 
     
INNER JOIN 
    gms.s_prod_int AS prod 
    ON prod.row_id = asset.prod_id 
 
LEFT OUTER JOIN 
    gms.s_prod_int AS nrp 
    ON nrp.row_id = asset.x_next_renewal_prod_id 
 
INNER JOIN 
    gms.s_contact AS con 
    ON con.row_id = asset.owner_con_id 
 
 
WHERE  
    asset.status_cd = "Active" 
    AND prod.prod_cd != "Promotion" 
    AND prod.sub_type_cd in ('RSA','Add-on') 
    AND (asset.x_next_renewal_prod_id IS NOT NULL or prod.sub_type_cd='Add-on') 
    AND DATE_ADD(asset.end_dt, 1) BETWEEN '2022-01-01' AND '2022-03-31' 
    and con_cd in ('Ordinary Member','Affiliate Member') 
     
    GROUP BY 1 
    --PC | NB 
    --NB 
  ) 
  select * from Main 
--  where owner_accnt_id='1-C7Q-2787' 
  where products rlike 'Classic' 
  and products rlike 'Premium' 
  and products rlike 'Plus' 
  --and products='Tow Plus | Autoclub Classic | Tow Plus | MVB Premium Care | MVB Premium Care' 
with main_asset as ( 
SELECT DISTINCT asset.owner_accnt_id 
                , con.csn 
                , asset.row_id 
                , prod.name 
                , nrp.name as next_renewable_prod 
                , v.lcns_num  
 
FROM 
    gms.s_asset AS asset 
     
inner join gms.s_Asset v  
on v.row_id=asset.service_point_id 
     
INNER JOIN 
    gms.s_prod_int AS prod 
    ON prod.row_id = asset.prod_id 
 
LEFT OUTER JOIN 
    gms.s_prod_int AS nrp 
    ON nrp.row_id = asset.x_next_renewal_prod_id 
 
INNER JOIN 
    gms.s_contact AS con 
    ON con.row_id = asset.owner_con_id 
 
 
WHERE  
    asset.status_cd = "Active" 
    AND prod.prod_cd != "Promotion" 
    AND prod.sub_type_cd='RSA' 
    AND asset.x_next_renewal_prod_id IS NOT NULL 
    AND DATE_ADD(asset.end_dt, 1) BETWEEN '2021-10-25' AND '2021-11-25' 
    and con_cd in ('Ordinary Member' , 'Affiliate Member') 
 
), addon as ( 
SELECT DISTINCT asset.owner_accnt_id, asset.par_asset_id, prod.name as addon 
 
FROM 
    gms.s_asset AS asset 
 
 
INNER JOIN 
    gms.s_prod_int AS prod 
    ON prod.row_id = asset.prod_id 
 
LEFT OUTER JOIN 
    gms.s_prod_int AS nrp 
    ON nrp.row_id = asset.x_next_renewal_prod_id 
 
 
WHERE  
    asset.status_cd = "Active" 
    AND prod.prod_cd != "Promotion" 
    AND prod.sub_type_cd='Add-on' 
    AND DATE_ADD(asset.end_dt, 1) BETWEEN '2021-10-25' AND '2021-11-25' 
    ) 
   -- , main as ( 
    select distinct 
              m.owner_accnt_id  as account_id 
            --, m.csn 
            --, m.row_id as asset_id 
            , m.lcns_num as rego 
            --, count(distinct m.row_id) as asset_id 
            , name as prod 
            , a.addon 
            , m.next_renewable_prod 
     
    from main_asset m 
 
    left join addon a 
    on a.par_asset_id=m.row_id 
   
--   where m.owner_con_id='1-DRM-1959'  
 --  ) 
--   select contact_id, count(distinct asset_id) 
    
--   from main 
--   where addon is not null 
--   group by 1 
--   --having count(distinct asset_id)>0 and addon is not null 
--     --group by 1 
--     order by 2 desc 
DROP table campaign_data.aa_dmc2420_brumbyprerenewal_dm_20210816 
create table campaign_data.aa_dmc2420_brumbyprerenewal_dm_20210816 as  
with rsa as ( 
SELECT DISTINCT asset.owner_accnt_id 
        , asset.row_id 
        , prod.name 
        , nrp.name as next_renewable_prod 
        , v.lcns_num  
 
FROM 
    gms.s_asset AS asset 
     
inner join gms.s_Asset v  
on v.row_id=asset.service_point_id 
     
INNER JOIN 
    gms.s_prod_int AS prod 
    ON prod.row_id = asset.prod_id 
 
LEFT OUTER JOIN 
    gms.s_prod_int AS nrp 
    ON nrp.row_id = asset.x_next_renewal_prod_id 
 
INNER JOIN 
    gms.s_contact AS con 
    ON con.row_id = asset.owner_con_id 
 
 
WHERE  
    asset.status_cd = "Active" 
    AND prod.prod_cd != "Promotion" 
    AND prod.sub_type_cd='RSA' 
    AND asset.x_next_renewal_prod_id IS NOT NULL 
    AND DATE_ADD(asset.end_dt, 1) BETWEEN '2021-10-25' AND '2021-11-25' 
    and con_cd in ('Ordinary Member' , 'Affiliate Member') 
    and nrp.name not in ('TC','F2G','BC','NB') 
 
), addon as ( 
SELECT DISTINCT asset.owner_accnt_id 
        , asset.par_asset_id 
        , prod.name as addon 
 
FROM 
    gms.s_asset AS asset 
     
INNER JOIN 
    gms.s_prod_int AS prod 
    ON prod.row_id = asset.prod_id 
 
LEFT OUTER JOIN 
    gms.s_prod_int AS nrp 
    ON nrp.row_id = asset.x_next_renewal_prod_id 
 
 
WHERE  
    asset.status_cd = "Active" 
    AND prod.prod_cd != "Promotion" 
    AND prod.sub_type_cd='Add-on' 
    AND DATE_ADD(asset.end_dt, 1) BETWEEN '2021-10-25' AND '2021-11-25' 
 
), main as ( 
    select distinct m.owner_accnt_id  as account_id 
            , m.row_id as asset_id 
            , m.lcns_num as rego 
            , name as prod 
            , a.addon 
            , m.next_renewable_prod 
            , case     
                when (m.name rlike 'Plus' and m.next_renewable_prod in ('PP','CO')) and addon='Pet Plus' then '1' 
                when (m.name rlike 'Plus' and m.next_renewable_prod in ('PP','CO')) and addon='Windscreen Plus' then '2' 
                when (m.name rlike 'Plus' and m.next_renewable_prod in ('PP','CO')) then '3' 
                when m.next_renewable_prod in ('PC','CO') and addon='Pet Plus' then '4' 
                when m.next_renewable_prod in ('PC','CO') and addon='Windscreen Plus' then '5' 
                when m.next_renewable_prod in ('PC','CO') then '6' 
                when m.next_renewable_prod in ('EV','EY','CC') then '7' 
                else '99' end Priority  
     
    from rsa m 
 
    left join addon a 
    on a.par_asset_id=m.row_id 
     
   )  
  , main2 as ( 
  select  account_id 
        , Priority 
        , rego 
        , rank() over(partition by account_id order by Priority asc) as rnk 
  from main 
  where Priority!='99' 
  ), final as ( 
  select account_id 
            , max(Priority) as letter_version  
            ,max(rego) as vehicle_rego  
  from main2 
  where rnk=1 
  group by 1 
  ) 
  SELECT distinct c.csn as membernumber 
        , c.cust_value_cd as memberType 
        , vehicle_rego 
        , c.x_nrma_title 
        , c.fst_name as first_name 
        --, c.mid_name as middle_name 
        , c.last_name as last_name 
        --, ad.addr_name as Full_address 
        , ad.addr as ad_line_1 
        , ad.addr_line_2 as ad_line_2 
        , ad.city as suburb 
        , ad.state 
        , ad.zipcode as post_code 
        , letter_version 
 
FROM  gms.s_contact c 
 
INNER JOIN gms.s_contact_x cx 
on cx.par_row_id=c.row_id 
 
INNER JOIN gms.s_contact_fnx fn 
on fn.par_row_id=c.row_id 
 
inner join gms.s_addr_per as ad         --813620 
on c.pr_per_addr_id = ad.row_id 
 
inner join gms.s_org_ext as org             --3.2m 
on org.pr_con_id = c.row_id 
 
inner join final f 
on f.account_id=org.row_id 
 
WHERE 1=1 
     
AND (COALESCE(c.x_inv_email_1, 'N') = 'Y' or c.email_addr IS  NULL)     --non-email able 
and c.cust_stat_cd = 'Active'       
and c.con_cd in ('Ordinary Member' , 'Affiliate Member')                 
and NVL(c.x_nrma_title,'no title') != 'Estate Of The Late'              --deceased check 
and NVL(fn.deceased_flg,'N') = 'N'                               
and c.csn is not null                                            
and case when trim(ad.premise_flg) = 'Y' or ad.addr_name is null then 'N' else 'Y' end = 'Y' --no return address 
--and nvl(cx.attrib_35,'Yes') <> 'No' --post permission           
and (UPPER(trim(NVL(ad.addr,'NULL')))<>'NULL' or UPPER(trim(NVL(ad.addr_line_2,'NULL')))<>'NULL')       --valid address 
 
   
SELECT * from campaign_data.aa_dmc2479_brumbytest_dm_20210805 
create table campaign_data.aa_dmc2479_brumbytest_dm_20210816 as  
with rsa as ( 
SELECT DISTINCT asset.owner_accnt_id 
        , asset.row_id 
        , prod.name 
        , nrp.name as next_renewable_prod 
        , v.lcns_num  
 
FROM 
    gms.s_asset AS asset 
     
inner join gms.s_Asset v  
on v.row_id=asset.service_point_id 
     
INNER JOIN 
    gms.s_prod_int AS prod 
    ON prod.row_id = asset.prod_id 
 
LEFT OUTER JOIN 
    gms.s_prod_int AS nrp 
    ON nrp.row_id = asset.x_next_renewal_prod_id 
 
INNER JOIN 
    gms.s_contact AS con 
    ON con.row_id = asset.owner_con_id 
 
 
WHERE  
    asset.status_cd = "Active" 
    AND prod.prod_cd != "Promotion" 
    AND prod.sub_type_cd='RSA' 
    AND asset.x_next_renewal_prod_id IS NOT NULL 
    AND DATE_ADD(asset.end_dt, 1) BETWEEN '2021-10-25' AND '2021-11-25' 
    and con_cd in ('Ordinary Member' , 'Affiliate Member') 
 
), addon as ( 
SELECT DISTINCT asset.owner_accnt_id 
        , asset.par_asset_id 
        , prod.name as addon 
 
FROM 
    gms.s_asset AS asset 
     
INNER JOIN 
    gms.s_prod_int AS prod 
    ON prod.row_id = asset.prod_id 
 
LEFT OUTER JOIN 
    gms.s_prod_int AS nrp 
    ON nrp.row_id = asset.x_next_renewal_prod_id 
 
 
WHERE  
    asset.status_cd = "Active" 
    AND prod.prod_cd != "Promotion" 
    AND prod.sub_type_cd='Add-on' 
    AND DATE_ADD(asset.end_dt, 1) BETWEEN '2021-10-25' AND '2021-11-25' 
 
), main as ( 
    select distinct m.owner_accnt_id  as account_id 
            , m.row_id as asset_id 
            , m.lcns_num as rego 
            , name as prod 
            , a.addon 
            , m.next_renewable_prod 
            , case     
                when next_renewable_prod='PP' and addon='Pet Plus' then '1' 
                when next_renewable_prod='PP' and addon='Windscreen Plus' then '2' 
                when next_renewable_prod='PP' then '3' 
                when next_renewable_prod rlike 'PC' and addon='Pet Plus' then '4' 
                when next_renewable_prod rlike 'PC' and addon='Windscreen Plus' then '5' 
                when next_renewable_prod rlike 'PC' then '6' 
                when next_renewable_prod rlike 'CC' then '7' 
                else '99' end Priority  
     
    from rsa m 
 
    left join addon a 
    on a.par_asset_id=m.row_id 
     
   ), main2 as ( 
  select  account_id 
        , Priority 
        , rego 
        , rank() over(partition by account_id order by Priority asc) as rnk 
  from main 
  where Priority!='99' 
  ), final as ( 
  select account_id 
            , max(Priority) as letter_version  
            ,max(rego) as vehicle_rego  
  from main2 
  where rnk=1 
  group by 1 
  ) 
  SELECT  c.csn as membernumber 
        , c.cust_value_cd as memberType 
        , vehicle_rego 
        , c.x_nrma_title 
        , c.fst_name as first_name 
        --, c.mid_name as middle_name 
        , c.last_name as last_name 
        --, ad.addr_name as Full_address 
        , ad.addr as ad_line_1 
        , ad.addr_line_2 as ad_line_2 
        , ad.city as suburb 
        , ad.state 
        , ad.zipcode as post_code 
        , letter_version 
 
FROM  gms.s_contact c 
 
INNER JOIN gms.s_contact_x cx 
on cx.par_row_id=c.row_id 
 
INNER JOIN gms.s_contact_fnx fn 
on fn.par_row_id=c.row_id 
 
inner join gms.s_addr_per as ad         --813620 
on c.pr_per_addr_id = ad.row_id 
 
inner join gms.s_org_ext as org             --3.2m 
on org.pr_con_id = c.row_id 
 
inner join final f 
on f.account_id=org.row_id 
 
WHERE 1=1 
     
AND (COALESCE(c.x_inv_email_1, 'N') = 'Y' or c.email_addr IS  NULL)     --non-email able 
and c.cust_stat_cd = 'Active'       
and c.con_cd in ('Ordinary Member' , 'Affiliate Member')                 
and NVL(c.x_nrma_title,'no title') != 'Estate Of The Late'              --deceased check 
and NVL(fn.deceased_flg,'N') = 'N'                               
and c.csn is not null                                            
and case when trim(ad.premise_flg) = 'Y' or ad.addr_name is null then 'N' else 'Y' end = 'Y' --no return address 
--and nvl(cx.attrib_35,'Yes') <> 'No' --post permission           
and (UPPER(trim(NVL(ad.addr,'NULL')))<>'NULL' or UPPER(trim(NVL(ad.addr_line_2,'NULL')))<>'NULL')       --valid address 
   
SELECT   c.x_nrma_title 
        , c.fst_name as first_name 
        , c.mid_name as middle_name 
        , c.last_name as last_name 
        -- , to_date(c.x_nrma_join_dt) as Join_date 
        -- , extract(now(), "year") - extract(to_date(c.birth_dt), "year") as Age 
        -- , cx.attrib_55 as ColourPlus 
        -- , cx.attrib_17 as Tenure 
        , c.cust_value_cd as memberType 
        -- , c.cust_value_cd as loyaltyColour -- gold, gold+, silver, and members 
        , ad.addr_name as Full_address 
        , ad.addr as ad_line_1 
        , ad.addr_line_2 as ad_line_2 
        , ad.city as suburb 
        , ad.state 
        , ad.zipcode as post_code 
        --, case when trim(c.x_inv_email_1) = 'Y' or c.email_addr is null or cx.attrib_36='No' then 'N'  else 'Y' end email_able 
        -- , if(org.pr_con_id <> c.row_id, 0, 1) as primary_account 
 
FROM  gms.s_contact c 
 
INNER JOIN gms.s_contact_x cx 
on cx.par_row_id=c.row_id 
 
INNER JOIN gms.s_contact_fnx fn 
on fn.par_row_id=c.row_id 
 
inner join gms.s_addr_per as ad         --813620 
on c.pr_per_addr_id = ad.row_id 
 
inner join gms.s_org_ext as org             --3.2m 
on org.pr_con_id = c.row_id 
 
 
WHERE 1=1 
     
AND (COALESCE(c.x_inv_email_1, 'N') = 'Y' or c.email_addr IS  NULL)     --non-email able 
and c.cust_stat_cd = 'Active'       
and c.con_cd in ('Ordinary Member' , 'Affiliate Member')                 
and NVL(c.x_nrma_title,'no title') != 'Estate Of The Late'              --deceased check 
and NVL(fn.deceased_flg,'N') = 'N'                               
and c.csn is not null                                            
and case when trim(ad.premise_flg) = 'Y' or ad.addr_name is null then 'N' else 'Y' end = 'Y' --no return address 
--and nvl(cx.attrib_35,'Yes') <> 'No' --post permission           
and (UPPER(trim(NVL(ad.addr,'NULL')))<>'NULL' or UPPER(trim(NVL(ad.addr_line_2,'NULL')))<>'NULL')       --valid address 
SELECT DISTINCT prod_name, nrp_prod_name from sandpit.renewal_base 
where prod_name RLIKE 'MVB' 
--and  
SELECT count(*) from campaign_data.aa_dmc2479_brumbytest_dm_20210805 
SELECT letter_version,count(DISTINCT membernumber) from campaign_data.aa_dmc2479_brumbytest_dm_20210816 
GROUP BY 1 
ORDER BY 1 
SELECT * from campaign_data.aa_dmc2420_brumbyprerenewal_dm_20210816 
where membernumber='756009901' 
SELECT letter_version,count(DISTINCT membernumber) from campaign_data.aa_dmc2420_brumbyprerenewal_dm_20210816 
GROUP BY 1 
ORDER BY 1 
select * from campaign_data.aa_dmc2479_brumbytest_dm_20210816 a 
 
left anti join aa_dmc2420_brumbyprerenewal_dm_20210816 b 
on a.membernumber=b.membernumber 
and a.letter_version=b.letter_version 
 
DROP table aa_dmc2420_brumbyprerenewal_dm_20210805 
SELECT campaign_name,campaign_id,count(*) from omc.campaign_info 
where lower(campaign_name) rlike 'prerenewal 
' 
GROUP BY 1,2 
ORDER BY 1,2 
--rlike '70' 
SELECT to_date(send_event_date), count(DISTINCT customer_id) from omc.send_level_summary 
where campaign_id='26786562' 
--and send_event_date>'2021-06-30' 
GROUP BY 1 
ORDER BY 1 DESC 
--create table campaign_data.aa_dmc2420_brumbyprerenewal_dm_20210813 as  
with rsa as ( 
SELECT DISTINCT asset.owner_accnt_id 
        , asset.row_id 
        , prod.name 
        , nrp.name as next_renewable_prod 
        , v.lcns_num  
 
FROM 
    gms.s_asset AS asset 
     
inner join gms.s_Asset v  
on v.row_id=asset.service_point_id 
     
INNER JOIN 
    gms.s_prod_int AS prod 
    ON prod.row_id = asset.prod_id 
 
LEFT OUTER JOIN 
    gms.s_prod_int AS nrp 
    ON nrp.row_id = asset.x_next_renewal_prod_id 
 
INNER JOIN 
    gms.s_contact AS con 
    ON con.row_id = asset.owner_con_id 
 
 
WHERE  
    asset.status_cd = "Active" 
    AND prod.prod_cd != "Promotion" 
    AND prod.sub_type_cd='RSA' 
    AND asset.x_next_renewal_prod_id IS NOT NULL 
    AND DATE_ADD(asset.end_dt, 1) BETWEEN '2021-10-25' AND '2021-11-25' 
    and con_cd in ('Ordinary Member' , 'Affiliate Member') 
    and nrp.name not in ('TC','F2G','BC','NB') 
 
), addon as ( 
SELECT DISTINCT asset.owner_accnt_id 
        , asset.par_asset_id 
        , prod.name as addon 
 
FROM 
    gms.s_asset AS asset 
     
INNER JOIN 
    gms.s_prod_int AS prod 
    ON prod.row_id = asset.prod_id 
 
LEFT OUTER JOIN 
    gms.s_prod_int AS nrp 
    ON nrp.row_id = asset.x_next_renewal_prod_id 
 
 
WHERE  
    asset.status_cd = "Active" 
    AND prod.prod_cd != "Promotion" 
    AND prod.sub_type_cd='Add-on' 
    AND DATE_ADD(asset.end_dt, 1) BETWEEN '2021-10-25' AND '2021-11-25' 
 
), main as ( 
    select distinct m.owner_accnt_id  as account_id 
            , m.row_id as asset_id 
            , m.lcns_num as rego 
            , name as prod 
            , a.addon 
            , m.next_renewable_prod 
            , case     
                when (m.name rlike 'Plus' and m.next_renewable_prod in ('PP','CO')) and addon='Pet Plus' then '1' 
                when (m.name rlike 'Plus' and m.next_renewable_prod in ('PP','CO')) and addon='Windscreen Plus' then '2' 
                when (m.name rlike 'Plus' and m.next_renewable_prod in ('PP','CO')) then '3' 
                when m.next_renewable_prod in ('PC','CO') and addon='Pet Plus' then '4' 
                when m.next_renewable_prod in ('PC','CO') and addon='Windscreen Plus' then '5' 
                when m.next_renewable_prod in ('PC','CO') then '6' 
                when m.next_renewable_prod in ('EV','CC') then '7' 
                else '99' end Priority  
     
    from rsa m 
 
    left join addon a 
    on a.par_asset_id=m.row_id 
     
   )  
  select * from main 
SELECT max( asset.end_dt) 
         
 
FROM 
    gms.s_asset AS asset 
     
inner join gms.s_Asset v  
on v.row_id=asset.service_point_id 
     
INNER JOIN 
    gms.s_prod_int AS prod 
    ON prod.row_id = asset.prod_id 
 
LEFT OUTER JOIN 
    gms.s_prod_int AS nrp 
    ON nrp.row_id = asset.x_next_renewal_prod_id 
 
where 1=1 
and year(asset.end_dt)=2021 
--='2021-09-26' 
and nrp.name='CO' 
 
SELECT DISTINCT row_id,contact_id, order_num from gms.s_order 
where to_date(created)='2021-08-15' 
SELECT * from campaign_data.aa_dmc2479_brumbytest_dm_20210805 
--create table campaign_data.aa_dmc2420_brumbyprerenewalbatch2_dm_20210816 as  
with rsa as ( 
SELECT DISTINCT asset.owner_accnt_id 
        , asset.row_id 
        , prod.name 
        , nrp.name as next_renewable_prod 
        , v.lcns_num  
 
FROM 
    gms.s_asset AS asset 
     
inner join gms.s_Asset v  
on v.row_id=asset.service_point_id 
     
INNER JOIN 
    gms.s_prod_int AS prod 
    ON prod.row_id = asset.prod_id 
 
LEFT OUTER JOIN 
    gms.s_prod_int AS nrp 
    ON nrp.row_id = asset.x_next_renewal_prod_id 
 
INNER JOIN 
    gms.s_contact AS con 
    ON con.row_id = asset.owner_con_id 
 
 
WHERE  
    asset.status_cd = "Active" 
    AND prod.prod_cd != "Promotion" 
    AND prod.sub_type_cd='RSA' 
    AND asset.x_next_renewal_prod_id IS NOT NULL 
    AND DATE_ADD(asset.end_dt, 1) BETWEEN '2021-11-26' AND '2021-12-31' 
    and con_cd in ('Ordinary Member' , 'Affiliate Member') 
    and nrp.name not in ('TC','F2G','BC','NB') 
 
), addon as ( 
SELECT DISTINCT asset.owner_accnt_id 
        , asset.par_asset_id 
        , prod.name as addon 
 
FROM 
    gms.s_asset AS asset 
     
INNER JOIN 
    gms.s_prod_int AS prod 
    ON prod.row_id = asset.prod_id 
 
LEFT OUTER JOIN 
    gms.s_prod_int AS nrp 
    ON nrp.row_id = asset.x_next_renewal_prod_id 
 
 
WHERE  
    asset.status_cd = "Active" 
    AND prod.prod_cd != "Promotion" 
    AND prod.sub_type_cd='Add-on' 
    AND DATE_ADD(asset.end_dt, 1) BETWEEN '2021-11-26' AND '2021-12-31' 
 
), main as ( 
    select distinct m.owner_accnt_id  as account_id 
            , m.row_id as asset_id 
            , m.lcns_num as rego 
            , name as prod 
            , a.addon 
            , m.next_renewable_prod 
            , case     
                when (m.name rlike 'Plus' and m.next_renewable_prod in ('PP','CO')) and addon='Pet Plus' then '1' 
                when (m.name rlike 'Plus' and m.next_renewable_prod in ('PP','CO')) and addon='Windscreen Plus' then '2' 
                when (m.name rlike 'Plus' and m.next_renewable_prod in ('PP','CO')) then '3' 
                when m.next_renewable_prod in ('PC','CO') and addon='Pet Plus' then '4' 
                when m.next_renewable_prod in ('PC','CO') and addon='Windscreen Plus' then '5' 
                when m.next_renewable_prod in ('PC','CO') then '6' 
                when m.next_renewable_prod in ('EV','EY','CC') then '7' 
                else '99' end Priority  
     
    from rsa m 
 
    left join addon a 
    on a.par_asset_id=m.row_id 
     
   )  
  , main2 as ( 
  select  account_id 
        , Priority 
        , rego 
        , rank() over(partition by account_id order by Priority asc) as rnk 
  from main 
  where Priority!='99' 
  ), final as ( 
  select account_id 
            , max(Priority) as letter_version  
            ,max(rego) as vehicle_rego  
  from main2 
  where rnk=1 
  group by 1 
  ) 
  SELECT distinct c.csn as membernumber 
        , c.cust_value_cd as memberType 
        , vehicle_rego 
        , c.x_nrma_title 
        , c.fst_name as first_name 
        --, c.mid_name as middle_name 
        , c.last_name as last_name 
        --, ad.addr_name as Full_address 
        , ad.addr as ad_line_1 
        , ad.addr_line_2 as ad_line_2 
        , ad.city as suburb 
        , ad.state 
        , ad.zipcode as post_code 
        , letter_version 
 
FROM  gms.s_contact c 
 
INNER JOIN gms.s_contact_x cx 
on cx.par_row_id=c.row_id 
 
INNER JOIN gms.s_contact_fnx fn 
on fn.par_row_id=c.row_id 
 
inner join gms.s_addr_per as ad         --813620 
on c.pr_per_addr_id = ad.row_id 
 
inner join gms.s_org_ext as org             --3.2m 
on org.pr_con_id = c.row_id 
 
inner join final f 
on f.account_id=org.row_id 
 
WHERE 1=1 
     
AND (COALESCE(c.x_inv_email_1, 'N') = 'Y' or c.email_addr IS  NULL)     --non-email able 
and c.cust_stat_cd = 'Active'       
and c.con_cd in ('Ordinary Member' , 'Affiliate Member')                 
and NVL(c.x_nrma_title,'no title') != 'Estate Of The Late'              --deceased check 
and NVL(fn.deceased_flg,'N') = 'N'                               
and c.csn is not null                                            
and case when trim(ad.premise_flg) = 'Y' or ad.addr_name is null then 'N' else 'Y' end = 'Y' --no return address 
--and nvl(cx.attrib_35,'Yes') <> 'No' --post permission           
and (UPPER(trim(NVL(ad.addr,'NULL')))<>'NULL' or UPPER(trim(NVL(ad.addr_line_2,'NULL')))<>'NULL')       --valid address 
 
   
SELECT min(DATE_ADD(asset.end_dt, 1)),max(DATE_ADD(asset.end_dt, 1)) 
 
FROM 
    gms.s_asset AS asset 
     
inner join gms.s_Asset v  
on v.row_id=asset.service_point_id 
     
INNER JOIN 
    gms.s_prod_int AS prod 
    ON prod.row_id = asset.prod_id 
 
LEFT OUTER JOIN 
    gms.s_prod_int AS nrp 
    ON nrp.row_id = asset.x_next_renewal_prod_id 
 
INNER JOIN 
    gms.s_contact AS con 
    ON con.row_id = asset.owner_con_id 
 
 
WHERE  
    asset.status_cd = "Active" 
    AND prod.prod_cd != "Promotion" 
    AND prod.sub_type_cd='RSA' 
    AND asset.x_next_renewal_prod_id IS NOT NULL 
    AND DATE_ADD(asset.end_dt, 1) BETWEEN '2021-11-26' AND '2021-12-31' 
    and con_cd in ('Ordinary Member' , 'Affiliate Member') 
    and nrp.name not in ('TC','F2G','BC','NB') 
SELECT * from campaign_data.aa_dmc2479_brumbytest_dm_20210805 
where membernumber='775480701' 
--create table campaign_data.aa_dmc2420_brumbyprerenewal2_dm_20210830 as  
with rsa as ( 
SELECT DISTINCT asset.owner_accnt_id 
        , asset.row_id 
        , prod.name 
        , nrp.name as next_renewable_prod 
        , v.lcns_num  
 
FROM 
    gms.s_asset AS asset 
     
inner join gms.s_Asset v  
on v.row_id=asset.service_point_id 
     
INNER JOIN 
    gms.s_prod_int AS prod 
    ON prod.row_id = asset.prod_id 
 
LEFT OUTER JOIN 
    gms.s_prod_int AS nrp 
    ON nrp.row_id = asset.x_next_renewal_prod_id 
 
INNER JOIN 
    gms.s_contact AS con 
    ON con.row_id = asset.owner_con_id 
 
 
WHERE  
    asset.status_cd = "Active" 
    AND prod.prod_cd != "Promotion" 
    AND prod.sub_type_cd='RSA' 
    AND asset.x_next_renewal_prod_id IS NOT NULL 
    AND DATE_ADD(asset.end_dt, 1) BETWEEN '2021-11-26' AND '2021-12-31' 
    and con_cd in ('Ordinary Member' , 'Affiliate Member') 
    and nrp.name not in ('TC','F2G','BC','NB') 
 
), addon as ( 
SELECT DISTINCT asset.owner_accnt_id 
        , asset.par_asset_id 
        , prod.name as addon 
 
FROM 
    gms.s_asset AS asset 
     
INNER JOIN 
    gms.s_prod_int AS prod 
    ON prod.row_id = asset.prod_id 
 
LEFT OUTER JOIN 
    gms.s_prod_int AS nrp 
    ON nrp.row_id = asset.x_next_renewal_prod_id 
 
 
WHERE  
    asset.status_cd = "Active" 
    AND prod.prod_cd != "Promotion" 
    AND prod.sub_type_cd='Add-on' 
    AND DATE_ADD(asset.end_dt, 1) BETWEEN '2021-11-26' AND '2021-12-31' 
 
), main as ( 
    select distinct m.owner_accnt_id  as account_id 
            , m.row_id as asset_id 
            , m.lcns_num as rego 
            , name as prod 
            , a.addon 
            , m.next_renewable_prod 
            , case     
                when (m.name rlike 'Plus' and m.next_renewable_prod in ('PP','CO')) and addon='Pet Plus' then '1' 
                when (m.name rlike 'Plus' and m.next_renewable_prod in ('PP','CO')) and addon='Windscreen Plus' then '2' 
                when (m.name rlike 'Plus' and m.next_renewable_prod in ('PP','CO')) then '3' 
                when m.next_renewable_prod in ('PC','CO') and addon='Pet Plus' then '4' 
                when m.next_renewable_prod in ('PC','CO') and addon='Windscreen Plus' then '5' 
                when m.next_renewable_prod in ('PC','CO') then '6' 
                when m.next_renewable_prod in ('EV','EY','CC') then '7' 
                else '99' end Priority  
     
    from rsa m 
 
    left join addon a 
    on a.par_asset_id=m.row_id 
     
   )  
  , main2 as ( 
  select  account_id 
        , Priority 
        , rego 
        , rank() over(partition by account_id order by Priority asc) as rnk 
  from main 
  where Priority!='99' 
  ), final as ( 
  select account_id 
            , max(Priority) as letter_version  
            ,max(rego) as vehicle_rego  
  from main2 
  where rnk=1 
  group by 1 
  ) 
  SELECT distinct c.csn as membernumber 
        , c.cust_value_cd as memberType 
        , vehicle_rego 
        , c.x_nrma_title 
        , c.fst_name as first_name 
        --, c.mid_name as middle_name 
        , c.last_name as last_name 
        --, ad.addr_name as Full_address 
        , ad.addr as ad_line_1 
        , ad.addr_line_2 as ad_line_2 
        , ad.city as suburb 
        , ad.state 
        , ad.zipcode as post_code 
        , letter_version 
 
FROM  gms.s_contact c 
 
INNER JOIN gms.s_contact_x cx 
on cx.par_row_id=c.row_id 
 
INNER JOIN gms.s_contact_fnx fn 
on fn.par_row_id=c.row_id 
 
inner join gms.s_addr_per as ad         --813620 
on c.pr_per_addr_id = ad.row_id 
 
inner join gms.s_org_ext as org             --3.2m 
on org.pr_con_id = c.row_id 
 
inner join final f 
on f.account_id=org.row_id 
 
WHERE 1=1 
     
AND (COALESCE(c.x_inv_email_1, 'N') = 'Y' or c.email_addr IS  NULL)     --non-email able 
and c.cust_stat_cd = 'Active'       
and c.con_cd in ('Ordinary Member' , 'Affiliate Member')                 
and NVL(c.x_nrma_title,'no title') != 'Estate Of The Late'              --deceased check 
and NVL(fn.deceased_flg,'N') = 'N'                               
and c.csn is not null                                            
and case when trim(ad.premise_flg) = 'Y' or ad.addr_name is null then 'N' else 'Y' end = 'Y' --no return address 
--and nvl(cx.attrib_35,'Yes') <> 'No' --post permission           
and (UPPER(trim(NVL(ad.addr,'NULL')))<>'NULL' or UPPER(trim(NVL(ad.addr_line_2,'NULL')))<>'NULL')       --valid address 
 
   
SELECT * from campaign_data.aa_dmc2420_brumbyprerenewal2_dm_20210830 
create table campaign_data.aa_dmc2556_brumbyprerenewal3_dm_20211013 as  
with rsa as ( 
SELECT DISTINCT asset.owner_accnt_id 
        , asset.row_id 
        , prod.name 
        , nrp.name as next_renewable_prod 
        , v.lcns_num  
 
FROM 
    gms.s_asset AS asset 
     
inner join gms.s_Asset v  
on v.row_id=asset.service_point_id 
     
INNER JOIN 
    gms.s_prod_int AS prod 
    ON prod.row_id = asset.prod_id 
 
LEFT OUTER JOIN 
    gms.s_prod_int AS nrp 
    ON nrp.row_id = asset.x_next_renewal_prod_id 
 
INNER JOIN 
    gms.s_contact AS con 
    ON con.row_id = asset.owner_con_id 
 
 
WHERE  
    asset.status_cd = "Active" 
    AND prod.prod_cd != "Promotion" 
    AND prod.sub_type_cd='RSA' 
    AND asset.x_next_renewal_prod_id IS NOT NULL 
    AND DATE_ADD(from_utc_timestamp(asset.end_dt,'AEST'), 1) BETWEEN '2022-01-01' AND '2022-03-31' 
    and con_cd in ('Ordinary Member' , 'Affiliate Member') 
    and nrp.name not in ('TC','F2G','BC','NB') 
 
), addon as ( 
SELECT DISTINCT asset.owner_accnt_id 
        , asset.par_asset_id 
        , prod.name as addon 
 
FROM 
    gms.s_asset AS asset 
     
INNER JOIN 
    gms.s_prod_int AS prod 
    ON prod.row_id = asset.prod_id 
 
LEFT OUTER JOIN 
    gms.s_prod_int AS nrp 
    ON nrp.row_id = asset.x_next_renewal_prod_id 
 
 
WHERE  
    asset.status_cd = "Active" 
    AND prod.prod_cd != "Promotion" 
    AND prod.sub_type_cd='Add-on' 
    AND DATE_ADD(from_utc_timestamp(asset.end_dt,'AEST'), 1) BETWEEN '2022-01-01' AND '2022-03-31' 
 
), main as ( 
    select distinct m.owner_accnt_id  as account_id 
            , m.row_id as asset_id 
            , m.lcns_num as rego 
            , name as prod 
            , a.addon 
            , m.next_renewable_prod 
            , case     
                when (m.name rlike 'Plus' and m.next_renewable_prod in ('PP','CO')) and addon='Pet Plus' then '1' 
                when (m.name rlike 'Plus' and m.next_renewable_prod in ('PP','CO')) and addon='Windscreen Plus' then '2' 
                when (m.name rlike 'Plus' and m.next_renewable_prod in ('PP','CO')) then '3' 
                when m.next_renewable_prod in ('PC','CO') and addon='Pet Plus' then '4' 
                when m.next_renewable_prod in ('PC','CO') and addon='Windscreen Plus' then '5' 
                when m.next_renewable_prod in ('PC','CO') then '6' 
                when m.next_renewable_prod in ('EV','EY','CC') then '7' 
                else '99' end Priority  
     
    from rsa m 
 
    left join addon a 
    on a.par_asset_id=m.row_id 
     
   )  
  , main2 as ( 
  select  account_id 
        , Priority 
        , rego 
        , rank() over(partition by account_id order by Priority asc) as rnk 
  from main 
  where Priority!='99' 
  ), final as ( 
  select account_id 
            , max(Priority) as letter_version  
            ,max(rego) as vehicle_rego  
  from main2 
  where rnk=1 
  group by 1 
  ) 
  SELECT distinct c.csn as membernumber 
        , c.cust_value_cd as memberType 
        , vehicle_rego 
        , c.x_nrma_title 
        , c.fst_name as first_name 
        --, c.mid_name as middle_name 
        , c.last_name as last_name 
        --, ad.addr_name as Full_address 
        , ad.addr as ad_line_1 
        , ad.addr_line_2 as ad_line_2 
        , ad.city as suburb 
        , ad.state 
        , ad.zipcode as post_code 
        , letter_version 
 
FROM  gms.s_contact c 
 
INNER JOIN gms.s_contact_x cx 
on cx.par_row_id=c.row_id 
 
INNER JOIN gms.s_contact_fnx fn 
on fn.par_row_id=c.row_id 
 
inner join gms.s_addr_per as ad         --813620 
on c.pr_per_addr_id = ad.row_id 
 
inner join gms.s_org_ext as org             --3.2m 
on org.pr_con_id = c.row_id 
 
inner join final f 
on f.account_id=org.row_id 
 
WHERE 1=1 
     
AND (COALESCE(c.x_inv_email_1, 'N') = 'Y' or c.email_addr IS  NULL)     --non-email able 
and c.cust_stat_cd = 'Active'       
and c.con_cd in ('Ordinary Member' , 'Affiliate Member')                 
and NVL(c.x_nrma_title,'no title') != 'Estate Of The Late'              --deceased check 
and NVL(fn.deceased_flg,'N') = 'N'                               
and c.csn is not null                                            
and case when trim(ad.premise_flg) = 'Y' or ad.addr_name is null then 'N' else 'Y' end = 'Y' --no return address 
--and nvl(cx.attrib_35,'Yes') <> 'No' --post permission           
and (UPPER(trim(NVL(ad.addr,'NULL')))<>'NULL' or UPPER(trim(NVL(ad.addr_line_2,'NULL')))<>'NULL')       --valid address 
 
   
SELECT DISTINCT asset.owner_accnt_id 
        , asset.row_id 
        , prod.name 
        , nrp.name as next_renewable_prod 
        , v.lcns_num  
        , from_utc_timestamp(asset.end_dt,'AEST') 
        , from_utc_timestamp(asset.end_dt,'AEDT') 
        , asset.end_dt 
 
FROM 
    gms.s_asset AS asset 
     
inner join gms.s_Asset v  
on v.row_id=asset.service_point_id 
     
INNER JOIN 
    gms.s_prod_int AS prod 
    ON prod.row_id = asset.prod_id 
 
LEFT OUTER JOIN 
    gms.s_prod_int AS nrp 
    ON nrp.row_id = asset.x_next_renewal_prod_id 
 
INNER JOIN 
    gms.s_contact AS con 
    ON con.row_id = asset.owner_con_id 
 
 
WHERE  
    asset.status_cd = "Active" 
    AND prod.prod_cd != "Promotion" 
    AND prod.sub_type_cd='RSA' 
    AND asset.x_next_renewal_prod_id IS NOT NULL 
    AND DATE_ADD(from_utc_timestamp(asset.end_dt,'AEST'), 1) BETWEEN '2022-01-01' AND '2022-03-31' 
    and con_cd in ('Ordinary Member' , 'Affiliate Member') 
    and nrp.name not in ('TC','F2G','BC','NB') 
    and con.csn='315723101' 
SELECT DISTINCT now(),from_utc_timestamp(NOW(), 'AEST'), to_utc_timestamp(NOW(), 'AEST') from gms.s_contact 
--SELECT count(*) from campaign_data.aa_dmc2556_brumbyprerenewal3_counts_20211005     --166683 
SELECT count(*) from campaign_data.aa_dmc2556_brumbyprerenewal3_dm_20211005v2       --85397 
SELECT letter_version, count(*) from campaign_data.aa_dmc2556_brumbyprerenewal3_counts_20211006 
GROUP BY 1 
order by 1 
drop table campaign_data.aa_dmc2556_brumbyprerenewal3_dm_20211005 
create table campaign_data.aa_dmc2556_brumbyprerenewal3_counts_20211013 as  
with rsa as ( 
SELECT DISTINCT asset.owner_accnt_id 
        , asset.row_id 
        , prod.name 
        , nrp.name as next_renewable_prod 
        , v.lcns_num  
 
FROM 
    gms.s_asset AS asset 
     
inner join gms.s_Asset v  
on v.row_id=asset.service_point_id 
     
INNER JOIN 
    gms.s_prod_int AS prod 
    ON prod.row_id = asset.prod_id 
 
LEFT OUTER JOIN 
    gms.s_prod_int AS nrp 
    ON nrp.row_id = asset.x_next_renewal_prod_id 
 
INNER JOIN 
    gms.s_contact AS con 
    ON con.row_id = asset.owner_con_id 
 
 
WHERE  
    asset.status_cd = "Active" 
    AND prod.prod_cd != "Promotion" 
    AND prod.sub_type_cd='RSA' 
    AND asset.x_next_renewal_prod_id IS NOT NULL 
    AND DATE_ADD(from_utc_timestamp(asset.end_dt,'AEST'), 1) BETWEEN '2022-01-01' AND '2022-03-31' 
    and con_cd in ('Ordinary Member' , 'Affiliate Member') 
    and nrp.name not in ('TC','F2G','BC','NB') 
 
), addon as ( 
SELECT DISTINCT asset.owner_accnt_id 
        , asset.par_asset_id 
        , prod.name as addon 
 
FROM 
    gms.s_asset AS asset 
     
INNER JOIN 
    gms.s_prod_int AS prod 
    ON prod.row_id = asset.prod_id 
 
LEFT OUTER JOIN 
    gms.s_prod_int AS nrp 
    ON nrp.row_id = asset.x_next_renewal_prod_id 
 
 
WHERE  
    asset.status_cd = "Active" 
    AND prod.prod_cd != "Promotion" 
    AND prod.sub_type_cd='Add-on' 
    AND DATE_ADD(from_utc_timestamp(asset.end_dt,'AEST'), 1) BETWEEN '2022-01-01' AND '2022-03-31' 
 
), main as ( 
    select distinct m.owner_accnt_id  as account_id 
            , m.row_id as asset_id 
            , m.lcns_num as rego 
            , name as prod 
            , a.addon 
            , m.next_renewable_prod 
            , case     
                when (m.name rlike 'Plus' and m.next_renewable_prod in ('PP','CO')) and addon='Pet Plus' then '1' 
                when (m.name rlike 'Plus' and m.next_renewable_prod in ('PP','CO')) and addon='Windscreen Plus' then '2' 
                when (m.name rlike 'Plus' and m.next_renewable_prod in ('PP','CO')) then '3' 
                when m.next_renewable_prod in ('PC','CO') and addon='Pet Plus' then '4' 
                when m.next_renewable_prod in ('PC','CO') and addon='Windscreen Plus' then '5' 
                when m.next_renewable_prod in ('PC','CO') then '6' 
                when m.next_renewable_prod in ('EV','EY','CC') then '7' 
                else '99' end Priority  
     
    from rsa m 
 
    left join addon a 
    on a.par_asset_id=m.row_id 
     
   )  
  , main2 as ( 
  select  account_id 
        , Priority 
        , rego 
        , rank() over(partition by account_id order by Priority asc) as rnk 
  from main 
  where Priority!='99' 
  ), final as ( 
  select account_id 
            , max(Priority) as letter_version  
            ,max(rego) as vehicle_rego  
  from main2 
  where rnk=1 
  group by 1 
  ) 
  SELECT distinct c.csn as membernumber 
        , c.cust_value_cd as memberType 
        , vehicle_rego 
        , c.x_nrma_title 
        , c.fst_name as first_name 
        --, c.mid_name as middle_name 
        , c.last_name as last_name 
        --, ad.addr_name as Full_address 
        , ad.addr as ad_line_1 
        , ad.addr_line_2 as ad_line_2 
        , ad.city as suburb 
        , ad.state 
        , ad.zipcode as post_code 
        , letter_version 
 
FROM  gms.s_contact c 
 
INNER JOIN gms.s_contact_x cx 
on cx.par_row_id=c.row_id 
 
INNER JOIN gms.s_contact_fnx fn 
on fn.par_row_id=c.row_id 
 
inner join gms.s_addr_per as ad         --813620 
on c.pr_per_addr_id = ad.row_id 
 
inner join gms.s_org_ext as org             --3.2m 
on org.pr_con_id = c.row_id 
 
inner join final f 
on f.account_id=org.row_id 
 
WHERE 1=1 
     
AND cx.attrib_46='Post'                                             --renewal preference 
--(COALESCE(c.x_inv_email_1, 'N') = 'Y' or c.email_addr IS  NULL)     --non-email able 
and c.cust_stat_cd = 'Active'       
and c.con_cd in ('Ordinary Member' , 'Affiliate Member')                 
and NVL(c.x_nrma_title,'no title') != 'Estate Of The Late'              --deceased check 
and NVL(fn.deceased_flg,'N') = 'N'                               
and c.csn is not null                                            
and case when trim(ad.premise_flg) = 'Y' or ad.addr_name is null then 'N' else 'Y' end = 'Y' --no return address 
--and nvl(cx.attrib_35,'Yes') <> 'No' --post permission           
and (UPPER(trim(NVL(ad.addr,'NULL')))<>'NULL' or UPPER(trim(NVL(ad.addr_line_2,'NULL')))<>'NULL')       --valid address 
 
   
--segment 2 check renewal preference to DM 
--can have valid email 
 
SELECT * from campaign_data.aa_dmc2556_brumbyprerenewal3_counts_20211006 
SELECT * from campaign_data.aa_dmc2556_brumbyprerenewal3_dm_20211013