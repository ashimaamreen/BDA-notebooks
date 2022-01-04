CBA_Accounts = spark.read.load("/user/aamreen/DMC-2252_CBA_Accounts.csv", format="csv",sep=",",inferSchema="true", header="true") 
CBA_Accounts.createOrReplaceTempView("CBA_Accounts") 
spark.sql(""" select * from CBA_Accounts """).show(20360,False) 
spark.sql(""" select * from CBA_Accounts """).count() 
spark.sql(""" 
create table campaign_data.aa_dmc2252_CBAaccounts_20201119 as 
select * from CBA_Accounts  
""").count() 
spark.sql(""" select * from CBA_Accounts """).show(10) 
#spark.sql(""" select * from CBA_Accounts """).count() 
select distinct a.receipt_method from campaign_data.aa_dmc2252_CBAaccounts_20201119 a 
-- inner join gms.s_contact c 
-- on c.csn=cast(a.member_number as string) 
select count(*), count(distinct cba.member_number) from campaign_data.aa_dmc2252_CBAaccounts_20201119 cba           --20358	16837 
 
left JOIN gms.s_org_ext o 
on cast(cba.MEMBER_NUMBER as string) = o.ou_num 
AND accnt_type_cd = 'Customer' 
 
left join gms.s_org_ext_x ox 
on ox.par_row_id=o.row_id 
 
LEFT JOIN gms.s_contact AS c 
ON c.row_id = o.pr_con_id 
 
LEFT JOIN  gms.s_addr_per as ad         --813620 
on c.pr_per_addr_id = ad.row_id 
 
left join gms.s_asset a 
on a.owner_accnt_id=o.par_row_id 
and a.status_cd='Active' 
WITH 
    price AS ( 
        SELECT 
            ordi.asset_integ_id, 
            ordi.net_pri/(YEAR(ordiom.service_end_dt) - YEAR(ord.order_dt)) AS net_pri, 
            ordt.name, 
            ordi.action_cd, 
            RANK() OVER (PARTITION BY ordi.asset_integ_id ORDER BY ord.order_dt DESC) AS idx 
          
        FROM 
            gms.s_order AS ord 
             
        INNER JOIN 
            gms.s_order_type AS ordt 
            ON ordt.row_id = ord.order_type_id 
             
        INNER JOIN 
            gms.s_order_item AS ordi 
            ON ordi.order_id = ord.row_id 
             
        LEFT JOIN 
            gms.s_order_item_om AS ordiom 
            ON ordiom.par_row_id = ordi.row_id 
         
        WHERE 
            ordi.asset_integ_id IS NOT NULL 
            AND ord.status_cd = 'Complete' 
    ) 
 
Select DISTINCT cba.customer_name 
        , cba.customer_number 
        , cba.member_number 
        , case when cba.masked_account_num like '%4434' then 'Commbank'  
                        When cba.receipt_method like 'BPAY%' or cba.receipt_method like 'NMS03%' then 'ANZ_Temp' else 'ANZ' end Type 
        , COALESCE(market_class_cd, '') as market_class 
        --, cba.member_number 
        , o.x_nrma_asset_count as fleet_size 
        , cba.member_number  
        , ox.attrib_25 as tenure 
        , c.row_id as primary_contact 
        , c.fst_name 
        , case when c.x_inv_email_1 = 'Y' or c.email_addr is null then 'N' else 'Y' end Emailable 
        , case when trim(c.VETERAN_FLG) = 'Y' or c.CELL_PH_NUM is null then 'N' else 'Y' end SMS 
        , case when trim(ad.premise_flg) = 'Y' or ad.addr_name is null then 'N' else 'Y' end Post 
        , NVL(a.x_nrma_pay_by_term,'N') as MPP_flag 
        --, py.pay_type_cd 
        , CASE WHEN (a.bill_profile_id IS NOT NULL AND a.x_nrma_pay_by_term != 'Y') THEN 'Y' ELSE 'N' END AS Asset_Direct_Debit 
        , net_pri as annual_revenue 
         
from campaign_data.aa_dmc2252_CBAaccounts_20201119 cba 
 
left JOIN gms.s_org_ext o 
on cast(cba.MEMBER_NUMBER as string) = o.ou_num 
AND accnt_type_cd = 'Customer' 
 
left join gms.s_org_ext_x ox 
on ox.par_row_id=o.row_id 
 
LEFT JOIN gms.s_contact AS c 
ON c.row_id = o.pr_con_id 
 
LEFT JOIN  gms.s_addr_per as ad         --813620 
on c.pr_per_addr_id = ad.row_id 
 
left join gms.s_asset a 
on a.owner_accnt_id=o.par_row_id 
 
left join (SELECT 
    asset_integ_id, 
    MAX(net_pri) AS net_pri 
FROM 
    price 
WHERE 
    idx = 1 
    AND name != 'Cancellation' 
    AND action_cd IN ('Add', 'Update') 
     
GROUP BY 
    1) oi 
on oi.asset_integ_id=a.integration_id 
 
where cba.member_number=2008785 
select * from campaign_data.aa_dmc2252_CBAaccounts_20201119 cba 
--with abc as( 
select case when cba.masked_account_num like '%0488' and (cba.receipt_method like 'ANZ-EFT%' or cba.receipt_method in ('ANZ-DDAR-837850488','Cash - NRMA Motoring Bank-ANZ')) then 'ANZ' 
            when cba.receipt_method like 'BPAY%' or cba.receipt_method like 'NMS%' then 'ANZ_Temp' 
            when cba.masked_account_num like '%4434' then 'Commbank'  
            else 'other' end type 
        ,count(distinct cba.member_number) 
 
from campaign_data.aa_dmc2252_CBAaccounts_20201119 cba 
group by 1 
with business_accounts as ( 
select distinct customer_name 
        , customer_number 
        , member_number 
        , receipt_method 
        , masked_account_num 
from campaign_data.aa_dmc2252_CBAaccounts_20201119 
--where member_number=2008785 
)  
Select cba.customer_name 
        , cba.customer_number 
        , cba.member_number 
        , case when cba.masked_account_num like '%4434' then 'Commbank'  
                        When cba.receipt_method like 'BPAY%' or cba.receipt_method like 'NMS03%' then 'ANZ_Temp' else 'ANZ' end Type 
        , COALESCE(market_class_cd, '') as market_class 
        , o.x_nrma_asset_count as fleet_size 
        , ox.attrib_25 as tenure 
        , c.row_id as primary_contact 
        , c.fst_name 
        , case when c.x_inv_email_1 = 'Y' or c.email_addr is null then 'N' else 'Y' end Emailable_ABR 
        , case when trim(c.VETERAN_FLG) = 'Y' or c.CELL_PH_NUM is null then 'N' else 'Y' end SMS_ABR 
        , case when trim(ad.premise_flg) = 'Y' or ad.addr_name is null then 'N' else 'Y' end Post_ABR 
        --, --colin to send 
        , NVL(a.x_nrma_pay_by_term,'N') as MPP_flag 
        --, py.pay_type_cd 
        , CASE WHEN (a.bill_profile_id IS NOT NULL AND a.x_nrma_pay_by_term != 'Y') THEN 'Y' ELSE 'N' END AS Direct_Debit 
        --, sum(oi.net_pri) as annual_revenue 
        , count(*) 
 
from business_accounts cba 
 
left JOIN gms.s_org_ext o 
on cast(cba.MEMBER_NUMBER as string) = o.ou_num 
AND accnt_type_cd = 'Customer' 
 
left join gms.s_org_ext_x ox 
on ox.par_row_id=o.row_id 
 
LEFT JOIN gms.s_contact AS c 
ON c.row_id = o.pr_con_id 
 
LEFT JOIN  gms.s_addr_per as ad         --813620 
on c.pr_per_addr_id = ad.row_id 
 
left join gms.s_asset a 
on a.owner_accnt_id=o.par_row_id 
and a.status_cd='Active' 
 
-- LEFT JOIN gms.s_order_item oi 
-- on oi.asset_id=a.row_id 
 
left join gms.s_prod_int p 
on p.row_id=a.prod_id 
 
 
where 1=1 
--cba.member_number=2008785 
and p.sub_type_cd in ('Non-RSA','RSA') 
and p.prod_cd='Product' 
 
group by 1,2,3,4,5,6,7,8,9,10,11,12,13,14 
spark.sql(''' 
with business_accounts as ( 
select distinct customer_name 
        , customer_number 
        , member_number 
        , receipt_method 
        , masked_account_num 
from campaign_data.aa_dmc2252_CBAaccounts_20201119 
--where member_number=2008785 
), price as (SELECT 
            ordi.asset_integ_id, 
            CASE 
                WHEN (YEAR(ordiom.service_end_dt) - YEAR(ord.order_dt)) < 1 THEN ordi.net_pri 
                ELSE ordi.net_pri/(YEAR(ordiom.service_end_dt) - YEAR(ord.order_dt)) 
            END AS net_pri, 
            ordt.name, 
            ordi.action_cd, 
            RANK() OVER (PARTITION BY ordi.asset_integ_id ORDER BY ord.order_dt DESC) AS idx 
          
        FROM 
            gms.s_order AS ord 
             
        INNER JOIN 
            gms.s_order_type AS ordt 
            ON ordt.row_id = ord.order_type_id 
             
        INNER JOIN 
            gms.s_order_item AS ordi 
            ON ordi.order_id = ord.row_id 
             
        LEFT JOIN 
            gms.s_order_item_om AS ordiom 
            ON ordiom.par_row_id = ordi.row_id 
         
        WHERE 
            ordi.asset_integ_id IS NOT NULL 
            AND ord.status_cd = 'Complete' 
    ) 
 
 
Select distinct 
        --cba.customer_name 
        -- cba.customer_number 
         cba.member_number 
        , case when cba.masked_account_num like '%0488' and (cba.receipt_method like 'ANZ-EFT%' or cba.receipt_method in ('ANZ-DDAR-837850488','Cash - NRMA Motoring Bank-ANZ')) then 'ANZ' 
            when cba.receipt_method like 'BPAY%' or cba.receipt_method like 'NMS%' then 'ANZ_Temp' 
            when cba.masked_account_num like '%4434' then 'Commbank'  
            else 'other' end type 
        , COALESCE(market_class_cd, '') as market_class 
        , o.x_nrma_asset_count as fleet_size 
        , ox.attrib_25 as tenure 
        --, c.row_id as primary_contact 
        --, c.fst_name 
        , case when c.x_inv_email_1 = 'Y' or c.email_addr is null then 'N' else 'Y' end Emailable_ABR 
        , case when trim(c.VETERAN_FLG) = 'Y' or c.CELL_PH_NUM is null then 'N' else 'Y' end SMS_ABR 
        , case when trim(ad.premise_flg) = 'Y' or ad.addr_name is null then 'N' else 'Y' end Post_ABR 
        , NVL(a.x_nrma_pay_by_term,'N') as Monhtly 
        --, py.pay_type_cd 
        , CASE WHEN (a.bill_profile_id IS NOT NULL AND a.x_nrma_pay_by_term != 'Y') THEN 'Y' ELSE 'N' END AS Direct_Debit 
        , sum(oi.net_pri) as annual_revenue 
        , count(*) 
 
from business_accounts cba 
 
left JOIN gms.s_org_ext o 
on cast(cba.MEMBER_NUMBER as string) = o.ou_num 
AND accnt_type_cd = 'Customer' 
 
left join gms.s_org_ext_x ox 
on ox.par_row_id=o.row_id 
 
LEFT JOIN gms.s_contact AS c 
ON c.row_id = o.pr_con_id 
 
LEFT JOIN  gms.s_addr_per as ad         --813620 
on c.pr_per_addr_id = ad.row_id 
 
left join gms.s_asset a 
on a.owner_accnt_id=o.par_row_id 
and a.status_cd='Active' 
 
LEFT JOIN (SELECT 
    asset_integ_id, 
    MAX(net_pri) AS net_pri 
FROM 
    price 
WHERE 
    idx = 1 
    AND name != 'Cancellation' 
    AND action_cd IN ('Add', 'Update') 
     
GROUP BY 
    1) oi 
on oi.asset_integ_id=a.integration_id 
 
left join gms.s_prod_int p 
on p.row_id=a.prod_id 
 
 
where 1=1 
--cba.member_number=2008785 
and p.sub_type_cd in ('Non-RSA','RSA') 
and p.prod_cd='Product' 
 
group by 1,2,3,4,5,6,7,8,9,10 
''').show(100000,False) 
select distinct customer_name 
        , customer_number 
        , member_number 
        , receipt_method 
        , masked_account_num 
        , receipt_date 
        , to_date(cast(from_unixtime(unix_timestamp(receipt_date,"dd-MMM-yy")) as timestamp)) as receipt_dt 
        , case when masked_account_num like '%4434' then 'Commbank' 
                        When receipt_method like 'BPAY%' or receipt_method like 'NMS03%' then 'ANZ_Temp' else 'ANZ' end Type 
from campaign_data.aa_dmc2252_CBAaccounts_20201119 
where member_number=33030 
--and type like '%ANZ%' 
with all_ANZ as ( 
select distinct customer_name 
        , customer_number 
        , member_number 
        , receipt_method 
        , masked_account_num 
        , receipt_date 
        , to_date(cast(from_unixtime(unix_timestamp(receipt_date,"dd-MMM-yy")) as timestamp)) as receipt_dt 
        --, case when masked_account_num like '%4434' then 'Commbank' 
        --                When receipt_method like 'BPAY%' or receipt_method like 'NMS03%' then 'ANZ_Temp' else 'ANZ' end Type 
from campaign_data.aa_dmc2252_CBAaccounts_20201119 
where masked_account_num not like '%4434' 
), all_CBA as ( 
select distinct customer_name 
        , customer_number 
        , member_number 
        , receipt_method 
        , masked_account_num 
        , receipt_date 
        , to_date(cast(from_unixtime(unix_timestamp(receipt_date,"dd-MMM-yy")) as timestamp)) as receipt_dt 
        --, case when masked_account_num like '%4434' then 'Commbank' 
        --                When receipt_method like 'BPAY%' or receipt_method like 'NMS03%' then 'ANZ_Temp' else 'ANZ' end Type 
from campaign_data.aa_dmc2252_CBAaccounts_20201119 
where masked_account_num like '%4434' 
) 
select count(distinct a.member_number) from all_ANZ a 
inner join all_CBA c 
on c.member_number=a.member_number 
where a.receipt_dt<c.receipt_dt 
spark.sql(''' 
with business_accounts as ( 
select distinct customer_name 
        , customer_number 
        , member_number 
        , receipt_method 
        , masked_account_num 
from campaign_data.aa_dmc2252_CBAaccounts_20201119 
--where member_number=2008785 
) 
Select distinct 
         cba.member_number 
        , case when cba.masked_account_num like '%4434' then 'Commbank' 
                        When cba.receipt_method like 'BPAY%' or cba.receipt_method like 'NMS03%' then 'ANZ_Temp' else 'ANZ' end Type 
        , CASE  
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
         
from business_accounts cba 
 
left JOIN gms.s_org_ext o 
on cast(cba.MEMBER_NUMBER as string) = o.ou_num 
AND accnt_type_cd = 'Customer' 
 
left join gms.s_org_ext_x ox 
on ox.par_row_id=o.row_id 
 
LEFT JOIN gms.s_contact AS c 
ON c.row_id = o.pr_con_id 
 
LEFT JOIN  gms.s_addr_per as ad         --813620 
on c.pr_per_addr_id = ad.row_id 
 
left join gms.s_asset a 
on a.owner_accnt_id=o.par_row_id 
and a.status_cd='Active' 
 
left join gms.s_prod_int p 
on p.row_id=a.prod_id 
 
 
where 1=1 
--cba.member_number=2008785 
and p.sub_type_cd in ('Non-RSA','RSA') 
and p.prod_cd='Product' 
''').show(100000,False) 
select distinct customer_name 
        , customer_number 
        , member_number 
        , receipt_method 
        , masked_account_num 
from campaign_data.aa_dmc2252_CBAaccounts_20201119 
where member_number=2008785 
spark.sql(""" select * from 
campaign_data.aa_dmc2252_CBAaccounts_20201119 cba 
""").count() 
spark.sql(""" 
Select distinct ou_num as membernumber, x_legal_entity_name as business_name,b.*  
from gms.s_org_ext a 
 
inner join CBA_Accounts b 
on b.acc_name=a.x_legal_entity_name 
and acc_type='Non-ADM' 
""").count() 
SELECT b.* from campaign_data.aa_dmc1989_CBAaccounts_20200326 b 
 
LEFT ANTI JOIN gms.s_org_ext o 
on o.x_legal_entity_name = b.acc_name 
WITH cba_account as ( 
 
select distinct org.ou_num as membernumber, org.x_legal_entity_name as business_name,b.*  
 
from gms.s_org_ext org 
 
inner join campaign_data.aa_dmc1989_CBAaccounts_20200326 b          --587 
on b.acc_name=org.x_legal_entity_name 
and acc_type='Non-ADM'   
 
inner join gms.s_contact con                                        --584 
on con.row_id = org.pr_con_id 
 
-- inner join gms.s_contact_fnx as confnx                              --267 
-- on confnx.par_row_id = con.row_id  
 
-- inner join gms.s_contact_x as conx                                  --256 
-- on conx.par_row_id = con.row_id 
 
where 1=1 
--and (org.X_INVLD_EMAIL_ADDR is null or org.X_INVLD_EMAIL_ADDR='N') 
-- AND COALESCE(conx.attrib_36, "") != "No" -- Opt-out 
-- AND COALESCE(confnx.brloc_attrib13, "") != "Y" --THEN "Yes" else "No" end ="No" -- Global opt out  
-- AND (con.x_inv_email_1 = "N" OR con.x_inv_email_1 IS NULL)  -- Valid email  --238 
-- AND con.email_addr IS NOT NULL                                              --199 
-- AND COALESCE(confnx.deceased_flg, "N") = "N" -- Deceased                    --199 
-- AND COALESCE(con.x_nrma_title, "") != "Estate Of The Late"  
) 
SELECT count(*) FROM cba_account 
WITH cba_account as ( 
 
select distinct org.ou_num as membernumber, org.x_legal_entity_name as business_name,b.*  
 
from gms.s_org_ext org 
 
inner join campaign_data.aa_dmc1989_CBAaccounts_20200326 b          --587 
on b.acc_name=org.x_legal_entity_name 
and acc_type='Non-ADM'   
 
inner join gms.s_contact con                                        --584 
on con.row_id = org.pr_con_id 
 
-- inner join gms.s_contact_fnx as confnx                              --267 
-- on confnx.par_row_id = con.row_id  
 
-- inner join gms.s_contact_x as conx                                  --256 
-- on conx.par_row_id = con.row_id 
 
where 1=1 
--and (org.X_INVLD_EMAIL_ADDR is null or org.X_INVLD_EMAIL_ADDR='N') 
-- AND COALESCE(conx.attrib_36, "") != "No" -- Opt-out 
-- AND COALESCE(confnx.brloc_attrib13, "") != "Y" --THEN "Yes" else "No" end ="No" -- Global opt out  
-- AND (con.x_inv_email_1 = "N" OR con.x_inv_email_1 IS NULL)  -- Valid email  --238 
-- AND con.email_addr IS NOT NULL                                              --199 
-- AND COALESCE(confnx.deceased_flg, "N") = "N" -- Deceased                    --199 
-- AND COALESCE(con.x_nrma_title, "") != "Estate Of The Late"  
) 
SELECT count(*) FROM cba_account 
select distinct X_INVLD_EMAIL_ADDR from gms.s_org_ext org 
where (org.X_INVLD_EMAIL_ADDR is null or org.X_INVLD_EMAIL_ADDR='N') 
spark.sql(""" 
 
Select distinct org.ou_num as membernumber, org.x_legal_entity_name as business_name,b.*  
from gms.s_contact con 
 
 
inner join gms.s_org_ext org 
on con.row_id = org.pr_con_id 
 
inner join CBA_Accounts b 
on b.acc_name=org.x_legal_entity_name 
and acc_type='Non-ADM' 
 
inner join gms.s_contact_fnx as confnx  
on confnx.par_row_id = con.row_id  
 
inner join gms.s_contact_x as conx 
on conx.par_row_id = con.row_id 
 
where 1=1 
-- AND COALESCE(conx.attrib_36, "") != "No" -- Opt-out 
-- AND COALESCE(confnx.brloc_attrib13, "") != "Y" --THEN "Yes" else "No" end ="No" -- Global opt out  
AND (con.x_inv_email_1 = "N" OR con.x_inv_email_1 IS NULL)  -- Valid email 
AND con.email_addr IS NOT NULL  
AND COALESCE(confnx.deceased_flg, "N") = "N" -- Deceased 
AND COALESCE(con.x_nrma_title, "") != "Estate Of The Late"  
 
 
""").count() 
select count(*), count(distinct acc_name) from campaign_data.aa_dmc1989_cbaaccounts_20200326 
where acc_type='Non-ADM'   
WITH cba_account as ( 
 
select DISTINCT b.acc_name,org.pr_con_id,org.ou_num as membernumber, org.x_legal_entity_name as business_name , con.row_id 
 
from campaign_data.aa_dmc1989_CBAaccounts_20200326 b                    --589    
 
INNER join gms.s_org_ext org                                            --571 
on b.acc_name=org.x_legal_entity_name 
 
INNER join gms.s_contact con                                            --571 
on con.row_id = org.pr_con_id 
 
LEFT ANTI join gms.s_contact_fnx as confnx                              --263 
on confnx.par_row_id = con.row_id  
 
 
where 1=1 
and acc_type='Non-ADM'   
--and (org.X_INVLD_EMAIL_ADDR is null or org.X_INVLD_EMAIL_ADDR='N') 
-- AND COALESCE(conx.attrib_36, "") != "No" -- Opt-out 
-- AND COALESCE(confnx.brloc_attrib13, "") != "Y" --THEN "Yes" else "No" end ="No" -- Global opt out  
-- AND (con.x_inv_email_1 = "N" OR con.x_inv_email_1 IS NULL)  -- Valid email  --238 
-- AND con.email_addr IS NOT NULL                                              --199 
-- AND COALESCE(confnx.deceased_flg, "N") = "N" -- Deceased                    --199 
AND COALESCE(con.x_nrma_title, "") != "Estate Of The Late"  
) 
SELECT * FROM cba_account --order by acc_name 
--where membernumber='8132' 
SELECT DISTINCT par_row_id from gms.s_contact_fnx 
where par_row_id='1-18S-415 
 
' 
select distinct c.row_id, o.pr_con_id, o.x_legal_entity_name, c.csn, o.ou_num from gms.s_contact c 
inner join gms.s_org_ext o 
on o.pr_con_id=c.row_id 
 
where o.x_legal_entity_name ='TEMORA SHIRE COUNCIL' 
WITH cba_account as ( 
 
select DISTINCT b.acc_name 
               -- , org.row_id as account_id  
                , org.pr_con_id primary_contact_id 
                , org.ou_num as account_membernumber 
                , org.x_legal_entity_name as Account_Leagl_entity_name  
 
from campaign_data.aa_dmc1989_CBAaccounts_20200326 b                    --589    
 
INNER join gms.s_org_ext org                                            --571 
on b.acc_name=org.x_legal_entity_name 
 
-- INNER JOIN gms.s_contact con                                            --571 
-- on con.row_id = org.pr_con_id 
 
 
where 1=1 
and acc_type='Non-ADM'   
--and (org.X_INVLD_EMAIL_ADDR is null or org.X_INVLD_EMAIL_ADDR='N') 
-- AND COALESCE(conx.attrib_36, "") != "No" -- Opt-out 
-- AND COALESCE(confnx.brloc_attrib13, "") != "Y" --THEN "Yes" else "No" end ="No" -- Global opt out  
-- AND (con.x_inv_email_1 = "N" OR con.x_inv_email_1 IS NULL)  -- Valid email  --238 
-- AND con.email_addr IS NOT NULL                                              --199 
-- AND COALESCE(confnx.deceased_flg, "N") = "N" -- Deceased                    --199 
--AND COALESCE(con.x_nrma_title, "") != "Estate Of The Late"  
) 
SELECT  * FROM cba_account  
where account_membernumber='10028'	 
1-4GNIB9W 
 
What can be added to our template - what is missing 
identify cluster analysis -  
high usage clusters 
low usage clusters 
important partners - 
opportunities in the market - what are we missing 
using our member data with consumer info roymorgan/abs/mosiac 
Product positioning overlay of benefits 
 
 
GOAL:  
-Product opportunities mapping for consumer memberbase categories 
-Overlay consumers 
-new partner benefits coming on board or revamp 
 
 
Web behaviour analysis 
- member types what is there behaviour and whatthey do 
member non-member mix 
 
Volume for stakeholders 
ROI for internal 
 
What space can i do fill in that the team cannot think about it 
 
 
CBA_Accounts_missing = spark.read.load("/user/aamreen/Bwise Missing Entity Name Accounts_20200401.csv", format="csv",sep=",",inferSchema="true", header="true") 
CBA_Accounts_missing.createOrReplaceTempView("CBA_Accounts_missing") 
spark.sql(""" select * from CBA_Accounts_missing """).show() 
spark.sql(""" select * from CBA_Accounts_missing """).count() 
spark.sql(""" 
 
Select distinct org.ou_num as membernumber, org.x_legal_entity_name as business_name,b.*  
from gms.s_org_ext org 
 
inner join CBA_Accounts_missing b 
on b.Account_name=org.x_legal_entity_name 
 
inner 
 
""").count() 
SELECT row_id from gms.s_contact 
where row_id in ('1-18S-415','1-19U-806','1-19M-741') 
-- 4	1-17J-1641 
-- 5	1-19A-382 
-- 6	1-17H-1873 
-- 7	1-19M-451 
-- 8	1-19F-1364 
-- 9	1-19E-1811 
-- 10	1-17N-436 
-- 11	1-17Z-1658 
-- 12	1-17Z-219 
-- 13	1-18O-247 
-- 14	1-199-1923 
-- 15	1-19Y-155 
-- 16	1-17Z-725 
-- 17	1-1A0-922 
-- 18	1-17Z-1799 
-- 19	1-17H-467 
-- 20	1-19K-164 
-- 21	1-18S-601 
-- 22	1-18Y-1064 
-- 23	1-19F-897 
-- 24	1-1A0-264 
-- 25	1-18S-1979 
-- 26	1-17J-1572 
-- 27	1-17J-363 
-- 28	1-19A-1901 
-- 29	1-19M-595 
-- 30	1-18S-804 
-- 31	1-19L-335 
-- 32	1-199-437 
-- 33	1-199-1698 
-- 34	1-193-88 
-- 35	1-19U-719 
-- 36	1-17N-852 
-- 37	1-17J-846 
-- 38	1-19W-612 
-- 39	1-19E-1026 
 
with business_accounts as ( 
select distinct customer_name 
        , customer_number 
        , member_number 
        , receipt_method 
        , masked_account_num 
from campaign_data.aa_dmc2252_CBAaccounts_20201119 
--where member_number=2008785 
), price as (SELECT 
            ordi.asset_integ_id, 
            CASE 
                WHEN (YEAR(ordiom.service_end_dt) - YEAR(ord.order_dt)) < 1 THEN ordi.net_pri 
                ELSE ordi.net_pri/(YEAR(ordiom.service_end_dt) - YEAR(ord.order_dt)) 
            END AS net_pri, 
            ordt.name, 
            ordi.action_cd, 
            RANK() OVER (PARTITION BY ordi.asset_integ_id ORDER BY ord.order_dt DESC) AS idx 
          
        FROM 
            gms.s_order AS ord 
             
        INNER JOIN 
            gms.s_order_type AS ordt 
            ON ordt.row_id = ord.order_type_id 
             
        INNER JOIN 
            gms.s_order_item AS ordi 
            ON ordi.order_id = ord.row_id 
             
        LEFT JOIN 
            gms.s_order_item_om AS ordiom 
            ON ordiom.par_row_id = ordi.row_id 
         
        WHERE 
            ordi.asset_integ_id IS NOT NULL 
            AND ord.status_cd = 'Complete' 
    ) 
 
 
Select  --cba.customer_name 
        -- cba.customer_number 
         cba.member_number 
        , case when cba.masked_account_num like '%0488' and (cba.receipt_method like 'ANZ-EFT%' or cba.receipt_method in ('ANZ-DDAR-837850488','Cash - NRMA Motoring Bank-ANZ')) then 'ANZ' 
            when cba.receipt_method like 'BPAY%' or cba.receipt_method like 'NMS%' then 'ANZ_Temp' 
            when cba.masked_account_num like '%4434' then 'Commbank'  
            else 'other' end type 
        , COALESCE(market_class_cd, '') as market_class 
        , o.x_nrma_asset_count as fleet_size 
        , ox.attrib_25 as tenure 
        --, c.row_id as primary_contact 
        --, c.fst_name 
        , case when c.x_inv_email_1 = 'Y' or c.email_addr is null then 'N' else 'Y' end Emailable_ABR 
        , case when trim(c.VETERAN_FLG) = 'Y' or c.CELL_PH_NUM is null then 'N' else 'Y' end SMS_ABR 
        , case when trim(ad.premise_flg) = 'Y' or ad.addr_name is null then 'N' else 'Y' end Post_ABR 
        , NVL(a.x_nrma_pay_by_term,'N') as Monhtly 
        --, py.pay_type_cd 
        , CASE WHEN (a.bill_profile_id IS NOT NULL AND a.x_nrma_pay_by_term != 'Y') THEN 'Y' ELSE 'N' END AS Direct_Debit 
        , sum(oi.net_pri) as annual_revenue 
        , count(*) 
 
from business_accounts cba 
 
left JOIN gms.s_org_ext o 
on cast(cba.MEMBER_NUMBER as string) = o.ou_num 
AND accnt_type_cd = 'Customer' 
 
left join gms.s_org_ext_x ox 
on ox.par_row_id=o.row_id 
 
LEFT JOIN gms.s_contact AS c 
ON c.row_id = o.pr_con_id 
 
LEFT JOIN  gms.s_addr_per as ad         --813620 
on c.pr_per_addr_id = ad.row_id 
 
left join gms.s_asset a 
on a.owner_accnt_id=o.par_row_id 
and a.status_cd='Active' 
 
LEFT JOIN (SELECT 
    asset_integ_id, 
    MAX(net_pri) AS net_pri 
FROM 
    price 
WHERE 
    idx = 1 
    AND name != 'Cancellation' 
    AND action_cd IN ('Add', 'Update') 
     
GROUP BY 
    1) oi 
on oi.asset_integ_id=a.integration_id 
 
left join gms.s_prod_int p 
on p.row_id=a.prod_id 
 
 
where 1=1 
--cba.member_number=2008785 
and p.sub_type_cd in ('Non-RSA','RSA') 
and p.prod_cd='Product' 
 
group by 1,2,3,4,5,6,7,8,9,10



