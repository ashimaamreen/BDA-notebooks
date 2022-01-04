select * from sandpit.model_cc2pc_scored 
where to_date(order_end_dt)>='2020-08-31' 
select count(distinct membernumber), order_id from sandpit.model_cc2pc_scored 
where renewal_yyyymm='2020-09' 
group by 2 
order by 1 desc 
select count(distinct membernumber),band from sandpit.model_cc2pc_scored 
where renewal_yyyymm in ('2020-09','2020-10') 
group by 2 
-- order by 1 desc 
select aa.*, case when rng<0.84565 then False else True end Control from 
(select a.*,rand(0) as rng from aa_dmc1860_carservicing_DM_20200213 a 
 
left anti join (select count(contact_id) as vol,full_address from aa_dmc1860_carservicing_DM_20200213 
group by full_address 
order by 1 desc) b 
on b.full_address=a.full_address 
and vol>1 
)aa 
select aa.* from ( 
SELECT  DISTINCT a.membernumber 
        , a.order_end_dt 
        , a.order_id 
        , a.renewal_yyyymm 
        , a.vehicle_rego 
        , a.probability 
        , a.band 
        , to_date(c.x_nrma_join_dt) as Join_date 
        , extract(now(), "year") - extract(to_date(c.birth_dt), "year") as Age 
        , cx.attrib_55 as ColourPlus 
        , cx.attrib_17 as Tenure 
        , c.con_cd as memberType 
        , c.cust_value_cd as loyaltyColour -- gold, gold+, silver, and members 
        , rand(0) as rng 
         
         
FROM gms.s_contact c 
 
inner join sandpit.model_cc2pc_scored a 
on c.csn=a.membernumber 
and a.renewal_yyyymm in ('2020-09','2020-10','2020-11') 
 
INNER JOIN gms.s_contact_x cx 
on cx.par_row_id=c.row_id 
 
INNER JOIN gms.s_contact_fnx fn 
on fn.par_row_id=c.row_id 
 
inner join gms.s_org_ext as org 
on org.row_id = c.pr_dept_ou_id 
 
 
where  1=1        --813576 
and c.cust_stat_cd = 'Active'         
and c.con_cd in ('Ordinary Member' , 'Affiliate Member')         
and NVL(c.x_nrma_title,'no title') != 'Estate Of The Late' 
and NVL(fn.deceased_flg,'N') = 'N'                               
and (cx.attrib_36 ='Yes' or Upper(cx.attrib_36) ='NULL')                                                                
and case when trim(c.x_inv_email_1) = 'Y' or c.email_addr is null then 'N' else 'Y' end = 'Y' 
and if(org.pr_con_id <> c.row_id, 0, 1) =1   
) aa 
 
where aa.rng<0.11 
 
--select * from target 
--create table campaign_data.aa_dmc2103_temp2 as  
 
with main_Table as ( 
select aa.* 
        , case when aa.rng<0.099 then 'Random' 
                when band=1 then 'Top Decile' 
                else 'None' end segment 
                 
 
from ( 
SELECT  DISTINCT a.membernumber 
        , a.order_end_dt 
        , a.order_id 
        , a.renewal_yyyymm 
        , a.vehicle_rego 
        , a.probability 
        , a.band 
        , to_date(c.x_nrma_join_dt) as Join_date 
        , extract(now(), "year") - extract(to_date(c.birth_dt), "year") as Age 
        , cx.attrib_55 as ColourPlus 
        , cx.attrib_17 as Tenure 
        , c.con_cd as memberType 
        , c.cust_value_cd as loyaltyColour -- gold, gold+, silver, and members 
        , rand(0) as rng 
         
         
FROM gms.s_contact c 
 
inner join sandpit.model_cc2pc_scored a 
on c.csn=a.membernumber 
and a.renewal_yyyymm in ('2020-09','2020-10','2020-11') 
 
INNER JOIN gms.s_contact_x cx 
on cx.par_row_id=c.row_id 
 
INNER JOIN gms.s_contact_fnx fn 
on fn.par_row_id=c.row_id 
 
inner join gms.s_org_ext as org 
on org.row_id = c.pr_dept_ou_id 
 
 
where  1=1        --813576 
and c.cust_stat_cd = 'Active'         
and c.con_cd in ('Ordinary Member' , 'Affiliate Member')         
and NVL(c.x_nrma_title,'no title') != 'Estate Of The Late' 
and NVL(fn.deceased_flg,'N') = 'N'                               
and (cx.attrib_36 ='Yes' or Upper(cx.attrib_36) ='NULL')                                                                
and case when trim(c.x_inv_email_1) = 'Y' or c.email_addr is null then 'N' else 'Y' end = 'Y' 
and if(org.pr_con_id <> c.row_id, 0, 1) =1   
) aa 
) 
 
select *, rand(0) as rng_control from main_Table  
where segment!='None' 
with random as ( 
select *, case when rng_control<0.1 then 'control' else ' target' end type 
from campaign_data.aa_dmc2103_temp2 
where segment='Random' 
) 
select type, count(membernumber) from random 
group by 1 
 
with top as ( 
select *, case when rng_control<0.1 then 'control' else ' target' end type 
from campaign_data.aa_dmc2103_temp2 
where segment='Top Decile' 
) 
select count(membernumber) from top 
--group by 1 
 
 
SELECT  count(DISTINCT a.membernumber) 
        --, a.band 
         
FROM gms.s_contact c 
 
inner join sandpit.model_cc2pc_scored a 
on c.csn=a.membernumber 
and a.renewal_yyyymm in ('2020-09','2020-10','2020-11') 
 
INNER JOIN gms.s_contact_x cx 
on cx.par_row_id=c.row_id 
 
INNER JOIN gms.s_contact_fnx fn 
on fn.par_row_id=c.row_id 
 
inner join gms.s_org_ext as org --122459 
on org.row_id = c.pr_dept_ou_id 
 
 
 
where  1=1        --813576 
and c.cust_stat_cd = 'Active'         
and c.con_cd in ('Ordinary Member' , 'Affiliate Member')         
and NVL(c.x_nrma_title,'no title') != 'Estate Of The Late' 
and NVL(fn.deceased_flg,'N') = 'N'                               
and (cx.attrib_36 ='Yes' or Upper(cx.attrib_36) ='NULL')                                                                
and case when trim(c.x_inv_email_1) = 'Y' or c.email_addr is null then 'N' else 'Y' end = 'Y' 
and if(org.pr_con_id <> c.row_id, 0, 1) =1                      --97351 
--and a.band =1 
 
--  GROUP BY 2 
--  order by 2 
select   distinct c.row_id ContactID 
        , o.order_num OrderNumber 
        , o.row_id as order_id 
        , to_date(o.ORDER_DT) as RenewalDueDate 
        , to_date(o.X_STAT_CMPLTD_DT) as OrderCompletionDate  
        , a.* 
        , rand(0) as rng 
 
 
from gms.s_order o 
 
inner join gms.s_order_type ot 
on o.order_type_id = ot.row_id 
 
inner join gms.s_order_item oi 
on o.row_id = oi.order_id 
and oi.ACTION_CD in ('Update','Add') --when they renew only update line but add is upgrade/downgrade and new 
 
inner join gms.s_prod_int p 
on oi.prod_id = p.row_id 
and p.sub_type_cd in ('Non-RSA','Add-on','RSA') 
and p.prod_cd = 'Product' 
 
inner join gms.s_contact c 
on o.contact_id = c.row_id      --146613 
 
inner join sandpit.model_cc2pc_scored a     --45117 
on o.row_id=a.order_id 
 
INNER JOIN gms.s_contact_x cx 
on cx.par_row_id=c.row_id 
 
INNER JOIN gms.s_contact_fnx fn 
on fn.par_row_id=c.row_id 
 
inner join gms.s_org_ext as org --146613 
on org.row_id = c.pr_dept_ou_id 
 
 
where (NVL(o.X_RENEWAL_RELATED, 'N') = 'Y' or ot.name = 'Renew') 
--and o.status_cd != 'Revised' 
and o.status_cd= 'Submitted' 
and o.x_payment_status = 'Required' 
and o.ORDER_DT between '2020-07-01' and '2020-08-30' 
and c.cust_stat_cd = 'Active'         
and c.con_cd in ('Ordinary Member' , 'Affiliate Member')     --145193    
and if(org.pr_con_id <> c.row_id, 0, 1) =1                 -- 145176 
and NVL(c.x_nrma_title,'no title') != 'Estate Of The Late' 
and NVL(fn.deceased_flg,'N') = 'N'                               
and (cx.attrib_36 ='Yes' or Upper(cx.attrib_36) ='NULL')                                                                
and case when trim(c.x_inv_email_1) = 'Y' or c.email_addr is null then 'N' else 'Y' end = 'Y'       --69601 
 
 
select * from sandpit.renewal_base 
where membernumber='990400694' 
and asset_num='28996232445' 
inner join sandpit.model_cc2pc_scored a 
on c.csn=a.membernumber 
and a.renewal_yyyymm in ('2020-09','2020-10','2020-11') 
 
INNER JOIN gms.s_contact_x cx 
on cx.par_row_id=c.row_id 
 
INNER JOIN gms.s_contact_fnx fn 
on fn.par_row_id=c.row_id 
 
inner join gms.s_org_ext as org --122459 
on org.row_id = c.pr_dept_ou_id 
 
 
 
where  1=1        --813576 
and c.cust_stat_cd = 'Active'         
and c.con_cd in ('Ordinary Member' , 'Affiliate Member')         
and NVL(c.x_nrma_title,'no title') != 'Estate Of The Late' 
and NVL(fn.deceased_flg,'N') = 'N'                               
and (cx.attrib_36 ='Yes' or Upper(cx.attrib_36) ='NULL')                                                                
and case when trim(c.x_inv_email_1) = 'Y' or c.email_addr is null then 'N' else 'Y' end = 'Y' 
and if(org.pr_con_id <> c.row_id, 0, 1) =1  
with main_Table as ( 
select aa.* 
        , case when aa.rng<0.1 then 'Random' 
                when band=1 then 'Top Decile' 
                else 'None' end segment 
 
from ( 
select   distinct c.row_id ContactID 
        , o.order_num OrderNumber 
        , o.row_id as order_id_gms 
        , to_date(o.ORDER_DT) as RenewalDueDate 
        , to_date(o.X_STAT_CMPLTD_DT) as OrderCompletionDate  
        , a.* 
        , rand(0) as rng 
 
 
from gms.s_order o 
 
inner join gms.s_order_type ot 
on o.order_type_id = ot.row_id 
 
inner join gms.s_order_item oi 
on o.row_id = oi.order_id 
and oi.ACTION_CD in ('Update','Add') --when they renew only update line but add is upgrade/downgrade and new 
 
inner join gms.s_prod_int p 
on oi.prod_id = p.row_id 
and p.sub_type_cd in ('Non-RSA','Add-on','RSA') 
and p.prod_cd = 'Product' 
 
inner join gms.s_contact c 
on o.contact_id = c.row_id      --146613 
 
inner join sandpit.model_cc2pc_scored a     --45117 
on c.csn=a.membernumber 
 
INNER JOIN gms.s_contact_x cx 
on cx.par_row_id=c.row_id 
 
INNER JOIN gms.s_contact_fnx fn 
on fn.par_row_id=c.row_id 
 
inner join gms.s_org_ext as org --146613 
on org.row_id = c.pr_dept_ou_id 
 
 
where (NVL(o.X_RENEWAL_RELATED, 'N') = 'Y' or ot.name = 'Renew') 
--and o.status_cd != 'Revised' 
and o.status_cd= 'Submitted' 
and o.x_payment_status = 'Required' 
and o.ORDER_DT between '2020-07-01' and '2020-08-30' 
and c.cust_stat_cd = 'Active'         
and c.con_cd in ('Ordinary Member' , 'Affiliate Member')     --145193    
and if(org.pr_con_id <> c.row_id, 0, 1) =1                 -- 145176 
and NVL(c.x_nrma_title,'no title') != 'Estate Of The Late' 
and NVL(fn.deceased_flg,'N') = 'N'                               
and (cx.attrib_36 ='Yes' or Upper(cx.attrib_36) ='NULL')                                                                
and case when trim(c.x_inv_email_1) = 'Y' or c.email_addr is null then 'N' else 'Y' end = 'Y'       --69601 
) aa 
) 
select segment, count(distinct contactid) from main_Table 
where segment!='None' 
group by 1 
 
 
create table campaign_data.aa_dmc2094_PCupgrade_campaign_edm_20200707_adhoc as 
 
with main_table as ( 
 
select a.* 
        , to_date(c.x_nrma_join_dt) as Join_date 
        , extract(now(), "year") - extract(to_date(c.birth_dt), "year") as Age 
        , cx.attrib_55 as ColourPlus 
        , cx.attrib_17 as Tenure 
        , c.con_cd as memberType 
        , c.cust_value_cd as loyaltyColour -- gold, gold+, silver, and members 
        --, 'Sweep' as type 
        ,rand(0) as rng from ( 
 
select  c.row_id ContactID 
        , a.membernumber 
        , min(a.band) as decile 
        --, rand(0) as rng 
 
 
from gms.s_order o 
 
inner join gms.s_order_type ot 
on o.order_type_id = ot.row_id 
 
inner join gms.s_order_item oi 
on o.row_id = oi.order_id 
and oi.ACTION_CD in ('Update','Add') --when they renew only update line but add is upgrade/downgrade and new 
 
inner join gms.s_prod_int p 
on oi.prod_id = p.row_id 
and p.sub_type_cd in ('RSA') 
and p.prod_cd = 'Product' 
 
inner join gms.s_contact c 
on o.contact_id = c.row_id      --146613 
 
inner join sandpit.model_cc2pc_scored a     --45117 
on c.csn=a.membernumber 
 
INNER JOIN gms.s_contact_fnx fn 
on fn.par_row_id=c.row_id 
 
where 1=1 
and (NVL(o.X_RENEWAL_RELATED, 'N') = 'Y' or ot.name = 'Renew') 
and o.status_cd= 'Submitted' 
and o.x_payment_status = 'Required' 
and o.X_STAT_CMPLTD_DT is null  
and o.ORDER_DT <= '2020-08-31' 
and a.renewal_yyyymm in ('2020-07','2020-08','2020-09','2020-06','2020-05','2020-04') 
and c.cust_stat_cd = 'Active'         
and NVL(c.x_nrma_title,'no title') != 'Estate Of The Late' 
and NVL(fn.deceased_flg,'N') = 'N'  
group by 1,2 
)a 
 
inner join gms.s_contact c 
on c.row_id=a.contactid 
 
INNER JOIN gms.s_contact_x cx 
on cx.par_row_id=c.row_id 
 
) 
select *, case when rng<0.1 then 'Random' 
                when decile=1 then 'Top Decile' 
                else 'None' end segment 
from main_table 
 
----PEER REVIEWED BIT 
create table campaign_data.aa_dmc2096_PCupgrade_campaign_edm_20200707_triggerd as 
with main_table as ( 
 
select a.* 
        , to_date(c.x_nrma_join_dt) as Join_date 
        , extract(now(), "year") - extract(to_date(c.birth_dt), "year") as Age 
        , cx.attrib_55 as ColourPlus 
        , cx.attrib_17 as Tenure 
        , c.con_cd as memberType 
        , c.cust_value_cd as loyaltyColour -- gold, gold+, silver, and members 
        ,rand(0) as rng from ( 
 
select  c.row_id ContactID 
        , a.membernumber 
        , min(a.band) as decile 
         
         
FROM gms.s_contact c 
 
inner join sandpit.model_cc2pc_scored a 
on c.csn=a.membernumber 
and a.renewal_yyyymm in ('2020-09','2020-10') 
 
INNER JOIN gms.s_contact_fnx fn 
on fn.par_row_id=c.row_id 
 
-- inner join gms.s_org_ext as org 
-- on org.row_id = c.pr_dept_ou_id 
 
 
where  1=1        --813576 
and c.cust_stat_cd = 'Active'         
--and c.con_cd in ('Ordinary Member' , 'Affiliate Member')         
and NVL(c.x_nrma_title,'no title') != 'Estate Of The Late' 
and NVL(fn.deceased_flg,'N') = 'N'        
--and o.ORDER_DT between '2020-09-01' and '2020-10-31' 
--and (cx.attrib_36 ='Yes' or Upper(cx.attrib_36) ='NULL')                                                                
--and case when trim(c.x_inv_email_1) = 'Y' or c.email_addr is null then 'N' else 'Y' end = 'Y' 
--and if(org.pr_con_id <> c.row_id, 0, 1) =1   
group by 1,2 
)a 
 
inner join gms.s_contact c 
on c.row_id=a.contactid 
 
INNER JOIN gms.s_contact_x cx 
on cx.par_row_id=c.row_id 
 
) 
select *, case when rng<0.1 then 'Random' 
                when decile=1 then 'Top Decile' 
                else 'None' end segment 
from main_table 
DROP TABLE campaign_data.aa_dmc2094_pcupgrade_campaign_edm_20200707_adhoc; 
SELECT contactid,segment FROM campaign_data.aa_dmc2094_pcupgrade_campaign_edm_20200707_adhoc 
where segment!='None' 
SELECT contactid,segment FROM campaign_data.aa_dmc2096_pcupgrade_campaign_edm_20200707_triggerd 
where segment!='None' 
SELECT count(DISTINCT contactid),segment FROM campaign_data.aa_dmc2094_pcupgrade_campaign_edm_20200707_adhoc 
where segment!='None' 
GROUP BY 2 
SELECT count(DISTINCT contactid),segment FROM campaign_data.aa_dmc2096_pcupgrade_campaign_edm_20200707_triggerd 
where segment!='None' 
GROUP BY 2 
SELECT  count(DISTINCT a.membernumber) 
 
FROM gms.s_contact c 
 
inner join sandpit.model_cc2pc_scored a 
on c.csn=a.membernumber 
and a.renewal_yyyymm in ('2020-09','2020-10','2020-11') 
 
INNER JOIN gms.s_contact_x cx 
on cx.par_row_id=c.row_id 
 
INNER JOIN gms.s_contact_fnx fn 
on fn.par_row_id=c.row_id 
 
inner join gms.s_org_ext as org 
on org.row_id = c.pr_dept_ou_id 
 
LEFT ANTI JOIN campaign_data.aa_dmc2096_pcupgrade_campaign_edm_20200707_triggerd t 
on t.contactid=c.row_id 
and t.segment in ('Random','Top Decile') 
 
left ANTI JOIN campaign_data.aa_dmc2094_pcupgrade_campaign_edm_20200707_adhoc pc 
on pc.contactid=c.row_id 
and pc.segment in ('Random','Top Decile') 
 
where  1=1        --813576 
and c.cust_stat_cd = 'Active'         
and c.con_cd in ('Ordinary Member' , 'Affiliate Member')         
and NVL(c.x_nrma_title,'no title') != 'Estate Of The Late' 
and NVL(fn.deceased_flg,'N') = 'N'                               
and (cx.attrib_36 ='Yes' or Upper(cx.attrib_36) ='NULL')                                                                
and case when trim(c.x_inv_email_1) = 'Y' or c.email_addr is null then 'N' else 'Y' end = 'Y' 
and if(org.pr_con_id <> c.row_id, 0, 1) =1