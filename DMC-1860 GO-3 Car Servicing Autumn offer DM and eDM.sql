Contents
SELECT a.branch_code,count(DISTINCT a.contact_id)  
 
FROM campaign_data.carservicing_crosssell_20200212 a 
 
inner join gms.s_contact c 
on c.row_id=a.contact_id 
 
INNER JOIN gms.s_contact_x cx 
on cx.par_row_id=c.row_id 
 
INNER JOIN gms.s_contact_fnx fn 
on fn.par_row_id=c.row_id 
 
inner join gms.s_addr_per as ad         --813620 
on c.pr_per_addr_id = ad.row_id 
 
 
where  c.cust_stat_cd = 'Active'        --813576 
and c.con_cd in ('Ordinary Member' , 'Affiliate Member')        --813562 
and NVL(c.x_nrma_title,'no title') != 'Estate Of The Late' 
and NVL(fn.deceased_flg,'N') = 'N'                              --811922 
and c.csn is not null                                           --811922 
and case when trim(ad.premise_flg) = 'Y' or ad.addr_name is null then 'N' else 'Y' end = 'Y' --no return address --811257 
and nvl(cx.attrib_35,'Yes') <> 'No' --post permission           --639273 
and a.decile=1                                                  --101844 
 
group by a.branch_code 
 
SELECT a.branch_code,count(DISTINCT a.contact_id)  
 
FROM campaign_data.carservicing_crosssell_20200212 a 
 
inner join gms.s_contact c 
on c.row_id=a.contact_id 
 
INNER JOIN gms.s_contact_x cx 
on cx.par_row_id=c.row_id 
 
INNER JOIN gms.s_contact_fnx fn 
on fn.par_row_id=c.row_id 
 
inner join gms.s_addr_per as ad         --813620 
on c.pr_per_addr_id = ad.row_id 
 
inner join gms.s_org_ext as org 
on org.row_id = c.pr_dept_ou_id 
 
 
where  c.cust_stat_cd = 'Active'        --813576 
and c.con_cd in ('Ordinary Member' , 'Affiliate Member')        --813562 
and NVL(c.x_nrma_title,'no title') != 'Estate Of The Late' 
and NVL(fn.deceased_flg,'N') = 'N'                              --811922 
and c.csn is not null                                           --811922 
and case when trim(ad.premise_flg) = 'Y' or ad.addr_name is null then 'N' else 'Y' end = 'Y' --no return address --811257 
and ad.add 
and nvl(cx.attrib_35,'Yes') <> 'No' --post permission           --639273 
and a.decile=1                                                --101844 
 
group by a.branch_code 
 
--drop table campaign_data.aa_dmc1860_carservicing_DM_20200213 
create table campaign_data.aa_dmc1860_carservicing_DM_20200213 as 
SELECT  distinct a.* 
        , c.x_nrma_title 
        , c.fst_name as first_name 
        , c.mid_name as middle_name 
        , c.last_name as last_name 
        , to_date(c.x_nrma_join_dt) as Join_date 
        , extract(now(), "year") - extract(to_date(c.birth_dt), "year") as Age 
        , cx.attrib_55 as ColourPlus 
        , cx.attrib_17 as Tenure 
        , c.con_cd as memberType 
        , c.cust_value_cd as loyaltyColour -- gold, gold+, silver, and members 
        , ad.addr_name as Full_address 
        , ad.addr as ad_line_1 
        , ad.addr_line_2 as ad_line_2 
        , ad.city as suburb 
        , ad.state 
        , case when trim(c.x_inv_email_1) = 'Y' or c.email_addr is null or cx.attrib_36='No' then 'N'  else 'Y' end email_able 
        , if(org.pr_con_id <> c.row_id, 0, 1) as primary_account 
         
         
FROM campaign_data.carservicing_crosssell_20200212 a 
 
inner join gms.s_contact c 
on c.row_id=a.contact_id 
 
INNER JOIN gms.s_contact_x cx 
on cx.par_row_id=c.row_id 
 
INNER JOIN gms.s_contact_fnx fn 
on fn.par_row_id=c.row_id 
 
inner join gms.s_addr_per as ad         --813620 
on c.pr_per_addr_id = ad.row_id 
 
inner join gms.s_org_ext as org 
on org.row_id = c.pr_dept_ou_id 
 
 
where  c.cust_stat_cd = 'Active'        --813576 
and c.con_cd in ('Ordinary Member' , 'Affiliate Member')        --813562 
and NVL(c.x_nrma_title,'no title') != 'Estate Of The Late' 
and NVL(fn.deceased_flg,'N') = 'N'                              --811922 
and c.csn is not null                                           --811922 
and case when trim(ad.premise_flg) = 'Y' or ad.addr_name is null then 'N' else 'Y' end = 'Y' --no return address --811257 
and nvl(cx.attrib_35,'Yes') <> 'No' --post permission           --639273 
and a.decile=1                                                  --101844 
and (UPPER(trim(NVL(ad.addr,'NULL')))<>'NULL' or UPPER(trim(NVL(ad.addr_line_2,'NULL')))<>'NULL') 
and if(org.pr_con_id <> c.row_id, 0, 1) =1                      --97351 
create table campaign_data.aa_dmc1860_carservicing_campaign_DM_20200213 as 
select aa.*, case when rng<0.84565 then False else True end Control from 
(select a.*,rand(0) as rng from aa_dmc1860_carservicing_DM_20200213 a 
 
left anti join (select count(contact_id) as vol,full_address from aa_dmc1860_carservicing_DM_20200213 
group by full_address 
order by 1 desc) b 
on b.full_address=a.full_address 
and vol>1 
)aa 
--where rng<0.84565 
 
select count(distinct full_address), contact_id from aa_dmc1860_carservicing_campaign_DM_20200213 
group by contact_id 
order by 1 desc 
--drop table campaign_data.aa_dmc1860_carservicing_campaign_DM_20200213 
-- create table campaign_data.aa_dmc1860_carservicing_campaign_DM_20200213 as 
select aa.*, case when rng<0.782444 then False else True end Control from 
-- (select *,rand(0) as rng from campaign_data.aa_dmc1860_carservicing_dm_20200213) aa 
select    membernumber 
        , x_nrma_title as title 
        , first_name 
        , middle_name 
        , last_name 
        , full_address 
        , postcode 
        , 'Car Servicing Autumn Campaign' as campaign_id 
        , 'AUTUMN20' as campaign_code 
        , 'DM' as treatment_code 
        , to_date(now()) as extraction_date 
from campaign_data.aa_dmc1860_carservicing_campaign_DM_20200213  
where control is false 
select    membernumber 
        , x_nrma_title as title 
        , first_name 
        , middle_name 
        , last_name 
        , full_address 
        , postcode   
        , 'Car Servicing Autumn Campaign' as campaign_id 
        , 'AUTUMN20' as campaign_code 
        , 'DM' as treatment_code 
        , to_date(current_timestamp()) as extraction_date 
from campaign_data.aa_dmc1860_carservicing_campaign_DM_20200213  
where not control 
select branch_code, count(distinct contact_id) from aa_dmc1860_carservicing_campaign_dm_20200213 
group by branch_code 
select branch_code, count(distinct contact_id) from aa_dmc1860_carservicing_campaign_dm_20200213 
where control is true 
group by branch_code 
select branch_code, count(distinct contact_id) from aa_dmc1860_carservicing_campaign_dm_20200213 
where control is False 
group by branch_code 
with dataX as (SELECT a.* 
    , nvl(r.name, "service centre") as branch 
  
FROM campaign_data.aa_dmc1860_carservicing_campaign_edm_20200221 a  
    left join gms.s_srv_regn as r 
    on trim(lower(a.branch_code)) = trim(lower(r.x_branch_code))) 
     
select count(contact_id) 
from dataX 
SELECT * FROM campaign_data.aa_dmc1860_carservicing_campaign_dm_20200213 LIMIT 100; 
--create table aa_dmc1860_carservicing_campaign_edm_20200302 as 
 
select aa.*, case when edM_rng>0.1 then False else True end Control_edm  
from 
(SELECT a.*, rand(0) as edM_rng, nvl(r.name, "service centre") as branch 
FROM campaign_data.aa_dmc1860_carservicing_campaign_dm_20200213 a 
 
inner join gms.s_contact c 
on c.row_id=a.contact_id 
 
INNER JOIN gms.s_contact_x cx 
on cx.par_row_id=c.row_id 
 
INNER JOIN gms.s_contact_fnx fn 
on fn.par_row_id=c.row_id 
 
left join gms.s_srv_regn as r 
on trim(lower(a.branch_code)) = trim(lower(r.x_branch_code)) 
 
where a.control IS FALSE 
and c.cust_stat_cd = 'Active'         
and c.con_cd in ('Ordinary Member' , 'Affiliate Member')         
and NVL(c.x_nrma_title,'no title') != 'Estate Of The Late' 
and NVL(fn.deceased_flg,'N') = 'N'                               
and (cx.attrib_36 ='Yes' or Upper(cx.attrib_36) ='NULL')                                                                
and case when trim(c.x_inv_email_1) = 'Y' or c.email_addr is null then 'N' else 'Y' end = 'Y' 
)aa 
--where edM_rng > 0.1 
SELECT branch_code, count(DISTINCT contact_id) FROM campaign_data.aa_dmc1860_carservicing_campaign_edm_20200221  
where control_edm is false 
group by branch_code 
order by 1  
SELECT branch_code, count(DISTINCT contact_id) FROM campaign_data.aa_dmc1860_carservicing_campaign_edm_20200221  
where control_edm is true 
group by branch_code 
order by 1  
SELECT branch_code, count(DISTINCT contact_id) FROM campaign_data.aa_dmc1860_carservicing_campaign_edm_20200302  
where control_edm is false 
group by branch_code 
order by 1  
SELECT count(DISTINCT contact_id) 
  
FROM campaign_data.aa_dmc1860_carservicing_campaign_edm_20200302  
where control_edm is true 
SELECT count(DISTINCT contact_id) 
  
FROM campaign_data.aa_dmc1860_carservicing_campaign_edm_20200302 
where control_edm is false 
SELECT contact_id, membernumber,branch_code ,branch 
FROM campaign_data.aa_dmc1860_carservicing_campaign_edm_20200302  
where not control_edm 