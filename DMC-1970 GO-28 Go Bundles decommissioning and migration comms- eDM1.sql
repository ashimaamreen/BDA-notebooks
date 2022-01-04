select  c.row_id as contact_id 
        , case when trim(ad.premise_flg) = 'Y' or ad.addr_name is null then 'N' else 'Y' end valid_address 
        , case when (trim(c.x_inv_email_1) = 'Y' or c.email_addr is null) then 'N' else 'Y' end valid_email 
        , case when (c.home_ph_num is not null and nvl(c.hard_to_reach,'N')<>'Y') then 'Y' else 'N' end valid_home_phone 
        , case when (c.cell_ph_num is not null and nvl(c.veteran_flg,'N')<>'Y' and regexp_replace(c.cell_ph_num, "[^0-9]+", "") not like '0_0000000_' and regexp_replace(c.cell_ph_num, "[^0-9]+", "") not like '041111111_') then 'Y' else 'N' end valid_mobile 
        , count(a.row_id) as Bundle_count 
from gms.s_contact as c 
     
    inner join gms.s_asset as a 
        on a.owner_con_id = c.row_id 
         
    inner join gms.s_prod_int as p 
        on a.prod_id = p.row_id 
         
    inner join gms.s_contact_fnx as fn 
        on fn.par_row_id = c.row_id 
     
    inner join gms.s_addr_per as ad          
        on c.pr_per_addr_id = ad.row_id 
   
 
where 1=1 
     and p.name like '%Bundle' 
     and a.status_cd = 'Active' 
     and p.type = 'Membership' 
     and p.prod_cd = 'Product' 
    -- --and NVL(fn.deceased_flg,'N') = 'N' 
     and c.csn is not null 
     and c.cust_stat_cd = 'Active'  --Active Customer               -- #4597 
     and NVL(fn.deceased_flg,'N') = 'N' --Non deceased 
     and NVL(c.x_nrma_title,'no title') != 'Estate Of The Late'     -- #4595 
     and c.con_cd in ('Ordinary Member' , 'Affiliate Member')       -- #4595 
 
group by c.row_id,c.csn 
        , case when trim(ad.premise_flg) = 'Y' or ad.addr_name is null then 'N' else 'Y' end  
        , case when (trim(c.x_inv_email_1) = 'Y' or c.email_addr is null) then 'N' else 'Y' end  
        , case when (c.home_ph_num is not null and nvl(c.hard_to_reach,'N')<>'Y') then 'Y' else 'N' end  
        , case when (c.cell_ph_num is not null and nvl(c.veteran_flg,'N')<>'Y' and regexp_replace(c.cell_ph_num, "[^0-9]+", "") not like '0_0000000_' and regexp_replace(c.cell_ph_num, "[^0-9]+", "") not like '041111111_') then 'Y' else 'N' end 
 
create table campaign_data.aa_dmc1970_goBundle_decommissioning_20200311 as  
select  c.row_id as contact_id, c.csn as member_number 
         , x.valid_address 
         , x.valid_email 
         , x.valid_home_phone 
         , x.valid_mobile 
         , x.Bundle_count 
         , case when (sum (case when p.name like '%Bundle' then 0 else 1 end) =0) then 'Go_Bundle' else 'Multiple' end asset 
         , count(a.row_id) as all_count 
        --,p.name,p.sub_type_cd 
--,c.csn as member_number,p.name as product,c.con_cd as member_type  
 
from gms.s_contact as c 
     
    inner join gms.s_asset as a 
        on a.owner_con_id = c.row_id 
         
    inner join gms.s_prod_int as p 
        on a.prod_id = p.row_id 
         
    inner join gms.s_contact_fnx as fn 
        on fn.par_row_id = c.row_id 
     
    inner join gms.s_addr_per as ad          
        on c.pr_per_addr_id = ad.row_id 
         
    inner join ( 
    select  c.row_id as contact_id 
        , case when trim(ad.premise_flg) = 'Y' or ad.addr_name is null then 'N' else 'Y' end valid_address 
        , case when (trim(c.x_inv_email_1) = 'Y' or c.email_addr is null) then 'N' else 'Y' end valid_email 
        , case when (c.home_ph_num is not null and nvl(c.hard_to_reach,'N')<>'Y') then 'Y' else 'N' end valid_home_phone 
        , case when (c.cell_ph_num is not null and nvl(c.veteran_flg,'N')<>'Y' and regexp_replace(c.cell_ph_num, "[^0-9]+", "") not like '0_0000000_' and regexp_replace(c.cell_ph_num, "[^0-9]+", "") not like '041111111_') then 'Y' else 'N' end valid_mobile 
        , count(a.row_id) as Bundle_count 
from gms.s_contact as c 
     
    inner join gms.s_asset as a 
        on a.owner_con_id = c.row_id 
         
    inner join gms.s_prod_int as p 
        on a.prod_id = p.row_id 
         
    inner join gms.s_contact_fnx as fn 
        on fn.par_row_id = c.row_id 
     
    inner join gms.s_addr_per as ad          
        on c.pr_per_addr_id = ad.row_id 
   
 
where 1=1 
     and p.name like '%Bundle' 
     and a.status_cd = 'Active' 
     and p.type = 'Membership' 
     and p.prod_cd = 'Product' 
    -- --and NVL(fn.deceased_flg,'N') = 'N' 
     and c.csn is not null 
     and c.cust_stat_cd = 'Active'  --Active Customer               -- #4597 
     and NVL(fn.deceased_flg,'N') = 'N' --Non deceased 
     and NVL(c.x_nrma_title,'no title') != 'Estate Of The Late'     -- #4595 
     and c.con_cd in ('Ordinary Member' , 'Affiliate Member')       -- #4595 
 
group by c.row_id,c.csn 
        , case when trim(ad.premise_flg) = 'Y' or ad.addr_name is null then 'N' else 'Y' end  
        , case when (trim(c.x_inv_email_1) = 'Y' or c.email_addr is null) then 'N' else 'Y' end  
        , case when (c.home_ph_num is not null and nvl(c.hard_to_reach,'N')<>'Y') then 'Y' else 'N' end  
        , case when (c.cell_ph_num is not null and nvl(c.veteran_flg,'N')<>'Y' and regexp_replace(c.cell_ph_num, "[^0-9]+", "") not like '0_0000000_' and regexp_replace(c.cell_ph_num, "[^0-9]+", "") not like '041111111_') then 'Y' else 'N' end 
) as x 
        on x.contact_id = c.row_id 
 
where 1=1 
     --and p.name like '%Bundle' 
     and a.status_cd = 'Active' 
     and p.type = 'Membership' 
     and p.sub_type_cd in ('Non-RSA','RSA') 
     and p.prod_cd = 'Product' 
    -- --and NVL(fn.deceased_flg,'N') = 'N' 
     and c.csn is not null 
     and c.cust_stat_cd = 'Active'  --Active Customer               -- #4597 
     and NVL(fn.deceased_flg,'N') = 'N' --Non deceased 
     and NVL(c.x_nrma_title,'no title') != 'Estate Of The Late'     -- #4595 
     and c.con_cd in ('Ordinary Member' , 'Affiliate Member')       -- #4595 
     --and c.row_id='1-HOH-2732' 
 
-- group by case when (sum (case when p.name like '%Bundle' then 0 else 1 end) =0) then 'Go_Bundle' else 'Multiple' end 
-- order by 1 desc 
group by 1,2,3,4,5,6,7 
select count(c.row_id) as contact_id 
 
from gms.s_contact as c 
     
    inner join gms.s_asset as a 
        on a.owner_con_id = c.row_id 
         
    inner join gms.s_prod_int as p 
        on a.prod_id = p.row_id 
 
where 1=1 
     and p.name like '%Bundle' 
     and a.status_cd = 'Active' 
SELECT   case when all_count=bundle_count then 'GoBundle_Only' else 'GoBundle_Plus_Other' end member_type 
        , case when all_count>1 then 'Multiple' else 'Single' end sub_type 
        , count(contact_id)  
--bundle_count,all_count, count(contact_id)  
 
FROM campaign_data.aa_dmc1970_gobundle_decommissioning_20200311  
--where all_count>1 
GROUP BY 1,2 
order by 1,2 
SELECT count(contact_id)  
--bundle_count,all_count, count(contact_id)  
 
FROM campaign_data.aa_dmc1970_gobundle_decommissioning_20200311  
where all_count>1 
and all_count=bundle_count 
select  count(distinct c.row_id) as contact_id 
        -- , case when trim(ad.premise_flg) = 'Y' or ad.addr_name is null then 'N' else 'Y' end valid_address 
        -- , case when (trim(c.x_inv_email_1) = 'Y' or c.email_addr is null) then 'N' else 'Y' end valid_email 
        -- , case when (c.home_ph_num is not null and nvl(c.hard_to_reach,'N')<>'Y') then 'Y' else 'N' end valid_home_phone 
        -- , case when (c.cell_ph_num is not null and nvl(c.veteran_flg,'N')<>'Y' and regexp_replace(c.cell_ph_num, "[^0-9]+", "") not like '0_0000000_' and regexp_replace(c.cell_ph_num, "[^0-9]+", "") not like '041111111_') then 'Y' else 'N' end valid_mobile 
        -- , count(a.row_id) as Bundle_count 
from gms.s_contact as c 
 
    inner join gms.s_org_ext o 
        on o.pr_con_id=c.row_id 
     
    inner join gms.s_asset as a 
        on a.owner_accnt_id = o.par_row_id 
         
    inner join gms.s_prod_int as p 
        on a.prod_id = p.row_id 
         
    inner join gms.s_contact_fnx as fn 
        on fn.par_row_id = c.row_id 
     
    inner join gms.s_addr_per as ad          
        on c.pr_per_addr_id = ad.row_id 
   
 
where 1=1 
     and p.name like '%Bundle' 
     and a.status_cd = 'Active' 
     and p.type = 'Membership' 
     and p.prod_cd = 'Product' 
     and c.csn is not null 
     and c.cust_stat_cd = 'Active'  --Active Customer               -- #4578 
     and NVL(fn.deceased_flg,'N') = 'N' --Non deceased 
     and NVL(c.x_nrma_title,'no title') != 'Estate Of The Late'     -- #4576 
     and c.con_cd in ('Ordinary Member' , 'Affiliate Member')       -- #4576 
 
-- group by c.row_id,c.csn 
--         , case when trim(ad.premise_flg) = 'Y' or ad.addr_name is null then 'N' else 'Y' end  
--         , case when (trim(c.x_inv_email_1) = 'Y' or c.email_addr is null) then 'N' else 'Y' end  
--         , case when (c.home_ph_num is not null and nvl(c.hard_to_reach,'N')<>'Y') then 'Y' else 'N' end  
--         , case when (c.cell_ph_num is not null and nvl(c.veteran_flg,'N')<>'Y' and regexp_replace(c.cell_ph_num, "[^0-9]+", "") not like '0_0000000_' and regexp_replace(c.cell_ph_num, "[^0-9]+", "") not like '041111111_') then 'Y' else 'N' end 
 
--create table campaign_data.aa_dmc1970_goBundle_decommissioning_20200311_v2 as  
select  count(distinct c.row_id) 
-- c.row_id as contact_id, c.csn as member_number 
--          , x.valid_address 
--          , x.valid_email 
--          , x.valid_home_phone 
--          , x.valid_mobile 
--          , x.Bundle_count 
--          , case when (sum (case when p.name like '%Bundle' then 0 else 1 end) =0) then 'Go_Bundle' else 'Multiple' end asset 
--          , count(a.row_id) as all_count 
        --,p.name,p.sub_type_cd 
--,c.csn as member_number,p.name as product,c.con_cd as member_type  
 
from gms.s_contact as c 
     
    inner join gms.s_org_ext o 
        on o.pr_con_id=c.row_id 
     
    inner join gms.s_asset as a 
        on a.owner_accnt_id = o.par_row_id 
         
    inner join gms.s_prod_int as p 
        on a.prod_id = p.row_id 
         
    inner join gms.s_contact_fnx as fn 
        on fn.par_row_id = c.row_id 
     
    inner join gms.s_addr_per as ad          
        on c.pr_per_addr_id = ad.row_id 
         
    inner join ( 
    select  c.row_id as contact_id 
        , case when trim(ad.premise_flg) = 'Y' or ad.addr_name is null then 'N' else 'Y' end valid_address 
        , case when (trim(c.x_inv_email_1) = 'Y' or c.email_addr is null) then 'N' else 'Y' end valid_email 
        , case when (c.home_ph_num is not null and nvl(c.hard_to_reach,'N')<>'Y') then 'Y' else 'N' end valid_home_phone 
        , case when (c.cell_ph_num is not null and nvl(c.veteran_flg,'N')<>'Y' and regexp_replace(c.cell_ph_num, "[^0-9]+", "") not like '0_0000000_' and regexp_replace(c.cell_ph_num, "[^0-9]+", "") not like '041111111_') then 'Y' else 'N' end valid_mobile 
        , count(a.row_id) as Bundle_count 
from gms.s_contact as c 
 
    inner join gms.s_org_ext o 
        on o.pr_con_id=c.row_id 
     
    inner join gms.s_asset as a 
        on a.owner_accnt_id = o.par_row_id 
         
    inner join gms.s_prod_int as p 
        on a.prod_id = p.row_id 
         
    inner join gms.s_contact_fnx as fn 
        on fn.par_row_id = c.row_id 
     
    inner join gms.s_addr_per as ad          
        on c.pr_per_addr_id = ad.row_id 
   
 
where 1=1 
     and p.name like '%Bundle' 
     and a.status_cd = 'Active' 
     and p.type = 'Membership' 
     and p.prod_cd = 'Product' 
     and c.csn is not null 
     and c.cust_stat_cd = 'Active'  --Active Customer               -- #4597 
     and NVL(fn.deceased_flg,'N') = 'N' --Non deceased 
     and NVL(c.x_nrma_title,'no title') != 'Estate Of The Late'     -- #4595 
     and c.con_cd in ('Ordinary Member' , 'Affiliate Member')       -- #4595 
 
group by c.row_id,c.csn 
        , case when trim(ad.premise_flg) = 'Y' or ad.addr_name is null then 'N' else 'Y' end  
        , case when (trim(c.x_inv_email_1) = 'Y' or c.email_addr is null) then 'N' else 'Y' end  
        , case when (c.home_ph_num is not null and nvl(c.hard_to_reach,'N')<>'Y') then 'Y' else 'N' end  
        , case when (c.cell_ph_num is not null and nvl(c.veteran_flg,'N')<>'Y' and regexp_replace(c.cell_ph_num, "[^0-9]+", "") not like '0_0000000_' and regexp_replace(c.cell_ph_num, "[^0-9]+", "") not like '041111111_') then 'Y' else 'N' end 
) as x 
        on x.contact_id = c.row_id 
 
where 1=1 
     --and p.name like '%Bundle' 
     and a.status_cd = 'Active' 
     and p.type = 'Membership' 
     and p.sub_type_cd in ('Non-RSA','RSA') 
     and p.prod_cd = 'Product' 
    -- --and NVL(fn.deceased_flg,'N') = 'N' 
     and c.csn is not null 
     and c.cust_stat_cd = 'Active'  --Active Customer               -- #4597 
     and NVL(fn.deceased_flg,'N') = 'N' --Non deceased 
     and NVL(c.x_nrma_title,'no title') != 'Estate Of The Late'     -- #4595 
     and c.con_cd in ('Ordinary Member' , 'Affiliate Member')       -- #4595 
     --and c.row_id='1-HOH-2732' 
 
-- group by case when (sum (case when p.name like '%Bundle' then 0 else 1 end) =0) then 'Go_Bundle' else 'Multiple' end 
-- order by 1 desc 
group by 1,2,3,4,5,6,7 
SELECT count(DISTINCT contact_id ) from campaign_data.aa_dmc1970_gobundle_decommissioning_20200311 
SELECT count(DISTINCT contact_id ) from campaign_data.aa_dmc1970_gobundle_decommissioning_20200311_v2 
SELECT   case when all_count=bundle_count then 'GoBundle_Only' else 'GoBundle_Plus_Other' end member_type 
        , case when all_count>1 then 'Multiple' else 'Single' end sub_type 
        , count(contact_id)  
--bundle_count,all_count, count(contact_id)  
 
FROM campaign_data.aa_dmc1970_gobundle_decommissioning_20200311_v2  
--where all_count>1 
GROUP BY 1,2 
order by 1,2 
SELECT   case when valid_email='Y' then 'Email' 
                when valid_mobile='Y' then 'SMS' 
                when valid_address='Y' then 'DM' else 'Phone' end Communication_Method 
        , count(contact_id)  
--bundle_count,all_count, count(contact_id)  
 
FROM campaign_data.aa_dmc1970_gobundle_decommissioning_20200311_v2  
--where all_count>1 
GROUP BY 1 
SELECT    count(contact_id)  
--bundle_count,all_count, count(contact_id)  
 
FROM campaign_data.aa_dmc1970_gobundle_decommissioning_20200311_v2  
where valid_address='Y'  
                -- when ='Y' then 'SMS' 
                -- when ='Y' then 'DM' else 'Phone' end Communication_Method 
--FINAL  
select  count(distinct c.row_id) as contact_id 
 
from gms.s_contact as c 
     
    inner join gms.s_asset as a 
        on a.owner_con_id = c.row_id 
         
    inner join gms.s_prod_int as p 
        on a.prod_id = p.row_id 
         
    inner join gms.s_contact_fnx as fn 
        on fn.par_row_id = c.row_id 
   
 
where 1=1 
     and p.name like '%Bundle' 
     and a.status_cd = 'Active' 
     and p.type = 'Membership' 
     and p.prod_cd = 'Product' 
     and c.csn is not null 
     and c.cust_stat_cd = 'Active'  --Active Customer               -- #4597 
     and NVL(fn.deceased_flg,'N') = 'N' --Non deceased 
     and NVL(c.x_nrma_title,'no title') != 'Estate Of The Late'     -- #4595 
     and c.con_cd in ('Ordinary Member' , 'Affiliate Member')       -- #4595    --#4396 
     and NVL(x_inv_email_1,'N') ='N'  
     and c.email_addr is not null 
select distinct x_inv_email_1 from gms.s_contact 
where NVL(x_inv_email_1,'N') ='N' 
--and trim(c.x_inv_email_1) !='Y' 
--and c.email_addr is not null 
select distinct name from gms.s_prod_int 
where name like '%Bundle'