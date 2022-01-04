With offer as( 
select  distinct c.row_id as contact_id 
        , a.row_id as asset_row_id 
        , p.prod_cd as asset_type 
        , p.name product_name 
        , case when p.name='NB_Parks_Resort_Bundle_Offer_2404' THEN 'P&R' else 'Thrifty' end OFFER_Product 
from gms.s_contact as c 
     
    inner join gms.s_asset as a 
        on a.owner_con_id = c.row_id 
         
    inner join gms.s_prod_int as p 
        on a.prod_id = p.row_id 
   
 
where 1=1 
    and p.name IN ('NB_Parks_Resort_Bundle_Offer_2404','NB_Thrifty Bundle Offer_NoJoinFee_0419') 
    and a.status_cd = 'Active' 
    --and to_date(adddate(a.end_dt,1)) BETWEEN '2020-05-01' and '2020-10-31' 
) select * 
FROM offer  
--group by 1 order by 2 DESC 
where contact_id in ('1-DRFXQ8T','1-DRFYPBR') 
--NB_Parks_Resort_Bundle_Offer_2404, NB_Simply_Energy_Blue_Offer_0320, NB_Thrifty Bundle_Offer_NoJoinFee_0419 
 
spark.sql(""" 
 
With offer as( 
select  distinct c.row_id as contact_id 
        , a.row_id as asset_row_id 
        , p.prod_cd as asset_type 
        , case when p.name='NB_Parks_Resort_Bundle_Offer_2404' THEN 'P&R' else 'Thrifty' end OFFER_Partner 
        , to_date(a.end_dt) as promotion_asset_end_dt 
from gms.s_contact as c 
     
    inner join gms.s_asset as a 
        on a.owner_con_id = c.row_id 
         
    inner join gms.s_prod_int as p 
        on a.prod_id = p.row_id 
   
 
where 1=1 
    and p.name IN ('NB_Parks_Resort_Bundle_Offer_2404','NB_Thrifty Bundle Offer_NoJoinFee_0419') 
    and a.status_cd = 'Active' 
    and to_date(date_add(a.end_dt,1)) BETWEEN '2020-06-08' and '2020-10-31'          --Due date between 08 June 2020 and 31 Oct 2020 
) 
 
select  distinct a.row_id as asset_row_id 
        , 'NB_Offer' 
         
 
from gms.s_contact as c 
     
    inner join gms.s_asset as a 
        on a.owner_con_id = c.row_id 
         
    inner join gms.s_prod_int as p 
        on a.prod_id = p.row_id 
     
    INNER JOIN offer o                                          --#13215 
        on o.contact_id=c.row_id 
     
    LEFT JOIN gms.s_prod_int nrp 
        ON a.x_next_renewal_prod_id=nrp.row_id  
     
    inner join gms.s_contact_fnx fn                                              
        on fn.par_row_id=c.row_id                                   --#5829/5831 extra two asset_is's 
 
    inner join gms.s_addr_per as ad          
        on c.pr_per_addr_id = ad.row_id 
     
    INNER JOIN gms.s_contact_x cx 
        on cx.par_row_id = c.row_id 
 
where 1=1 
    and lower(p.name) ='nrma blue' 
    and a.status_cd = 'Active'                                      
    and p.type = 'Membership'                                       --#5829 
     
    and nrp.name='NB'    
    and c.csn is not null                                           --#5779 
    and c.cust_stat_cd = 'Active'                                   --#5700 
     
    and NVL(fn.deceased_flg,'N') = 'N' --Non deceased 
    and NVL(c.x_nrma_title,'no title') != 'Estate Of The Late'      --#5700 
     
    and NVL(ad.country,'Australia') = 'Australia'                   --#5686 
    and COALESCE(cx.attrib_44, '') not like 'Honorary%' 
    and a.row_id not in ('1-DRGC1FP','1-DRGC4AC','1-DRGC1EW') 
    --order by 1 
    """).show(10000,False) 
spark.sql(""" 
 
With offer as( 
select  distinct c.row_id as contact_id 
        , a.row_id as asset_row_id 
        , p.prod_cd as asset_type 
        , case when p.name='NB_Parks_Resort_Bundle_Offer_2404' THEN 'P&R' else 'Thrifty' end OFFER_Partner 
        , to_date(a.end_dt) as promotion_asset_end_dt 
from gms.s_contact as c 
     
    inner join gms.s_asset as a 
        on a.owner_con_id = c.row_id 
         
    inner join gms.s_prod_int as p 
        on a.prod_id = p.row_id 
   
 
where 1=1 
    and p.name IN ('NB_Parks_Resort_Bundle_Offer_2404','NB_Thrifty Bundle Offer_NoJoinFee_0419') 
    and a.status_cd = 'Active' 
    and to_date(date_add(a.end_dt,1)) BETWEEN '2020-06-08' and '2020-10-31'          --Due date between 08 June 2020 and 31 Oct 2020 
) 
 
select  distinct o.asset_row_id 
        , 'NB_Offer' 
         
 
from gms.s_contact as c 
     
    inner join gms.s_asset as a 
        on a.owner_con_id = c.row_id 
         
    inner join gms.s_prod_int as p 
        on a.prod_id = p.row_id 
     
    INNER JOIN offer o                                          --#13215 
        on o.contact_id=c.row_id 
     
    LEFT JOIN gms.s_prod_int nrp 
        ON a.x_next_renewal_prod_id=nrp.row_id  
     
    inner join gms.s_contact_fnx fn                                              
        on fn.par_row_id=c.row_id                                   --#5829/5831 extra two asset_is's 
 
    inner join gms.s_addr_per as ad          
        on c.pr_per_addr_id = ad.row_id 
     
    INNER JOIN gms.s_contact_x cx 
        on cx.par_row_id = c.row_id 
 
where 1=1 
    and lower(p.name) ='nrma blue' 
    and a.status_cd = 'Active'                                      
    and p.type = 'Membership'                                       --#5829 
     
    and nrp.name='NB'    
    and c.csn is not null                                           --#5779 
    and c.cust_stat_cd = 'Active'                                   --#5700 
     
    and NVL(fn.deceased_flg,'N') = 'N' --Non deceased 
    and NVL(c.x_nrma_title,'no title') != 'Estate Of The Late'      --#5700 
     
    and NVL(ad.country,'Australia') = 'Australia'                   --#5686 
    and COALESCE(cx.attrib_44, '') not like 'Honorary%' 
    and a.row_id not in ('1-DRGC1FP','1-DRGC4AC','1-DRGC1EW') 
    and o.asset_row_id not in ('1-DRGC1FP','1-DRGC4AC','1-DRGC1EW') 
   -- order by 1 
    """).show(10000,False) 
With offer as( 
select  distinct c.row_id as contact_id 
        , case when p.name='NB_Parks_Resort_Bundle_Offer_2404' THEN 'P&R' else 'Thrifty' end OFFER_Product 
from gms.s_contact as c 
     
    inner join gms.s_asset as a 
        on a.owner_con_id = c.row_id 
         
    inner join gms.s_prod_int as p 
        on a.prod_id = p.row_id 
   
 
where 1=1 
    and p.name IN ('NB_Parks_Resort_Bundle_Offer_2404','NB_Thrifty Bundle Offer_NoJoinFee_0419') 
    and a.status_cd = 'Active' 
) 
 
select  o.OFFER_Product,month(adddate(a.end_dt,1)),count(distinct c.row_id) as contact_id, count(a.row_id) as assets 
-- 
        --, p.name 
        --, o.OFFER_Product 
        --, nrp.name 
from gms.s_contact as c 
     
inner join gms.s_asset as a 
on a.owner_con_id = c.row_id 
         
inner join gms.s_prod_int p 
on a.x_next_renewal_prod_id=p.row_id 
 
inner join gms.s_contact_fnx fn                                             --#78726 
on fn.par_row_id=c.row_id 
 
INNER JOIN offer o 
on o.contact_id=a.owner_con_id                                              --#5707 
 
where 1=1 
    and p.name ='NB' 
    and a.status_cd = 'Active'                                       
    and p.type = 'Membership' 
    and to_date(adddate(a.end_dt,1)) BETWEEN '2020-05-01' and '2020-10-31'      --#36458 
    and NVL(fn.deceased_flg,'N') = 'N' 
    and c.csn is not null 
    and c.cust_stat_cd = 'Active'                                                    
    and c.con_cd in ('Ordinary Member')  
    and NVL(c.x_nrma_title,'no title') != 'Estate Of The Late'    
 
GROUP BY 1,2 
ORDER BY 1,2 
select a.owner_con_id from gms.s_asset a 
 
inner JOIN gms.s_prod_int p 
on a.x_next_renewal_prod_id=p.row_id 
 
where p.name='NB' 
 
select distinct row_id as blue_prod_row_id from gms.s_prod_int 
where lower(name) ='nrma blue' 
select row_id, name from gms.s_prod_int 
where name in ('NB_Parks_Resort_Bundle_Offer_2404','NB_Thrifty Bundle Offer_NoJoinFee_0419') 
select distinct x_next_renewal_prod_id from gms.s_asset a 
 
 
INNER JOIN gms.s_prod_int p 
ON a.prod_id=p.row_id 
 
LEFT JOIN gms.s_prod_int nrp 
ON a.x_next_renewal_prod_id=nrp.row_id  
With offer as( 
select  distinct c.row_id as contact_id 
        , a.row_id as asset_row_id 
        , p.prod_cd as asset_type 
        , p.name product_name 
        , case when p.name='NB_Parks_Resort_Bundle_Offer_2404' THEN 'P&R' else 'Thrifty' end OFFER_Product 
from gms.s_contact as c 
     
    inner join gms.s_asset as a 
        on a.owner_con_id = c.row_id 
         
    inner join gms.s_prod_int as p 
        on a.prod_id = p.row_id 
   
 
where 1=1 
    and p.name IN ('NB_Parks_Resort_Bundle_Offer_2404','NB_Thrifty Bundle Offer_NoJoinFee_0419') 
    and a.status_cd = 'Active' 
) select * FROM offer  
--group by 1 order by 2 DESC 
where contact_id in ('1-DRFXQ8T','1-DRFYPBR') 
select count(distinct c.contact_id) from campaign_data.aa_dmc2029_bluesubsretention_edm_20200501 c 
 
inner join gms.s_asset as a 
        on a.owner_con_id = c.contactid 
 
inner join gms.s_prod_int mp 
        on a.prod_id=mp.row_id 
 
where mp.name ='Membership' 
and a.status_cd = 'Active'  
With offer as( 
select  distinct c.row_id as contact_id 
        , a.row_id as asset_row_id_prom 
        , p.prod_cd as asset_type 
        , case when p.name='NB_Parks_Resort_Bundle_Offer_2404' THEN 'P&R' else 'Thrifty' end OFFER_Partner 
        , to_date(a.end_dt) as promotion_asset_end_dt 
from gms.s_contact as c 
     
    inner join gms.s_asset as a 
        on a.owner_con_id = c.row_id 
         
    inner join gms.s_prod_int as p 
        on a.prod_id = p.row_id 
   
 
where 1=1 
    and p.name IN ('NB_Parks_Resort_Bundle_Offer_2404','NB_Thrifty Bundle Offer_NoJoinFee_0419') 
    and a.status_cd = 'Active' 
    and to_date(adddate(a.end_dt,1)) BETWEEN '2020-06-15' and '2020-10-31' 
) 
 
select   count(DISTINCT c.row_id) as contact_id, count(a.row_id) 
--,c.x_nrma_household_flg  
--, org.pr_con_id 
        -- , p.name as current_prod 
        -- , nrp.name as next_renew_prod 
        -- , a.row_id as asset_row_id 
        -- , to_date(a.end_dt) as asset_end_dt 
        -- , p.prod_cd as asset_type 
        -- , o.* 
         
 
from gms.s_contact as c 
     
    inner join gms.s_org_ext as org  
	    on c.pr_dept_ou_id = org.row_id 
 
     
    inner join gms.s_asset as a 
        on a.owner_con_id = c.row_id 
         
    inner join gms.s_prod_int as p 
        on a.prod_id = p.row_id 
     
    INNER JOIN offer o                                          --#13215 
        on o.contact_id=c.row_id 
     
    LEFT JOIN gms.s_prod_int nrp 
        ON a.x_next_renewal_prod_id=nrp.row_id  
     
    inner join gms.s_contact_fnx fn                                              
        on fn.par_row_id=c.row_id                                   --#5829/5831 extra two asset_is's 
 
    inner join gms.s_addr_per as ad          
        on c.pr_per_addr_id = ad.row_id  
         
    INNER JOIN gms.s_contact_x cx 
        on cx.par_row_id = c.row_id 
     
     
where 1=1 
    and lower(p.name) ='nrma blue' 
    and a.status_cd = 'Active'                                      
    and p.type = 'Membership'                                       --#5829 
     
    and nrp.name='NB'    
    and c.csn is not null                                           --#5779 
    and c.cust_stat_cd = 'Active'                                   --#5700 
     
    and NVL(fn.deceased_flg,'N') = 'N' --Non deceased 
    and NVL(c.x_nrma_title,'no title') != 'Estate Of The Late'      --#5700 
     
    and NVL(ad.country,'Australia') = 'Australia'                   --#5686 
     
    and case when trim(c.x_inv_email_1) = 'Y' or c.email_addr is null then 'N' else 'Y' end = 'Y' 
    and NVL(cx.attrib_36,'Yes') != 'No'      
    
    and COALESCE(cx.attrib_44, '') not like 'Honorary%'  
    and NVL(cx.ATTRIB_46,'Email')!='Post'           --renewal statement preference  
     
    and a.row_id not in ('1-DRGC1FP','1-DRGC4AC','1-DRGC1EW') 
    and o.asset_row_id_prom not in ('1-DRGC1FP','1-DRGC4AC','1-DRGC1EW') 
     
drop table campaign_data.aa_dmc2029_blueSubsRetention_edm_20200501 
create table campaign_data.aa_dmc2029_blueSubsRetention_edm_20200501_v3 as 
With offer as( 
select  distinct c.row_id as contact_id 
        , a.row_id as asset_row_id_prom 
        , p.prod_cd as asset_type_prom 
        , case when p.name='NB_Parks_Resort_Bundle_Offer_2404' THEN 'P&R' else 'Thrifty' end OFFER_Partner 
        , to_date(a.end_dt) as promotion_asset_end_dt 
from gms.s_contact as c 
     
    inner join gms.s_asset as a 
        on a.owner_con_id = c.row_id 
         
    inner join gms.s_prod_int as p 
        on a.prod_id = p.row_id 
   
 
where 1=1 
    and p.name IN ('NB_Parks_Resort_Bundle_Offer_2404','NB_Thrifty Bundle Offer_NoJoinFee_0419') 
    and a.status_cd = 'Active' 
    and to_date(date_add(a.end_dt,1)) BETWEEN '2020-06-16' and '2020-10-31'          --Due date between 16 June 2020 and 31 Oct 2020 
) 
 
select  distinct c.row_id as contactid 
        , p.name as current_prod 
        , nrp.name as next_renew_prod 
        , a.row_id as asset_row_id_prod 
        , to_date(a.end_dt) as asset_end_dt 
        , p.prod_cd as asset_type_prod 
        , o.* 
         
 
from gms.s_contact as c 
     
    inner join gms.s_asset as a 
        on a.owner_con_id = c.row_id 
         
    inner join gms.s_prod_int as p 
        on a.prod_id = p.row_id 
     
    INNER JOIN offer o                                          --#13215 
        on o.contact_id=c.row_id 
     
    LEFT JOIN gms.s_prod_int nrp 
        ON a.x_next_renewal_prod_id=nrp.row_id  
     
    inner join gms.s_contact_fnx fn                                              
        on fn.par_row_id=c.row_id                                   --#5829/5831 extra two asset_is's 
 
    inner join gms.s_addr_per as ad          
        on c.pr_per_addr_id = ad.row_id 
     
    INNER JOIN gms.s_contact_x cx 
        on cx.par_row_id = c.row_id 
 
where 1=1 
    and lower(p.name) ='nrma blue' 
    and a.status_cd = 'Active'                                      
    and p.type = 'Membership'                                       --#5829 
     
    and nrp.name='NB'    
    and c.csn is not null                                           --#5779 
    and c.cust_stat_cd = 'Active'                                   --#5700 
     
    and NVL(fn.deceased_flg,'N') = 'N' --Non deceased 
    and NVL(c.x_nrma_title,'no title') != 'Estate Of The Late'      --#5700 
     
    and NVL(ad.country,'Australia') = 'Australia'                   --#5686 
    and COALESCE(cx.attrib_44, '') not like 'Honorary%' 
    and a.row_id not in ('1-DRGC1FP','1-DRGC4AC','1-DRGC1EW') 
    and o.asset_row_id_prom not in ('1-DRGC1FP','1-DRGC4AC','1-DRGC1EW') 
     
    and case when trim(c.x_inv_email_1) = 'Y' or c.email_addr is null then 'N' else 'Y' end = 'Y' 
    and NVL(cx.attrib_36,'Yes') != 'No'      
    and NVL(cx.ATTRIB_46,'Email')!='Post'           --renewal statement preference  
   
create table campaign_data.aa_dmc2029_bluesubsretention_edm_20200504 as 
select c.*,a.row_id as membership_asset_id,mp.name from campaign_data.aa_dmc2029_blueSubsRetention_edm_20200501_v3 c 
 
inner join gms.s_asset as a 
        on a.owner_con_id = c.contactid 
 
inner join gms.s_prod_int mp 
        on a.prod_id=mp.row_id 
 
where mp.name ='Membership' 
and a.status_cd = 'Active'  
and a.row_id!='1-DRGC1EE' 
SELECT count(*), count(DISTINCT contact_id) FROM campaign_data.aa_dmc2029_bluesubsretention_edm_20200504  
--where contact_id='1-DRFYPBR' 
select contactid,asset_end_dt from campaign_data.aa_dmc2029_blueSubsRetention_edm_20200504 
Total 4514 
email valid 4467 
email permission 3964 
spark.sql(''' 
With offer as( 
select  distinct c.row_id as contact_id 
        , a.row_id as asset_row_id 
        , p.prod_cd as asset_type 
        , case when p.name='NB_Parks_Resort_Bundle_Offer_2404' THEN 'P&R' else 'Thrifty' end OFFER_Partner 
        , to_date(a.end_dt) as promotion_asset_end_dt 
from gms.s_contact as c 
     
    inner join gms.s_asset as a 
        on a.owner_con_id = c.row_id 
         
    inner join gms.s_prod_int as p 
        on a.prod_id = p.row_id 
   
 
where 1=1 
    and p.name IN ('NB_Parks_Resort_Bundle_Offer_2404','NB_Thrifty Bundle Offer_NoJoinFee_0419') 
    and a.status_cd = 'Active' 
    and to_date(date_add(a.end_dt,1)) BETWEEN '2020-06-08' and '2020-10-31' 
) 
 
select   c.row_id as contact_id 
--,c.x_nrma_household_flg  
, org.pr_con_id,a.owner_con_id 
        -- , p.name as current_prod 
        -- , nrp.name as next_renew_prod 
        -- , a.row_id as asset_row_id 
        -- , to_date(a.end_dt) as asset_end_dt 
        -- , p.prod_cd as asset_type 
        -- , o.* 
         
 
from gms.s_contact as c 
     
    inner join gms.s_org_ext as org  
	    on c.pr_dept_ou_id = org.row_id 
 
     
    inner join gms.s_asset as a 
        on a.owner_con_id = c.row_id 
         
    inner join gms.s_prod_int as p 
        on a.prod_id = p.row_id 
     
    INNER JOIN offer o                                          --#13215 
        on o.contact_id=c.row_id 
     
    LEFT JOIN gms.s_prod_int nrp 
        ON a.x_next_renewal_prod_id=nrp.row_id  
     
    inner join gms.s_contact_fnx fn                                              
        on fn.par_row_id=c.row_id                                   --#5829/5831 extra two asset_is's 
 
    inner join gms.s_addr_per as ad          
        on c.pr_per_addr_id = ad.row_id  
         
    INNER JOIN gms.s_contact_x cx 
        on cx.par_row_id = c.row_id 
 
where 1=1 
    and lower(p.name) ='nrma blue' 
    and a.status_cd = 'Active'                                      
    and p.type = 'Membership'                                       --#5829 
     
    and nrp.name='NB'    
    and c.csn is not null                                           --#5779 
    and c.cust_stat_cd = 'Active'                                   --#5700 
     
    and NVL(fn.deceased_flg,'N') = 'N' --Non deceased 
    and NVL(c.x_nrma_title,'no title') != 'Estate Of The Late'      --#5700 
     
    and NVL(ad.country,'Australia') = 'Australia'                   --#5686 
     
    and case when trim(c.x_inv_email_1) = 'Y' or c.email_addr is null then 'N' else 'Y' end = 'Y' 
    and NVL(cx.attrib_36,'Yes') != 'No'      
    
    and COALESCE(cx.attrib_44, '') not like 'Honorary%'  
    --and COALESCE(fn.brloc_attrib13, '') != 'Y'  
    and NVL(cx.ATTRIB_46,'Email')!='Post'           --renewal statement preference  
     
     
    ''').show(10000,False) 
spark.sql(''' 
With offer as( 
select  distinct c.row_id as contact_id 
        , a.row_id as asset_row_id_prom 
        , p.prod_cd as asset_type 
        , case when p.name='NB_Parks_Resort_Bundle_Offer_2404' THEN 'P&R' else 'Thrifty' end OFFER_Partner 
        , to_date(a.end_dt) as promotion_asset_end_dt 
from gms.s_contact as c 
     
    inner join gms.s_asset as a 
        on a.owner_con_id = c.row_id 
         
    inner join gms.s_prod_int as p 
        on a.prod_id = p.row_id 
   
 
where 1=1 
    and p.name IN ('NB_Parks_Resort_Bundle_Offer_2404','NB_Thrifty Bundle Offer_NoJoinFee_0419') 
    and a.status_cd = 'Active' 
    and to_date(date_add(a.end_dt,1)) BETWEEN '2020-06-15' and '2020-10-31' 
) 
 
select   distinct a.row_id as asset_row_id,'NB_Offer' 
         
 
from gms.s_contact as c 
     
    inner join gms.s_org_ext as org  
	    on c.pr_dept_ou_id = org.row_id 
 
     
    inner join gms.s_asset as a 
        on a.owner_con_id = c.row_id 
         
    inner join gms.s_prod_int as p 
        on a.prod_id = p.row_id 
     
    INNER JOIN offer o                                          --#13215 
        on o.contact_id=c.row_id 
     
    LEFT JOIN gms.s_prod_int nrp 
        ON a.x_next_renewal_prod_id=nrp.row_id  
     
    inner join gms.s_contact_fnx fn                                              
        on fn.par_row_id=c.row_id                                   --#5829/5831 extra two asset_is's 
 
    inner join gms.s_addr_per as ad          
        on c.pr_per_addr_id = ad.row_id  
         
    INNER JOIN gms.s_contact_x cx 
        on cx.par_row_id = c.row_id 
 
where 1=1 
    and lower(p.name) ='nrma blue' 
    and a.status_cd = 'Active'                                      
    and p.type = 'Membership'                                       --#5829 
     
    and nrp.name='NB'    
    and c.csn is not null                                           --#5779 
    and c.cust_stat_cd = 'Active'                                   --#5700 
     
    and NVL(fn.deceased_flg,'N') = 'N' --Non deceased 
    and NVL(c.x_nrma_title,'no title') != 'Estate Of The Late'      --#5700 
     
    and NVL(ad.country,'Australia') = 'Australia'                   --#5686 
    and COALESCE(cx.attrib_44, '') not like 'Honorary%' 
    and a.row_id not in ('1-DRGC1FP','1-DRGC4AC','1-DRGC1EW') 
    and o.asset_row_id_prom not in ('1-DRGC1FP','1-DRGC4AC','1-DRGC1EW') 
     
    and case when trim(c.x_inv_email_1) = 'Y' or c.email_addr is null then 'N' else 'Y' end = 'Y' 
    and NVL(cx.attrib_36,'Yes') != 'No'      
    and NVL(cx.ATTRIB_46,'Email')!='Post'           --renewal statement preference  
     
     
    ''').show(10000,False) 
spark.sql(''' 
SELECT contactid,asset_row_id_prod,'NB_Offer' as NB_Offer FROM campaign_data.aa_dmc2029_bluesubsretention_edm_20200504 
''').show(10000,False) 
 
select  count(distinct c.row_id) as contact_id, count(a.x_next_renewal_prod_id) 
        --, count(oi.order_id) as OrderLineItemID 
        -- , p.name as current_prod 
        -- , nrp.name as next_renew_prod 
        -- , a.row_id as asset_row_id 
        -- , to_date(a.end_dt) as asset_end_dt 
        -- , p.prod_cd as asset_type 
        -- , o.* 
         
 
from gms.s_contact as c 
     
    inner join gms.s_asset as a 
        on a.owner_con_id = c.row_id 
         
    inner JOIN gms.s_prod_int nrp 
        ON a.x_next_renewal_prod_id=nrp.row_id 
     
    inner join gms.s_order_item oi 
        on oi.prod_id = nrp.row_id 
 
where 1=1 
    and nrp.name='NB_Offer'                                   --#5700 
select count(distinct oi.row_id) from gms.s_prod_int p 
 
inner join gms.s_order_item oi 
on oi.prod_id = p.row_id 
 
where name='NB_Offer' 
select count(distinct a.row_id) from gms.s_prod_int p 
 
inner join gms.s_asset a 
on a.prod_id = p.row_id 
 
where p.name='NB_Offer' 
simply_blue = spark.read.load("/user/aamreen/Simply_Freeblue_eligible_06_08.csv",format="csv", sep=",", inferSchema="true", header="true") 
simply_blue.createOrReplaceTempView("simply_blue") 
spark.sql('select * from simply_blue ').count() 
spark.sql(""" 
 
select  count(distinct REWARD_PLAN_CARD_ID) member_number, count(c.csn) 
--c.row_id as contact_id 
        --, a.row_id as asset_row_id 
        --, p.prod_cd as asset_type 
        --, p.name product_name 
from simply_blue s 
     
    --inner join gms.s_asset as a 
    --    on a.owner_con_id = c.row_id 
         
    --inner join gms.s_prod_int as p 
    --    on a.prod_id = p.row_id 
     
    left join gms.s_contact as c 
        on s.REWARD_PLAN_CARD_ID=c.csn 
   
 
where 1=1 
    --and p.name IN ('NB_Simply_Energy_Blue_Offer_0320') 
    --and a.status_cd = 'Active' 
    --ofcourseand to_date(adddate(a.end_dt,1)) BETWEEN '2020-04-01' and '2020-05-31' 
--REWARD_PLAN_CARD_ID 
""").show(1000,False) 
With offer_simply as( 
select  distinct c.row_id as contact_id 
        , a.row_id as asset_row_id 
        , p.prod_cd as asset_type 
        , p.name product_name 
from gms.s_contact as c 
     
    inner join gms.s_asset as a 
        on a.owner_con_id = c.row_id 
         
    inner join gms.s_prod_int as p 
        on a.prod_id = p.row_id 
   
 
where 1=1 
    and p.name IN ('NB_Simply_Energy_Blue_Offer_0320') 
    and a.status_cd = 'Active' 
    --and to_date(adddate(a.end_dt,1)) BETWEEN '2020-06-01' and '2020-10-31' 
 
) select count(*), count(DISTINCT contact_id) FROM offer_simply  
spark.sql(''' 
 
select   count(distinct a.row_id) as asset_row_id 
                ,count(distinct c.row_id) 
 
from gms.s_contact as c 
     
    inner join gms.s_org_ext as org  
	    on c.pr_dept_ou_id = org.row_id 
 
    inner join gms.s_asset as a 
        on a.owner_con_id = c.row_id 
         
    inner join gms.s_prod_int as p 
        on a.prod_id = p.row_id 
     
    INNER JOIN simply_blue o                                          --#13215 
        on o.REWARD_PLAN_CARD_ID=c.csn 
     
    LEFT JOIN gms.s_prod_int nrp 
        ON a.x_next_renewal_prod_id=nrp.row_id  
     
    inner join gms.s_contact_fnx fn                                              
        on fn.par_row_id=c.row_id                                   --#5829/5831 extra two asset_is's 
 
    inner join gms.s_addr_per as ad          
        on c.pr_per_addr_id = ad.row_id  
         
    INNER JOIN gms.s_contact_x cx 
        on cx.par_row_id = c.row_id 
 
where 1=1 
    and lower(p.name) ='nrma blue' 
    and a.status_cd = 'Active'                                      
    and p.type = 'Membership'                                       --#5829 
     
    and nrp.name='NB'    
    and c.csn is not null                                           --#5779 
    and c.cust_stat_cd = 'Active'                                   --#5700 
     
    and NVL(fn.deceased_flg,'N') = 'N' --Non deceased 
    and NVL(c.x_nrma_title,'no title') != 'Estate Of The Late'      --#5700 
     
    and NVL(ad.country,'Australia') = 'Australia'                   --#5686 
    and COALESCE(cx.attrib_44, '') not like 'Honorary%' 
     
    and case when trim(c.x_inv_email_1) = 'Y' or c.email_addr is null then 'N' else 'Y' end = 'Y' 
    and NVL(cx.attrib_36,'Yes') != 'No'      
    and NVL(cx.ATTRIB_46,'Email')!='Post'           --renewal statement preference  
     
     
    ''').show(10000,False) 
--spark.sql(''' 
select o.row_id 
--o.contact_id,o.row_id,o.x_ro_id,oi.order_id, oi.row_id ,to_date(o.ORDER_DT)  
 
from gms.s_order o 
 
inner join gms.s_order_type ot 
on o.order_type_id = ot.row_id 
 
inner join gms.s_order_item oi 
on o.row_id = oi.order_id 
and oi.ACTION_CD in ('Update','Add') --when they renew only update line but add is upgrade/downgrade and new 
 
inner join gms.s_prod_int p 
on oi.prod_id = p.row_id 
and p.prod_cd = 'Promotion' 
 
where 1=1 
 
and ot.name = 'Renew' 
--and o.status_cd != 'Revised' 
and p.name='NB_Offer' 
--and o.row_id in ('1-E5ODYL1','1-E5ODYQB','1-E5OSEUI') 
and o.status_cd='Submitted' 
 
---RESULTS ARE EXACT MATCH TO OMC LIST 
--order by 2 
--''').show(1000,False) 
 
spark.sql(''' 
select      o.row_id  
        ,to_date(o.ORDER_DT) 
        ,c.cust_stat_cd 
       -- ,fn.deceased_flg 
        ,c.x_nrma_title 
        , c.x_inv_email_1 
        , cx.attrib_36 
        , cx.ATTRIB_46 
--,o.contact_id,o.row_id,o.x_ro_id,oi.order_id, oi.row_id 
 
from gms.s_order o 
 
inner join gms.s_contact c 
on c.row_id = o.contact_id 
 
INNER JOIN gms.s_contact_x cx 
on cx.par_row_id = c.row_id 
 
inner join gms.s_order_type ot 
on o.order_type_id = ot.row_id 
 
inner join gms.s_order_item oi 
on o.row_id = oi.order_id 
and oi.ACTION_CD in ('Update','Add') --when they renew only update line but add is upgrade/downgrade and new 
 
inner join gms.s_prod_int p 
on oi.prod_id = p.row_id 
and p.prod_cd = 'Promotion' 
 
left anti join simply_blue s 
on s.DYNAMIC_SALES_ID=o.row_id 
 
where 1=1 
 
and ot.name = 'Renew' 
--and o.status_cd != 'Revised' 
and p.name='NB_Offer' 
--and o.row_id in ('1-E5ODYL1','1-E5ODYQB','1-E5OSEUI') 
and o.status_cd='Submitted' 
''').show(1000) 
simply_blue = spark.read.load("/user/aamreen/Consumer_Free_Blue_Retention_Orders_20200512.csv",format="csv", sep=",", inferSchema="true", header="true") 
simply_blue.createOrReplaceTempView("simply_blue") 
spark.sql('select * from simply_blue ').count() 
and lower(p.name) ='nrma blue' 
    and a.status_cd = 'Active'                                      
    and p.type = 'Membership'                                       --#5829 
     
    and nrp.name='NB'    
    and c.csn is not null                                           --#5779 
    and c.cust_stat_cd = 'Active'                                   --#5700 
     
    and NVL(fn.deceased_flg,'N') = 'N' --Non deceased 
    and NVL(c.x_nrma_title,'no title') != 'Estate Of The Late'      --#5700 
     
    and NVL(ad.country,'Australia') = 'Australia'                   --#5686 
    and COALESCE(cx.attrib_44, '') not like 'Honorary%' 
     
    and case when trim(c.x_inv_email_1) = 'Y' or c.email_addr is null then 'N' else 'Y' end = 'Y' 
    and NVL(cx.attrib_36,'Yes') != 'No'      
    and NVL(cx.ATTRIB_46,'Email')!='Post'           --renewal statement preference  