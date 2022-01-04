<p>Refer to notebook for pre-campaign analysis / tables creation : DMC-2280 batteries | Precampaign analysis</p><p>Refer to notebook for experimental design : DMC-2340 batteries | EOL | Exp Desgin<br></p> 
select * from campaign_data.ek_dmc2340_battery_exp_count 
-- drop table campaign_data.ek_dmc2340_battery_exp_count purge; 
create table campaign_data.ek_dmc2340_battery_exp_count as 
select ms.name, out.* 
from  
 
campaign_data.ek_dmc2280_battery_precount_0107 out 
 
left join gms.s_contact con   
on con.csn = out.csn  
  
left join gms.s_addr_per addr   
on addr.row_id = con.pr_per_addr_id  
  
left join geospatial.geocoded_mem_addr geo   
on geo.addr_id = addr.row_id   
  
left join geospatial.mb2wrka ms   
on cast(ms.mb_code16 as string) = geo.gnaf_mb   
  
where last_purchased in ('2017Q4','2018Q1','2018Q2') 
AND COALESCE(con.x_inv_email_1, 'N') = 'N' 
-- group by 1 
 
create table campaign_data.ek_dmc2341_battery_campaign_dmedm_20210428_adhoc as 
with pool as  
(select  cnt.* 
        , con.x_nrma_title 
        , con.fst_name 
        , con.mid_name 
        , con.last_name 
        , addr.addr_name 
        , addr.addr 
        , addr.addr_line_2 
        , addr.addr_line_3 
        , addr.addr_line_4 
        , addr.addr_line_5 
        , addr.city 
        , addr.zipcode 
        , addr.country 
        , con.email_addr 
        , case when dm_consent = 'Y' and edm_consent = 'Y' then 1 
               when dm_consent = 'Y' and edm_consent = 'N' then 2 
               else 0 end profile 
        ,rand() rnk 
from campaign_data.ek_dmc2340_battery_exp_count cnt 
inner join gms.s_contact con 
on cnt.csn = con.csn 
inner join gms.s_contact_fnx fn 
on fn.par_row_id = con.row_id 
 
inner join gms.s_addr_per as addr  
on addr.row_id = con.pr_per_addr_id  
 
where staff = 'N' 
        and con.cust_stat_cd = 'Active' 
        and con.con_cd in ('Ordinary Member','Affiliate Member') 
        and NVL(fn.deceased_flg,'N') = 'N' 
        and NVL(con.x_nrma_title, '') <> 'Estate Of The Late' 
        and dm_consent = 'Y' and name <> 'CSC'  
) 
,  
 
next_loop as 
( 
select *, rank() over (partition by profile order by rnk asc) rank_no 
         
from pool 
) 
 
select *, 
        (case    when profile = 1 and rank_no between 1 and 9798        then 'prof1_target_dm' 
                 when profile = 1 and rank_no between 9798 and 19596    then 'prof1_target_both' 
                 when profile = 1 and rank_no between 19596 and 29394   then 'prof1_target_edm' 
                 when profile = 1 and rank_no > 29394                   then 'prof1_control' 
                 when profile = 2 and rank_no between 1 and 12561       then 'prof2_target_dm' 
                 when profile = 2 and rank_no > 12561                   then 'prof2_control' 
                 else '' end) segment 
from next_loop 
 
 
drop table campaign_data.ek_dmc2341_battery_campaign_dmedm_20210428_adhoc purge 
select * from campaign_data.ek_dmc2341_battery_campaign_dmedm_20210428_adhoc  
select state, count(*) from  
( 
select distinct x_nrma_title, fst_name, mid_name, last_name, addr_name, addr as addr_line_1, addr_line_2, addr_line_3, addr_line_4, city, state, zipcode, country 
from campaign_data.ek_dmc2341_battery_campaign_dmedm_20210428_adhoc  
where segment in ('prof1_target_both','prof1_target_dm','prof2_target_dm') 
and state in ('ACT','NSW') 
) t 
group by 1 
select segment, count(*) from  
( 
select distinct x_nrma_title, fst_name, mid_name, last_name, addr_name, addr as addr_line_1, addr_line_2, addr_line_3, addr_line_4, city, state, zipcode, country, segment 
from campaign_data.ek_dmc2341_battery_campaign_dmedm_20210428_adhoc  
where state in ('ACT','NSW') 
) t 
group by 1 
-- DM 
select state,count(*) from  
( 
select distinct x_nrma_title, fst_name, mid_name, last_name, addr_name, addr as addr_line_1, addr_line_2, addr_line_3, addr_line_4, city, state, zipcode, country  
from campaign_data.ek_dmc2341_battery_campaign_dmedm_20210428_adhoc  
where state in ('ACT','NSW') 
        and segment in ('prof1_target_both','prof1_target_dm','prof2_target_dm') 
        ) a 
        group by 1 
 
-- edm 
select count(*) from  
( 
select distinct contact_id, segment 
        , case when segment in ('prof1_target_both', 'prof1_target_edm') then 'T' else 'C' end edm_target 
        , case when segment in ('prof1_control', 'prof2_control') then 'C' else 'T' end overall_campaign_target_control 
from campaign_data.ek_dmc2341_battery_campaign_dmedm_20210428_adhoc  
where state in ('ACT','NSW') 
) a 