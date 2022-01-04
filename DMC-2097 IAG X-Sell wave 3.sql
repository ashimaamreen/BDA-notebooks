select distinct campaign_id, campaign_name from omc.member_campaign_summary_table 
where lower(campaign_name) like '%iag%' 
SELECT count(DISTINCT member_number) from omc.send_level_summary 
where campaign_id='43214642' 
select * from sandpit.iag_mot_base_20200715_ma_gbtclassifier_pred 
select prediction, max(probability), min(probability) from sandpit.iag_mot_base_20200715_ma_gbtclassifier_pred 
group by 1 
order by 1 
select m.membernumber, min(m.decile) from ( 
select * 
        , ntile(10) over (order by cast(x.probability as float) desc) as decile 
 
from sandpit.iag_mot_base_20200715_ma_gbtclassifier_pred x 
) m 
group by 1 
with main_table as( 
select m.membernumber, min(m.decile) as band from  
(select * 
        --, rank() over (order by cast(x.probability as float) desc) as ranking 
        , ntile(10) over (order by cast(x.probability as float) desc) as decile 
 
from sandpit.iag_mot_base_20200715_ma_gbtclassifier_pred x) m 
group by 1 
) 
select m.band, count(distinct c.csn)  
 
from gms.s_contact c                     --1951754 
 
inner join main_table m       --1951754 
on c.csn=m.membernumber 
 
left anti join omc.send_level_summary e     --1862527 
on e.member_number=m.membernumber 
and campaign_id ='43214642' 
 
left anti join m4m.return_feed_header iag       --760535 
on iag.member_number=c.csn 
and partner in ('IAG','NRMA Multi Policy Discount','NRMA MPD Insurance','NRMA Insurance') 
and datediff(now(),iag.time_stamp)<1100 
 
inner JOIN gms.s_contact_x cx 
on cx.par_row_id=c.row_id 
 
inner JOIN gms.s_contact_fnx fn             --760535 
on fn.par_row_id=c.row_id 
 
inner join gms.s_org_ext as org 
on org.pr_con_id = c.row_id 
 
where 1=1 
and c.csn is not null 
and c.con_cd in ('Ordinary Member' , 'Affiliate Member')        --893688 
 
and NVL(fn.deceased_flg,'N') = 'N' 
and NVL(c.x_nrma_title,'no title') != 'Estate Of The Late'      --751244 
 
and (cx.attrib_36 ='Yes' or Upper(cx.attrib_36) ='NULL')                                                                
and case when trim(c.x_inv_email_1) = 'Y' or c.email_addr is null then 'N' else 'Y' end = 'Y'       --343614 
 
group by 1 
order by 1 
select band,count(distinct contact_id)  from ( 
with main_table as( 
select m.membernumber, min(m.decile) as band from  
(select * 
        --, rank() over (order by cast(x.probability as float) desc) as ranking 
        , ntile(10) over (order by cast(x.probability as float) desc) as decile 
 
from sandpit.iag_mot_base_20200715_ma_gbtclassifier_pred x) m 
group by 1 
) 
select distinct c.row_id as contact_id,band,rand(0) as rng  
 
from gms.s_contact c                     --1951754 
 
inner join main_table m       --1951754 
on c.csn=m.membernumber 
 
left anti join omc.send_level_summary e     --1862527 
on e.member_number=m.membernumber 
and campaign_id ='43214642' 
 
left anti join m4m.return_feed_header iag       --903490 
on iag.member_number=c.csn 
and partner in ('IAG','NRMA Multi Policy Discount','NRMA MPD Insurance','NRMA Insurance') 
and datediff(now(),iag.time_stamp)<1100 
 
inner JOIN gms.s_contact_x cx 
on cx.par_row_id=c.row_id 
 
inner JOIN gms.s_contact_fnx fn             --903490 
on fn.par_row_id=c.row_id 
 
inner join gms.s_org_ext as org 
on org.pr_con_id = c.row_id 
 
where 1=1 
and c.csn is not null 
 
and NVL(fn.deceased_flg,'N') = 'N' 
and NVL(c.x_nrma_title,'no title') != 'Estate Of The Late'      --899132 
 
and c.con_cd in ('Ordinary Member' , 'Affiliate Member')        --893688 
 
and (cx.attrib_36 ='Yes' or Upper(cx.attrib_36) ='NULL')                                                                
and case when trim(c.x_inv_email_1) = 'Y' or c.email_addr is null then 'N' else 'Y' end = 'Y'       --459346 
 
) aa 
where aa.rng<0.2 
group by 1 
order by 1 
with main_table as( 
select m.membernumber, min(m.decile) as band from  
 
(select * 
        , ntile(10) over (order by cast(x.probability as float) desc) as decile 
 
from sandpit.iag_mot_base_20200715_ma_gbtclassifier_pred x) m 
group by 1 
) 
with main_table as( 
select m.membernumber, min(m.decile) as band from  
 
(select * 
        , ntile(10) over (order by cast(x.probability as float) desc) as decile 
 
from sandpit.iag_mot_base_20200715_ma_gbtclassifier_pred x) m 
group by 1 
) 
select distinct c.row_id as contact_id,band,rand(0) as rng  
 
from gms.s_contact c                     --1951754 
 
inner join main_table m       --1951754 
on c.csn=m.membernumber 
 
left anti join omc.send_level_summary e     --1862527 
on e.member_number=m.membernumber 
and campaign_id ='43214642' 
 
left anti join m4m.return_feed_header iag       --903490 
on iag.member_number=c.csn 
and partner in ('IAG','NRMA Multi Policy Discount','NRMA MPD Insurance','NRMA Insurance') 
and datediff(now(),iag.time_stamp)<1100 
 
inner JOIN gms.s_contact_x cx 
on cx.par_row_id=c.row_id 
 
inner JOIN gms.s_contact_fnx fn             --903490 
on fn.par_row_id=c.row_id 
 
inner join gms.s_org_ext as org 
on org.pr_con_id = c.row_id 
 
where 1=1 
and c.csn is not null 
 
and NVL(fn.deceased_flg,'N') = 'N' 
and NVL(c.x_nrma_title,'no title') != 'Estate Of The Late'      --899132 
 
and c.con_cd in ('Ordinary Member' , 'Affiliate Member')        --893688 
 
and (cx.attrib_36 ='Yes' or Upper(cx.attrib_36) ='NULL')                                                                
and case when trim(c.x_inv_email_1) = 'Y' or c.email_addr is null then 'N' else 'Y' end = 'Y'       --459346 
 
create table campaign_data.aa_dmc2097_IAGwave3_campaign_edm_20200720_adhoc as 
select *, 
        case when rng<0.1 then '3A_Random' 
            when band in (1,2) then '3A_Top decile' 
            when rng<0.38 then '3B_TPFT' else 'None' end segment 
    from ( 
with main_table as( 
select m.membernumber, min(m.decile) as band from  
(select * 
        , ntile(10) over (order by cast(x.probability as float) desc) as decile 
 
from sandpit.iag_mot_base_20200715_ma_gbtclassifier_pred x) m 
group by 1 
) 
select distinct c.row_id as contact_id 
        --, c.csn as member_number 
        , m.band 
        , rand(0) as rng  
 
from gms.s_contact c                     --1951754 
 
inner join main_table m       --1951754 
on c.csn=m.membernumber 
 
left anti join omc.send_level_summary e     --1862527 
on e.member_number=m.membernumber 
and campaign_id ='43214642' 
 
left anti join m4m.return_feed_header iag       --903490 
on iag.member_number=c.csn 
and partner in ('IAG','NRMA Multi Policy Discount','NRMA MPD Insurance','NRMA Insurance') 
--and datediff(now(),iag.time_stamp)<1100 
 
inner JOIN gms.s_contact_x cx 
on cx.par_row_id=c.row_id 
 
inner JOIN gms.s_contact_fnx fn             --903490 
on fn.par_row_id=c.row_id 
 
where 1=1 
and c.csn is not null 
 
and NVL(fn.deceased_flg,'N') = 'N' 
and NVL(c.x_nrma_title,'no title') != 'Estate Of The Late'      --899132 
 
-- and c.con_cd in ('Ordinary Member' , 'Affiliate Member')        --893688 
 
-- and (cx.attrib_36 ='Yes' or Upper(cx.attrib_36) ='NULL')                                                                
-- and case when trim(c.x_inv_email_1) = 'Y' or c.email_addr is null then 'N' else 'Y' end = 'Y'       --459346 
) aa 
--where aa.rng<0.1 
select contact_id, segment from campaign_data.aa_dmc2097_IAGwave3_campaign_edm_20200720_adhoc 
where segment<>'None' 
select band, count(distinct row_id) from campaign_data.aa_dmc2097_IAGwave3_campaign_edm_20200720_adhoc 
where segment='3B_TPFT' 
group by 1 
order by 1 
DROP TABLE campaign_data.aa_dmc2097_iagwave3_campaign_edm_20200720_adhoc; 