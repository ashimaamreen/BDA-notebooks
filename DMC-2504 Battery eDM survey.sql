with cad_jobs as ( 
select distinct to_date(s.created) as created_dt 
    , s.asset_id 
    , a.owner_accnt_id 
    , a.owner_con_id 
    , s.sr_category_cd as bkdn_cd 
    , s.root_cause_cd as bkdn_reason 
 
from gms.s_srv_req_x as srx  
 
inner join gms.s_srv_req as s 
on srx.row_id = s.row_id 
and srx.attrib_04 is not null 
 
inner join gms.s_asset a 
on a.row_id=s.asset_id 
 
where 
    1=1 
    and to_date(s.created)> '2021-07-10' 
    and s.x_bulk_source = "CAD"  
    and s.sr_category_cd in ('202','203','204','212','213','214','882','885') 
), main as ( 
select   c.row_id as contact_id 
        ,c.csn as Member_Number 
        ,c.fst_name AS First_Name 
        ,c.last_name AS Last_Name 
        ,c.email_addr AS Email_Address  
        ,cx.attrib_55  AS Colour_Plus 
        ,case   when o.segment rlike 'ACT' then 'ACT' 
                when o.segment rlike 'Metro' then 'Metro' 
                else 'Other NSW' end segment_location 
        ,case   when o.open_event_date is not null or o.click_event_date is not null then 'Y' else 'N' end eDM_Open 
        ,case   when o.click_event_date is not null then 'Y' else 'N' end eDM_Click 
        ,o.clicked_elements 
        ,group_concat(cad.bkdn_cd,' | ') as breakdown_cd 
 
from gms.s_contact as c 
 
inner join gms.s_contact_x as cx  
on c.row_id = cx.par_row_id 
 
inner join omc.send_level_summary o 
on o.customer_id=c.row_id 
 
left join cad_jobs cad 
on cad.owner_con_id=c.row_id                                                -- 
--and datediff(to_date(o.send_event_date),cad.created_dt) in (1,2)          --2807 
--cad.created_dt<to_date(o.send_event_date) 
 
where c.cust_stat_cd = 'Active' 
        and c.con_cd in ('Ordinary Member' , 'Affiliate Member') 
        and NVL(c.x_nrma_title,'no title') != 'Estate Of The Late' 
        and case when trim(c.x_inv_email_1) = 'Y' or c.email_addr is null then 'N' else 'Y' end = 'Y' 
        and o.campaign_id='51388702' 
        and control IS FALSE 
        and datediff(now(),o.send_event_date) between 0 and 42 
        --and cad.created_dt<to_date(o.send_event_date) 
         
group by 1,2,3,4,5,6,7,8,9,10 
) 
-- select Member_Number,count(*) from main 
-- group by 1 
-- order by 2 desc 
 
select count(distinct Member_Number) from main 
where breakdown_cd is null 
-- where Member_Number in ('68975401','990741892') 
drop table campaign_data.aa_dmc2504_stream4batteryeDM_research_others_20210825 
create table campaign_data.aa_dmc2504_stream4batteryeDM_research_others_20210829 as 
with cad_jobs as ( 
select distinct to_date(s.created) as created_dt 
    , s.asset_id 
    , a.owner_accnt_id 
    , a.owner_con_id 
    , s.sr_category_cd as bkdn_cd 
    , s.root_cause_cd as bkdn_reason 
 
from gms.s_srv_req_x as srx  
 
inner join gms.s_srv_req as s 
on srx.row_id = s.row_id 
and srx.attrib_04 is not null 
 
inner join gms.s_asset a 
on a.row_id=s.asset_id 
 
where 
    1=1 
    and to_date(s.created)> '2021-07-10' 
    and s.x_bulk_source = "CAD"  
    and s.sr_category_cd in ('202','203','204','212','213','214','882','885') 
) 
select   c.row_id as contact_id 
        ,c.csn as Member_Number 
        ,c.fst_name AS First_Name 
        ,c.last_name AS Last_Name 
        ,c.email_addr AS Email_Address  
        ,cx.attrib_55  AS Colour_Plus 
        ,case   when o.segment rlike 'ACT' then 'ACT' 
                when o.segment rlike 'Metro' then 'Metro' 
                else 'Other NSW' end segment_location 
        ,case   when o.open_event_date is not null or o.click_event_date is not null then 'Y' else 'N' end eDM_Open 
        ,case   when o.click_event_date is not null then 'Y' else 'N' end eDM_Click 
        ,o.clicked_elements 
        ,group_concat(cad.bkdn_cd,' | ') as breakdown_codes 
 
from gms.s_contact as c 
 
inner join gms.s_contact_x as cx  
on c.row_id = cx.par_row_id 
 
inner join omc.send_level_summary o 
on o.customer_id=c.row_id 
 
left join cad_jobs cad 
on cad.owner_con_id=c.row_id 
--and datediff(to_date(o.send_event_date),cad.created_dt) in (1,2) 
 
where c.cust_stat_cd = 'Active' 
        and c.con_cd in ('Ordinary Member' , 'Affiliate Member') 
        and NVL(c.x_nrma_title,'no title') != 'Estate Of The Late' 
        and case when trim(c.x_inv_email_1) = 'Y' or c.email_addr is null then 'N' else 'Y' end = 'Y' 
        and o.campaign_id='51388702' 
        and control IS FALSE 
        and datediff(now(),o.send_event_date) between 0 and 42 
         
group by 1,2,3,4,5,6,7,8,9,10 
SELECT * from campaign_data.aa_dmc2504_stream4batteryeDM_research_others_20210829 
SELECT count(DISTINCT o.customer_id)  
 
from omc.send_level_summary o 
 
 
where o.campaign_id='51388702' 
and control IS FALSE 
and datediff(now(),o.send_event_date) between 0 and 42 
--and o.bounce_event_date is 
 
SELECT case when o.segment rlike 'ACT' then 'ACT' 
            when o.segment rlike 'Metro' then 'Metro' 
            else 'Other NSW' end segment 
        , count(DISTINCT o.customer_id)  
 
from omc.send_level_summary o 
 
 
where o.campaign_id='51388702' 
and control IS FALSE 
and datediff(now(),o.send_event_date) between 0 and 42 
--and o.bounce_event_date is 
GROUP BY 1 
order by 1 
select distinct to_date(s.created) as created_dt 
    , s.cst_ou_id -- member's organisation id 
    , s.cst_con_id -- member contact id 
    , s.asset_id 
    , a.owner_accnt_id 
    , a.owner_con_id 
    , s.sr_category_cd as bkdn_cd 
    , s.root_cause_cd as bkdn_reason 
 
from gms.s_srv_req_x as srx  
 
inner join gms.s_srv_req as s 
on srx.row_id = s.row_id 
and srx.attrib_04 is not null 
 
inner join gms.s_asset a 
on a.row_id=s.asset_id 
 
where 
    1=1 
    and to_date(s.created)> '2021-07-10' 
    and s.x_bulk_source = "CAD"  
    and s.sr_category_cd in ('202','203','204','212','213','214','882','885') 
select max(s.created) as created_dt 
    -- , s.cst_ou_id -- member's organisation id 
    -- , s.cst_con_id -- member contact id 
    -- , s.asset_id 
    -- , a.owner_accnt_id 
    -- , a.owner_con_id 
    -- , s.sr_category_cd as bkdn_cd 
    -- , s.root_cause_cd as bkdn_reason 
 
from gms.s_srv_req_x as srx  
 
inner join gms.s_srv_req as s 
on srx.row_id = s.row_id 
and srx.attrib_04 is not null 
 
inner join gms.s_asset a 
on a.row_id=s.asset_id 
 
where 
    1=1 
    and to_date(s.created)> '2021-07-10' 
    and s.x_bulk_source = "CAD"  
    and s.sr_category_cd in ('202','203','204','212','213','214','882','885') 