SELECT e.campaign_id,e.control,count(DISTINCT e.customer_id) from omc.send_level_summary e 
where e.program_id='57355262' 
GROUP BY 1,2 
ORDER BY 1,2 
SELECT * from omc.member_campaign_summary_table 
where program_id='57355262' 
SELECT e.campaign_id,e.control,count(DISTINCT h.customer_id) from omc.send_level_summary e 
 
INNER JOIN ek_dmc2353_hack_20210503 h 
on h.riid=e.riid 
where e.program_id='57355262' 
GROUP BY 1,2 
ORDER BY 1,2 
SELECT e.campaign_id 
        ,case when e.campaign_id='55469582' then 'RSA Non-Open' 
            when e.campaign_id='57354642' then 'Non-RSA Non-Open' 
            when e.campaign_id='57354722' then 'RSA No-Clcik' 
            when e.campaign_id='57354802' then 'Non-RSA No-Clcik' 
            else 'other' end Segment 
        ,e.control,count(DISTINCT e.customer_id) from omc.send_level_summary e 
 
where e.program_id='57355262' 
and e.bounce_event_date is null 
-- and (e.open_event_date is not null or e.click_event_date is not null) 
-- and e.click_event_date is not null 
GROUP BY 1,2,3 
ORDER BY 1,2,3 
SELECT e.campaign_id 
        , e.control 
        --, count(DISTINCT e.customer_id)  
        , count(DISTINCT n.customer_id) 
 
from omc.send_level_summary e 
 
left join omc.send_level_summary n 
on n.customer_id=e.customer_id 
 
where e.program_id='57355262' 
and e.bounce_event_date is null 
and n.send_event_date>e.send_event_date 
and n.click_event_date is not null 
and datediff(n.send_event_date,e.send_event_date) between 1 and 60 
 
GROUP BY 1,2 
ORDER BY 1,2 
SELECT e.campaign_id 
        ,case when e.campaign_id='55469582' then 'RSA Non-Open' 
            when e.campaign_id='57354642' then 'Non-RSA Non-Open' 
            when e.campaign_id='57354722' then 'RSA No-Clcik' 
            when e.campaign_id='57354802' then 'Non-RSA No-Clcik' 
            else 'other' end Segment 
        , e.control 
        --, count(DISTINCT e.customer_id)  
        , count(DISTINCT n.customer_id) 
 
from omc.send_level_summary e 
 
INNER JOIN gms.s_contact c 
on c.row_id=e.customer_id 
 
left join omc.send_level_summary n 
on n.customer_id=e.customer_id 
 
where e.program_id='57355262' 
and e.bounce_event_date is null 
and n.send_event_date>e.send_event_date 
and n.click_event_date is not null 
and datediff(n.send_event_date,e.send_event_date) between 1 and 60 
 
GROUP BY 1,2,3 
ORDER BY 1,2,3 
SELECT m.partner 
        , e.control 
        --, count(DISTINCT e.customer_id)  
        , count(DISTINCT n.customer_id) 
 
from omc.send_level_summary e 
 
INNER JOIN gms.s_contact c 
on c.row_id=e.customer_id 
 
left join omc.send_level_summary n 
on n.customer_id=e.customer_id 
 
inner JOIN m4m.return_feed_header m 
on m.member_number=c.csn 
and m.partner not in ('IAG','NRMA Multi Policy Discount','NRMA MPD Insurance','NRMA Insurance') 
 
where e.program_id='57355262' 
and e.bounce_event_date is null 
--and n.send_event_date>e.send_event_date 
--and n.click_event_date is not null 
and datediff(to_date(m.time_stamp),to_date(e.send_event_date)) between 1 and 60 
and e.campaign_id='55469582' 
 
 
GROUP BY 1,2 
ORDER BY 1,2 
SELECT e.campaign_id 
        , e.control 
        --, count(DISTINCT e.customer_id)  
        , count(DISTINCT n.customer_id) 
 
from omc.send_level_summary e 
 
INNER JOIN gms.s_contact c 
on c.row_id=e.customer_id 
 
left join omc.send_level_summary n 
on n.customer_id=e.customer_id 
 
inner JOIN m4m.return_feed_header m 
on m.member_number=c.csn 
and m.partner not in ('IAG','NRMA Multi Policy Discount','NRMA MPD Insurance','NRMA Insurance') 
 
where e.program_id='57355262' 
and e.bounce_event_date is null 
--and n.send_event_date>e.send_event_date 
--and n.click_event_date is not null 
and datediff(to_date(m.time_stamp),to_date(e.send_event_date)) between 1 and 60 
 
GROUP BY 1,2 
ORDER BY 1,2 
with opens as ( 
SELECT  customer_id, sum(case when e.open_event_date is not null then 1 else 0 end) opens 
        --, count(DISTINCT customer_id)  
 
from omc.send_level_summary e 
 
INNER JOIN gms.s_contact c 
on c.row_id=e.customer_id 
and to_date(c.x_nrma_join_dt) BETWEEN '2020-01-01' and '2020-12-31' 
 
where datediff(e.send_event_date,c.x_nrma_join_dt) BETWEEN 150 and 180 
and e.bounce_event_date is null 
and e.channel='Email' 
GROUP BY 1 
) 
select case when opens=0 then 'No Email' else 'others' end type, count(customer_id) from opens 
group by 1 
SELECT e.campaign_id 
        , e.control 
        --, count(DISTINCT e.customer_id)  
        , count(DISTINCT n.customer_id) 
 
from omc.send_level_summary e 
 
INNER JOIN gms.s_contact c 
on c.row_id=e.customer_id 
 
left join omc.send_level_summary n 
on n.customer_id=e.customer_id 
 
inner join sandpit.ga_appsession app 
on app.membernumber=c.csn                                                             --24980 
and datediff(app.event_dt,e.send_event_date) between 0 and 60 
 
where e.program_id='57355262' 
and e.bounce_event_date is null 
--and n.send_event_date>e.send_event_date 
--and n.click_event_date is not null 
--and datediff(to_date(m.time_stamp),to_date(e.send_event_date)) between 1 and 60 
 
GROUP BY 1,2 
ORDER BY 1,2 
with opens as ( 
SELECT  customer_id, sum(case when e.open_event_date is not null then 1 else 0 end) opened 
 
from omc.send_level_summary e 
 
INNER JOIN gms.s_contact c 
on c.row_id=e.customer_id 
and year(c.x_nrma_join_dt)=2020 
 
where datediff(e.send_event_date,c.x_nrma_join_dt)<151 
and e.bounce_event_date is null 
and e.channel='Email' 
GROUP BY 1 
), 
--select case when opens=0 then 'No Email' else 'others' end type, count(customer_id) from opens 
--group by 1 
 
blue_eligible as ( 
select distinct 
         c.csn as member_number 
        ,c.row_id as contact_id 
    --    ,o.row_id as account_id 
        ,c.cust_stat_cd as cust_stat 
        ,c.con_cd as contact_type 
	    ,c.x_nrma_household_flg as hh 
    --	,nvl(o.pr_con_id,'N') = 'Y' as primary 
    	,c.email_addr as email_address 
    	, c.x_nrma_join_dt as join_date 
    	, o.opened 
 
    from gms.s_contact as c  
     
	--inner join gms.s_org_ext as o  
	--on c.pr_dept_ou_id = o.row_id 
 
	inner join gms.s_asset as a  
	on a.owner_accnt_id = c.pr_dept_ou_id 
 
	inner join gms.s_prod_int as p 
       on a.prod_id = p.row_id 
        
    inner join gms.s_contact_fnx as fn  
       on fn.par_row_id = c.row_id  
      
    inner join opens o 
    on o.customer_id=c.row_id 
     
    where a.status_cd = 'Active' 
    and p.type = 'Membership' 
    and p.prod_cd = 'Promotion' 
    and NVL(fn.deceased_flg,'N') = 'N' 
    and c.csn is not null 
) 
select  month(join_date) 
--regexp_replace(email_address,'.*@','') 
        , count(distinct contact_id) 
from blue_eligible 
where opened=0 
group by 1 
order by 1 
with opens as ( 
SELECT  customer_id, sum(case when e.click_event_date is not null then 1 else 0 end) clicked 
 
from omc.send_level_summary e 
 
INNER JOIN gms.s_contact c 
on c.row_id=e.customer_id 
and year(c.x_nrma_join_dt)=2020 
 
where datediff(e.send_event_date,c.x_nrma_join_dt)<151 
and e.bounce_event_date is null 
and e.channel='Email' 
GROUP BY 1 
), 
--select case when opens=0 then 'No Email' else 'others' end type, count(customer_id) from opens 
--group by 1 
 
blue_eligible as ( 
select distinct 
         c.csn as member_number 
        ,c.row_id as contact_id 
    --    ,o.row_id as account_id 
        ,c.cust_stat_cd as cust_stat 
        ,c.con_cd as contact_type 
	    ,c.x_nrma_household_flg as hh 
    --	,nvl(o.pr_con_id,'N') = 'Y' as primary 
    	,c.email_addr as email_address 
    	, c.x_nrma_join_dt as join_date 
    	, o.clicked 
 
    from gms.s_contact as c  
     
	--inner join gms.s_org_ext as o  
	--on c.pr_dept_ou_id = o.row_id 
 
	inner join gms.s_asset as a  
	on a.owner_accnt_id = c.pr_dept_ou_id 
 
	inner join gms.s_prod_int as p 
       on a.prod_id = p.row_id 
        
    inner join gms.s_contact_fnx as fn  
       on fn.par_row_id = c.row_id  
      
    inner join opens o 
    on o.customer_id=c.row_id 
     
    where a.status_cd = 'Active' 
    and p.type = 'Membership' 
    and p.prod_cd = 'Promotion' 
    and NVL(fn.deceased_flg,'N') = 'N' 
    and c.csn is not null 
) 
select month(join_date) 
        , count(distinct contact_id) 
from blue_eligible 
where clicked=0 
group by 1 
order by 1 
with first_open as ( 
SELECT customer_id 
        , to_date(c.x_nrma_join_dt) as join_date 
        , to_date(min(e.open_event_date)) frst 
 
from omc.send_level_summary e 
 
INNER JOIN gms.s_contact c 
on c.row_id=e.customer_id 
 
inner join gms.s_contact_x cx 
on cx.par_row_id=c.row_id 
 
 
where e.send_event_date>c.x_nrma_join_dt 
and e.bounce_event_date is null 
and e.channel='Email' 
and c.cust_stat_cd = 'Active' 
and c.con_cd in ('Ordinary Member' , 'Affiliate Member') 
and case when trim(c.x_inv_email_1) = 'Y' or c.email_addr is null or cx.attrib_36='No' then 'N'  else 'Y' end = 'Y' 
and year(c.x_nrma_join_dt)=2020 
 
GROUP BY 1,2 
) 
select round(datediff(frst,join_date)/30) as months_since_join  
        ,count(distinct customer_id)  
from first_open 
 
group by 1 
order by 1 
with opens as ( 
SELECT  customer_id 
        , sum(case when e.click_event_date is not null then 1 else 0 end) clicked 
        , sum(case when e.open_event_date is not null then 1 else 0 end) opened 
 
from omc.send_level_summary e 
 
INNER JOIN gms.s_contact c 
on c.row_id=e.customer_id 
and year(c.x_nrma_join_dt)=2020 
 
where datediff(e.send_event_date,c.x_nrma_join_dt)<151 
and e.bounce_event_date is null 
and e.channel='Email' 
GROUP BY 1 
), blue_eligible as ( 
 
select distinct 
         c.csn as member_number 
        ,c.row_id as contact_id 
    --    ,o.row_id as account_id 
        ,c.cust_stat_cd as cust_stat 
        ,c.con_cd as contact_type 
	    ,c.x_nrma_household_flg as hh 
    --	,nvl(o.pr_con_id,'N') = 'Y' as primary 
    	,c.email_addr as email_address 
    	, c.x_nrma_join_dt as join_date 
    	, o.clicked 
    	, o.opened 
    	, case when renewed_order_id is null then 'Non-renewed' else 'Renewed' end renewal 
 
    from gms.s_contact as c  
     
	--inner join gms.s_org_ext as o  
	--on c.pr_dept_ou_id = o.row_id 
 
	inner join gms.s_asset as a  
	on a.owner_accnt_id = c.pr_dept_ou_id 
 
	inner join gms.s_prod_int as p 
       on a.prod_id = p.row_id 
        
    inner join gms.s_contact_fnx as fn  
       on fn.par_row_id = c.row_id  
      
    inner join opens o 
    on o.customer_id=c.row_id 
     
    left join sandpit.renewal_base b 
    on b.contact_id=c.row_id 
    and year(order_end_dt)=2021 
     
    where a.status_cd = 'Active' 
    and p.type = 'Membership' 
    and p.prod_cd = 'Promotion' 
    and NVL(fn.deceased_flg,'N') = 'N' 
    and c.csn is not null 
) 
 
select renewal 
        , count(distinct contact_id) 
 
from blue_eligible 
where opened<>0 
group by 1 
order by 1 
SELECT case when renewed_order_id is null then 'Non-renewed' else 'Renewed' end renewal 
        , count(DISTINCT contact_id) 
from sandpit.renewal_base b 
 
where to_date(date_add(order_end_dt,2))>'2021-01-01'  
--and item_base_price=96 
 
group by 1 
order by 1 
select m.campaign_name 
        , e.campaign_id 
        , count(e.customer_id) 
        , sum(case when e.open_event_date is not null then 1 else 0 end) 
        , sum(case when e.click_event_date is not null then 1 else 0 end) 
 
from omc.send_level_summary e 
 
inner join omc.member_campaign_summary_table m 
on m.campaign_id=e.campaign_id 
 
where e.bounce_event_date is null 
and e.send_event_date>'2020-01-01' 
and e.channel='Email' 
and m.campaign_type='PromotionalCampaign' 
 
 
group by 1,2 
order by 3 desc 
select m.campaign_name 
        , e.campaign_id 
        , count(e.customer_id) 
        , sum(case when e.open_event_date is not null then 1 else 0 end) 
        , sum(case when e.click_event_date is not null then 1 else 0 end) 
 
from omc.send_level_summary e 
 
inner join omc.member_campaign_summary_table m 
on m.campaign_id=e.campaign_id 
 
where e.bounce_event_date is null 
and e.send_event_date>'2020-01-01' 
and e.channel='Email' 
and m.campaign_type='PromotionalCampaign' 
 
 
group by 1,2 
order by 3 desc 
with main as ( 
SELECT  DISTINCT  
         e.customer_id 
        ,case when e.campaign_id='55469582' then 'RSA Non-Open' 
            when e.campaign_id='57354642' then 'Non-RSA Non-Open' 
            when e.campaign_id='57354722' then 'RSA No-Clcik' 
            when e.campaign_id='57354802' then 'Non-RSA No-Clcik' 
            else 'other' end Segment 
        , e.control 
        , to_date(e.send_event_date) as eng_send 
        , case when e.open_event_date is not null or e.click_event_date is not null then 1 else 0 end open 
        , case when e.click_event_date is not null then 1 else 0 end click 
         
from omc.send_level_summary e 
 
where e.program_id='57355262' 
) 
select  case  
            when lower(c.campaign_name) rlike 'winback' then 'Winback' 
            when lower(c.campaign_name) rlike 'enews' then 'eNews' 
            when lower(c.campaign_name) rlike 'solus' then 'Solus' 
            when lower(c.campaign_name) rlike 'mpp' then 'MPP' 
            when lower(c.campaign_name) rlike 'renew'  
              or lower(c.campaign_name) rlike 'reminder'  
              or lower(c.campaign_name) rlike 'payment' 
              or lower(c.campaign_name) rlike 'expiry'  
            then 'Renewal' 
            when lower(c.campaign_name) rlike 'travel' then 'Travel' 
            else 'Others' end type 
        , m.control 
        , m.open 
        , count(distinct m.customer_id) 
 
from main m 
 
inner join omc.send_level_summary s 
on s.customer_id=m.customer_id 
 
inner join omc.campaign_info c 
on c.campaign_id=s.campaign_id 
and c.channel='Email' 
 
where datediff(to_date(s.send_event_date),m.eng_send) between 1 and 60 
and (s.open_event_date is not null or s.click_event_date is not null) 
and m.Segment rlike 'Non-Open' 
 
--and m.open=1 
 
group by 1,2,3 
order by 1,2,3 
 
        , case when (REGEXP_EXTRACT(lower(c.campaign_name), '^consumer_([^_]+)', 1))='push'  
                            then REGEXP_EXTRACT(lower(c.campaign_name), '^consumer_push_([^_]+)', 1) 
                            else REGEXP_EXTRACT(lower(c.campaign_name), '^consumer_([^_]+)', 1) end name 
SELECT  e.control, count(DISTINCT e.customer_id) 
        -- ,case when e.campaign_id='55469582' then 'RSA Non-Open' 
        --     when e.campaign_id='57354642' then 'Non-RSA Non-Open' 
        --     when e.campaign_id='57354722' then 'RSA No-Clcik' 
        --     when e.campaign_id='57354802' then 'Non-RSA No-Clcik' 
        --     else 'other' end Segment 
        -- , e.control 
        -- , to_date(e.send_event_date) as eng_send 
        -- , case when e.open_event_date is not null or e.click_event_date is not null then 1 else 0 end open 
        -- , case when e.click_event_date is not null then 1 else 0 end click 
         
from omc.send_level_summary e 
 
INNER JOIN gms.s_contact c 
on c.row_id=e.customer_id 
 
-- left join omc.send_level_summary n 
-- on n.customer_id=e.customer_id 
-- and n.send_event_date>e.send_event_date 
-- and n.click_event_date is not null 
-- and datediff(n.send_event_date,e.send_event_date) between 1 and 60 
 
-- inner JOIN m4m.return_feed_header m 
-- on m.member_number=c.csn 
-- and m.partner not in ('IAG','NRMA Multi Policy Discount','NRMA MPD Insurance','NRMA Insurance') 
-- and datediff(to_date(m.time_stamp),to_date(e.send_event_date)) between 1 and 60 
 
inner join sandpit.ga_appsession app 
on app.membernumber=c.csn                                                             --24980 
and datediff(app.event_dt,e.send_event_date) between 0 and 60 
 
where e.program_id='57355262' 
and e.campaign_id in ('55469582','57354642') 
 
and (e.open_event_date is not null or e.click_event_date is not null) 
--and e.click_event_date is not null 
 
GROUP BY 1 
SELECT  count(DISTINCT e.customer_id) 
        -- ,case when e.campaign_id='55469582' then 'RSA Non-Open' 
        --     when e.campaign_id='57354642' then 'Non-RSA Non-Open' 
        --     when e.campaign_id='57354722' then 'RSA No-Clcik' 
        --     when e.campaign_id='57354802' then 'Non-RSA No-Clcik' 
        --     else 'other' end Segment 
        -- , e.control 
        -- , to_date(e.send_event_date) as eng_send 
        -- , case when e.open_event_date is not null or e.click_event_date is not null then 1 else 0 end open 
        -- , case when e.click_event_date is not null then 1 else 0 end click 
         
from omc.send_level_summary e 
 
INNER JOIN gms.s_contact c 
on c.row_id=e.customer_id 
 
-- inner join omc.send_level_summary n 
-- on n.customer_id=e.customer_id 
-- and n.send_event_date>e.send_event_date 
-- and n.open_event_date is not null 
-- and datediff(n.send_event_date,e.send_event_date) between 1 and 60 
 
-- inner JOIN m4m.return_feed_header m 
-- on m.member_number=c.csn 
-- and m.partner not in ('IAG','NRMA Multi Policy Discount','NRMA MPD Insurance','NRMA Insurance') 
-- and datediff(to_date(m.time_stamp),to_date(e.send_event_date)) between 1 and 60 
 
inner join sandpit.ga_appsession app 
on app.membernumber=c.csn                                                             --24980 
and datediff(app.event_dt,e.send_event_date) between 0 and 60 
 
where e.program_id='57355262' 
and e.campaign_id in ('55469582','57354642') 
and e.control is FALSE 
and (e.open_event_date is not null or e.click_event_date is not null) 
SELECT * 
        --, case when e.open_event_date is not null or e.click_event_date is not null then 1 else 0 end open 
from omc.send_level_summary e 
where e.program_id='57355262' 
SELECT  count(DISTINCT e.customer_id) 
        -- ,case when e.campaign_id='55469582' then 'RSA Non-Open' 
        --     when e.campaign_id='57354642' then 'Non-RSA Non-Open' 
        --     when e.campaign_id='57354722' then 'RSA No-Clcik' 
        --     when e.campaign_id='57354802' then 'Non-RSA No-Clcik' 
        --     else 'other' end Segment 
        -- , e.control 
        -- , to_date(e.send_event_date) as eng_send 
        -- , case when e.open_event_date is not null or e.click_event_date is not null then 1 else 0 end open 
        -- , case when e.click_event_date is not null then 1 else 0 end click 
         
from omc.send_level_summary e 
 
INNER JOIN gms.s_contact c 
on c.row_id=e.customer_id 
 
inner join omc.send_level_summary n 
on n.customer_id=e.customer_id 
and n.send_event_date>e.send_event_date 
and n.click_event_date is not null 
and datediff(n.send_event_date,e.send_event_date) between 1 and 60 
 
-- inner JOIN m4m.return_feed_header m 
-- on m.member_number=c.csn 
-- and m.partner not in ('IAG','NRMA Multi Policy Discount','NRMA MPD Insurance','NRMA Insurance') 
-- and datediff(to_date(m.time_stamp),to_date(e.send_event_date)) between 1 and 60 
 
-- inner join sandpit.ga_appsession app 
-- on app.membernumber=c.csn                                                             --24980 
-- and datediff(app.event_dt,e.send_event_date) between 0 and 60 
 
where e.program_id='57355262' 
and e.campaign_id not in ('55469582','57354642') 
and e.control is TRUE 
--and e.click_event_date is not null