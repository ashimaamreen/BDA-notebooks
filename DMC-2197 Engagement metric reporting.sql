select max(yyyymm) from sandpit.engagement_score 
spark.sql("drop table sandpit.engagement_base") 
spark.sql(""" 
CREATE TABLE sandpit.engagement_base AS  
with ams_exclude as ( 
    SELECT "NRMA Insurance" as `partner`   
    UNION ALL 
    SELECT "NRMA Multi Policy Discount" as `partner`   
    UNION ALL 
    SELECT "NRMA MPD Insurance" as `partner`   
    UNION ALL 
    SELECT "NRMA car servicing" as `partner`   
    UNION ALL  
    SELECT "NRMA car servicing" as `partner`  
    UNION ALL  
    SELECT "NRMA Insurance ControlPro" as `partner`  
    UNION ALL  
    SELECT "NRMA Insurance Branch" as `partner`  
    UNION ALL  
    SELECT "NRMA Branch" as `partner`  
    UNION ALL  
    SELECT "NRMA Travel Insurance" as `partner`  
    UNION ALL  
    SELECT "" as `partner`  
    UNION ALL  
    SELECT "" as `partner`  
),eng_weight as (  
    SELECT cast(0 as int) as ams , cast(1 as int) as omc , cast(0 as int) as opened, cast( 0 as int) as clicked, 0      as score UNION ALL  
    SELECT cast(0 as int) as ams , cast(1 as int) as omc , cast(1 as int) as opened, cast( 0 as int) as clicked, 0.4    as score UNION ALL  
    SELECT cast(0 as int) as ams , cast(1 as int) as omc , cast(1 as int) as opened, cast( 1 as int) as clicked, 0.5    as score UNION ALL  
    SELECT cast(1 as int) as ams , cast(1 as int) as omc , cast(0 as int) as opened, cast( 0 as int) as clicked, 0.75   as score UNION ALL  
    SELECT cast(1 as int) as ams , cast(1 as int) as omc , cast(1 as int) as opened, cast( 0 as int) as clicked, 0.85   as score UNION ALL  
    SELECT cast(1 as int) as ams , cast(1 as int) as omc , cast(1 as int) as opened, cast( 1 as int) as clicked, 0.95   as score  
) , ams as ( 
    select  
        b.primarymembernumber as membernumber 
    ,   b.time_stamp 
    ,   c.category 
    ,   b.partner  
    ,   total_amount 
    ,   discount 
    from m4m.return_feed_header b  
    inner join m4m.m4m_partner_category c  
        on b.partner = c.partner  
    left join ams_exclude ex  
        on b.partner = ex.partner  
    where b.time_stamp >= "2018-10-01 00:00:00"  
        and b.time_stamp <= "2020-08-31 23:59:59"  
        and ex.partner IS NULL  
), omc as ( 
    select  
        c.csn as membernumber 
    ,   c.row_id as contact_id  
    ,   a.riid  
    ,   a.campaign_id   
    -- metrics 
    ,   a.send_event_date AS sendtime 
    ,   CASE WHEN a.open_event_date IS NOT NULL THEN 1 ELSE 0 END AS opened 
    ,   CASE WHEN a.click_event_date IS NOT NULL THEN 1 ELSE 0 END AS clicked 
    ,   0 AS converted  
    from omc.send_level_summary  a  
    left join omc.campaign_info b 
        ON a.campaign_id = b.campaign_id 
        AND COALESCE(a.program_id, '') = COALESCE(b.program_id, '') 
    left join gms.s_contact c  
        ON a.customer_id = c.row_id  
    where b.purpose = 'P' 
        and lower(b.campaign_name) like "consumer%" 
        and (lower(b.campaign_name) not like "%winback%" and lower(campaign_name) not like "%renewal%") 
        and a.send_event_date >= "2018-10-01 00:00:00"  
        and a.send_event_date <= "2020-08-31 23:59:59"  
        and c.csn is not null 
), base as  ( 
    SELECT  
        omc.membernumber 
    ,   omc.contact_id  
    -- OMC  
    ,   omc.riid            as omc_riid  
    ,   omc.campaign_id     as omc_campaign_id  
    ,   omc.sendtime        as omc_sendtime 
    ,   CASE  
            WHEN omc.converted = 1 THEN 1  
            WHEN omc.clicked  = 1 then 1  
            ELSE omc.opened 
        end                     as omc_opened  
    ,   CASE  
            WHEN omc.converted = 1 THEN 1  
            ELSE omc.clicked 
        END                     as omc_clicked  
    ,   omc.converted           as omc_converted 
    -- ams 
    ,   case when datediff(ams.time_stamp,omc.sendtime) between 0 and 14 then ams.category else NULL end         as ams_cateogry 
    ,   case when datediff(ams.time_stamp,omc.sendtime) between 0 and 14 then ams.partner else NULL end           as ams_partner  
    ,   case when datediff(ams.time_stamp,omc.sendtime) between 0 and 14 then ams.time_stamp else NULL end       as ams_time  
    ,   case when datediff(ams.time_stamp,omc.sendtime) between 0 and 14 then ams.total_amount else NULL end     as ams_amt  
    FROM omc  
    Left join ams 
        on  omc.membernumber = ams.membernumber 
) 
select  
    b.*  
,   e.score 
from base b 
left join eng_weight e  
    ON  1=e.omc 
    AND case when b.ams_amt is not null then 1 else 0 end  = e.ams  
    AND b.omc_opened = e.opened  
    AND b.omc_clicked = e.clicked  
-- where (ams.time_stamp is null or date_add(ams.time_stamp,0) between date_add(omc.sendtime,0) and date_add(omc.sendtime, 14)) 
""") 
spark.sql("drop table if exists sandpit.engagement_score" ) 
spark.sql(""" 
create table sandpit.engagement_score as  
with eng_weight as (  
    SELECT cast(0 as int) as ams , cast(1 as int) as omc , cast(0 as int) as opened, cast( 0 as int) as clicked, 0      as score UNION ALL  
    SELECT cast(0 as int) as ams , cast(1 as int) as omc , cast(1 as int) as opened, cast( 0 as int) as clicked, 0.4    as score UNION ALL  
    SELECT cast(0 as int) as ams , cast(1 as int) as omc , cast(1 as int) as opened, cast( 1 as int) as clicked, 0.5    as score UNION ALL  
    SELECT cast(1 as int) as ams , cast(1 as int) as omc , cast(0 as int) as opened, cast( 0 as int) as clicked, 0.75   as score UNION ALL  
    SELECT cast(1 as int) as ams , cast(1 as int) as omc , cast(1 as int) as opened, cast( 0 as int) as clicked, 0.85   as score UNION ALL  
    SELECT cast(1 as int) as ams , cast(1 as int) as omc , cast(1 as int) as opened, cast( 1 as int) as clicked, 0.95   as score  
),   member_basket  as ( 
    select  
        membernumber 
    -- ,   omc.contact_id  
    -- OMC  
    ,   substr(cast(omc_sendtime as string) , 1, 7)             as yyyymm  
    ,   max(1)                                                  as omc  
    ,   COUNT(DISTINCT(omc_campaign_id))                               as emails  
    ,   max(omc_opened)                                         as opened 
    ,   max(omc_clicked)                                        as clicked  
    ,   max(case when ams_amt is not null then 1 else 0 end )   as ams 
    ,   count(*) as nrow 
    from sandpit.engagement_base 
    group by  
        membernumber 
    ,   substr(cast(omc_sendtime as string) , 1, 7) 
) 
SELECT  
    m.* 
,   e.score 
FROM member_basket  m  
LEFT JOIN eng_weight e  
    ON  m.omc       =   e.omc 
    AND m.ams       =   e.ams  
    AND m.opened    =   e.opened  
    AND m.clicked   =   e.clicked  
""") 
select max(send_event_date) from omc.send_level_summary 
create table sandpit.engagement_renewals as  
 
with history_dt as ( 
    select 30-1 as lagg, 3 as pre_renewal union all  
    select 60-2 as lagg ,6 as pre_renewal union all  
    select 90-2 as lagg ,9 as pre_renewal union all  
    select 120-2 as lagg ,12 as pre_renewal union all  
    select 150-2 as lagg ,15 as pre_renewal  
), base as ( 
select  
    pre_renewal 
,   a.prod_grp 
-- member attributes 
,   b.member_colour 
,   case  
        when b.tenure_member BETWEEN -5 AND 1    THEN 'y_0' 
        when b.tenure_member BETWEEN 1 AND 2     THEN 'y_1' 
        when b.tenure_member BETWEEN 2 AND 3     THEN 'y_2' 
        when b.tenure_member BETWEEN 3 AND 4     THEN 'y_3' 
        when b.tenure_member BETWEEN 4 AND 7     THEN 'y_4-6'  
        when b.tenure_member BETWEEN 7 AND 10    THEN 'y_7-10' 
        when b.tenure_member > 10                THEN 'y_11+' 
        else NULL 
    end as member_tenure 
    ,   b.membernumber 
    ,   b.integration_id  
    ,   b.order_id  
-- summary  
,   1                           as renewals  
,   max(c.renewal_cd)           as renewed 
 
from sandpit.renewal_base b 
 
left join  sandpit.util_prod_budget  a  
    on b.prod_budget = a.prod_budget 
    and case when b.order_payment_term = "Y" then 'MPP' else 'Annual' end = a.payment_plan 
 
inner join sandpit.util_renew_summ c  
    on b.match_rnk = c.match_rnk  
 
cross join history_dt  dt  
 
where    
        -- renewal filter 
        date_add(b.order_end_dt, 2) between "2019-01-01 00:00:00" and "2019-12-31 23:59:59"  
    and COALESCE(c.type_rnk, 0)  <> 1  
    and COALESCE(b.member_staff, 0)  = 0  
    AND a.removeID = 0  
    and a.prod_grp in ("Classic Care", "Premium Care") 
    and b.order_payment_cd in('Reconciled','Payment Taken') 
    and b.order_status_cd = 'Complete' 
    and b.membernumber is not null  
    and asset_end_dt >= date_sub(order_end_dt,40) 
     
group by 
    pre_renewal 
,   a.prod_grp 
    -- a.orderID 
-- ,   r_a.orderID 
-- ,   b.match_rnk 
-- ,   B.renewal_yyyymm 
-- member attributes 
,   b.member_colour 
,   b.membernumber 
,   b.integration_id  
,   b.order_id  
,   case  
        when b.tenure_member BETWEEN -5 AND 1    THEN 'y_0' 
        when b.tenure_member BETWEEN 1 AND 2     THEN 'y_1' 
        when b.tenure_member BETWEEN 2 AND 3     THEN 'y_2' 
        when b.tenure_member BETWEEN 3 AND 4     THEN 'y_3' 
        when b.tenure_member BETWEEN 4 AND 7     THEN 'y_4-6'  
        when b.tenure_member BETWEEN 7 AND 10    THEN 'y_7-10' 
        when b.tenure_member > 10                THEN 'y_11+' 
        else NULL 
    end 
) , base_eng as ( 
    SELECT  
        pre_renewal   
    ,   B.membernumber 
    ,   B.integration_id 
    ,   B.order_id 
    -- agg 
    ,   sum(case when e.score is not null then 1 else 0 END) as eng_scores  
    ,   sum(e.score) as summ_score  
    ,   max(e.score) as maxx_score 
    from sandpit.renewal_base  b 
    inner join sandpit.engagement_score e  
    on b.membernumber = e.membernumber  
    cross join history_dt  dt  
    where date_add(cast(concat(e.yyyymm,"-01 00:00:00") as timestamp),0)  
        between date_sub(b.order_end_dt,lagg) and date_sub(b.order_end_dt,0) 
    GROUP BY  
        pre_renewal   
    ,   B.membernumber 
    ,   B.integration_id 
    ,   B.order_id 
) 
select  
    base.pre_renewal 
,   COALESCE(maxx_score,-1)         AS maxx_score 
,   member_colour 
,   member_tenure 
,   sum(base.renewals)              as renewals 
,   sum(base.renewed)               as renewed 
,   sum(base.renewed)/sum(base.renewals) AS RENEW_rate 
,   sum(base_eng.eng_scores)        as valid_score 
,   sum(base_eng.summ_score)        as summ_score 
from base 
left join base_eng 
    ON  base.membernumber = base_eng.membernumber 
    AND base.integration_id = base_eng.integration_id 
    AND base.order_id = base_eng.order_id 
    AND base.pre_renewal = base_eng.pre_renewal 
group by 1,2,3,4 
SELECT max(yyyymm) FROM sandpit.engagement_score 
with current_score as ( 
select distinct membernumber 
        , score as monthly_score 
         
from sandpit.engagement_score 
where yyyymm='2020-08' 
), L3_score as ( 
select membernumber, max(score) as L3_score from sandpit.engagement_score 
where yyyymm in ('2020-06','2020-07','2020-08') 
group by 1 
), L6_score as ( 
select membernumber, max(score) as L6_score from sandpit.engagement_score 
where yyyymm in ('2020-03','2020-04','2020-05','2020-06','2020-07','2020-08') 
group by 1 
) 
with current_score as ( 
select distinct membernumber 
        , score as monthly_score 
 
from sandpit.engagement_score 
where yyyymm='2020-08' 
), L3_score as ( 
select membernumber, max(score) as L3_score from sandpit.engagement_score 
where yyyymm in ('2020-06','2020-07','2020-08') 
group by 1 
), L6_score as ( 
select membernumber, max(score) as L6_score from sandpit.engagement_score 
where yyyymm in ('2020-03','2020-04','2020-05','2020-06','2020-07','2020-08') 
group by 1 
), renewal_month as( 
select  membernumber, max(renewal_yyyymm) as renewal_due 
from sandpit.renewal_base 
group by 1 
) 
--812196901 
-- select  c.csn as member_number 
--         , case when trim(c.x_inv_email_1) = 'Y' or c.email_addr is null then 'No email'  
--             when cx.attrib_36='No' then 'No Mkt' 
--             else 'Email-able' end Member_type 
--         , e.monthly_score 
--         , le.L3_score 
--         , lg.L6_score 
--         , c.CUST_VALUE_CD as loyaltycolor 
--         , cx.attrib_55  AS Colour_Plus 
--         , rm.renewal_due 
--         , b.prod_name  
--         , case when b.item_base_price>0 then 'Paid' else 'Free' end Free/Paid 
--         , case  
--             when b.tenure_member BETWEEN -5 AND 1    THEN 'y_0' 
--             when b.tenure_member BETWEEN 1 AND 2     THEN 'y_1' 
--             when b.tenure_member BETWEEN 2 AND 3     THEN 'y_2' 
--             when b.tenure_member BETWEEN 3 AND 4     THEN 'y_3' 
--             when b.tenure_member BETWEEN 4 AND 7     THEN 'y_4-6'  
--             when b.tenure_member BETWEEN 7 AND 10    THEN 'y_7-10' 
--             when b.tenure_member > 10                THEN 'y_11+' 
--             else NULL 
--         end as member_tenure 
 
select count(distinct csn)  
from gms.s_contact c 
 
inner join gms.s_contact_fnx as fn  
on fn.par_row_id = c.row_id  
 
inner join gms.s_contact_x cx 
on cx.par_row_id=c.row_id 
 
inner join renewal_month rm 
on rm.membernumber=c.csn 
 
inner join sandpit.renewal_base b       --2,226,189 
on b.renewal_yyyymm=rm.renewal_due 
and b.membernumber=rm.membernumber 
and b.prod_cd='Product' 
and b.prod_type in ('RSA','Non-RSA') 
 
left join current_score e    
on c.csn=e.membernumber 
 
left join L3_score le 
on le.membernumber=c.csn 
 
left join L6_score lg 
on lg.membernumber=c.csn 
 
 
 
where 1=1 
and c.cust_stat_cd = 'Active' 
and c.con_cd in ('Ordinary Member' , 'Affiliate Member') 
and NVL(c.x_nrma_title,'no title') != 'Estate Of The Late'       
and NVL(fn.deceased_flg,'N') = 'N'                              --2,577,176 
and c.csn is not null 
DROP table campaign_data.aa_dmc2197_engagementreporting_data_analysis_20200930 
create table campaign_data.aa_dmc2197_engagementreporting_data_analysis_20200930 as 
with current_score as ( 
select distinct membernumber 
        , score as monthly_score 
 
from sandpit.engagement_score 
where yyyymm='2020-08' 
), L3_score as ( 
select membernumber, max(score) as L3_score from sandpit.engagement_score 
where yyyymm in ('2020-06','2020-07','2020-08') 
group by 1 
), L6_score as ( 
select membernumber, max(score) as L6_score from sandpit.engagement_score 
where yyyymm in ('2020-03','2020-04','2020-05','2020-06','2020-07','2020-08') 
group by 1 
), renewal_month as( 
select  membernumber, max(renewal_yyyymm) as renewal_due 
from sandpit.renewal_base 
group by 1 
) 
--812196901 
select distinct c.csn as member_number 
        , case when trim(c.x_inv_email_1) = 'Y' or c.email_addr is null then 'No email'  
            when cx.attrib_36='No' then 'No Mkt' 
            else 'Email-able' end Member_type 
        , e.monthly_score 
        , le.L3_score 
        , lg.L6_score 
        , c.CUST_VALUE_CD as loyaltycolor 
        , cx.attrib_55  AS Colour_Plus 
        , rm.renewal_due 
        , b.prod_name  
        , case when b.item_base_price>0 then 'Paid' else 'Free' end Free_Paid 
        , case  
            when b.tenure_member BETWEEN -5 AND 1    THEN 'y_0' 
            when b.tenure_member BETWEEN 1 AND 2     THEN 'y_1' 
            when b.tenure_member BETWEEN 2 AND 3     THEN 'y_2' 
            when b.tenure_member BETWEEN 3 AND 4     THEN 'y_3' 
            when b.tenure_member BETWEEN 4 AND 7     THEN 'y_4-6'  
            when b.tenure_member BETWEEN 7 AND 10    THEN 'y_7-10' 
            when b.tenure_member > 10                THEN 'y_11+' 
            else NULL 
        end as member_tenure 
 
--select count(distinct csn)  
from gms.s_contact c 
 
inner join gms.s_contact_fnx as fn  
on fn.par_row_id = c.row_id  
 
inner join gms.s_contact_x cx 
on cx.par_row_id=c.row_id 
 
inner join renewal_month rm 
on rm.membernumber=c.csn 
 
inner join sandpit.renewal_base b       --2,226,189 
on b.renewal_yyyymm=rm.renewal_due 
and b.membernumber=rm.membernumber 
and b.prod_cd='Product' 
and b.prod_type in ('RSA','Non-RSA') 
 
left join current_score e    
on c.csn=e.membernumber 
 
left join L3_score le 
on le.membernumber=c.csn 
 
left join L6_score lg 
on lg.membernumber=c.csn 
 
 
 
where 1=1 
and c.cust_stat_cd = 'Active' 
and c.con_cd in ('Ordinary Member' , 'Affiliate Member') 
and NVL(c.x_nrma_title,'no title') != 'Estate Of The Late'       
and NVL(fn.deceased_flg,'N') = 'N'                              --2,577,176 
and c.csn is not null 
spark.sql(""" 
select monthly_score 
        , l3_score 
        , l6_score 
        , member_type as null_reason 
        , colour_plus 
        , loyaltycolor 
        , renewal_due 
        , prod_name 
        , free_paid 
        , member_tenure 
        , count(distinct member_number)  
 
from campaign_data.aa_dmc2197_engagementreporting_data_analysis_20200930 
 
group by monthly_score 
        , l3_score 
        , l6_score 
        , member_type 
        , colour_plus 
        , loyaltycolor 
        , renewal_due 
        , prod_name 
        , free_paid 
        , member_tenure 
order by 1,2,3,4,5,6,7,8,9,10 
""").show(1000000,False) 
df = spark.sql(""" 
select monthly_score 
        , l3_score 
        , l6_score 
        , member_type as null_reason 
        , colour_plus 
        , loyaltycolor 
        , renewal_due 
        , prod_name 
        , free_paid 
        , member_tenure 
        , count(distinct member_number)  
 
from campaign_data.aa_dmc2197_engagementreporting_data_analysis_20200930 
 
group by 1,2,3,4,5,6,7,8,9,10 
order by 1,2,3,4,5,6,7,8,9,10 
""").collect() 
print(len(df)) 
seg=0 
mult=52026 
columns = spark.sql(''' 
select monthly_score 
        , l3_score 
        , l6_score 
        , member_type as null_reason 
        , colour_plus 
        , loyaltycolor 
        , renewal_due 
        , prod_name 
        , free_paid 
        , member_tenure 
        , count(distinct member_number)  
 
from campaign_data.aa_dmc2197_engagementreporting_data_analysis_20200930 
 
group by 1,2,3,4,5,6,7,8,9,10 
order by 1,2,3,4,5,6,7,8,9,10 
''').schema.names 
from __future__ import print_function 
print(seg) 
print('') 
 
print(','.join(columns)) 
 
for i in range(mult*seg, len(df)): 
    print(','.join([str(df[i][c]) for c in columns])) 
  
seg += 1 