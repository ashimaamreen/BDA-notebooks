create table campaign_data.cad_model_20201120 as 
with base as ( 
    select p.membernumber 
    , p.order_id 
    , p.integration_id 
    , p.r9_probability 
    , b.prod_name 
    , b.renewal_yyyymm 
    , r.r9 
from sandpit.response_df_pred as p 
    left join sandpit.renewal_base as b 
    on p.membernumber = b.membernumber 
    and p.order_id = b.order_id 
    and p.integration_id = b.integration_id 
     
    -- response 
    left join sandpit.response_df as r 
    on p.membernumber = r.membernumber 
    and p.order_id = r.order_id 
    and p.integration_id = r.integration_id 
 
where 
    1 = 1 
    and p.r9_scope = 'in' 
    and p.r9_probability is not null 
) 
 
 select membernumber, max(r9_probability) as max_prob, ntile(10) over (order by max(r9_probability) desc) decile 
 from base 
group by 1 
order by 1 
with base as ( 
    select p.membernumber 
    , p.order_id 
    , p.integration_id 
    , p.r9_probability 
    , b.prod_name 
    , b.renewal_yyyymm 
    , r.r9 
from sandpit.response_df_pred as p 
    left join sandpit.renewal_base as b 
    on p.membernumber = b.membernumber 
    and p.order_id = b.order_id 
    and p.integration_id = b.integration_id 
     
    -- response 
    left join sandpit.response_df as r 
    on p.membernumber = r.membernumber 
    and p.order_id = r.order_id 
    and p.integration_id = r.integration_id 
 
where 
    1 = 1 
    and p.r9_scope = 'in' 
    and p.r9_probability is not null 
) 
 select renewal_yyyymm, count(*) as vol, avg(r9_probability) as avg_prob, sum(r9) /  sum(case when r9 is not null then 1 else 0 end) as run_rate 
 from base 
group by 1 
order by 1 
-- select decile, avg(transactionprobability) from campaign_data.carbatteries_crossell_20201116 group by 1 
select * from campaign_data.carbatteries_crossell_20201116  
select  
    -- datediff(current_timestamp(), sr.created) 
    -- count(distinct sc.csn) 
    -- , cst_con_id,  
    count(distinct sr.created) 
     
from gms.s_contact as sc 
 
    -- join with case managmenet table 
    inner join gms.s_srv_req as sr 
    on sc.row_id = sr.cst_con_id 
     
    -- inner join gms.ek_20200901_c_dmc2167_stream4 as outcome 
    -- on sc.csn = outcome.member_id 
     
where 
        -- audience who had a battery sale in last 30D  
        sr.sr_category_cd = "881" 
    and sr.sr_stat_id = 'COMPLETED'  -- case status 
    and sr.sr_cat_type_cd = 'CAD'  -- case type 
    and from_timestamp(sr.created,'yyyy-MM-dd') between '2017-01-01' and '2017-12-31' 
 
--2019 
-- 159614 call outs 
-- 156848 csn 
 
--2017 
-- 154722 member 
-- 157826 trans 
drop table campaign_data.dmc2280_cad purge 
create table campaign_data.dmc2280_cad as 
select  
    srx.attrib_03, -- membernumber 
    case when srx.attrib_05 in ('BOAT','PERMIT','REGO','NO REGO','#UNKOWN','UNREG','TBA','&&&','TBC','N/A','NOREGO','UNKNOWN','UNKNOWN.','ABC123','TRACTOR','UNREGISTER','TBA123','XXXXXX','0000','000','.','//','///','////','/////') then 'UNKNOWN' 
         when srx.attrib_05 like '%UNKNOWN%' then 'UNKNOWN' 
         when srx.attrib_05 like '%?%' then 'UNKNOWN' 
         when srx.attrib_05 rlike '/' then 'UNKNOWN' 
         when srx.attrib_05 like '%*%' then 'UNKNOWN' 
         when srx.attrib_05 like '%.%' then 'UNKNOWN' 
         when srx.attrib_05 rlike '-' then 'UNKNOWN' 
         when srx.attrib_05 rlike 'BOAT' then 'UNKNOWN' 
         when regexp_replace(srx.attrib_05, "\W", "") like '' then 'UNKNOWN' 
    else srx.attrib_05 end  
    rego-- rego 
    -- , count(distinct sr.created) 
    , sr.created 
 
from gms.s_srv_req sr 
inner join gms.s_srv_req_x as srx  
on srx.row_id = sr.row_id 
and srx.attrib_04 is not null    
 
where 
        -- audience who had a battery sale in last 30D  
        sr.sr_category_cd = "881" 
    and sr.sr_stat_id = 'COMPLETED'  -- case status 
    and sr.sr_cat_type_cd = 'CAD'  -- case type 
     
    -- and  
    --  case when srx.attrib_05 in ('BOAT','PERMIT','REGO','NO REGO','#UNKOWN','UNREG','TBA','TBC','&&&','N/A','NOREGO','UNKNOWN','UNKNOWN.','ABC123','TRACTOR','UNREGISTER','TBA123','XXXXXX','0000','000','.','//','///','////','/////') then 'UNKNOWN' 
    --      when srx.attrib_05 like '%UNKNOWN%' then 'UNKNOWN' 
    --      when srx.attrib_05 like '%?%' then 'UNKNOWN' 
    --      when srx.attrib_05 rlike '/' then 'UNKNOWN' 
    --      when srx.attrib_05 rlike '%*%' then 'UNKNOWN' 
    --      when srx.attrib_05 rlike '-' then 'UNKNOWN' 
    --      when srx.attrib_05 like '%.%' then 'UNKNOWN' 
    --      when srx.attrib_05 rlike 'BOAT' then 'UNKNOWN' 
    --      when regexp_replace(srx.attrib_05, "\W", "") like '' then 'UNKNOWN' 
    -- else srx.attrib_05 end <> 'UNKNOWN' 
 
 
 
-- colourplus and the tenure  
 
select  -- con.cust_value_cd 
        conx.attrib_55 
        -- case when base.membernumber is null then 'N' else 'Y' end rsa 
        ,count(*) from  
 
(select attrib_03,count(*) cnt from campaign_data.dmc2280_cad group by 1  
--   having cnt > 1 
) as A 
 
inner join gms.s_contact con 
on A.attrib_03 = con.csn 
 
inner join gms.s_contact_x conx 
ON conx.par_row_id = con.row_id  
where region =  
 
group by 1 
 
-- more detail profiling 
select  -- con.cust_value_cd 
        -- conx.attrib_55 
        prod_name 
        ,count(distinct csn) from  
 
(select attrib_03,count(*) cnt from campaign_data.dmc2280_cad group by 1  
 -- having cnt > 1 
) as A 
 
inner join gms.s_contact con 
on A.attrib_03 = con.csn 
 
inner join gms.s_contact_x conx 
ON conx.par_row_id = con.row_id  
 
 
left outer join (select distinct membernumber, prod_name from sandpit.renewal_base where asset_status_cd = 'Active' and prod_type = 'RSA') base 
on base.membernumber = con.csn 
 
group by 1 
 
 
 
 
 
 
-- by quarter 
 
select  con.cust_value_cd 
        -- conx.attrib_55 
        , quarter 
        ,count(*) from  
 
(select attrib_03, 
        case  
         when from_timestamp(created,'yyyy-MM-dd') between '2016-01-01' and '2016-03-31' then '2016Q1' 
         when from_timestamp(created,'yyyy-MM-dd') between '2016-04-01' and '2016-06-30' then '2016Q2' 
         when from_timestamp(created,'yyyy-MM-dd') between '2016-07-01' and '2016-09-30' then '2016Q3' 
         when from_timestamp(created,'yyyy-MM-dd') between '2016-10-01' and '2016-12-31' then '2016Q4' 
         when from_timestamp(created,'yyyy-MM-dd') between '2017-01-01' and '2017-03-31' then '2017Q1' 
         when from_timestamp(created,'yyyy-MM-dd') between '2017-04-01' and '2017-06-30' then '2017Q2' 
         when from_timestamp(created,'yyyy-MM-dd') between '2017-07-01' and '2017-09-30' then '2017Q3' 
         when from_timestamp(created,'yyyy-MM-dd') between '2017-10-01' and '2017-12-31' then '2017Q4' 
         when from_timestamp(created,'yyyy-MM-dd') between '2018-01-01' and '2018-03-31' then '2018Q1' 
         when from_timestamp(created,'yyyy-MM-dd') between '2018-04-01' and '2018-06-30' then '2018Q2' 
         when from_timestamp(created,'yyyy-MM-dd') between '2018-07-01' and '2018-09-30' then '2018Q3' 
         when from_timestamp(created,'yyyy-MM-dd') between '2018-10-01' and '2018-12-31' then '2018Q4' 
         when from_timestamp(created,'yyyy-MM-dd') between '2019-01-01' and '2019-03-31' then '2019Q1' 
         when from_timestamp(created,'yyyy-MM-dd') between '2019-04-01' and '2019-06-30' then '2019Q2' 
         when from_timestamp(created,'yyyy-MM-dd') between '2019-07-01' and '2019-09-30' then '2019Q3' 
         when from_timestamp(created,'yyyy-MM-dd') between '2019-10-01' and '2019-12-31' then '2019Q4' 
         when from_timestamp(created,'yyyy-MM-dd') between '2020-01-01' and '2020-03-31' then '2020Q1' 
         when from_timestamp(created,'yyyy-MM-dd') between '2020-04-01' and '2020-06-30' then '2020Q2' 
         when from_timestamp(created,'yyyy-MM-dd') between '2020-07-01' and '2020-09-30' then '2020Q3' 
         when from_timestamp(created,'yyyy-MM-dd') between '2020-10-01' and '2020-12-31' then '2020Q4' 
          
          
        else '' 
        end quarter 
        , count(*) cnt from campaign_data.dmc2280_cad group by 1,2 
-- having cnt > 1 
) as A 
 
inner join gms.s_contact con 
on A.attrib_03 = con.csn 
 
inner join gms.s_contact_x conx 
ON conx.par_row_id = con.row_id  
 
group by 1,2 
 
  
 
 
-- Repurchase Frequency 
 
select   cnt 
        ,count(*) from  
 
(select attrib_03, count(*) cnt from campaign_data.dmc2280_cad  
-- having cnt >1 
group by 1 
) as A 
 
inner join gms.s_contact con 
on A.attrib_03 = con.csn 
 
inner join gms.s_contact_x conx 
ON conx.par_row_id = con.row_id  
 
group by 1 
 
 
-- repeated purchase frequency 
With fst as  
( 
select   A.attrib_03, A.rego, A.created as fst_date FROM (select attrib_03, rego, created, rank() over (partition by rego order by created) rnk from campaign_data.dmc2280_cad where attrib_03 is not null) as A 
 
inner join gms.s_contact con 
on A.attrib_03 = con.csn 
 
inner join gms.s_contact_x conx 
ON conx.par_row_id = con.row_id  
 
where rnk = 1 
) 
, 
scn as  
( 
select  A.attrib_03, A.rego, A.created as scn_date  FROM (select attrib_03, rego, created, rank() over (partition by rego order by created) rnk from campaign_data.dmc2280_cad where attrib_03 is not null) as A 
 
inner join gms.s_contact con 
on A.attrib_03 = con.csn 
 
inner join gms.s_contact_x conx 
ON conx.par_row_id = con.row_id  
 
where rnk = 2 
) 
 
select trunc(datediff(scn.scn_date,fst.fst_date) /100)*100 ,count(*) from scn inner join fst on scn.rego = fst.rego group by 1 order by 2 desc 
 
-- 3rd repurchase 
With fst as  
( 
select   A.attrib_03, A.rego, A.created as fst_date FROM (select attrib_03, rego, created, rank() over (partition by rego order by created) rnk from campaign_data.dmc2280_cad where attrib_03 is not null) as A 
 
inner join gms.s_contact con 
on A.attrib_03 = con.csn 
 
inner join gms.s_contact_x conx 
ON conx.par_row_id = con.row_id  
 
where rnk = 2 
) 
, 
scn as  
( 
select  A.attrib_03, A.rego, A.created as scn_date  FROM (select attrib_03, rego, created, rank() over (partition by rego order by created) rnk from campaign_data.dmc2280_cad where attrib_03 is not null) as A 
 
inner join gms.s_contact con 
on A.attrib_03 = con.csn 
 
inner join gms.s_contact_x conx 
ON conx.par_row_id = con.row_id  
 
where rnk = 3 
) 
 
select trunc(datediff(scn.scn_date,fst.fst_date) /100)*100 ,count(*) from scn inner join fst on scn.rego = fst.rego group by 1 order by 2 desc 
 
 
-- date 
select min(created) from gms.s_srv_req 
 
 
-- metro regional distribution 
-- purchase rate by metro regional 
 
select  conx.attrib_55,  
        CASE  
            WHEN  
                (  
                    addr.zipcode >= '2000' AND addr.zipcode <= '2082' OR  
                    addr.zipcode >= '2084' AND addr.zipcode <= '2234' OR  
                    addr.zipcode >= '2555' AND addr.zipcode <= '2574' OR  
                    addr.zipcode >= '2745' AND addr.zipcode <= '2770' OR  
                    addr.zipcode >= '2775' AND addr.zipcode <= '2775'  
                )  
                AND  
                (  
                    UPPER(addr.country) = 'AUSTRALIA' OR  
                    UPPER(addr.country) = 'AU'  
                )  
            THEN  
                'METROPOLITAN'  
            WHEN  
                (  
                    addr.zipcode >= '2083' AND addr.zipcode <= '2083' OR  
                    addr.zipcode >= '2250' AND addr.zipcode <= '2338' OR  
                    addr.zipcode >= '2415' AND addr.zipcode <= '2423' OR  
                    addr.zipcode >= '2425' AND addr.zipcode <= '2425' OR  
                    addr.zipcode >= '2428' AND addr.zipcode <= '2428' OR  
                    addr.zipcode >= '2500' AND addr.zipcode <= '2535' OR  
                    addr.zipcode >= '2538' AND addr.zipcode <= '2541' OR  
                    addr.zipcode >= '2575' AND addr.zipcode <= '2578' OR  
                    addr.zipcode >= '2600' AND addr.zipcode <= '2617' OR  
                    addr.zipcode >= '2773' AND addr.zipcode <= '2774' OR  
                    addr.zipcode >= '2776' AND addr.zipcode <= '2786' OR  
                    addr.zipcode >= '2900' AND addr.zipcode <= '2914'  
                )  
                AND  
                (  
                    UPPER(addr.country) = 'AUSTRALIA' OR  
                    UPPER(addr.country) = 'AU'  
                )  
            THEN  
                'REGIONAL'  
            WHEN  
                (  
                    addr.zipcode >= '2339' AND addr.zipcode <= '2411' OR  
                    addr.zipcode >= '2424' AND addr.zipcode <= '2424' OR  
                    addr.zipcode >= '2426' AND addr.zipcode <= '2427' OR  
                    addr.zipcode >= '2429' AND addr.zipcode <= '2490' OR  
                    addr.zipcode >= '2536' AND addr.zipcode <= '2537' OR  
                    addr.zipcode >= '2545' AND addr.zipcode <= '2551' OR  
                    addr.zipcode >= '2579' AND addr.zipcode <= '2594' OR  
                    addr.zipcode >= '2618' AND addr.zipcode <= '2739' OR  
                    addr.zipcode >= '2787' AND addr.zipcode <= '2898' OR  
                    addr.zipcode >= '6798' AND addr.zipcode <= '6799'  
                )  
                AND  
                (  
                    UPPER(addr.country) = 'AUSTRALIA' OR  
                    UPPER(addr.country) = 'AU'  
                )  
            THEN  
                'RURAL'  
            WHEN  
                (  
                    addr.zipcode >= '0800' AND addr.zipcode <= '0886' OR  
                    addr.zipcode >= '3000' AND addr.zipcode <= '6770' OR  
                    addr.zipcode >= '6907' AND addr.zipcode <= '7470' OR  
                    addr.zipcode >= '7471'   
                )  
                AND  
                (  
                    UPPER(addr.country) = 'AUSTRALIA' OR  
                    UPPER(addr.country) = 'AU'  
                )  
            THEN  
                'INTERSTATE'  
            ELSE  
                'UNKNOWN'  
            END                                                                                                                 AS region  
          
          
        --, addr.state                                                                                                            AS state  
        ,count(*) from  
 
(select attrib_03,count(*) cnt from campaign_data.dmc2280_cad group by 1  
--having cnt > 1 
)  
as A 
 
inner join gms.s_contact con 
on A.attrib_03 = con.csn 
 
inner join gms.s_contact_x conx 
ON conx.par_row_id = con.row_id  
 
inner join gms.s_addr_per as addr  
on addr.row_id = con.pr_per_addr_id  
 
where state = 'NSW' 
 
group by 1,2 
 
 
 
 
--  
 
 
 
-- battery type,  
-- select  a.row_id  
--         ,a.ref_number_2 
--         ,a.ref_number_2 
--         ,a.make_cd 
--         ,a.model_cd 
--         ,sr.* 
-- from gms.s_srv_req sr 
-- inner join gms.s_srv_req_x as srx  
-- on srx.row_id = sr.row_id 
-- and srx.attrib_04 is not null    
 
-- left join gms.s_asset a 
-- on a.row_id = sr.asset_id 
 
-- where 
--         -- audience who had a battery sale in last 30D  
--         sr.sr_category_cd = "881" 
--     and sr.sr_stat_id = 'COMPLETED'  -- case status 
--     and sr.sr_cat_type_cd = 'CAD'  -- case type 
select * from sandpit.renewal_base where asset_status_cd = 'Active' and prod_type = 'RSA' and prod_name is null 
With fst as  
( 
select   from_timestamp(A.created,'yyyy-MM') mth, A.attrib_03, A.created as fst_date  
FROM (select attrib_03, created, rank() over (partition by attrib_03 order by created) rnk from campaign_data.dmc2280_cad where attrib_03 is not null) as A 
 
inner join gms.s_contact con 
on A.attrib_03 = con.csn 
 
inner join gms.s_contact_x conx 
ON conx.par_row_id = con.row_id  
 
where rnk = 1 
) 
, 
scn as  
( 
select  A.attrib_03, A.created as scn_date  FROM (select attrib_03, created, rank() over (partition by attrib_03 order by created) rnk from campaign_data.dmc2280_cad where attrib_03 is not null) as A 
 
inner join gms.s_contact con 
on A.attrib_03 = con.csn 
 
inner join gms.s_contact_x conx 
ON conx.par_row_id = con.row_id  
 
where rnk = 2 
) 
 
select fst.mth, count(fst.attrib_03) from fst LEFT OUTER JOIN scn on scn.attrib_03 = fst.attrib_03  
where scn.scn_date is NOT NULL 
-- trunc(datediff(scn.scn_date,fst.fst_date) /100)*100 between 1100 and 1200 
group by 1 order by 1 
With fst as  
( 
select   from_timestamp(A.created,'yyyy-MM') mth, A.attrib_03, A.rego, A.created as fst_date  
FROM (select attrib_03, rego, created, rank() over (partition by rego order by created) rnk from campaign_data.dmc2280_cad where attrib_03 is not null) as A 
 
inner join gms.s_contact con 
on A.attrib_03 = con.csn 
 
inner join gms.s_contact_x conx 
ON conx.par_row_id = con.row_id  
 
where rnk = 1 
) 
, 
scn as  
( 
select  A.attrib_03, A.rego, A.created as scn_date  FROM (select attrib_03, rego, created, rank() over (partition by rego order by created) rnk from campaign_data.dmc2280_cad where attrib_03 is not null) as A 
 
inner join gms.s_contact con 
on A.attrib_03 = con.csn 
 
inner join gms.s_contact_x conx 
ON conx.par_row_id = con.row_id  
 
where rnk = 2 
) 
 
select fst.mth, count(fst.attrib_03) from fst LEFT OUTER JOIN scn on scn.rego = fst.rego  
where scn.scn_date is NOT NULL 
-- trunc(datediff(scn.scn_date,fst.fst_date) /100)*100 between 1100 and 1200 
group by 1 order by 1 
-- select distinct x_bat_wrty_proided_by from gms.s_asset_x limit 100 
-- select distinct ref_number_3 from gms.s_asset 
 
-- select distinct contact_cd from sandpit.renewal_base limit 100 
 
 
select count(distinct attrib_03) 
-- , max(created) created 
 , count(a.created) trans from campaign_data.dmc2280_cad a 
 inner join gms.s_contact sc on sc.csn = a.attrib_03 
 where attrib_03 is not null  
 and from_timestamp(a.created,'yyyy-MM-dd') between '2017-01-01' and '2017-12-31' 
 -- group by 1 
  
 -- before join contact table  
 -- 159,628 members, 172,196 trans 
  
 -- joined contact table 
 -- 154,717 members, 158,943 trans 
  
create table campaign_data.ek_dmc2280_battery_precount_0107 as 
 
-- select count(*) from 
-- ( 
select  
    csn, con.row_id contact_id, conx.attrib_55 color_plus, con.cust_value_cd loyalty_color, con_cd, cust_stat_cd,  
    case  
         when from_timestamp(A.created,'yyyy-MM-dd') between '2016-01-01' and '2016-03-31' then '2016Q1' 
         when from_timestamp(A.created,'yyyy-MM-dd') between '2016-04-01' and '2016-06-30' then '2016Q2' 
         when from_timestamp(A.created,'yyyy-MM-dd') between '2016-07-01' and '2016-09-30' then '2016Q3' 
         when from_timestamp(A.created,'yyyy-MM-dd') between '2016-10-01' and '2016-12-31' then '2016Q4' 
         when from_timestamp(A.created,'yyyy-MM-dd') between '2017-01-01' and '2017-03-31' then '2017Q1' 
         when from_timestamp(A.created,'yyyy-MM-dd') between '2017-04-01' and '2017-06-30' then '2017Q2' 
         when from_timestamp(A.created,'yyyy-MM-dd') between '2017-07-01' and '2017-09-30' then '2017Q3' 
         when from_timestamp(A.created,'yyyy-MM-dd') between '2017-10-01' and '2017-12-31' then '2017Q4' 
         when from_timestamp(A.created,'yyyy-MM-dd') between '2018-01-01' and '2018-03-31' then '2018Q1' 
         when from_timestamp(A.created,'yyyy-MM-dd') between '2018-04-01' and '2018-06-30' then '2018Q2' 
         when from_timestamp(A.created,'yyyy-MM-dd') between '2018-07-01' and '2018-09-30' then '2018Q3' 
         when from_timestamp(A.created,'yyyy-MM-dd') between '2018-10-01' and '2018-12-31' then '2018Q4' 
         when from_timestamp(A.created,'yyyy-MM-dd') between '2019-01-01' and '2019-03-31' then '2019Q1' 
          
    else '' 
    end last_purchased,  
    CASE when A.trans > 1 then 1 else 0 end repeat_customer, 
    CASE  
            WHEN  
                (  
                    addr.zipcode >= '2000' AND addr.zipcode <= '2082' OR  
                    addr.zipcode >= '2084' AND addr.zipcode <= '2234' OR  
                    addr.zipcode >= '2555' AND addr.zipcode <= '2574' OR  
                    addr.zipcode >= '2745' AND addr.zipcode <= '2770' OR  
                    addr.zipcode >= '2775' AND addr.zipcode <= '2775'  
                )  
                AND  
                (  
                    UPPER(addr.country) = 'AUSTRALIA' OR  
                    UPPER(addr.country) = 'AU'  
                )  
            THEN  
                'METROPOLITAN'  
            WHEN  
                (  
                    addr.zipcode >= '2083' AND addr.zipcode <= '2083' OR  
                    addr.zipcode >= '2250' AND addr.zipcode <= '2338' OR  
                    addr.zipcode >= '2415' AND addr.zipcode <= '2423' OR  
                    addr.zipcode >= '2425' AND addr.zipcode <= '2425' OR  
                    addr.zipcode >= '2428' AND addr.zipcode <= '2428' OR  
                    addr.zipcode >= '2500' AND addr.zipcode <= '2535' OR  
                    addr.zipcode >= '2538' AND addr.zipcode <= '2541' OR  
                    addr.zipcode >= '2575' AND addr.zipcode <= '2578' OR  
                    addr.zipcode >= '2600' AND addr.zipcode <= '2617' OR  
                    addr.zipcode >= '2773' AND addr.zipcode <= '2774' OR  
                    addr.zipcode >= '2776' AND addr.zipcode <= '2786' OR  
                    addr.zipcode >= '2900' AND addr.zipcode <= '2914'  
                )  
                AND  
                (  
                    UPPER(addr.country) = 'AUSTRALIA' OR  
                    UPPER(addr.country) = 'AU'  
                )  
            THEN  
                'REGIONAL'  
            WHEN  
                (  
                    addr.zipcode >= '2339' AND addr.zipcode <= '2411' OR  
                    addr.zipcode >= '2424' AND addr.zipcode <= '2424' OR  
                    addr.zipcode >= '2426' AND addr.zipcode <= '2427' OR  
                    addr.zipcode >= '2429' AND addr.zipcode <= '2490' OR  
                    addr.zipcode >= '2536' AND addr.zipcode <= '2537' OR  
                    addr.zipcode >= '2545' AND addr.zipcode <= '2551' OR  
                    addr.zipcode >= '2579' AND addr.zipcode <= '2594' OR  
                    addr.zipcode >= '2618' AND addr.zipcode <= '2739' OR  
                    addr.zipcode >= '2787' AND addr.zipcode <= '2898' OR  
                    addr.zipcode >= '6798' AND addr.zipcode <= '6799'  
                )  
                AND  
                (  
                    UPPER(addr.country) = 'AUSTRALIA' OR  
                    UPPER(addr.country) = 'AU'  
                )  
            THEN  
                'RURAL'  
            WHEN  
                (  
                    addr.zipcode >= '0800' AND addr.zipcode <= '0886' OR  
                    addr.zipcode >= '3000' AND addr.zipcode <= '6770' OR  
                    addr.zipcode >= '6907' AND addr.zipcode <= '7470' OR  
                    addr.zipcode >= '7471'   
                )  
                AND  
                (  
                    UPPER(addr.country) = 'AUSTRALIA' OR  
                    UPPER(addr.country) = 'AU'  
                )  
            THEN  
                'INTERSTATE'  
            ELSE  
                'UNKNOWN'  
            END                                                                                                                 AS region  
          
          
        , addr.state                                                                                                            AS state  
        , case when  
                (lower(conx.attrib_36) in ("yes", "null") or conx.attrib_36 is null)  
                and (fn.brloc_attrib13 is null or fn.brloc_attrib13 = 'N')  
                and (con.email_addr is not null) then 'Y' else 'N' 
            end                                                                                                                 aS eDM_Consent 
        , case when  
                (lower(conx.attrib_35) in ("yes", "null") or conx.attrib_35 is null) -- DM consent 
                and (UPPER(trim(NVL(addr.addr,'NULL')))<>'NULL' or UPPER(trim(NVL(addr.addr_line_2,'NULL')))<>'NULL') --return2sender 
                then 'Y' else 'N' 
            end                                                                                                                 as DM_Consent 
        , case when (con.veteran_flg = 'N' or con.veteran_flg is null) and (lower(conx.attrib_42) in ("yes", "null") or conx.attrib_42 is null)  
            and (con.CELL_PH_NUM is not null) and (fn.brloc_attrib13 is null or fn.brloc_attrib13 = 'N') then 'Y' else 'N' end as SMS_Consent 
        -- , case when push_hist.customer_id is null then 'N' else 'Y' end                                                         as push_history 
        , case when NVL(conx.attrib_44,'') rlike 'Staff' then 'Y' else 'N' end as Staff 
        , case when base.membernumber is null then 'N' else 'Y' end                                                         as RSA 
        , Q.decile  as q_decile 
        , Q.transactionprobability as q_score 
        , cad.cad_decile  
        , cad.cad_score 
        , trans 
from (select attrib_03, max(created) created, count(created) trans from campaign_data.dmc2280_cad where attrib_03 is not null group by 1) as A 
 
inner join gms.s_contact con 
on A.attrib_03 = con.csn 
 
inner join gms.s_contact_x conx 
ON conx.par_row_id = con.row_id  
 
inner join gms.s_addr_per as addr  
on addr.row_id = con.pr_per_addr_id  
 
inner join gms.s_contact_fnx as fn 
on fn.par_row_id = con.row_id 
 
left outer join (select distinct customer_id from omc.send_level_summary where channel = 'Push') push_hist 
on push_hist.customer_id = con.row_id 
 
left outer join (select distinct membernumber from sandpit.renewal_base where asset_status_cd = 'Active' and prod_type = 'RSA') base 
on con.csn = base.membernumber 
 
left outer join (select cast(membernumber as string) membernumber, transactionprobability, decile from campaign_data.carbatteries_crossell_20201116) as Q 
on Q.membernumber = con.csn 
 
 
left outer join  
( 
 
with base as ( 
    select p.membernumber 
    , p.order_id 
    , p.integration_id 
    , p.r9_probability 
    , b.prod_name 
    , b.renewal_yyyymm 
    , r.r9 
from sandpit.response_df_pred as p 
    left join sandpit.renewal_base as b 
    on p.membernumber = b.membernumber 
    and p.order_id = b.order_id 
    and p.integration_id = b.integration_id 
     
    -- response 
    left join sandpit.response_df as r 
    on p.membernumber = r.membernumber 
    and p.order_id = r.order_id 
    and p.integration_id = r.integration_id 
 
where 
    1 = 1 
    and p.r9_scope = 'in' 
    and p.r9_probability is not null 
) 
 
 select membernumber, max(r9_probability) as cad_score, ntile(10) over (order by max(r9_probability) desc) cad_decile 
 from base 
group by 1 
order by 1 
) cad 
 
on con.csn = cad.membernumber 
 
where 
    case  
         when from_timestamp(A.created,'yyyy-MM-dd') between '2016-01-01' and '2016-03-31' then '2016Q1' 
         when from_timestamp(A.created,'yyyy-MM-dd') between '2016-04-01' and '2016-06-30' then '2016Q2' 
         when from_timestamp(A.created,'yyyy-MM-dd') between '2016-07-01' and '2016-09-30' then '2016Q3' 
         when from_timestamp(A.created,'yyyy-MM-dd') between '2016-10-01' and '2016-12-31' then '2016Q4' 
         when from_timestamp(A.created,'yyyy-MM-dd') between '2017-01-01' and '2017-03-31' then '2017Q1' 
         when from_timestamp(A.created,'yyyy-MM-dd') between '2017-04-01' and '2017-06-30' then '2017Q2' 
         when from_timestamp(A.created,'yyyy-MM-dd') between '2017-07-01' and '2017-09-30' then '2017Q3' 
         when from_timestamp(A.created,'yyyy-MM-dd') between '2017-10-01' and '2017-12-31' then '2017Q4' 
         when from_timestamp(A.created,'yyyy-MM-dd') between '2018-01-01' and '2018-03-31' then '2018Q1' 
         when from_timestamp(A.created,'yyyy-MM-dd') between '2018-04-01' and '2018-06-30' then '2018Q2' 
         when from_timestamp(A.created,'yyyy-MM-dd') between '2018-07-01' and '2018-09-30' then '2018Q3' 
         when from_timestamp(A.created,'yyyy-MM-dd') between '2018-10-01' and '2018-12-31' then '2018Q4' 
         when from_timestamp(A.created,'yyyy-MM-dd') between '2019-01-01' and '2019-03-31' then '2019Q1' 
          
    else '' 
    end <> '' 
    and NVL(fn.deceased_flg,'N') = 'N' 
    and NVL(con.x_nrma_title, '') <> 'Estate Of The Late' 
     
-- ) x 
-- group by 1,2 
drop table campaign_data.ek_dmc2280_battery_precount_temp purge 
-- select * from campaign_data.ek_dmc2280_battery_precount  
 
-- select attrib_03, count(created) trans from campaign_data.dmc2280_cad where attrib_03 is null group by 1 
select   count(distinct csn) 
        , rsa 
from campaign_data.ek_dmc2280_battery_precount_0107 
-- where  
-- last_purchased in ('2017Q3','2017Q4','2017Q1','2017Q2') 
group by 2 
-- color_plus 
--         , loyalty_color 
--         , con_cd 
--         , cust_stat_cd 
--         , last_purchased 
--         , repeat_customer 
--         , region 
--         , state 
--         , edm_consent 
--         , dm_consent 
--         , sms_consent 
--         , push_history 
--         , staff 
--         , rsa 
--         , q_decile 
--         , cad_decile 
output = spark.sql('''select   
        color_plus 
        , loyalty_color 
        --         , con_cd 
--         , cust_stat_cd 
         , last_purchased 
--         , repeat_customer 
--         , region 
--         , state 
--         , edm_consent 
--         , dm_consent 
--         , sms_consent 
--         , push_history 
--         , staff 
--         , rsa 
--         , q_decile 
--         , cad_decile 
        , count(distinct csn) 
--        , count(*) 
from campaign_data.ek_dmc2280_battery_precount 
where last_purchased in ('2018Q3','2018Q4','2019Q1') 
group by  
color_plus 
        , loyalty_color 
        , con_cd 
        , cust_stat_cd 
        , last_purchased 
        , repeat_customer 
        , region 
        , state 
        , edm_consent 
        , dm_consent 
        , sms_consent 
        , push_history 
        , staff 
        , rsa 
        , q_decile 
        , cad_decile 
''')# 
output.createOrReplaceTempView('output')# 
spark.sql('select * from output').show(100000)# 
 
 
select decile, count(distinct membernumber) from campaign_data.cad_model_20201120 group by 1 
create table campaign_data.ek_dmc2280_battery_precount_model as 
select  
    csn, con.row_id contact_id, conx.attrib_55 color_plus, con.cust_value_cd loyalty_color, con_cd, cust_stat_cd,  
    case  
         when from_timestamp(A.created,'yyyy-MM-dd') between '2016-01-01' and '2016-03-31' then '2016Q1' 
         when from_timestamp(A.created,'yyyy-MM-dd') between '2016-04-01' and '2016-06-30' then '2016Q2' 
         when from_timestamp(A.created,'yyyy-MM-dd') between '2016-07-01' and '2016-09-30' then '2016Q3' 
         when from_timestamp(A.created,'yyyy-MM-dd') between '2016-10-01' and '2016-12-31' then '2016Q4' 
         when from_timestamp(A.created,'yyyy-MM-dd') between '2017-01-01' and '2017-03-31' then '2017Q1' 
         when from_timestamp(A.created,'yyyy-MM-dd') between '2017-04-01' and '2017-06-30' then '2017Q2' 
         when from_timestamp(A.created,'yyyy-MM-dd') between '2017-07-01' and '2017-09-30' then '2017Q3' 
         when from_timestamp(A.created,'yyyy-MM-dd') between '2017-10-01' and '2017-12-31' then '2017Q4' 
         when from_timestamp(A.created,'yyyy-MM-dd') between '2018-01-01' and '2018-03-31' then '2018Q1' 
         when from_timestamp(A.created,'yyyy-MM-dd') between '2018-04-01' and '2018-06-30' then '2018Q2' 
         when from_timestamp(A.created,'yyyy-MM-dd') between '2018-07-01' and '2018-09-30' then '2018Q3' 
         when from_timestamp(A.created,'yyyy-MM-dd') between '2018-10-01' and '2018-12-31' then '2018Q4' 
         when from_timestamp(A.created,'yyyy-MM-dd') between '2019-01-01' and '2019-03-31' then '2019Q1' 
          
    else '' 
    end last_purchased,  
    CASE when A.trans > 1 then 1 else 0 end repeat_customer, 
    CASE  
            WHEN  
                (  
                    addr.zipcode >= '2000' AND addr.zipcode <= '2082' OR  
                    addr.zipcode >= '2084' AND addr.zipcode <= '2234' OR  
                    addr.zipcode >= '2555' AND addr.zipcode <= '2574' OR  
                    addr.zipcode >= '2745' AND addr.zipcode <= '2770' OR  
                    addr.zipcode >= '2775' AND addr.zipcode <= '2775'  
                )  
                AND  
                (  
                    UPPER(addr.country) = 'AUSTRALIA' OR  
                    UPPER(addr.country) = 'AU'  
                )  
            THEN  
                'METROPOLITAN'  
            WHEN  
                (  
                    addr.zipcode >= '2083' AND addr.zipcode <= '2083' OR  
                    addr.zipcode >= '2250' AND addr.zipcode <= '2338' OR  
                    addr.zipcode >= '2415' AND addr.zipcode <= '2423' OR  
                    addr.zipcode >= '2425' AND addr.zipcode <= '2425' OR  
                    addr.zipcode >= '2428' AND addr.zipcode <= '2428' OR  
                    addr.zipcode >= '2500' AND addr.zipcode <= '2535' OR  
                    addr.zipcode >= '2538' AND addr.zipcode <= '2541' OR  
                    addr.zipcode >= '2575' AND addr.zipcode <= '2578' OR  
                    addr.zipcode >= '2600' AND addr.zipcode <= '2617' OR  
                    addr.zipcode >= '2773' AND addr.zipcode <= '2774' OR  
                    addr.zipcode >= '2776' AND addr.zipcode <= '2786' OR  
                    addr.zipcode >= '2900' AND addr.zipcode <= '2914'  
                )  
                AND  
                (  
                    UPPER(addr.country) = 'AUSTRALIA' OR  
                    UPPER(addr.country) = 'AU'  
                )  
            THEN  
                'REGIONAL'  
            WHEN  
                (  
                    addr.zipcode >= '2339' AND addr.zipcode <= '2411' OR  
                    addr.zipcode >= '2424' AND addr.zipcode <= '2424' OR  
                    addr.zipcode >= '2426' AND addr.zipcode <= '2427' OR  
                    addr.zipcode >= '2429' AND addr.zipcode <= '2490' OR  
                    addr.zipcode >= '2536' AND addr.zipcode <= '2537' OR  
                    addr.zipcode >= '2545' AND addr.zipcode <= '2551' OR  
                    addr.zipcode >= '2579' AND addr.zipcode <= '2594' OR  
                    addr.zipcode >= '2618' AND addr.zipcode <= '2739' OR  
                    addr.zipcode >= '2787' AND addr.zipcode <= '2898' OR  
                    addr.zipcode >= '6798' AND addr.zipcode <= '6799'  
                )  
                AND  
                (  
                    UPPER(addr.country) = 'AUSTRALIA' OR  
                    UPPER(addr.country) = 'AU'  
                )  
            THEN  
                'RURAL'  
            WHEN  
                (  
                    addr.zipcode >= '0800' AND addr.zipcode <= '0886' OR  
                    addr.zipcode >= '3000' AND addr.zipcode <= '6770' OR  
                    addr.zipcode >= '6907' AND addr.zipcode <= '7470' OR  
                    addr.zipcode >= '7471'   
                )  
                AND  
                (  
                    UPPER(addr.country) = 'AUSTRALIA' OR  
                    UPPER(addr.country) = 'AU'  
                )  
            THEN  
                'INTERSTATE'  
            ELSE  
                'UNKNOWN'  
            END                                                                                                                 AS region  
          
          
        , addr.state                                                                                                            AS state  
        , case when  
                (lower(conx.attrib_36) in ("yes", "null") or conx.attrib_36 is null)  
                and (fn.brloc_attrib13 is null or fn.brloc_attrib13 = 'N')  
                and (con.email_addr is not null) then 'Y' else 'N' 
            end                                                                                                                 aS eDM_Consent 
        , case when  
                (lower(conx.attrib_35) in ("yes", "null") or conx.attrib_35 is null) -- DM consent 
                and (UPPER(trim(NVL(addr.addr,'NULL')))<>'NULL' or UPPER(trim(NVL(addr.addr_line_2,'NULL')))<>'NULL') --return2sender 
                then 'Y' else 'N' 
            end                                                                                                                 as DM_Consent 
        , case when (con.veteran_flg = 'N' or con.veteran_flg is null) and (lower(conx.attrib_42) in ("yes", "null") or conx.attrib_42 is null)  
            and (con.CELL_PH_NUM is not null) and (fn.brloc_attrib13 is null or fn.brloc_attrib13 = 'N') then 'Y' else 'N' end as SMS_Consent 
        , case when push_hist.customer_id is null then 'N' else 'Y' end                                                         as push_history 
        , case when NVL(conx.attrib_44,'') rlike 'Staff' then 'Y' else 'N' end as Staff 
        , case when base.membernumber is null then 'N' else 'Y' end                                                         as RSA 
        , Q.decile  as q_decile 
        , Q.transactionprobability as q_score 
        , cad.cad_decile  
        , cad.cad_score 
         
from (select attrib_03, max(created) created, count(created) trans from campaign_data.dmc2280_cad where attrib_03 is not null group by 1) as A 
 
inner join gms.s_contact con 
on A.attrib_03 = con.csn 
 
inner join gms.s_contact_x conx 
ON conx.par_row_id = con.row_id  
 
inner join gms.s_addr_per as addr  
on addr.row_id = con.pr_per_addr_id  
 
inner join gms.s_contact_fnx as fn 
on fn.par_row_id = con.row_id 
 
left outer join (select distinct customer_id from omc.send_level_summary where channel = 'Push') push_hist 
on push_hist.customer_id = con.row_id 
 
left outer join (select distinct membernumber from sandpit.renewal_base where asset_status_cd = 'Active' and prod_type = 'RSA') base 
on con.csn = base.membernumber 
 
left outer join (select cast(membernumber as string) membernumber, transactionprobability, decile from campaign_data.carbatteries_crossell_20201116) as Q 
on Q.membernumber = con.csn 
 
 
left outer join  
( 
 
with base as ( 
    select p.membernumber 
    , p.order_id 
    , p.integration_id 
    , p.r9_probability 
    , b.prod_name 
    , b.renewal_yyyymm 
    , r.r9 
from sandpit.response_df_pred as p 
    left join sandpit.renewal_base as b 
    on p.membernumber = b.membernumber 
    and p.order_id = b.order_id 
    and p.integration_id = b.integration_id 
     
    -- response 
    left join sandpit.response_df as r 
    on p.membernumber = r.membernumber 
    and p.order_id = r.order_id 
    and p.integration_id = r.integration_id 
 
where 
    1 = 1 
    and p.r9_scope = 'in' 
    and p.r9_probability is not null 
) 
 
 select membernumber, max(r9_probability) as cad_score, ntile(10) over (order by max(r9_probability) desc) cad_decile 
 from base 
group by 1 
order by 1 
) cad 
 
on con.csn = cad.membernumber 
 
where 
    case  
         when from_timestamp(A.created,'yyyy-MM-dd') between '2016-01-01' and '2016-03-31' then '2016Q1' 
         when from_timestamp(A.created,'yyyy-MM-dd') between '2016-04-01' and '2016-06-30' then '2016Q2' 
         when from_timestamp(A.created,'yyyy-MM-dd') between '2016-07-01' and '2016-09-30' then '2016Q3' 
         when from_timestamp(A.created,'yyyy-MM-dd') between '2016-10-01' and '2016-12-31' then '2016Q4' 
         when from_timestamp(A.created,'yyyy-MM-dd') between '2017-01-01' and '2017-03-31' then '2017Q1' 
         when from_timestamp(A.created,'yyyy-MM-dd') between '2017-04-01' and '2017-06-30' then '2017Q2' 
         when from_timestamp(A.created,'yyyy-MM-dd') between '2017-07-01' and '2017-09-30' then '2017Q3' 
         when from_timestamp(A.created,'yyyy-MM-dd') between '2017-10-01' and '2017-12-31' then '2017Q4' 
         when from_timestamp(A.created,'yyyy-MM-dd') between '2018-01-01' and '2018-03-31' then '2018Q1' 
         when from_timestamp(A.created,'yyyy-MM-dd') between '2018-04-01' and '2018-06-30' then '2018Q2' 
         when from_timestamp(A.created,'yyyy-MM-dd') between '2018-07-01' and '2018-09-30' then '2018Q3' 
         when from_timestamp(A.created,'yyyy-MM-dd') between '2018-10-01' and '2018-12-31' then '2018Q4' 
         when from_timestamp(A.created,'yyyy-MM-dd') between '2019-01-01' and '2019-03-31' then '2019Q1' 
          
    else '' 
    end <> '' 
    and NVL(fn.deceased_flg,'N') = 'N' 
    and NVL(con.x_nrma_title, '') <> 'Estate Of The Late' 
     
     
select count(*) from m4m.return_feed_header where partner = 'NRMA Batteries' and from_timestamp(time_stamp,'yyyy-MM-dd') between '2017-01-01' and '2017-12-31' 
 
-- 2019 
-- 167678 count all 
-- 158926  
 
-- 2017 
-- 71487 trans 
-- 69675 member 
select * from omc.send_level_summary limit 100 
select from_timestamp(B.created,'yyyy-MM') mth, New_customer, count(*) 
    from 
( 
select  A.attrib_03,  
        A.created,  
        case when fst.attrib_03 is null then 'N' else 'Y' end New_customer 
from  
campaign_data.dmc2280_cad A 
left outer join  
( 
select attrib_03  
    , from_timestamp(min(created),'yyyy-MM-dd') first_date 
          
      from campaign_data.dmc2280_cad  
       
      group by 1 
       
) fst 
on A.attrib_03 = fst.attrib_03 
    and from_timestamp(A.created,'yyyy-MM-dd') = fst.first_date 
 
) B 
group by 1,2 
order by 1,2 