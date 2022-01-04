drop table aa_dmc2540_q4batteries_campaign_edm_20210928 
create table campaign_data.aa_dmc2540_q4batteries_campaign_edm_20211018 as 
 
select distinct 
    csn, con.row_id contact_id, conx.attrib_55 color_plus, con.cust_value_cd loyalty_color, con_cd, cust_stat_cd, ms.name, 
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
 
left join geospatial.geocoded_mem_addr geo   
on geo.addr_id = addr.row_id   
  
left join geospatial.mb2wrka ms   
on cast(ms.mb_code16 as string) = geo.gnaf_mb 
 
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
create table campaign_data.aa_dmc2552_batteryseg1_campaign_edm_20211018_adhoc as 
with pool as ( 
select  cnt.* 
        ,rand() rnk 
 
from campaign_data.aa_dmc2540_q4batteries_campaign_edm_20210928 cnt 
 
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
        and cnt.last_purchased in ('2018Q2','2018Q3','2018Q4') 
        AND CNT.edm_consent='Y'     --41303 
        AND CNT.dm_consent='Y'      --41303 
) 
select * 
        , case when rnk between 0 and 0.10 then 0 
                else 1 end target 
from pool 
DROP table campaign_data.aa_dmc2552_battery_campaign_edm_20211018_adhoc 
SELECT target, count(*) from campaign_data.aa_dmc2552_battery_campaign_edm_20211018_adhoc 
GROUP BY 1 
ORDER BY 1 
drop table campaign_data.aa_dmc2552_batteryseg2_campaign_edm_20211018_adhoc 
create table campaign_data.aa_dmc2552_batteryseg2_campaign_edm_20211018_adhoc as 
with contactable as ( 
SELECT a.*, ntile(3) over (order by purchase_pred_n_v) as gms_decile  
 
from sandpit.`20210923_battery_repeat_prob_active` a 
), pool as  
( 
 
select distinct coalesce(a.membernumber,b.membernumber) as member_number 
                , a.* 
                , rand() rnk 
                , c.row_id as contact_id 
                 
from contactable a 
 
left join sandpit.renewal_base b 
on b.vehicle_rego=a.rego 
--and to_timestamp(past_job_number) between b.asset_start_dt and b.asset_end_dt 
--and b.asset_status_cd='Active' 
 
INNER JOIN gms.s_contact c 
on c.csn=b.membernumber 
 
inner join gms.s_contact_x conx 
ON conx.par_row_id = c.row_id  
 
inner join gms.s_contact_fnx as fn 
on fn.par_row_id = c.row_id 
 
LEFT ANTI JOIN campaign_data.aa_dmc2552_batteryseg1_campaign_edm_20211018_adhoc seg1 
on seg1.csn=c.csn 
 
where c.cust_stat_cd = 'Active' 
and c.con_cd in ('Ordinary Member' , 'Affiliate Member') 
and NVL(c.x_nrma_title,'no title') != 'Estate Of The Late' 
and NVL(fn.deceased_flg,'Yes') != 'No' 
and case when trim(c.x_inv_email_1) = 'Y' or c.email_addr is null then 'N' else 'Y' end = 'Y' 
and NVL(conx.attrib_36,'Yes') != 'No' 
and (fn.brloc_attrib13 is null or fn.brloc_attrib13 = 'N')  
and gms_decile=3 
) 
select * 
        , case when rnk between 0 and 0.10 then 0 
                else 1 end target 
from pool 
--where gms_decile=3 
with main as ( 
SELECT a.*, ntile(3) over (order by purchase_pred_n_v) as gms_decile  
 
from sandpit.`20210923_battery_repeat_prob_active` a 
) 
select gms_decile, max(purchase_pred_n_v) from main 
group by 1 
order by 1 
SELECT target, count(DISTINCT member_number) 
from campaign_data.aa_dmc2552_batteryseg2_campaign_edm_20211018_adhoc 
GROUP BY 1 
order by 1 
--where gms_decile=3 
SELECT target, count(*) from campaign_data.aa_dmc2552_batteryseg1_campaign_edm_20211018_adhoc 
GROUP BY 1 
ORDER BY 1 
select * from campaign_data.aa_dmc2552_batteryseg2_campaign_edm_20211018_adhoc 
SELECT DISTINCT csn as membernumber, contact_id, 'seg1' as flag from campaign_data.aa_dmc2552_batteryseg1_campaign_edm_20211018_adhoc 
where target=1 
SELECT target, count(DISTINCT contact_id) from campaign_data.aa_dmc2552_batteryseg2_campaign_edm_20211018_adhoc 
GROUP BY 1 
SELECT DISTINCT membernumber,contact_id, 'seg2' as flag from campaign_data.aa_dmc2552_batteryseg2_campaign_edm_20211018_adhoc 
where target=1 