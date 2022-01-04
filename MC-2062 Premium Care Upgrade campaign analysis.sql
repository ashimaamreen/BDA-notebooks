s_asset = spark.table("gms.s_asset") 
s_asset.createOrReplaceTempView("s_asset") 
 
s_order = spark.table("gms.s_order") 
s_order.createOrReplaceTempView("s_order") 
 
s_order_type = spark.table("gms.s_order_type") 
s_order_type.createOrReplaceTempView("s_order_type") 
 
s_order_item = spark.table("gms.s_order_item") 
s_order_item.createOrReplaceTempView("s_order_item") 
 
s_order_item_x = spark.table("gms.s_order_item_x") 
s_order_item_x.createOrReplaceTempView("s_order_item_x") 
 
s_prod_int = spark.table("gms.s_prod_int") 
s_prod_int.createOrReplaceTempView("s_prod_int") 
 
s_contact = spark.table("gms.s_contact") 
s_contact.createOrReplaceTempView("s_contact") 
 
s_org_ext = spark.table("gms.s_org_ext") 
s_org_ext.createOrReplaceTempView("s_org_ext") 
 
s_src_payment = spark.table("gms.s_src_payment") 
s_src_payment.createOrReplaceTempView("s_src_payment") 
Order_Payment = spark.sql("""   
 
select    c.row_id ContactID 
        , o.order_num OrderNumber 
        , oi.order_id as OrderLineItemID 
        , to_date(o.ORDER_DT) as RenewalDueDate 
        , min(to_date(py.txn_dt)) as PaymentDate 
        , to_date(o.X_STAT_CMPLTD_DT) as OrderCompletionDate  
 
 
from s_order o 
 
inner join s_order_type ot 
on o.order_type_id = ot.row_id 
 
inner join s_order_item oi 
on o.row_id = oi.order_id 
and oi.ACTION_CD in ('Update','Add') --when they renew only update line but add is upgrade/downgrade and new 
 
inner join s_prod_int p 
on oi.prod_id = p.row_id 
and p.sub_type_cd in ('Non-RSA','Add-on','RSA') 
and p.prod_cd = 'Product' 
 
left join s_src_payment py 
on o.row_id = py.order_id 
AND py.pay_stat_cd != 'Cancelled' 
 
inner join s_contact c 
on o.contact_id = c.row_id 
 
 
where (NVL(o.X_RENEWAL_RELATED, 'N') = 'Y' or ot.name = 'Renew') 
and o.status_cd != 'Revised' 
and to_date(o.X_STAT_CMPLTD_DT)= '2020-05-25' 
--and '2019-05-25' 
 
group by c.row_id  
        , o.order_num  
        , oi.order_id  
        , to_date(o.ORDER_DT) 
        , to_date(o.X_STAT_CMPLTD_DT) 
 
""") 
Order_Payment.createOrReplaceTempView("Order_Payment") 
spark.sql('select * from Order_Payment').show(10000,False) 
Renewal_Order = spark.sql("""   
 
select    c.row_id ContactID 
        , case when c.csn is null then org.ou_num else c.csn end MemberNumber 
        , c.CON_CD as contact_type 
        , o.order_num OrderNumber 
        , oi.order_id as OrderLineItemID 
        , ot.name OrderType 
        , replace(p.name,'|',',') Product 
        , p.sub_type_cd as Product_type 
        , o.status_cd OrderStatus 
        , o.X_RENEWAL_RELATED RenewalRelated_flag 
        , o.X_PAY_BY_TERM as MPP_flag 
        , o.X_PAYMENT_STATUS as PaymentStatus 
        , oi.ACTION_CD as Action --if =add then upgrade/dwngrade/new 
        , ox.ATTRIB_04 as UpgradeDowngrade_flag 
        , to_date(o.ORDER_DT) as RenewalDueDate 
        , py.PaymentDate 
        , to_date(o.X_STAT_CMPLTD_DT) as OrderCompletionDate  
 
 
from s_order o 
 
inner join s_order_type ot 
on o.order_type_id = ot.row_id 
 
inner join s_order_item oi 
on o.row_id = oi.order_id 
and oi.ACTION_CD in ('Update','Add') --when they renew only update line but add is upgrade/downgrade and new 
 
left join s_order_item_x ox 
on oi.row_id = ox.par_row_id 
 
inner join s_prod_int p 
on oi.prod_id = p.row_id 
and p.sub_type_cd in ('Non-RSA','Add-on','RSA') 
and p.prod_cd = 'Product' 
 
left join Order_Payment py 
on o.order_num = py.OrderNumber 
 
inner join s_contact c 
on o.contact_id = c.row_id 
 
left join s_org_ext org 
on o.accnt_id = org.row_id 
 
where (NVL(o.X_RENEWAL_RELATED, 'N') = 'Y' or ot.name = 'Renew') 
and o.status_cd != 'Revised' 
--and o.ORDER_DT between '2020-03-01' and '2020-03-20' 
 
""") 
Renewal_Order.createOrReplaceTempView("Renewal_Order") 
spark.sql('select * from Renewal_Order').count() 
Renewals_summary = spark.sql(""" 
select    ContactID 
        , MemberNumber 
        , contact_type 
        , OrderNumber 
        , OrderLineItemID 
        , OrderType 
        , Product 
        , Product_type 
        , OrderStatus 
        , PaymentStatus 
        , Action --if =add then upgrade/dwngrade/new 
        , UpgradeDowngrade_flag 
        , RenewalDueDate 
        , PaymentDate 
        , OrderCompletionDate 
        , case when PaymentStatus in ('Reconciled','Payment Taken') and PaymentDate<=RenewalDueDate then 'Y' else 'N' end POT 
        , case when PaymentStatus in ('Reconciled','Payment Taken') then 'Y' else 'N' end Renewed 
        , case when Action = 'Add' and UpgradeDowngrade_flag is NULL then 'Y' else 'N' end NewAdded 
        , RenewalRelated_flag 
        , MPP_flag 
         
from Renewal_Order 
--where RenewalDueDate ='2019-02-01' 
""") 
Renewals_summary.createOrReplaceTempView("Renewals_summary") 
spark.sql('select * from Renewals_summary').show(10000) 
spark.sql("""select RenewalDueDate, MPP_flag 
                    , count(distinct OrderLineItemID) RenewalsDue 
                    , count(distinct(case when POT = 'Y' then OrderLineItemID end)) as POT 
                    , count(distinct(case when Renewed = 'Y' then OrderLineItemID end)) as TotalRenewed 
                    , count(distinct(case when NewAdded = 'Y' then OrderLineItemID end)) as NewSubsAdded 
                    , count(distinct MemberNumber) MembersDue 
                    , count(distinct(case when POT = 'Y' then MemberNumber end)) as POT_Members 
                    , count(distinct(case when Renewed = 'Y' then MemberNumber end)) as TotalRenewed_Members 
                    -- , count(distinct(case when MPP_flag = 'Y' then MemberNumber end)) as MPP_Members 
            from Renewals_summary  
            where contact_type !='Business Contact' 
            -- and RenewalDueDate = '' 
            group by RenewalDueDate, MPP_flag 
""").show(1000,False) 
spark.sql("""select RenewalDueDate 
                    , count(distinct OrderLineItemID) RenewalsDue 
                    , sum(case when POT = 'Y' then 1 else 0 end) POT 
                    , sum(case when Renewed = 'Y' then 1 else 0 end) TotalRenewed 
                    , sum(case when MPP_flag = 'Y' then 1 else 0 end) MPP 
                    ,sum(case when NewAdded = 'Y' then 1 else 0 end) NewAdded  
            from Renewals_summary where contact_type !='Business Contact' 
            group by RenewalDueDate 
""").show(1000,False) 
SELECT DISTINCT attrib_04 FROM gms.s_order_item_x; 
select * from gms.s_order where order_dt = '2018-02-01' 
The acquisition needs to look at the product they lapsed from and also how long ago they lapsed 
SELECT * FROM gms.s_order 
where order_num = '23879158989' 
 
--|1-FU5-3159|   823056101|23879158989|      1-B4BFMGH|    Renew|        Classic Care|         RSA|     Complete|   Reconciled|Update|                 null|    2019-02-01| 2019-02-05|        Renew|       N|                  Y|       Y| 
select from_utc_timestamp(actl_pay_dt,'AEST') actl_pay_dt_conv , actl_pay_dt , from_utc_timestamp(txn_dt,'AEST') txn_dt_conv , txn_dt  
from gms.s_src_payment 
where order_id = '1-B4BFMGH' 
 
spark.sql(""" 
 
select actl_pay_dt , txn_dt  
from gms.s_src_payment 
where order_id = '1-B4BFMGH' 
 
""").show() 
spark.sql("""select * from Renewal_Order where PaymentDate < OrderCompletionDate """).count() 
create table sandpit.cad_base as  
with cad as ( 
 
    select  
        srx.attrib_04 as cad_num 
        , s.x_bus_type as job_type 
        , s.sr_subtype_cd as job_subtype 
        , s.sr_category_cd as bkdn_cd 
        , s.root_cause_cd as bkdn_reason 
        , s.sr_stat_id as job_status 
        , srx.attrib_05 as vehicle_rego 
        , srx.attrib_03 as membernumber 
        , srx.attrib_27 as receive_dt 
    from gms.s_srv_req_x as srx  
 
        inner join gms.s_srv_req as s 
        on srx.row_id = s.row_id 
        and srx.attrib_04 is not null 
         
    where 
        1=1 
        and s.x_bulk_source = "CAD"  
        and s.x_bus_type in ("PATROL", "AUTOELEC", "OTHER", "CARELEC", "BATTERY", "TOW") 
 
), base as ( 
 
    -- 95% of subs are annual subs 
 
    select distinct  
        -- RENEWAL BASE COLUMNS  
        b.membernumber 
    ,   b.integration_id 
    ,   b.order_id 
    ,   b.renewal_yyyymm 
    --,   substr(cast(b.order_end_dt as string), 1,7) as sub_yyyymm 
    --,   datediff(b.order_end_dt, b.order_start_dt) as sub_length 
    ,   substr(b.renewal_yyyymm, 1,4) as renewal_yyyy 
    ,   b.order_start_dt 
    ,   b.order_end_dt 
    --  RSA subscription 
    ,   b.order_completed_dt 
    ,   coalesce(b.x_renewal_count,-99) as x_renewal_count 
    ,   b.order_channel 
    ,   b.order_pay_type 
        -- member attributes 
    ,   coalesce(b.member_colour,'UNKOWNN')                    AS member_colour 
    ,   coalesce(b.member_loyalty_colour,'UNKOWNN')            AS member_loyalty_colour 
    ,   CASE WHEN b.member_gender NOT IN ('Male','Female') THEN B.member_gender ELSE 'UNKNOWN' END AS member_gender 
    ,   b.prod_name 
    ,   coalesce(a.prod_grp,'Classic Care')     AS prod_grp 
    ,   coalesce(b.tenure_member,-99)           AS tenure_member 
    ,   b.x_iag_loc_desc 
    ,   b.item_net_PRICE 
    ,   coalesce(b.item_base_price,b.item_net_PRICE)    AS item_base_price 
        -- vehicle details  
    ,   b.vehicle_rego  
    ,   b.vehicle_make 
    ,   b.vehicle_model  
    ,   COALESCE(year(b.order_start_dt) - cast(b.vehicle_yr as int),-99)      as vehicle_age 
        --   vehicle attributes 
    ,   COALESCE(v1.vehicle_ancap,v2.vehicle_ancap,-99)             as vehicle_ancap 
    ,   COALESCE(v1.vehicle_body,v2.vehicle_body,"UNKNOWN")         as vehicle_body 
    ,   COALESCE(v1.vehicle_bore,v2.vehicle_bore,-99)               as vehicle_bore 
    ,   COALESCE(v1.vehicle_stroke,v2.vehicle_stroke,-99)           as vehicle_stroke 
    ,   COALESCE(cast(coalesce(v1.vehicle_co2,v2.vehicle_co2) as float),-99)   as vehicle_co2 
    ,   COALESCE(v1.vehicle_country,v2.vehicle_country,"UNKNOWN")   as vehicle_country 
    ,   COALESCE(v1.vehicle_cylinders,v2.vehicle_cylinders,-99)     as vehicle_cylinders 
    ,   CASE  
            WHEN COALESCE(v1.vehicle_drive,v2.vehicle_drive,"UNKNOWN") in ("4x4") then "FOUR WHEEL DRIVE" 
            WHEN COALESCE(v1.vehicle_drive,v2.vehicle_drive,"UNKNOWN") in ("4x2") then "ALL WHEEL DRIVE" 
            ELSE COALESCE(v1.vehicle_drive,v2.vehicle_drive,"UNKNOWN")  
        END  as vehicle_drive 
    ,   COALESCE(v1.vehicle_engine,v2.vehicle_engine,-99)           as vehicle_engine 
    ,   COALESCE(v1.vehicle_ratio,v2.vehicle_ratio,-99)             as vehicle_ratio 
    ,   COALESCE(v1.vehicle_efficiency,v2.vehicle_efficiency,-99)   as vehicle_efficiency 
    ,   COALESCE(v1.vehicle_tank,v2.vehicle_tank,-99)               as vehicle_tank 
    ,   COALESCE(v1.vehicle_fuel,v2.vehicle_fuel,"UNKNOWN")         as vehicle_fuel 
    ,   COALESCE(v1.vehicle_clearance,v2.vehicle_clearance,-99)     as vehicle_clearance 
    ,   COALESCE(v1.vehicle_height,v2.vehicle_height,-99)           as vehicle_height 
    ,   COALESCE(v1.vehicle_weight,v2.vehicle_weight,-99)           as vehicle_weight 
    ,   COALESCE(v1.vehicle_power,v2.vehicle_power,-99)             as vehicle_power 
    ,   COALESCE(v1.vehicle_rpm,v2.vehicle_rpm,-99)                 as vehicle_rpm 
    ,   COALESCE(v1.vehicle_torque,v2.vehicle_torque,-99)           as vehicle_torque 
    ,   COALESCE(v1.vehicle_power_r,v2.vehicle_power_r,-99)         as vehicle_power_r 
    ,   COALESCE(v1.vehicle_tow,v2.vehicle_tow,-99)                 as vehicle_tow 
    ,   COALESCE(v1.vehicle_turn,v2.vehicle_turn,-99)               as vehicle_turn 
    ,   COALESCE(v1.vehicle_width,v2.vehicle_width,-99)             as vehicle_width 
 
 
    from sandpit.renewal_base b  
 
    left join sandpit.vehicle_summ1 v1 
        on  b.vehicle_make = v1.vehicle_make 
        and b.vehicle_model = v1.vehicle_model  
        and cast(b.vehicle_yr as int) = cast(v1.vehicle_yr as int) 
 
    left join sandpit.vehicle_summ2 v2 
        on  b.vehicle_make = v2.vehicle_make 
        and b.vehicle_model = v2.vehicle_model  
 
    left join  sandpit.util_prod_budget  a  
        on b.prod_budget = a.prod_budget 
        and case when b.order_payment_term = "Y" then 'MPP' else 'Annual' end = a.payment_plan 
 
    where 
        1=1 
        and substr(cast(b.order_end_dt as string), 1,7) between "2019-01" and "2020-12" 
        and b.prod_sub_type = 'RSA' 
        and a.prod_grp in ("Classic Care","Premium Care","Premium Plus", "Free2Go (All)") 
        and b.membernumber is not null 
        -- and datediff(b.order_end_dt, b.order_start_dt) between 360 and 370 
 
), cad_to_base as ( 
 
    -- battery takes about 30% share of jobs, patrol is 53% and tow is 16% 
    -- the three jobs take 99% of cad jobs 
 
    select  
        b.membernumber 
        ,   b.vehicle_rego 
        ,   b.order_id 
        ,   count(distinct c.cad_num) as job_vol 
        ,   max(case when c.job_type = "PATROL" then 1 else 0 end) as is_patrol 
        --,   max(case when c.job_type = "AUTOELEC" then 1 else 0 end) as is_autoelec 
        --,   max(case when c.job_type = "OTHER" then 1 else 0 end) as is_other 
        --,   max(case when c.job_type = "CARELEC" then 1 else 0 end) as is_carelec 
        ,   max(case when c.job_type = "BATTERY" then 1 else 0 end) as is_battery 
        ,   max(case when c.job_type = "TOW" then 1 else 0 end) as is_tow 
    from base as b 
        inner join cad as c 
        on b.membernumber = c.membernumber 
        and b.vehicle_rego = c.vehicle_rego 
    where 
        to_date(b.order_start_dt) <= to_date(c.receive_dt) 
        and to_date(b.order_end_dt) >= to_date(c.receive_dt) 
    group by b.membernumber, b.vehicle_rego, b.order_id 
) 
 
 
select b.* 
    , coalesce(r.reliability_a, 1) as reliability_a 
    -- different responses 
    , case when coalesce(ctb.job_vol, 0) > 0 then 1 else 0 end as has_jobs 
    , coalesce(ctb.is_patrol,0) as is_patrol 
    , coalesce(ctb.is_battery,0) as is_battery 
    , coalesce(ctb.is_tow,0) as is_tow 
    , case when b.vehicle_body = "UNKNOWN" then "UNKNOWN" else "KNOWN" end as is_matched_flg 
from base as b 
    left join cad_to_base as ctb 
    on ctb.membernumber = b.membernumber 
    and ctb.vehicle_rego = b.vehicle_rego 
    and ctb.order_id = b.order_id 
 
    left join sandpit.util_car_reliability as r 
    on upper(b.vehicle_model) = upper(r.vehicle_model) 
    and upper(b.vehicle_make) = upper(r.vehicle_make) 
 
 
 
 
/* ########################################################### 
The following code is to show the breakdown by subs 
for historical records, we see per month the breakdown per vehicle is 0.23 
into sub ending 2020-08 and onwards, it's decreasing to 0.13.  
########################################################### */ 
 
spark.sql(""" 
 
    select renewal_yyyymm 
        , count(distinct vehicle_rego) as vehicle_num 
        , count(distinct vehicle_rego) / count(distinct order_id) as vehicle_per_order   
        , sum(has_jobs) as has_jobs 
        , sum(has_jobs) / count(distinct vehicle_rego) as percent 
    from sandpit.cad_base 
    group by renewal_yyyymm 
    order by renewal_yyyymm 
 
    """).show(100,False) 
 
spark.sql(''' 
SELECT DISTINCT 
    account_id, 
    order_end_dt, 
    membernumber, 
    total_value 
 
FROM 
    ( 
        SELECT  
            base.membernumber, 
            base.account_id, 
            base.order_end_dt, 
            value.monthid, 
            value.total_value, 
            MAX(value.monthid) OVER (PARTITION BY base.account_id, base.order_end_dt, base.membernumber) AS latest 
         
        FROM 
            sandpit.renewal_base AS base 
 
        INNER JOIN         
            mlpipelinedb.member_value_matrix_final AS value 
            ON value.membernumber = base.membernumber 
             
        WHERE 
            (value.monthid < YEAR(DATE_SUB(base.order_end_dt, 41))*100 + MONTH(DATE_SUB(base.order_end_dt, 41))) 
    ) AS latest_value 
 
WHERE 
    latest_value.monthid = latest_value.latest 
''').createOrReplaceTempView('value') 
 
spark.sql(''' 
SELECT DISTINCT 
    account_id, 
    order_end_dt, 
    asset_row_id, 
    prediction_probability 
 
FROM 
    ( 
        SELECT  
            base.asset_row_id, 
            base.account_id, 
            base.order_end_dt, 
            churn.prediction_date, 
            churn.prediction_probability, 
            MAX(churn.prediction_date) OVER (PARTITION BY base.account_id, base.order_end_dt, base.asset_row_id) AS latest 
         
        FROM 
            sandpit.renewal_base AS base 
 
        INNER JOIN         
            mlpipelinedb.churn_predictions_history AS churn 
            ON churn.asset_id = base.asset_row_id 
             
        WHERE 
            churn.prediction_date < DATE_SUB(base.order_end_dt, 41) 
    ) AS latest_churn 
 
WHERE 
    latest_churn.prediction_date = latest_churn.latest 
''').createOrReplaceTempView('churn') 
 
spark.sql(''' 
SELECT DISTINCT 
    base.order_id, 
    base.asset_row_id, 
    base.account_id, 
    base.contact_id, 
    value.total_value AS value, 
    churn.prediction_probability*100 AS churn 
 
FROM 
    sandpit.renewal_base AS base 
 
LEFT OUTER JOIN 
    churn 
    ON churn.account_id = base.account_id AND churn.order_end_dt = base.order_end_dt AND churn.asset_row_id = base.asset_row_id 
 
LEFT OUTER JOIN 
    value 
    ON value.account_id = base.account_id AND value.order_end_dt = base.order_end_dt AND value.membernumber = base.membernumber 
     
WHERE 
    base.membernumber IS NOT NULL 
    AND COALESCE(base.member_staff, 0) = 0 
    AND base.prod_name = 'Classic Care' 
    AND base.renewed_prod_name = 'Premium Care' 
''').show(250, False) 
The churn and value score codes should be able to join to other datasets too,  
main thing is that the Churn view join on the Account ID, Asset ID, and the DATE ONE DAY BEFORE THE RENEWAL  
(i.e. the effective to date of the previous order cast to a DATE not timestamp). 
 
Value joins on the same triplet albeit with csn instead of asset_id. 
Main thing to note is that CVM wasn't really online until like 
late 2018 I think? 
  
 
--all order ending in or after FY2019 
-- gone into renewal fy19 onwards 
SELECT order_id 
        ,org_type_cd 
        ,account_id 
        , contact_id 
        , membernumber 
        , member_colour 
        , member_loyalty_colour 
          
FROM sandpit.renewal_base  
WHERE prod_name = 'Classic Care' AND renewed_prod_name = 'Premium Care' 
and left join to itself for current premium care 
--and contact_cd='Ordinary Member' --not neccessary active for analysis? 
--and asset_status_cd='Active' 
--people who joined on premium care 
select * from sandpit.renewal_base 
where tenure_member=0 
and prod_name='Premium Care'                                                                                                                                                                                                                                                                           
--Vehicle data 
select * from sandpit.renewal_base b 
left join sandpit.model_veh_order veh 
on b.membernumber=veh.membernumber 
--and b.vehicle_rego=veh.vehicle_rego 
and b.integration_id=veh.integration_id 
and b.order_id=veh.order_id 
select vehicle_weight from sandpit.model_veh_order limit 10