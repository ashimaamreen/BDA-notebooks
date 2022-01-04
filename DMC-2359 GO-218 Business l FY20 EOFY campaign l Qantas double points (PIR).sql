SELECT DISTINCT item_action_cd, item_new_cd, order_type, item_order_type, item_action_delete, addon_count from sandpit.renewal_base 
where contact_cd IN ('Business Contact') 
--and order_id='1-EUUOHWM' 
and item_action_cd='Add' 
--and item_order_type is null 
and item_new_cd='Y' 
SELECT case when c.campaign_id='57441282' then 'QBR' 
            when c.campaign_id='57438802' then 'Non QBR' 
            else 'Other' end Segment 
        , c.control 
        , count(DISTINCT c.customer_id) 
 
from omc.send_level_summary c 
 
where c.campaign_id in ('57441282','57438802') 
--and c.bounce_event_date is null 
--and (c.open_event_date is not null or c.click_event_date is not null) 
--and c.click_event_date is not null 
--and c.unsubbed_event_date is null 
GROUP BY 1,2 
ORDER BY 1,2 
--ORDER BY 3 DESC 
 
SELECT case when campaign_id='57441282' then 'QBR' 
            when e.campaign_id='57438802' then 'Non-QBR' 
            else 'not sent' end type 
        , count(DISTINCT e.customer_id) 
 
 
 
FROM 
sandpit.renewal_base AS B 
 
LEFT JOIN 
sandpit.util_prod_budget AS A 
ON A.prod_budget = B.prod_budget 
--AND CASE WHEN B.order_payment_term = 'Y' THEN 'MPP' ELSE 'Annual' END = A.payment_plan 
 
INNER JOIN 
sandpit.util_renew_summ AS C 
ON C.match_rnk = B.match_rnk 
 
INNER JOIN 
gms.s_org_ext AS org 
ON B.account_id = org.row_id 
 
inner JOIN omc.send_level_summary e 
on e.customer_id=b.contact_id 
and e.campaign_id in ('57441282','57438802') 
 
 
WHERE 
COALESCE(C.type_rnk, 0) != 1 
AND COALESCE(B.member_staff, 0) = 0 
AND (DATE_ADD(B.order_end_dt, 2) >= A.dt_min OR A.dt_min IS NULL) 
--AND B.prod_type IN ('RSA', 'Non-RSA','Add-on') 
 
AND B.contact_cd IN ('Business Contact') 
AND DATE_ADD(order_end_dt, 1) BETWEEN ('2021-05-13') AND ('2021-06-30') 
 
AND COALESCE(market_class_cd, '') NOT LIKE 'ADM%' 
AND COALESCE(market_class_cd, '') NOT LIKE '50+%' 
 
and e.bounce_event_date is null 
--and (e.open_event_date is not null or e.click_event_date is not null) 
and e.click_event_date is not null  
 
group by 1 
ORDER BY 1 
 
SELECT case when campaign_id='57441282' then 'QBR' 
            when e.campaign_id='57438802' then 'Non-QBR' 
            else 'not sent' end type 
        , control 
        , sum(1)                      AS renewals 
        , sum(renewal_cd)           as renewed 
        , sum(case when b.renewed_completed_dt <= date_add(order_end_dt,2) then 1 else 0 end  ) as renewed_pot 
        , sum(case when b.item_action_cd='Add' and item_new_cd='Y' then 1 else 0 end) as new_added 
        --, count(DISTINCT B.account_id) 
 
 
 
FROM 
sandpit.renewal_base AS B 
 
LEFT JOIN 
sandpit.util_prod_budget AS A 
ON A.prod_budget = B.prod_budget 
--AND CASE WHEN B.order_payment_term = 'Y' THEN 'MPP' ELSE 'Annual' END = A.payment_plan 
 
INNER JOIN 
sandpit.util_renew_summ AS C 
ON C.match_rnk = B.match_rnk 
 
INNER JOIN 
gms.s_org_ext AS org 
ON B.account_id = org.row_id 
 
LEFT JOIN omc.send_level_summary e 
on e.customer_id=b.contact_id 
and e.campaign_id in ('57441282','57438802') 
 
 
WHERE 
COALESCE(C.type_rnk, 0) != 1 
AND COALESCE(B.member_staff, 0) = 0 
AND (DATE_ADD(B.order_end_dt, 2) >= A.dt_min OR A.dt_min IS NULL) 
--AND B.prod_type IN ('RSA', 'Non-RSA','Add-on') 
 
AND B.contact_cd IN ('Business Contact') 
AND DATE_ADD(order_end_dt, 1) BETWEEN ('2021-05-13') AND ('2021-06-30') 
 
AND COALESCE(market_class_cd, '') NOT LIKE 'ADM%' 
AND COALESCE(market_class_cd, '') NOT LIKE '50+%' 
 
group by 1,2 
ORDER BY 1,2 
 
SELECT case when campaign_id='57441282' then 'QBR' 
            when e.campaign_id='57438802' then 'Non-QBR' 
            else 'not sent' end type 
        , control 
        , count(DISTINCT e.customer_id) as contacts 
        , sum(case when b.integration_id is not null then 1 else 0 end)                      AS renewals 
        , sum(renewal_cd)           as renewed 
        , sum(case when b.renewed_completed_dt <= date_add(order_end_dt,2) then 1 else 0 end  ) as renewed_pot 
        , sum(case when b.item_action_cd='Add' and item_new_cd='Y' then 1 else 0 end) as new_added 
        --, count(DISTINCT B.account_id) 
 
 
 
FROM omc.send_level_summary e 
 
LEFT JOIN sandpit.renewal_base AS B 
on e.customer_id=b.contact_id 
AND DATE_ADD(order_end_dt, 1) BETWEEN ('2021-05-13') AND ('2021-06-30') 
AND COALESCE(B.member_staff, 0) = 0 
AND B.contact_cd IN ('Business Contact') 
 
 
LEFT JOIN sandpit.util_prod_budget AS A 
ON A.prod_budget = B.prod_budget 
 
left JOIN sandpit.util_renew_summ AS C 
ON C.match_rnk = B.match_rnk 
AND (DATE_ADD(B.order_end_dt, 2) >= A.dt_min OR A.dt_min IS NULL) 
and COALESCE(C.type_rnk, 0) != 1 
 
left JOIN gms.s_org_ext AS org 
ON B.account_id = org.row_id 
AND COALESCE(market_class_cd, '') NOT LIKE 'ADM%' 
AND COALESCE(market_class_cd, '') NOT LIKE '50+%' 
 
 
WHERE 1=1 
and e.campaign_id in ('57441282','57438802') 
 
 
 
group by 1,2 
ORDER BY 1,2 
 
SELECT case when campaign_id='57441282' then 'QBR' 
            when e.campaign_id='57438802' then 'Non-QBR' 
            else 'not sent' end type 
        , control 
        -- , count(DISTINCT e.customer_id) as contacts 
        -- , sum(case when b.integration_id is not null then 1 else 0 end)                      AS renewals 
        -- , sum(renewal_cd)           as renewed 
        -- , sum(case when b.renewed_completed_dt <= date_add(order_end_dt,2) then 1 else 0 end  ) as renewed_pot 
        , sum(case when b.item_action_cd='Add' then 1 else 0 end) as new_added 
        --, count(DISTINCT B.account_id) 
 
 
 
FROM omc.send_level_summary e 
 
LEFT JOIN sandpit.renewal_base AS B 
on e.customer_id=b.contact_id 
AND DATE_ADD(order_end_dt, 1) BETWEEN ('2021-05-13') AND ('2021-06-30') 
AND COALESCE(B.member_staff, 0) = 0 
AND B.contact_cd IN ('Business Contact') 
 
 
LEFT JOIN sandpit.util_prod_budget AS A 
ON A.prod_budget = B.prod_budget 
 
left JOIN sandpit.util_renew_summ AS C 
ON C.match_rnk = B.match_rnk 
AND (DATE_ADD(B.order_end_dt, 2) >= A.dt_min OR A.dt_min IS NULL) 
and COALESCE(C.type_rnk, 0) != 1 
 
left JOIN gms.s_org_ext AS org 
ON B.account_id = org.row_id 
AND COALESCE(market_class_cd, '') NOT LIKE 'ADM%' 
AND COALESCE(market_class_cd, '') NOT LIKE '50+%' 
 
 
WHERE 1=1 
and e.campaign_id in ('57441282','57438802') 
 
 
 
group by 1,2 
ORDER BY 1,2 
spark.sql(''' 
WITH 
    join_orders AS ( 
        SELECT 
            org.row_id AS acc_id, 
            ord.order_dt, 
            RANK() OVER(PARTITION BY org.row_id ORDER BY ord.order_dt DESC) AS idx 
         
        FROM 
            gms.s_order AS ord 
         
        LEFT JOIN 
            gms.s_order_type AS ordt 
            ON ord.order_type_id = ordt.row_id 
         
        LEFT JOIN 
            gms.s_order_item AS ordi 
            ON ord.row_id = ordi.order_id 
         
        LEFT JOIN 
            gms.s_prod_int AS prod 
            ON ordi.prod_id = prod.row_id 
         
        LEFT JOIN 
            gms.s_org_ext AS org 
            ON ord.accnt_id = org.row_id 
         
        WHERE 1=1 
            AND ordt.name IN ('New') 
            AND ordi.action_cd IN ('Add') 
            AND ord.status_cd = 'Complete' 
            AND org.ou_type_cd IN ('Member Organisation') 
            AND prod.name IN ('Membership') 
            AND ord.x_payment_status IN ('Reconciled', 'Payment Taken', 'Not Required') 
            AND COALESCE(market_class_cd, '') NOT LIKE 'ADM%' 
            AND COALESCE(market_class_cd, '') NOT LIKE '50+%' 
    ) 
 
SELECT DISTINCT 
    join_orders.acc_id, 
    MIN(join_orders.order_dt) AS order_dt 
 
FROM 
    join_orders 
     
WHERE 
    idx = 1 
     
GROUP BY 
    1 
''').createOrReplaceTempView('joined') 
spark.sql(""" 
select year(order_dt), month(order_dt), count(distinct acc_id) from joined 
group by 1,2 
order by 1,2 
""").show(10000,False) 
 
SELECT case when campaign_id='57441282' then 'QBR' 
            when e.campaign_id='57438802' then 'Non-QBR' 
            else 'not sent' end type 
        , case when b.item_action_cd='Add' then 1 else 0 end new 
        , B.member_colour 
        , count(DISTINCT e.customer_id) as contacts 
        -- , sum(case when b.integration_id is not null then 1 else 0 end)                      AS renewals 
        -- , sum(renewal_cd)           as renewed 
        -- , sum(case when b.renewed_completed_dt <= date_add(order_end_dt,2) then 1 else 0 end  ) as renewed_pot 
        --, sum(case when b.item_action_cd='Add' then 1 else 0 end) as new_added 
        --, count(DISTINCT B.account_id) 
 
 
 
FROM omc.send_level_summary e 
 
LEFT JOIN sandpit.renewal_base AS B 
on e.customer_id=b.contact_id 
AND DATE_ADD(order_end_dt, 1) BETWEEN ('2021-05-13') AND ('2021-06-30') 
AND COALESCE(B.member_staff, 0) = 0 
AND B.contact_cd IN ('Business Contact') 
 
 
LEFT JOIN sandpit.util_prod_budget AS A 
ON A.prod_budget = B.prod_budget 
 
left JOIN sandpit.util_renew_summ AS C 
ON C.match_rnk = B.match_rnk 
AND (DATE_ADD(B.order_end_dt, 2) >= A.dt_min OR A.dt_min IS NULL) 
and COALESCE(C.type_rnk, 0) != 1 
 
left JOIN gms.s_org_ext AS org 
ON B.account_id = org.row_id 
AND COALESCE(market_class_cd, '') NOT LIKE 'ADM%' 
AND COALESCE(market_class_cd, '') NOT LIKE '50+%' 
 
 
WHERE 1=1 
and e.campaign_id in ('57441282','57438802') 
and e.control is false 
 
 
group by 1,2,3 
ORDER BY 1,2,3 