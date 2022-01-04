spark.read.load('qbr_master.csv', format = 'csv', sep = ',', inferSchema = True, header = True).createOrReplaceTempView('df') 
 
 
# spark.read.load('FY20_B_Acq.csv', format = 'csv', sep = ',', inferSchema = True, header = True).createOrReplaceTempView('OBI') 
spark.sql(''' 
SELECT 
    `SALES ORDER NUMBER` AS order_num, 
    `MEMBERSHIP NUMBER` AS csn, 
    TO_TIMESTAMP(STRING(`TRANSACTION START DATE`), 'yyyyMMdd') AS start_dt, 
    TO_TIMESTAMP(STRING(`TRANSACTION END DATE`), 'yyyyMMdd') AS end_dt, 
    `GROSS AMOUNT` AS gross_amount, 
    `TOTAL POINTS` AS total_points 
 
FROM 
    df 
     
WHERE 
    `ACCEPTED OR REJECT` = 'A' 
''').createOrReplaceTempView('df') 
spark.sql(''' 
SELECT DISTINCT 
    ord.row_id AS ord_id, 
    ord.order_num, 
    ord.accnt_id, 
    ord.order_dt 
     
FROM 
    df  
 
LEFT JOIN 
    gms.s_order AS ord 
    ON ord.order_num = df.order_num 
''').createOrReplaceTempView('qbr_ord') 
 
spark.sql(''' 
SELECT DISTINCT 
    accnt_id 
     
FROM 
    qbr_ord 
''').createOrReplaceTempView('qbr_acc') 
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
 
spark.sql(''' 
SELECT 
    joined.acc_id, 
    joined.order_dt, 
    SUM(ordi.net_pri/(YEAR(ordiom.service_end_dt) - YEAR(ord.order_dt))) AS total_net, 
    COUNT(*) AS subs, 
    MAX(qbr_ord.ord_id IS NOT NULL) AS qbr_joined, 
    MAX(qbr_acc.accnt_id IS NOT NULL) AS qbr_ever 
 
FROM 
    joined 
 
INNER JOIN 
    gms.s_order AS ord 
    ON ord.accnt_id = joined.acc_id 
    AND ABS(DATEDIFF(ord.order_dt, joined.order_dt)) <= 7 
 
INNER JOIN 
    gms.s_order_type AS ordt 
    ON ordt.row_id = ord.order_type_id 
 
INNER JOIN 
    gms.s_order_item AS ordi 
    ON ordi.order_id = ord.row_id 
     
INNER JOIN 
    gms.s_prod_int AS prod 
    ON prod.row_id = ordi.prod_id 
    AND prod.prod_cd = 'Product' 
    AND prod.sub_type_cd IN ('RSA', 'Non-RSA') 
     
LEFT JOIN 
    qbr_ord 
    ON ord.row_id = qbr_ord.ord_id 
 
LEFT JOIN 
    gms.s_order_item_om AS ordiom 
    ON ordiom.par_row_id = ordi.row_id 
 
LEFT JOIN 
    qbr_acc 
    ON ord.accnt_id = qbr_acc.accnt_id 
     
WHERE 1=1 
    AND ordt.name = 'New' 
    AND ordi.action_cd IN ('Add') 
    AND ord.status_cd = 'Complete'  
     
GROUP BY 
    1, 
    2 
''').createOrReplaceTempView('initial') 
 
spark.sql(''' 
SELECT 
    CASE 
        WHEN DATE(order_dt) BETWEEN DATE('2019-07-01') AND DATE('2020-06-30') THEN '2020' 
        WHEN DATE(order_dt) BETWEEN DATE('2018-07-01') AND DATE('2019-06-30') THEN '2019' 
    END AS fy, 
    qbr_joined, 
    COUNT(DISTINCT acc_id) AS members, 
    MIN(subs), 
    MAX(subs), 
    SUM(subs) AS subs, 
    SUM(total_net) AS total_net 
     
FROM 
    initial 
     
WHERE 
    DATE(order_dt) BETWEEN DATE('2018-07-01') AND DATE('2020-06-30') 
     
GROUP BY 
    1, 
    2 
 
ORDER BY 
    1, 
    2 
''').show(250, False) 
spark.sql(''' 
SELECT * FROM joined WHERE acc_id = '1-3J28MFS' 
''').show(250, False) 
 
 
spark.sql(''' 
SELECT 
    joined.acc_id, 
    ord.row_id, 
    ord.order_dt, 
    ordi.asset_integ_id, 
    prod.name 
 
FROM 
    joined 
 
INNER JOIN 
    gms.s_order AS ord 
    ON ord.accnt_id = joined.acc_id 
    AND ABS(DATEDIFF(ord.order_dt, joined.order_dt)) <= 7 
 
INNER JOIN 
    gms.s_order_type AS ordt 
    ON ordt.row_id = ord.order_type_id 
 
INNER JOIN 
    gms.s_order_item AS ordi 
    ON ordi.order_id = ord.row_id 
     
INNER JOIN 
    gms.s_prod_int AS prod 
    ON prod.row_id = ordi.prod_id 
    AND prod.prod_cd = 'Product' 
    AND prod.sub_type_cd IN ('RSA', 'Non-RSA') 
 
LEFT JOIN 
    qbr_ord 
    ON ord.row_id = qbr_ord.ord_id 
     
LEFT JOIN 
   qbr_acc 
    ON ord.accnt_id = qbr_acc.accnt_id 
 
WHERE 1=1 
    AND ordt.name = 'New' 
    AND ordi.action_cd IN ('Add') 
    AND ord.status_cd = 'Complete'  
    AND joined.acc_id = '1-3J28MFS' 
 
''').show(250, False) 
spark.sql(''' 
SELECT * FROM qbr_flag WHERE accnt_id = '1-3J28MFS' 
''').show(250, False) 
spark.sql(''' 
SELECT * FROM initial WHERE subs > 40 
''').show(250, False) 
 
 
spark.sql(''' 
SELECT 
    qbr_acc.accnt_id IS NOT NULL AS qbr, 
    B.renewal_yyyymm, 
    SUM(1) AS renewals, 
    SUM(C.renewal_cd) AS renewed, 
    SUM(C.renewal_cd)/SUM(1) AS rate, 
    SUM(CASE WHEN B.renewed_completed_dt <= DATE_ADD(order_end_dt, 2) THEN 1 ELSE 0 END) AS renewed_pot, 
    SUM(CASE WHEN B.renewed_completed_dt <= DATE_ADD(order_end_dt, 2) THEN 1 ELSE 0 END)/SUM(1) AS pot_rate 
 
FROM 
    sandpit.renewal_base AS B 
     
LEFT JOIN 
    sandpit.util_prod_budget AS A 
    ON A.prod_budget = B.prod_budget 
    AND CASE WHEN B.order_payment_term = 'Y' THEN 'MPP' ELSE 'Annual' END = A.payment_plan 
 
INNER JOIN 
    sandpit.util_renew_summ AS C 
    ON C.match_rnk = B.match_rnk 
     
LEFT JOIN 
    qbr_acc 
    ON B.account_id = qbr_acc.accnt_id 
 
INNER JOIN 
    gms.s_org_ext AS org 
    ON B.account_id = org.row_id 
 
WHERE 
    COALESCE(C.type_rnk, 0) != 1  
    AND COALESCE(B.member_staff, 0) = 0  
    -- AND A.removeID = 0  
    AND (DATE_ADD(B.order_end_dt, 2) >= A.dt_min  OR A.dt_min IS NULL) 
    AND B.prod_type IN ('RSA', 'Non-RSA') 
     
    AND B.contact_cd IN ('Business Contact') 
    AND DATE(DATE_ADD(order_end_dt, 1)) BETWEEN DATE('2018-07-01') AND DATE('2020-06-30') 
     
    AND COALESCE(market_class_cd, '') NOT LIKE 'ADM%' 
    AND COALESCE(market_class_cd, '') NOT LIKE '50+%' 
 
GROUP BY 
    2, 
    1 
     
ORDER BY 
    2, 
    1 
''').show(250, False) 
 
 
 
 
spark.sql(''' 
SELECT 
    qbr_acc.accnt_id IS NOT NULL AS qbr, 
    COUNT(DISTINCT B.account_id) AS acc_due, 
    COUNT(DISTINCT CASE WHEN C.renewal_cd > 0 THEN B.account_id ELSE NULL END) AS acc_ren, 
    SUM(1) AS subs_due, 
    SUM(C.renewal_cd) AS subs_ren, 
    SUM(item_net_price/(YEAR(order_end_dt) - YEAR(order_start_dt))) AS rev_due, 
    SUM(CASE WHEN C.renewal_cd > 0 THEN item_net_price/(YEAR(order_end_dt) - YEAR(order_start_dt)) ELSE 0 END) AS rev_ren 
 
FROM 
    sandpit.renewal_base AS B 
     
LEFT JOIN 
    sandpit.util_prod_budget AS A 
    ON A.prod_budget = B.prod_budget 
    AND CASE WHEN B.order_payment_term = 'Y' THEN 'MPP' ELSE 'Annual' END = A.payment_plan 
 
INNER JOIN 
    sandpit.util_renew_summ AS C 
    ON C.match_rnk = B.match_rnk 
     
LEFT JOIN 
    qbr_acc 
    ON B.account_id = qbr_acc.accnt_id 
 
INNER JOIN 
    gms.s_org_ext AS org 
    ON B.account_id = org.row_id 
 
WHERE 
    COALESCE(C.type_rnk, 0) != 1  
    AND COALESCE(B.member_staff, 0) = 0  
    -- AND A.removeID = 0  
    AND (DATE_ADD(B.order_end_dt, 2) >= A.dt_min  OR A.dt_min IS NULL) 
    AND B.prod_type IN ('RSA', 'Non-RSA') 
     
    AND B.contact_cd IN ('Business Contact') 
    AND DATE(DATE_ADD(order_end_dt, 1)) BETWEEN DATE('2019-07-01') AND DATE('2020-06-30') 
     
    AND COALESCE(market_class_cd, '') NOT LIKE 'ADM%' 
    AND COALESCE(market_class_cd, '') NOT LIKE '50+%' 
 
GROUP BY 
    1 
''').show(250, False) 
 
 
 
spark.sql(''' 
SELECT 
    qbr_acc.accnt_id IS NOT NULL AS qbr, 
    YEAR(DATE_ADD(order_end_dt, 1)) AS yr, 
    SUM(1) AS renewals, 
    SUM(C.renewal_cd) AS renewed, 
    SUM(C.renewal_cd)/SUM(1) AS rate, 
    SUM(CASE WHEN B.renewed_completed_dt <= DATE_ADD(order_end_dt, 2) THEN 1 ELSE 0 END) AS renewed_pot, 
    SUM(CASE WHEN B.renewed_completed_dt <= DATE_ADD(order_end_dt, 2) THEN 1 ELSE 0 END)/SUM(1) AS pot_rate 
 
FROM 
    sandpit.renewal_base AS B 
     
LEFT JOIN 
    sandpit.util_prod_budget AS A 
    ON A.prod_budget = B.prod_budget 
    AND CASE WHEN B.order_payment_term = 'Y' THEN 'MPP' ELSE 'Annual' END = A.payment_plan 
 
INNER JOIN 
    sandpit.util_renew_summ AS C 
    ON C.match_rnk = B.match_rnk 
     
LEFT JOIN 
    qbr_acc 
    ON B.account_id = qbr_acc.accnt_id 
 
INNER JOIN 
    gms.s_org_ext AS org 
    ON B.account_id = org.row_id 
 
WHERE 
    COALESCE(C.type_rnk, 0) != 1  
    AND COALESCE(B.member_staff, 0) = 0  
    -- AND A.removeID = 0  
    AND (DATE_ADD(B.order_end_dt, 2) >= A.dt_min  OR A.dt_min IS NULL) 
    AND B.prod_type IN ('RSA', 'Non-RSA') 
     
    AND B.contact_cd IN ('Business Contact') 
    AND( 
        DATE(DATE_ADD(order_end_dt, 1)) BETWEEN DATE('2019-03-01') AND DATE('2019-06-30') 
        OR DATE(DATE_ADD(order_end_dt, 1)) BETWEEN DATE('2020-03-01') AND DATE('2020-06-30') 
    ) 
     
    AND COALESCE(market_class_cd, '') NOT LIKE 'ADM%' 
    AND COALESCE(market_class_cd, '') NOT LIKE '50+%' 
 
GROUP BY 
    2, 
    1 
     
ORDER BY 
    2, 
    1 
''').show(250, False) 
 
spark.sql(''' 
SELECT 
    B.account_id, 
    SUM(1) AS renewals, 
    SUM(C.renewal_cd) AS renewed, 
    SUM(CASE WHEN B.renewed_completed_dt <= DATE_ADD(order_end_dt, 2) THEN 1 ELSE 0 END) AS renewed_pot 
 
FROM 
    sandpit.renewal_base AS B 
     
LEFT JOIN 
    sandpit.util_prod_budget AS A 
    ON A.prod_budget = B.prod_budget 
    AND CASE WHEN B.order_payment_term = 'Y' THEN 'MPP' ELSE 'Annual' END = A.payment_plan 
 
INNER JOIN 
    sandpit.util_renew_summ AS C 
    ON C.match_rnk = B.match_rnk 
     
INNER JOIN 
    gms.s_org_ext AS org 
    ON B.account_id = org.row_id 
 
WHERE 
    COALESCE(C.type_rnk, 0) != 1  
    AND COALESCE(B.member_staff, 0) = 0  
    -- AND A.removeID = 0  
    AND (DATE_ADD(B.order_end_dt, 2) >= A.dt_min  OR A.dt_min IS NULL) 
    AND B.prod_type IN ('RSA', 'Non-RSA') 
     
    AND B.contact_cd IN ('Business Contact') 
    AND DATE(DATE_ADD(order_end_dt, 1)) BETWEEN DATE('2019-07-01') AND DATE('2020-06-30') 
     
    AND COALESCE(market_class_cd, '') NOT LIKE 'ADM%' 
    AND COALESCE(market_class_cd, '') NOT LIKE '50+%' 
 
GROUP BY 
    1 
''').createOrReplaceTempView('fy20') 
 
 
spark.sql(''' 
SELECT 
    B.account_id, 
    SUM(1) AS renewals, 
    SUM(C.renewal_cd) AS renewed, 
    SUM(CASE WHEN B.renewed_completed_dt <= DATE_ADD(order_end_dt, 2) THEN 1 ELSE 0 END) AS renewed_pot 
 
FROM 
    sandpit.renewal_base AS B 
     
LEFT JOIN 
    sandpit.util_prod_budget AS A 
    ON A.prod_budget = B.prod_budget 
    AND CASE WHEN B.order_payment_term = 'Y' THEN 'MPP' ELSE 'Annual' END = A.payment_plan 
 
INNER JOIN 
    sandpit.util_renew_summ AS C 
    ON C.match_rnk = B.match_rnk 
 
INNER JOIN 
    gms.s_org_ext AS org 
    ON B.account_id = org.row_id 
 
WHERE 
    COALESCE(C.type_rnk, 0) != 1  
    AND COALESCE(B.member_staff, 0) = 0  
    -- AND A.removeID = 0  
    AND (DATE_ADD(B.order_end_dt, 2) >= A.dt_min  OR A.dt_min IS NULL) 
    AND B.prod_type IN ('RSA', 'Non-RSA') 
     
    AND B.contact_cd IN ('Business Contact') 
    AND DATE(DATE_ADD(order_end_dt, 1)) BETWEEN DATE('2018-07-01') AND DATE('2019-06-30') 
     
    AND COALESCE(market_class_cd, '') NOT LIKE 'ADM%' 
    AND COALESCE(market_class_cd, '') NOT LIKE '50+%' 
     
GROUP BY 
    1 
''').createOrReplaceTempView('fy19') 
spark.sql(''' 
SELECT 
    qbr_acc.accnt_id IS NOT NULL AS qbr, 
    CASE 
        WHEN fy19.renewed_pot = fy19.renewals THEN 'full' 
        WHEN fy19.renewed_pot = 0 THEN 'no' 
        ELSE 'mixed' 
    END AS pat19, 
    CASE 
        WHEN fy20.renewed_pot = fy20.renewals THEN 'full' 
        WHEN fy20.renewed_pot = 0 THEN 'no' 
        ELSE 'mixed' 
    END AS pat20, 
    COUNT(DISTINCT fy19.account_id), 
    SUM(fy19.renewed_pot) AS pot19, 
    SUM(fy20.renewed_pot) AS pot20, 
    SUM(fy19.renewals) AS ren19, 
    SUM(fy20.renewals) AS ren20 
     
FROM 
    fy19 
 
INNER JOIN 
    fy20 
    ON fy20.account_id = fy19.account_id 
     
LEFT JOIN 
    qbr_acc 
    ON fy19.account_id = qbr_acc.accnt_id 
 
GROUP BY 
    1, 
    2, 
    3 
 
ORDER BY 
    1, 
    2, 
    3 
''').show(250, False) 
 
 
 
 
spark.sql(''' 
SELECT 
    qbr_acc.accnt_id IS NOT NULL AS qbr, 
     
    CASE 
        WHEN renewed_pot = renewals THEN 'full' 
        WHEN renewed_pot = 0 THEN 'no' 
        ELSE 'mixed' 
    END AS pot, 
    CASE 
        WHEN renewed_pot = renewed THEN 'full' 
        WHEN renewed_pot = 0 THEN 'no' 
        ELSE 'mixed' 
    END AS ren, 
    COUNT(DISTINCT account_id), 
    SUM(renewed_pot), 
    SUM(renewals) 
     
FROM 
    fy19 
     
LEFT JOIN 
    qbr_acc 
    ON fy19.account_id = qbr_acc.accnt_id 
 
GROUP BY 1,2,3 ORDER BY 1,2,3 
''').show(250, False) 
 
spark.sql(''' 
SELECT 
    qbr_acc.accnt_id IS NOT NULL AS qbr, 
     
    CASE 
        WHEN renewed_pot = renewals THEN 'full' 
        WHEN renewed_pot = 0 THEN 'no' 
        ELSE 'mixed' 
    END AS pot, 
    COUNT(DISTINCT account_id), 
    SUM(renewed_pot), 
    SUM(renewals) 
     
FROM 
    fy19 
     
LEFT JOIN 
    qbr_acc 
    ON fy19.account_id = qbr_acc.accnt_id 
 
GROUP BY 1,2 ORDER BY 1,2 
''').show(250, False) 
 
spark.sql(''' 
SELECT 
    qbr_acc.accnt_id IS NOT NULL AS qbr, 
    CASE 
        WHEN renewed_pot = renewed THEN 'full' 
        WHEN renewed_pot = 0 THEN 'no' 
        ELSE 'mixed' 
    END AS ren, 
    COUNT(DISTINCT account_id), 
    SUM(renewed_pot), 
    SUM(renewals) 
     
FROM 
    fy19 
     
LEFT JOIN 
    qbr_acc 
    ON fy19.account_id = qbr_acc.accnt_id 
 
GROUP BY 1,2 ORDER BY 1,2 
''').show(250, False) 
 
# spark.sql(''' 
# SELECT 
#     qbr_acc.accnt_id IS NOT NULL AS qbr, 
     
#     CASE 
#         WHEN renewed_pot = renewals THEN 'full' 
#         WHEN renewed_pot = 0 THEN 'no' 
#         ELSE 'mixed' 
#     END AS pot, 
#     COUNT(DISTINCT account_id), 
#     SUM(renewed_pot), 
#     SUM(renewals) 
     
# FROM 
#     fy20 
     
# LEFT JOIN 
#     qbr_acc 
#     ON fy20.account_id = qbr_acc.accnt_id 
 
# GROUP BY 1,2 ORDER BY 1,2 
# ''').show(250, False) 
 
 
spark.sql(''' 
SELECT 
    * 
     
FROM 
    fy19 
     
LEFT JOIN 
    qbr_acc 
    ON fy19.account_id = qbr_acc.accnt_id 
 
WHERE renewed_pot != renewals AND renewed_pot != 0 
''').show(250, False) 
# DONE 
# ARPU 2019 
# ARPU 2020 
 
# DONE 
# QBR Acquisition 2019 
# QBR Acquisition 2020 
 
# DONE 
# Current Value with QBR 
# Current Value without QBR 
 
# DONE 
# POT QBR vs. Non QBR 
 
# DONE 
# POT Before vs. POT After 
spark.sql(''' 
WITH 
    price AS ( 
        SELECT 
            ordi.asset_integ_id, 
            ordi.net_pri/(YEAR(ordiom.service_end_dt) - YEAR(ord.order_dt)) AS net_pri, 
            ordt.name, 
            ordi.action_cd, 
            RANK() OVER (PARTITION BY ordi.asset_integ_id ORDER BY ord.order_dt DESC) AS idx 
          
        FROM 
            gms.s_order AS ord 
             
        INNER JOIN 
            gms.s_order_type AS ordt 
            ON ordt.row_id = ord.order_type_id 
             
        INNER JOIN 
            gms.s_order_item AS ordi 
            ON ordi.order_id = ord.row_id 
             
        LEFT JOIN 
            gms.s_order_item_om AS ordiom 
            ON ordiom.par_row_id = ordi.row_id 
         
        WHERE 
            ordi.asset_integ_id IS NOT NULL 
            AND ord.status_cd = 'Complete' 
    ) 
 
SELECT 
    asset_integ_id, 
    MAX(net_pri) AS net_pri 
 
FROM 
    price 
 
WHERE 
    idx = 1 
    AND name != 'Cancellation' 
    AND action_cd IN ('Add', 'Update') 
     
GROUP BY 
    1 
''').createOrReplaceTempView('asset_price') 
 
 
spark.sql(''' 
SELECT 
    asset.owner_accnt_id, 
    --SUM(CASE WHEN prod.name = 'Assist' THEN 1 ELSE 0 END) AS assist, 
    --SUM(CASE WHEN prod.name = 'Absolute' THEN 1 ELSE 0 END) AS absolute 
    SUM(1) AS subs, 
    SUM(asset_price.net_pri) AS total_net 
 
FROM 
    gms.s_asset AS asset 
 
INNER JOIN 
    gms.s_prod_int AS prod 
    ON prod.row_id = asset.prod_id 
     
LEFT JOIN 
    asset_price 
    ON asset_price.asset_integ_id = asset.integration_id 
 
WHERE 
    asset.status_cd = 'Active' 
    AND prod.sub_type_cd IN ('RSA', 'Non-RSA') 
    AND prod.prod_cd = 'Product' 
     
GROUP BY 
    1 
''').createOrReplaceTempView('current') 
 
 
spark.sql(''' 
SELECT 
    initial.acc_id, 
    INT(DATEDIFF(NOW(), order_dt)/365) AS since_join, 
    initial.subs AS initial_subs, 
    current.subs AS current_subs, 
    initial.total_net AS initial_rev, 
    current.total_net AS current_rev, 
    current.subs - initial.subs AS sub_flux, 
    current.total_net - initial.total_net AS rev_flux, 
    initial.qbr_joined, 
    initial.qbr_ever 
 
FROM 
    initial 
     
INNER JOIN 
    current 
    ON current.owner_accnt_id = initial.acc_id 
''').createOrReplaceTempView('flux') 
spark.sql(''' 
SELECT 
    since_join, 
    qbr_ever, 
    SUM(initial_subs), 
    SUM(current_subs), 
    SUM(initial_rev), 
    SUM(current_rev), 
    COUNT(*), 
    AVG(sub_flux), 
    AVG(rev_flux) 
 
FROM 
    flux 
     
GROUP BY 
    1, 
    2 
     
ORDER BY 
    1, 
    2 
''').show(250, False) 
 
spark.sql(''' 
SELECT 
    qbr_ever, 
    SUM(initial_subs), 
    SUM(current_subs), 
    SUM(initial_rev), 
    SUM(current_rev), 
    COUNT(*), 
    AVG(sub_flux), 
    AVG(rev_flux) 
 
FROM 
    flux 
     
GROUP BY 
    1 
     
ORDER BY 
    1 
''').show(250, False) 
 
spark.sql(''' 
SELECT 
    * 
 
FROM 
    flux 
     
ORDER BY 
    qbr_ever DESC 
''').show(250, False) 
 
spark.sql(''' 
SELECT 
    qbr_acc.accnt_id IS NOT NULL AS qbr, 
    COUNT(DISTINCT owner_accnt_id), 
    SUM(subs), 
    SUM(total_net) 
 
FROM 
    current 
     
INNER JOIN 
    gms.s_org_ext AS org 
    ON org.row_id = current.owner_accnt_id 
 
LEFT JOIN 
    qbr_acc 
    ON qbr_acc.accnt_id = current.owner_accnt_id 
 
WHERE 
    org.ou_type_cd IN ('Member Organisation') 
    AND COALESCE(market_class_cd, '') NOT LIKE 'ADM%' 
    AND COALESCE(market_class_cd, '') NOT LIKE '50+%' 
 
GROUP BY 
    1 
''').show(250, False) 
 
 
# spark.sql(''' 
# SELECT 
#     owner_accnt_id, 
#     MIN(qbr_ord.order_dt) AS fst_dt, 
#     MAX(qbr_ord.order_dt) AS lst_dt 
 
# FROM 
#     current 
     
# INNER JOIN 
#     gms.s_org_ext AS org 
#     ON org.row_id = current.owner_accnt_id 
     
# INNER JOIN 
#     qbr_ord 
#     ON qbr_ord.accnt_id = current.owner_accnt_id 
 
# WHERE 
#     org.ou_type_cd IN ('Member Organisation') 
#     AND COALESCE(market_class_cd, '') NOT LIKE 'ADM%' 
#     AND COALESCE(market_class_cd, '') NOT LIKE '50+%' 
 
# GROUP BY 
#     1 
# ''').createOrReplaceTempView('fstlst') 
# spark.sql(''' 
# SELECT 
#     INT(DATEDIFF(DATE('2020-06-30'), fst_dt)/365), 
#     COUNT(*) 
     
# FROM 
#     fstlst 
 
# GROUP BY 
#     1 
 
# ORDER BY 
#     1 
# ''').show(25, False) 
 
# spark.sql(''' 
# SELECT 
#     INT(DATEDIFF(DATE('2020-06-30'), lst_dt)/365), 
#     COUNT(*) 
     
# FROM 
#     fstlst 
 
# GROUP BY 
#     1 
 
# ORDER BY 
#     1 
# ''').show(25, False) 
# spark.sql(''' 
# SELECT attrib_25 FROM gms.s_org_ext 
# INNER JOIN gms.s_org_ext_x 
# ON s_org_ext_x.par_row_id = s_org_ext.row_id 
# WHERE s_org_ext.row_id = '1-B3S-4250' 
# ''').show(25, False) 
 
 
spark.sql(''' 
SELECT 
    COUNT(*) 
 
FROM 
    current 
     
INNER JOIN 
    gms.s_org_ext AS org 
    ON current.owner_accnt_id = org.row_id 
 
WHERE 
    org.ou_type_cd IN ('Member Organisation') 
    AND COALESCE(market_class_cd, '') NOT LIKE 'ADM%' 
    AND COALESCE(market_class_cd, '') NOT LIKE '50+%' 
''').show(250, False) 
# 222022 / 51170 
# 2412580 / 1785638 
# 30328 / 11589 
 
 
spark.sql(''' 
SELECT 
    * 
 
FROM 
    flux 
     
WHERE 
    acc_id = '1-14A10LJ' 
''').show(250, False) 
 
 
spark.read.load('vehicles.csv', format = 'csv', sep = ',', inferSchema = True, header = True).createOrReplaceTempView('veh') 
# |Account ID|# Vehicles| 
spark.sql(''' 
SELECT `Account ID` AS acc_id, `# Vehicles` AS subs 
FROM veh 
''').createOrReplaceTempView('veh') 
spark.sql(''' 
SELECT * FROM veh INNER JOIN flux 
ON veh.acc_id = flux.acc_id 
WHERE subs != current_subs 
''').show(250, False) 
 
spark.sql(''' 
SELECT * FROM current LEFT ANTI JOIN veh 
ON veh.acc_id = current.owner_accnt_id 
''').show(250, False) 
spark.sql(''' 
SELECT 
    YEAR(order_dt), COUNT(DISTINCT acc_id) FROM initial GROUP BY 1 ORDER BY 1 
''').show(250, False) 
spark.sql(''' 
SELECT 
    qbr_acc.accnt_id IS NOT NULL, 
    COUNT(*), 
    AVG(org.x_nrma_asset_count) 
     
FROM 
    gms.s_org_ext AS org 
  
INNER JOIN 
    gms.s_org_ext_x AS orgx 
    ON orgx.par_row_id = org.row_id 
 
LEFT JOIN 
    qbr_acc 
    ON qbr_acc.accnt_id = org.row_id 
     
WHERE 
    org.ou_type_cd IN ('Member Organisation') 
    AND COALESCE(market_class_cd, '') NOT LIKE 'ADM%' 
    AND COALESCE(market_class_cd, '') NOT LIKE '50+%' 
    AND org.active_flg = 'Y' 
    AND EXISTS ( 
    SELECT 
    asset.owner_accnt_id 
 
FROM 
    gms.s_asset AS asset 
 
INNER JOIN 
    gms.s_prod_int AS prod 
    ON prod.row_id = asset.prod_id 
 
WHERE 
    asset.status_cd = 'Active' 
    AND prod.sub_type_cd IN ('RSA', 'Non-RSA') 
    AND prod.prod_cd = 'Product' 
    AND asset.owner_accnt_id = org.row_id) 
     
GROUP BY 
    1 
     
ORDER BY 
    1 ASC 
''').show(250, False) 