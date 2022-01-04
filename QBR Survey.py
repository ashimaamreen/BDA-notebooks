spark.read.load('qbr_master_upd.csv', format = 'csv', sep = ',', inferSchema = True, header = True).createOrReplaceTempView('df') 
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
SELECT 
    org.ou_num AS member_id, 
    org.row_id AS account_id, 
    org.name AS business_name, 
    CONCAT_WS(' ', COALESCE(con.fst_name, ''), COALESCE(con.last_name, '')) AS contact_name, 
    con.email_addr AS email, 
    org.x_nrma_asset_count AS current_rsa, 
    MIN(ord.order_dt) AS first_qbr, 
    MAX((ordt.name = 'New' AND DATEDIFF(NOW(), ord.order_dt) <= 365)) AS add_qbr, 
    MAX((ordt.name = 'Renew' AND DATEDIFF(NOW(), ord.order_dt) <= 365)) AS ren_qbr 
     
FROM 
    df  
 
LEFT JOIN -- Double check to make sure it only grabs df 
    gms.s_order AS ord 
    ON ord.order_num = df.order_num 
     
LEFT JOIN 
    gms.s_order_type AS ordt 
    ON ordt.row_id = ord.order_type_id 
     
LEFT JOIN 
    gms.s_org_ext AS org 
    ON org.row_id = ord.accnt_id 
 
LEFT JOIN 
    gms.s_contact AS con 
    ON con.row_id = org.pr_con_id 
     
LEFT JOIN 
    gms.s_contact_fnx AS confnx 
    ON confnx.par_row_id = con.row_id 
 
LEFT JOIN 
    gms.s_asset AS asset 
    ON asset.owner_accnt_id = org.row_id 
 
LEFT JOIN 
    gms.s_prod_int AS prod 
    ON prod.row_id = asset.prod_id 
     
WHERE 
    asset.status_cd = 'Active' 
    AND prod.type = 'Membership' 
    AND prod.prod_cd = 'Promotion' 
    AND org.ou_type_cd IN ('Member Organisation') 
    AND COALESCE(org.market_class_cd, '') NOT LIKE "%ADM%" 
    AND COALESCE(org.market_class_cd, '') NOT LIKE "%50+%" 
    AND con.cust_stat_cd = 'Active' 
    AND org.cust_stat_cd = 'Active' 
    AND COALESCE(confnx.deceased_flg,'N') = 'N' 
    AND COALESCE(con.x_nrma_title, '') != 'Estate Of The Late' 
    AND COALESCE(con.x_inv_email_1, 'N') = 'N' 
    AND con.email_addr IS NOT NULL 
     
GROUP BY 
    1, 
    2, 
    3, 
    4, 
    5, 
    6 
     
HAVING 
    MAX(DATEDIFF(NOW(), ord.order_dt) <= 365) 
''').createOrReplaceTempView('base') 
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
SELECT DISTINCT 
    joined.acc_id 
 
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
     
INNER JOIN 
    qbr_ord 
    ON ord.row_id = qbr_ord.ord_id 
     
WHERE 1=1 
    AND ordt.name = 'New' 
    AND ordi.action_cd IN ('Add') 
    AND ord.status_cd = 'Complete'  
''').createOrReplaceTempView('initial') 
spark.sql(''' 
SELECT 
    account_id,  
    COUNT(*) AS prev_rsa 
 
FROM 
    sandpit.renewal_base 
 
WHERE 
    DATE_SUB(NOW(), 365) BETWEEN order_start_dt AND order_end_dt 
    AND NOT (asset_status_mod = 'Cancelled' AND asset_end_dt < DATE_SUB(NOW(), 365)) 
    AND prod_type = 'RSA' 
     
GROUP BY 
    1 
''').createOrReplaceTempView('prev_total') 
spark.sql(''' 
SELECT DISTINCT 
    base.member_id, 
    base.business_name, 
    base.contact_name, 
    base.email, 
    base.current_rsa, 
    base.current_rsa - prev_total.prev_rsa AS added_rsa, 
    base.first_qbr, 
    base.add_qbr, 
    base.ren_qbr, 
    initial.acc_id IS NOT NULL AS joined_qbr 
     
FROM 
    base 
 
LEFT JOIN 
    initial 
    ON initial.acc_id = base.account_id 
     
LEFT JOIN 
    prev_total 
    ON prev_total.account_id = base.account_id 
 
''').repartition(1).write.saveAsTable('campaign_data.cc_dmc2160_qbr_research_edm_20200906_adhoc') 