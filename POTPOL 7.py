spark.sql(''' 
WITH 
    pay AS ( 
    SELECT 
        ord.row_id AS ord_id, 
        MIN(COALESCE(pay.received_dt, pay.txn_dt)) AS pay_date 
 
    FROM 
        gms.s_order AS ord 
     
    LEFT OUTER JOIN 
        gms.s_src_payment AS pay 
        ON ord.row_id = pay.order_id 
         
    GROUP BY 
        ord_id 
    ) 
 
SELECT DISTINCT 
    con.csn 
 
FROM 
    gms.s_order AS ord 
 
INNER JOIN 
    gms.s_order_item AS ordi 
    ON ordi.order_id = ord.row_id 
     
INNER JOIN 
    gms.s_prod_int AS prod 
    ON prod.row_id = ordi.prod_id 
 
INNER JOIN 
    gms.s_order_type AS ordt 
    ON ordt.row_id = ord.order_type_id 
 
INNER JOIN 
    gms.s_contact AS con 
    ON con.row_id = ord.contact_id 
     
INNER JOIN 
    gms.s_contact_x AS conx 
    ON conx.par_row_id = con.row_id 
     
INNER JOIN 
    gms.s_contact_fnx AS confnx 
    ON confnx.par_row_id = con.row_id 
     
INNER JOIN 
    gms.s_addr_per AS addr 
    ON addr.row_id = con.pr_per_addr_id 
 
INNER JOIN 
    pay 
    ON pay.ord_id = ord.row_id 
 
LEFT JOIN  
    gms.s_mktg_offr AS offer 
    ON offer.row_id = ord.x_digital_offer_id 
 
WHERE 1=1 
    AND (offer.offer_num IN ('DIGFUELCOMPNOV19') OR prod.name IN ('CVM Fuelcomp')) 
    AND ordt.name = 'Renew' 
    AND ord.x_payment_status IN ('Reconciled', 'Payment Taken') 
    AND con.cust_stat_cd = 'Active' 
    AND pay.pay_date <= ord.order_dt 
    AND pay.pay_date BETWEEN '2021-02-04 00:00:00' AND '2021-05-04 23:59:00' 
    AND con.con_cd IN ('Ordinary Member', 'Affiliate Member') 
    AND COALESCE(con.x_inv_email_1, 'N') = 'N' 
    AND con.email_addr IS NOT NULL 
    AND COALESCE(confnx.deceased_flg, 'N') = 'N' 
    AND COALESCE(con.x_nrma_title, '') != 'Estate Of The Late' 
    AND COALESCE(conx.attrib_44, '') NOT RLIKE 'Honorary' 
    AND FLOOR(DATEDIFF(NOW(), con.birth_dt)/365) >= 18 
    AND COALESCE(addr.state, '') IN ('NSW', 'ACT') 
''').count() 
 
spark.sql(''' 
WITH 
    pay AS ( 
    SELECT 
        ord.row_id AS ord_id, 
        MIN(COALESCE(pay.received_dt, pay.txn_dt)) AS pay_date 
 
    FROM 
        gms.s_order AS ord 
     
    LEFT OUTER JOIN 
        gms.s_src_payment AS pay 
        ON ord.row_id = pay.order_id 
         
    GROUP BY 
        ord_id 
    ) 
 
SELECT DISTINCT 
    con.csn 
 
FROM 
    gms.s_order AS ord 
 
INNER JOIN 
    gms.s_order_item AS ordi 
    ON ordi.order_id = ord.row_id 
     
INNER JOIN 
    gms.s_prod_int AS prod 
    ON prod.row_id = ordi.prod_id 
 
INNER JOIN 
    gms.s_order_type AS ordt 
    ON ordt.row_id = ord.order_type_id 
 
INNER JOIN 
    gms.s_contact AS con 
    ON con.row_id = ord.contact_id 
     
INNER JOIN 
    gms.s_contact_x AS conx 
    ON conx.par_row_id = con.row_id 
     
INNER JOIN 
    gms.s_contact_fnx AS confnx 
    ON confnx.par_row_id = con.row_id 
     
INNER JOIN 
    gms.s_addr_per AS addr 
    ON addr.row_id = con.pr_per_addr_id 
 
INNER JOIN 
    pay 
    ON pay.ord_id = ord.row_id 
 
LEFT JOIN  
    gms.s_mktg_offr AS offer 
    ON offer.row_id = ord.x_digital_offer_id 
 
WHERE 1=1 
    AND (offer.offer_num IN ('DIGWOOLIESCOMPJUL20') OR prod.name IN ('CVM Wooliescomp')) 
    AND ordt.name = 'Renew' 
    AND ord.x_payment_status IN ('Reconciled', 'Payment Taken') 
    AND con.cust_stat_cd = 'Active' 
    AND pay.pay_date <= ord.order_dt 
    AND pay.pay_date BETWEEN '2021-02-04 00:00:00' AND '2021-05-04 23:59:00' 
    AND con.con_cd IN ('Ordinary Member', 'Affiliate Member') 
    AND COALESCE(con.x_inv_email_1, 'N') = 'N' 
    AND con.email_addr IS NOT NULL 
    AND COALESCE(confnx.deceased_flg, 'N') = 'N' 
    AND COALESCE(con.x_nrma_title, '') != 'Estate Of The Late' 
    AND COALESCE(conx.attrib_44, '') NOT RLIKE 'Honorary' 
    AND FLOOR(DATEDIFF(NOW(), con.birth_dt)/365) >= 18 
    AND COALESCE(addr.state, '') IN ('NSW', 'ACT') 
''').count() 