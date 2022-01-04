spark.sql(''' 
WITH 
    pay AS ( 
    SELECT 
        ord.row_id AS ord_id, 
        MIN(pay.txn_dt) AS pay_date 
        -- actl_pay_dt 
         
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
    --, prod.name 
 
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
 
-- LEFT JOIN 
--     gms.s_order_item_om AS ordiom 
--     ON ordiom.par_row_id = ordi.row_id 
 
INNER JOIN 
    pay 
    ON pay.ord_id = ord.row_id 
 
-- LEFT JOIN  
--     gms.s_mktg_offr offer 
--     on offer.row_id =ord.x_digital_offer_id 
 
WHERE  
    --offer.offer_num ='DIGFUELCOMPNOV19'       --Order with offer code 
    prod.name = 'CVM Fuelcomp' 
    AND ordt.name = 'Renew'                     --Renewal order 
    AND ord.x_payment_status IN ('Reconciled', 'Payment Taken')     --Paid 
    AND con.cust_stat_cd = 'Active'                                 --Active customer 
    AND pay.pay_date <= ord.order_dt                                --Payment date less than equal to orde date=POT 
    AND pay.pay_date BETWEEN '2020-08-04 09:00:00' AND '2020-11-04 23:59:00'        --Paid during promotional period 
    AND con.con_cd IN ('Ordinary Member', 'Affiliate Member')                        
    AND COALESCE(con.x_inv_email_1, 'N') = 'N'                                      --Valid email 
    AND con.email_addr IS NOT NULL                                                  --Valid email 
    AND COALESCE(confnx.deceased_flg, 'N') = 'N' 
    AND COALESCE(con.x_nrma_title, '') != 'Estate Of The Late'                      --non deceased 
    AND COALESCE(conx.attrib_44, '') NOT RLIKE 'Honorary'                           --Not staff 
    AND FLOOR(DATEDIFF(NOW(), con.birth_dt)/365) >= 18                              --Above 18 
    AND COALESCE(addr.state, '') IN ('NSW', 'ACT')                                  --Lives in ACT and NSW 
    ''').show(100000,False) 
spark.sql(''' 
WITH 
    pay AS ( 
    SELECT 
        ord.row_id AS ord_id, 
        MIN(pay.txn_dt) AS pay_date 
        -- actl_pay_dt 
         
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
    --, ord.x_digital_offer_id, prod.name, offer.offer_num 
 
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
 
-- LEFT JOIN 
--     gms.s_order_item_om AS ordiom 
--     ON ordiom.par_row_id = ordi.row_id 
 
INNER JOIN 
    pay 
    ON pay.ord_id = ord.row_id 
 
-- LEFT JOIN  
--     gms.s_mktg_offr offer 
--     on offer.row_id =ord.x_digital_offer_id 
 
WHERE  
--offer.offer_num='DIGWOOLIESCOMPJUL20'                                         --Order with offer code 
    prod.name = 'CVM Wooliescomp' 
    AND ordt.name = 'Renew'                     --Renewal order 
    AND ord.x_payment_status IN ('Reconciled', 'Payment Taken')                     --Paid 
    AND con.cust_stat_cd = 'Active'                                                 --Active customer 
    AND pay.pay_date <= ord.order_dt                                                --Payment date less than equal to orde date=POT 
    AND pay.pay_date BETWEEN '2020-08-04 09:00:00' AND '2020-11-04 23:59:00'        --Paid during promotional period 
    AND con.con_cd IN ('Ordinary Member', 'Affiliate Member')                        
    AND COALESCE(con.x_inv_email_1, 'N') = 'N'                                      --Valid email 
    AND con.email_addr IS NOT NULL                                                  --Valid email 
    AND COALESCE(confnx.deceased_flg, 'N') = 'N' 
    AND COALESCE(con.x_nrma_title, '') != 'Estate Of The Late'                      --non deceased 
    AND COALESCE(conx.attrib_44, '') NOT RLIKE 'Honorary'                           --Not staff 
    AND FLOOR(DATEDIFF(NOW(), con.birth_dt)/365) >= 18                              --Above 18 
    AND COALESCE(addr.state, '') IN ('NSW', 'ACT')                                  --Lives in ACT and NSW 
''').show(1000000,False) 
select p.x_nrma_offer_id as offer_id, p.name, p.row_id, p.desc_text, p.x_type_1, p.x_type_2, p.x_paybyterm  
 
from gms.s_prod_int p 
where 1=1 
--and p.x_type_1='DIGFUELCOMPNOV19' 
--p.row_id in ('1-DKHX3RO','1-DKHX3PS') 
--where p.x_type_1='JOINOFFER' 
--and p.sub_type_cd in ('Non-RSA','Add-on','RSA') 
--and p.prod_cd = 'Product' 
--on c.row_id = a.owner_con_id 
 
--inner join gms.s_prod_int p 
--on a.prod_id = p.row_id 
 
--where 1=1 
--and p.name in ('$20k travelcomp') 
and x_nrma_offer_id in ('1-DFU82FD','1-DFU82FH','1-DFU82F8') 
--and a.owner_con_id='1-HO4-3755' 
SELECT * FROM gms.s_mktg_offr 
where offer_num in ('DIGFUELCOMPNOV19','DIGWOOLIESCOMPJUL20') 
select o.contact_id 
        , o.prev_order_rev_id 
        , o.x_portal_pay_option 
        , o.x_digital_offer_id 
        , o.row_id 
        , o.par_order_id 
        , prod.name 
        , o.x_nrma_offer_id 
from gms.s_order AS o 
 
INNER JOIN 
    gms.s_order_item AS ordi 
    ON ordi.order_id = o.row_id 
     
INNER JOIN 
    gms.s_prod_int AS prod 
    ON prod.row_id = ordi.prod_id 
     
where prod.name='CVM Fuelcomp' 
--o.x_digital_offer_id in ('1-DFU82FD','1-DFU82FH','1-DFU82F8') 
--and o.contact_id='1-FWU-1998' 