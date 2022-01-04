# SPREADSHEET NEEDS TO BE RECALCULATED! 
# Prepare one version that just has the fixed membership fee revenue 
# Pull in product and offer code 
spark.sql(''' 
SELECT 
    ord.row_id AS order_id, 
    SUM(ordi.net_pri) AS mem_fee 
     
FROM 
    gms.s_order AS ord 
     
LEFT JOIN 
    gms.s_order_type AS ordt 
    ON ordt.row_id = ord.order_type_id 
     
LEFT JOIN 
    gms.s_order_item AS ordi 
    ON ordi.order_id = ord.row_id 
     
LEFT JOIN 
    gms.s_prod_int AS prod 
    ON prod.row_id = ordi.prod_id 
     
WHERE 
    ord.status_cd = 'Complete' 
    AND prod_cd = 'Product' 
    AND ordt.name = 'New' 
    AND prod.name = 'Membership' 
 
GROUP BY 
    1 
''').createOrReplaceTempView('membership') 
spark.sql(''' 
SELECT DISTINCT 
    base.contact_id, 
    base.order_id, 
    base.order_start_dt 
     
FROM 
    sandpit.renewal_base AS base 
 
WHERE 
    base.prod_type IN ('RSA') 
    AND base.order_type = 'New' 
''').createOrReplaceTempView('acquisitions') 
 
# Note: There is no feed for me to filter by order type and status here 
# because the renewal base only has New and Renew that have been completed 
spark.sql(''' 
SELECT 
    acquisitions.order_id, 
    acquisitions.order_start_dt AS start_dt, 
    MAX(base.order_end_dt) AS end_dt, 
    CASE 
        WHEN DATEDIFF(acquisitions.order_start_dt, MAX(base.order_end_dt)) <= 90    THEN 'Active' 
        WHEN DATEDIFF(acquisitions.order_start_dt, MAX(base.order_end_dt)) <= 365   THEN 'Y0' 
        WHEN DATEDIFF(acquisitions.order_start_dt, MAX(base.order_end_dt)) <= 365*2 THEN 'Y1' 
        WHEN DATEDIFF(acquisitions.order_start_dt, MAX(base.order_end_dt)) <= 365*3 THEN 'Y2' 
                                                                                    ELSE 'New' 
    END AS acq_type 
 
FROM 
    acquisitions 
 
LEFT JOIN 
    sandpit.renewal_base AS base 
    ON base.contact_id = acquisitions.contact_id 
    AND DATE(base.order_start_dt) < DATE(acquisitions.order_start_dt) 
     
GROUP BY 
    1, 
    2 
''').createOrReplaceTempView('acquisitions') 
# The membership fee is retrieved at the order level 
# and has to be joined at the order level (I don't believe it  
# exists in renewal base so I can't just broaden my product filters) 
# but all my data when I run my calculations is at the order line item level 
# so for every subscription/add-on I include, the membership fee would be added again 
 
# The quick and dirty way to compensate for this is to count how many records I would be 
# including for each order, and divide the membership fee by that count 
# So if say there are two records, I'd be adding membership fee twice but also dividing 
# that fee by two, balancing it out 
 
spark.sql(''' 
SELECT 
    base.order_id, 
    SUM(1) AS divider 
 
FROM 
    sandpit.renewal_base AS base 
 
WHERE 
    base.prod_type IN ('RSA', 'Add-On') 
    AND base.order_type = 'New' 
    AND YEAR(base.order_start_dt) IN (2019, 2020, 2021) 
    AND base.businessmembernumber IS NULL 
 
GROUP BY 
    1 
''').createOrReplaceTempView('compensator') 
 
 
 
 
 
spark.sql(''' 
SELECT 
    TRUNC(base.order_start_dt, 'MONTH') AS order_month, 
    acquisitions.acq_type, 
    base.prod_name, 
    conx.attrib_55 AS colour_plus, 
    CASE 
        WHEN 
            ( 
                addr.zipcode >= "2000" AND addr.zipcode <= "2082" OR 
                addr.zipcode >= "2084" AND addr.zipcode <= "2234" OR 
                addr.zipcode >= "2555" AND addr.zipcode <= "2574" OR 
                addr.zipcode >= "2745" AND addr.zipcode <= "2770" OR 
                addr.zipcode >= "2775" AND addr.zipcode <= "2775" 
            ) 
            AND 
            ( 
                UPPER(addr.country) = "AUSTRALIA" OR 
                UPPER(addr.country) = "AU" 
            ) 
        THEN 
            "METROPOLITAN" 
 
        WHEN 
            ( 
                addr.zipcode >= "2083" AND addr.zipcode <= "2083" OR 
                addr.zipcode >= "2250" AND addr.zipcode <= "2338" OR 
                addr.zipcode >= "2415" AND addr.zipcode <= "2423" OR 
                addr.zipcode >= "2425" AND addr.zipcode <= "2425" OR 
                addr.zipcode >= "2428" AND addr.zipcode <= "2428" OR 
                addr.zipcode >= "2500" AND addr.zipcode <= "2535" OR 
                addr.zipcode >= "2538" AND addr.zipcode <= "2541" OR 
                addr.zipcode >= "2575" AND addr.zipcode <= "2578" OR 
                addr.zipcode >= "2600" AND addr.zipcode <= "2617" OR 
                addr.zipcode >= "2773" AND addr.zipcode <= "2774" OR 
                addr.zipcode >= "2776" AND addr.zipcode <= "2786" OR 
                addr.zipcode >= "2900" AND addr.zipcode <= "2914" 
            ) 
            AND 
            ( 
                UPPER(addr.country) = "AUSTRALIA" OR 
                UPPER(addr.country) = "AU" 
            ) 
        THEN 
            "REGIONAL" 
         
        WHEN 
            ( 
                addr.zipcode >= "2339" AND addr.zipcode <= "2411" OR 
                addr.zipcode >= "2424" AND addr.zipcode <= "2424" OR 
                addr.zipcode >= "2426" AND addr.zipcode <= "2427" OR 
                addr.zipcode >= "2429" AND addr.zipcode <= "2490" OR 
                addr.zipcode >= "2536" AND addr.zipcode <= "2537" OR 
                addr.zipcode >= "2545" AND addr.zipcode <= "2551" OR 
                addr.zipcode >= "2579" AND addr.zipcode <= "2594" OR 
                addr.zipcode >= "2618" AND addr.zipcode <= "2739" OR 
                addr.zipcode >= "2787" AND addr.zipcode <= "2898" OR 
                addr.zipcode >= "6798" AND addr.zipcode <= "6799" 
            ) 
            AND 
            ( 
                UPPER(addr.country) = "AUSTRALIA" OR 
                UPPER(addr.country) = "AU" 
            ) 
        THEN 
            "RURAL" 
         
        WHEN 
            ( 
                addr.zipcode >= "0800" AND addr.zipcode <= "0886" OR 
                addr.zipcode >= "3000" AND addr.zipcode <= "6770" OR 
                addr.zipcode >= "6907" AND addr.zipcode <= "7470" OR 
                addr.zipcode >= "7471"  
            ) 
            AND 
            ( 
                UPPER(addr.country) = "AUSTRALIA" OR 
                UPPER(addr.country) = "AU" 
            ) 
        THEN 
            "INTERSTATE" 
 
        ELSE 
            "UNKNOWN" 
    END AS region, 
    CASE 
        WHEN YEAR(NOW()) - YEAR(con.birth_dt) > 85 THEN 85 
        WHEN YEAR(NOW()) - YEAR(con.birth_dt) < 25 THEN YEAR(NOW()) - YEAR(con.birth_dt) 
        ELSE 5*INT((YEAR(NOW()) - YEAR(con.birth_dt) - 30)/5) + 30 
    END AS age, 
    SUM(membership.mem_fee/COALESCE(compensator.divider, 1)) AS mem_rev, 
    SUM(base.item_net_price) AS sub_rev, 
    SUM(CASE WHEN prod_type = 'RSA' AND UPPER(item_promo_name) RLIKE 'NOJOINFEE' THEN base.item_net_price ELSE 0 END) AS sub_rev_promo, 
    SUM(CASE WHEN prod_type = 'RSA' THEN 1 ELSE 0 END) AS rsa_vol, 
    SUM(CASE WHEN prod_type = 'RSA' AND UPPER(item_promo_name) RLIKE 'NOJOINFEE' THEN 1 ELSE 0 END) AS rsa_promo_vol, 
    SUM(CASE WHEN prod_type = 'RSA' AND UPPER(item_promo_name) NOT RLIKE 'NOJOINFEE' AND COALESCE(mem_fee, 0) = 0  THEN 1 ELSE 0 END) AS rsa_nomem_vol 
     
FROM 
    sandpit.renewal_base AS base 
 
LEFT JOIN 
    acquisitions 
    ON acquisitions.order_id = base.order_id 
 
LEFT JOIN 
    membership 
    ON membership.order_id = base.order_id 
     
LEFT JOIN 
    compensator 
    ON compensator.order_id = base.order_id 
     
LEFT JOIN 
    gms.s_contact AS con 
    ON con.row_id = base.contact_id 
 
LEFT JOIN 
    gms.s_contact_x AS conx 
    ON conx.par_row_id = con.row_id 
 
LEFT JOIN 
    gms.s_addr_per AS addr 
    ON addr.row_id = con.pr_per_addr_id 
     
WHERE 
    base.prod_type IN ('RSA', 'Add-On') 
    AND base.order_type = 'New' 
    AND YEAR(base.order_start_dt) IN (2019, 2020, 2021) 
    AND base.businessmembernumber IS NULL 
     
GROUP BY 
    1, 
    2, 
    3, 
    4, 
    5, 
    6 
     
ORDER BY 
    1, 
    2, 
    3, 
    4, 
    5, 
    6 
''').show(250000, False) 
 
 
 
# Same as above but by week 
 
 
 
spark.sql(''' 
SELECT 
    DATE_SUB(NEXT_DAY(base.order_start_dt, 'Sunday'), 7) AS order_week, 
    acquisitions.acq_type, 
    base.prod_name, 
    conx.attrib_55 AS colour_plus, 
    CASE 
        WHEN 
            ( 
                addr.zipcode >= "2000" AND addr.zipcode <= "2082" OR 
                addr.zipcode >= "2084" AND addr.zipcode <= "2234" OR 
                addr.zipcode >= "2555" AND addr.zipcode <= "2574" OR 
                addr.zipcode >= "2745" AND addr.zipcode <= "2770" OR 
                addr.zipcode >= "2775" AND addr.zipcode <= "2775" 
            ) 
            AND 
            ( 
                UPPER(addr.country) = "AUSTRALIA" OR 
                UPPER(addr.country) = "AU" 
            ) 
        THEN 
            "METROPOLITAN" 
 
        WHEN 
            ( 
                addr.zipcode >= "2083" AND addr.zipcode <= "2083" OR 
                addr.zipcode >= "2250" AND addr.zipcode <= "2338" OR 
                addr.zipcode >= "2415" AND addr.zipcode <= "2423" OR 
                addr.zipcode >= "2425" AND addr.zipcode <= "2425" OR 
                addr.zipcode >= "2428" AND addr.zipcode <= "2428" OR 
                addr.zipcode >= "2500" AND addr.zipcode <= "2535" OR 
                addr.zipcode >= "2538" AND addr.zipcode <= "2541" OR 
                addr.zipcode >= "2575" AND addr.zipcode <= "2578" OR 
                addr.zipcode >= "2600" AND addr.zipcode <= "2617" OR 
                addr.zipcode >= "2773" AND addr.zipcode <= "2774" OR 
                addr.zipcode >= "2776" AND addr.zipcode <= "2786" OR 
                addr.zipcode >= "2900" AND addr.zipcode <= "2914" 
            ) 
            AND 
            ( 
                UPPER(addr.country) = "AUSTRALIA" OR 
                UPPER(addr.country) = "AU" 
            ) 
        THEN 
            "REGIONAL" 
         
        WHEN 
            ( 
                addr.zipcode >= "2339" AND addr.zipcode <= "2411" OR 
                addr.zipcode >= "2424" AND addr.zipcode <= "2424" OR 
                addr.zipcode >= "2426" AND addr.zipcode <= "2427" OR 
                addr.zipcode >= "2429" AND addr.zipcode <= "2490" OR 
                addr.zipcode >= "2536" AND addr.zipcode <= "2537" OR 
                addr.zipcode >= "2545" AND addr.zipcode <= "2551" OR 
                addr.zipcode >= "2579" AND addr.zipcode <= "2594" OR 
                addr.zipcode >= "2618" AND addr.zipcode <= "2739" OR 
                addr.zipcode >= "2787" AND addr.zipcode <= "2898" OR 
                addr.zipcode >= "6798" AND addr.zipcode <= "6799" 
            ) 
            AND 
            ( 
                UPPER(addr.country) = "AUSTRALIA" OR 
                UPPER(addr.country) = "AU" 
            ) 
        THEN 
            "RURAL" 
         
        WHEN 
            ( 
                addr.zipcode >= "0800" AND addr.zipcode <= "0886" OR 
                addr.zipcode >= "3000" AND addr.zipcode <= "6770" OR 
                addr.zipcode >= "6907" AND addr.zipcode <= "7470" OR 
                addr.zipcode >= "7471"  
            ) 
            AND 
            ( 
                UPPER(addr.country) = "AUSTRALIA" OR 
                UPPER(addr.country) = "AU" 
            ) 
        THEN 
            "INTERSTATE" 
 
        ELSE 
            "UNKNOWN" 
    END AS region, 
    CASE 
        WHEN YEAR(NOW()) - YEAR(con.birth_dt) > 85 THEN 85 
        WHEN YEAR(NOW()) - YEAR(con.birth_dt) < 25 THEN YEAR(NOW()) - YEAR(con.birth_dt) 
        ELSE 5*INT((YEAR(NOW()) - YEAR(con.birth_dt) - 30)/5) + 30 
    END AS age, 
    SUM(membership.mem_fee/COALESCE(compensator.divider, 1)) AS mem_rev, 
    SUM(base.item_net_price) AS sub_rev, 
    SUM(CASE WHEN prod_type = 'RSA' AND UPPER(item_promo_name) RLIKE 'NOJOINFEE' THEN base.item_net_price ELSE 0 END) AS sub_rev_promo, 
    SUM(CASE WHEN prod_type = 'RSA' THEN 1 ELSE 0 END) AS rsa_vol, 
    SUM(CASE WHEN prod_type = 'RSA' AND UPPER(item_promo_name) RLIKE 'NOJOINFEE' THEN 1 ELSE 0 END) AS rsa_promo_vol, 
    SUM(CASE WHEN prod_type = 'RSA' AND UPPER(item_promo_name) NOT RLIKE 'NOJOINFEE' AND COALESCE(mem_fee, 0) = 0  THEN 1 ELSE 0 END) AS rsa_nomem_vol 
     
FROM 
    sandpit.renewal_base AS base 
 
LEFT JOIN 
    acquisitions 
    ON acquisitions.order_id = base.order_id 
 
LEFT JOIN 
    membership 
    ON membership.order_id = base.order_id 
     
LEFT JOIN 
    compensator 
    ON compensator.order_id = base.order_id 
     
LEFT JOIN 
    gms.s_contact AS con 
    ON con.row_id = base.contact_id 
 
LEFT JOIN 
    gms.s_contact_x AS conx 
    ON conx.par_row_id = con.row_id 
 
LEFT JOIN 
    gms.s_addr_per AS addr 
    ON addr.row_id = con.pr_per_addr_id 
     
WHERE 
    base.prod_type IN ('RSA', 'Add-On') 
    AND base.order_type = 'New' 
    AND YEAR(base.order_start_dt) IN (2019, 2020, 2021) 
    AND base.businessmembernumber IS NULL 
     
GROUP BY 
    1, 
    2, 
    3, 
    4, 
    5, 
    6 
     
ORDER BY 
    1, 
    2, 
    3, 
    4, 
    5, 
    6 
''').show(250000, False) 
 
 
# Same as above but looking into channel 
 
 
 
 
 
spark.sql(''' 
SELECT 
    DATE_SUB(NEXT_DAY(base.order_start_dt, 'Sunday'), 7) AS order_week, 
    acquisitions.acq_type, 
    base.prod_name, 
    chan.channel_group, 
    SUM(base.item_net_price) AS sub_rev, 
    SUM(CASE WHEN prod_type = 'RSA' AND UPPER(item_promo_name) RLIKE 'NOJOINFEE' THEN base.item_net_price ELSE 0 END) AS sub_rev_promo, 
    SUM(CASE WHEN prod_type = 'RSA' THEN 1 ELSE 0 END) AS rsa_vol, 
    SUM(CASE WHEN prod_type = 'RSA' AND UPPER(item_promo_name) RLIKE 'NOJOINFEE' THEN 1 ELSE 0 END) AS rsa_promo_vol 
     
FROM 
    sandpit.renewal_base AS base 
 
LEFT JOIN 
    acquisitions 
    ON acquisitions.order_id = base.order_id 
 
LEFT JOIN 
    gms.s_contact AS con 
    ON con.row_id = base.contact_id 
 
LEFT JOIN 
    gms.s_contact_x AS conx 
    ON conx.par_row_id = con.row_id 
 
LEFT JOIN 
    gms.s_addr_per AS addr 
    ON addr.row_id = con.pr_per_addr_id 
     
LEFT JOIN 
    gms.dw_order_item AS chan 
    ON chan.order_item_id = base.item_row_id 
         
WHERE 
    base.prod_type IN ('RSA', 'Add-On') 
    AND base.order_type = 'New' 
    AND YEAR(base.order_start_dt) IN (2020, 2021) 
    AND base.businessmembernumber IS NULL 
     
GROUP BY 
    1, 
    2, 
    3, 
    4 
     
ORDER BY 
    1, 
    2, 
    3, 
    4 
''').show(7500, False) 
 
 
 
 
 
 
 
 
 
 
spark.sql(''' 
SELECT 
    UPPER(item_promo_name), 
    SUM(CASE WHEN prod_type = 'RSA' THEN 1 ELSE 0 END) AS rsa_vol 
     
FROM 
    sandpit.renewal_base AS base 
 
LEFT JOIN 
    acquisitions 
    ON acquisitions.order_id = base.order_id 
     
WHERE 
    base.prod_type IN ('RSA', 'Add-On') 
    AND base.order_type = 'New' 
    AND base.order_start_dt BETWEEN '2020-06-01' AND '2021-02-01' 
    AND base.businessmembernumber IS NULL 
    AND UPPER(item_promo_name) RLIKE 'NOJOINFEE' 
     
GROUP BY 
    1 
     
ORDER BY 
    2 DESC 
''').show(250, False) 
# Membership is being multiplied! 
 
spark.sql(''' 
SELECT 
    asset.owner_con_id, 
    MAX(asset.start_dt) AS latest 
     
FROM 
    gms.s_asset AS asset 
     
LEFT JOIN 
    gms.s_prod_int AS prod 
    ON prod.row_id = asset.prod_id 
 
WHERE 
    prod.name = 'Membership' 
    AND asset.status_cd = 'Active' 
     
GROUP BY 
    1 
''').createOrReplaceTempView('active_mem') 
 
 
spark.sql(''' 
SELECT 
    DISTINCT asset.owner_con_id, 
    asset.row_id, 
    asset.start_dt, 
    asset.end_dt, 
    asset.row_id 
     
     
FROM 
    gms.s_asset AS asset 
     
LEFT JOIN 
    gms.s_prod_int AS prod 
    ON prod.row_id = asset.prod_id 
 
WHERE 
    prod.name = 'Membership' 
    AND asset.status_cd = 'Active' 
    AND asset.owner_con_id = '1-DGL-3224' 
''').show(250, False) 
# Week logic 
spark.sql(''' 
SELECT NOW(), DATE_SUB(NEXT_DAY(NOW(), 'Sunday'), 7) 
''').show(25, False) 