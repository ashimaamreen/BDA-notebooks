select * from sandpit.renewal_base 
where item_net_price=80.480003356933594 
SELECT year(member_join_dt) 
        , month(member_join_dt) 
        , count(distinct membernumber) as member_number  
 
from sandpit.renewal_base 
 
where contact_cd in ('Ordinary Member','Affiliate Member') 
spark.sql(''' 
SELECT 
    TRUNC(order_start_dt, 'MONTH') AS order_month, 
    member_colour, 
    SUM(1) AS rsa_vol 
     
FROM 
    sandpit.renewal_base 
     
WHERE 
    prod_type = 'RSA' 
    AND order_type = 'New' 
    AND YEAR(order_start_dt) IN (2018, 2019, 2020, 2021) 
    AND businessmembernumber IS NULL 
 
GROUP BY 
    1, 
    2 
     
ORDER BY 
    1 ASC, 
    2 
''').show(500, False) 
select case when to_date(c.x_nrma_join_dt) between '2019-07-01' and '2020-06-30' then 'FY-2020' 
            when to_date(c.x_nrma_join_dt) between '2020-07-01' and '2021-06-30' then 'FY-2021' 
            else 'other' end Join_time 
        , cx.attrib_55 as colour_plus   
        , CASE WHEN prod_type = 'RSA' AND UPPER(item_promo_name) RLIKE 'NOJOINFEE' THEN 1 ELSE 0 END join_offer 
        , count(distinct c.csn) as member_number 
   
    from gms.s_contact as c  
     
    inner join gms.s_contact_x cx 
    on cx.par_row_id=c.row_id 
     
    inner join sandpit.renewal_base b 
    on b.contact_id=c.row_id 
     
     
    where c.cust_stat_cd='Active' 
    and c.con_cd in ('Affiliate Member','Ordinary Member') 
    and to_date(c.x_nrma_join_dt)>'2019-06-30' 
    and to_date(c.x_nrma_join_dt)<'2021-07-01' 
    and b.prod_type = 'RSA' 
    AND b.order_type = 'New' 
    AND businessmembernumber IS NULL 
     
    group by 1,2,3 
    order by 1,2,3 
 
select case when to_date(c.x_nrma_join_dt) between '2019-07-01' and '2020-06-30' then 'FY-2020' 
            when to_date(c.x_nrma_join_dt) between '2020-07-01' and '2021-06-30' then 'FY-2021' 
            else 'other' end Join_time 
        , case  when upper(b.prod_name) rlike 'CLASSIC' then 'Classic Care' 
                when upper(b.prod_name) rlike 'PLUS' then 'Premium Plus' 
                when upper(b.prod_name) rlike 'PREMIUM' then 'Premium Care' 
                when b.prod_name='Free2go' THEN 'Free2go' 
                ELSE 'Others' end product 
                 
        , CASE WHEN prod_type = 'RSA' AND UPPER(item_promo_name) RLIKE 'NOJOINFEE' THEN 1 ELSE 0 END join_offer 
        , count(distinct c.csn) as member_number 
   
    from gms.s_contact as c  
     
    inner join gms.s_contact_x cx 
    on cx.par_row_id=c.row_id 
     
    inner join sandpit.renewal_base b 
    on b.contact_id=c.row_id 
     
     
    where c.cust_stat_cd='Active' 
    and c.con_cd in ('Affiliate Member','Ordinary Member') 
    and to_date(c.x_nrma_join_dt)>'2019-06-30' 
    and to_date(c.x_nrma_join_dt)<'2021-07-01' 
    and b.prod_type = 'RSA' 
    AND b.order_type = 'New' 
    AND businessmembernumber IS NULL 
     
    group by 1,2,3 
    order by 1,2,3 
 
select case when to_date(c.x_nrma_join_dt) between '2019-07-01' and '2020-06-30' then 'FY-2020' 
            when to_date(c.x_nrma_join_dt) between '2020-07-01' and '2021-06-30' then 'FY-2021' 
            else 'other' end Join_time 
        , CASE WHEN prod_type = 'RSA' AND UPPER(item_promo_name) RLIKE 'NOJOINFEE' THEN 1 ELSE 0 END join_offer 
        , CASE 
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
    END AS region 
        , count(distinct c.csn) as member_number 
   
    from gms.s_contact as c  
     
    inner join gms.s_contact_x cx 
    on cx.par_row_id=c.row_id 
     
    inner join sandpit.renewal_base b 
    on b.contact_id=c.row_id 
     
     
    LEFT JOIN 
    gms.s_addr_per AS addr 
    ON addr.row_id = c.pr_per_addr_id 
     
     
    where c.cust_stat_cd='Active' 
    and c.con_cd in ('Affiliate Member','Ordinary Member') 
    and to_date(c.x_nrma_join_dt)>'2019-06-30' 
    and b.prod_type = 'RSA' 
    AND b.order_type = 'New' 
    AND businessmembernumber IS NULL 
     
    group by 1,2,3 
    order by 1,2,3 
spark.sql(''' 
select case when to_date(c.x_nrma_join_dt) between '2019-07-01' and '2020-06-30' then 'FY-2020' 
            when to_date(c.x_nrma_join_dt) between '2020-07-01' and '2021-06-30' then 'FY-2021' 
            else 'other' end Join_time 
        , CASE WHEN prod_type = 'RSA' AND UPPER(item_promo_name) RLIKE 'NOJOINFEE' THEN 1 ELSE 0 END join_offer 
        , CASE WHEN YEAR(NOW()) - YEAR(c.birth_dt) > 85 THEN 85 
                WHEN YEAR(NOW()) - YEAR(c.birth_dt) < 25 THEN YEAR(NOW()) - YEAR(c.birth_dt) 
                ELSE 5*INT((YEAR(NOW()) - YEAR(c.birth_dt) - 30)/5) + 30 
                END AS age 
        , count(distinct c.csn) as member_number 
   
    from gms.s_contact as c  
     
    inner join gms.s_contact_x cx 
    on cx.par_row_id=c.row_id 
     
    inner join sandpit.renewal_base b 
    on b.contact_id=c.row_id 
     
     
    LEFT JOIN 
    gms.s_addr_per AS addr 
    ON addr.row_id = c.pr_per_addr_id 
     
     
    where c.cust_stat_cd='Active' 
    and c.con_cd in ('Affiliate Member','Ordinary Member') 
    and to_date(c.x_nrma_join_dt)>'2019-06-30' 
    and b.prod_type = 'RSA' 
    AND b.order_type = 'New' 
    AND businessmembernumber IS NULL 
     
    group by 1,2,3 
    order by 1,2,3 
    ''').show(10000,False) 
spark.sql(''' 
select case when to_date(c.x_nrma_join_dt) between '2019-07-01' and '2020-06-30' then 'FY-2020' 
            when to_date(c.x_nrma_join_dt) between '2020-07-01' and '2021-06-30' then 'FY-2021' 
            else 'other' end Join_time 
        , CASE WHEN prod_type = 'RSA' AND UPPER(item_promo_name) RLIKE 'NOJOINFEE' THEN 1 ELSE 0 END join_offer 
        , (case when partner in ('IAG','NRMA Multi Policy Discount','NRMA MPD Insurance','NRMA Insurance') then 'IAG'  
                when partner LIKE 'NRMA Holiday%' then 'NRMA Parks and Resorts'  
                when partner LIKE 'HP%' then 'NRMA Parks and Resorts'  
                when partner LIKE 'BIG4%' then 'NRMA Parks and Resorts'  
                when partner LIKE 'NRMA Parks and Resorts%' then 'NRMA Parks and Resorts'  
                when partner LIKE 'NRMA Bowen Beachfront Holiday Park' then 'NRMA Parks and Resorts'  
                else partner end) as partner 
 
        --, case when m.member_number is not null then 1 else 0 end blue_benefit 
        , count(distinct c.csn) as member_number 
   
    from gms.s_contact as c  
     
    inner join gms.s_contact_x cx 
    on cx.par_row_id=c.row_id 
     
    inner join sandpit.renewal_base b 
    on b.contact_id=c.row_id 
     
     
   inner join m4m.return_feed_header m 
   on m.member_number=c.csn 
   and datediff(m.time_stamp,b.x_nrma_join_dt) between 0 and 60 
     
     
    where c.cust_stat_cd='Active' 
    and c.con_cd in ('Affiliate Member','Ordinary Member') 
    and to_date(c.x_nrma_join_dt)>'2019-06-30' 
    and b.prod_type = 'RSA' 
    AND b.order_type = 'New' 
    AND businessmembernumber IS NULL 
     
    group by 1,2,3 
    order by 1,2,3 
    ''').show(100000,False) 
select year(c.x_nrma_join_dt) 
        , month(c.x_nrma_join_dt) 
        , count(distinct c.csn) as member_number 
   
    from gms.s_contact as c  
     
    inner join  
     
    where c.cust_stat_cd='Active' 
    and c.con_cd in ('Affiliate Member','Ordinary Member') 
    and year(c.x_nrma_join_dt)>2017 
     
    group by 1,2 
    order by 1,2 
 
--spark.sql(''' 
SELECT 
    ordi.contact_id, 
    TRUNC(ord.order_dt, 'MONTH') AS ord_month, 
    --MAX(ordi.net_pri) AS mem_fee 
     
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
    --AND ordi.net_pri > 0 
 
GROUP BY 
    1, 
    2 
     
ORDER BY 
    3 DESC 
--''').createOrReplaceTempView('membership') 
 
spark.sql(''' 
SELECT prod_type, 
    TRUNC(order_start_dt, 'MONTH') AS order_month, 
    SUM(1) AS rsa_vol 
     
FROM 
    sandpit.renewal_base 
     
WHERE 1=1 
    AND prod_type != 'Add-on' 
    AND order_type = 'New' 
    AND YEAR(order_start_dt) IN (2018, 2019, 2020, 2021) 
    AND businessmembernumber IS NULL 
 
GROUP BY 
    1,2 
     
ORDER BY 
    1,2 
''').show(500, False) 
 
 
 
spark.sql(''' 
SELECT 
    renewal_yyyymm AS order_month, 
    SUM(1) AS rsa_vol 
     
FROM 
    sandpit.renewal_base 
     
WHERE 1=1 
    --prod_type = 'RSA' 
    AND order_type = 'New' 
    AND YEAR(order_start_dt) IN (2018, 2019, 2020, 2021) 
    AND businessmembernumber IS NULL 
 
GROUP BY 
    1 
     
ORDER BY 
    1 ASC 
''').show(500, False) 
 
 
 
SELECT DISTINCT item_promo_name, prod_name, item_net_price, item_base_price from sandpit.renewal_base 
where prod_type='Non-RSA' 
SELECT 
    renewal_yyyymm AS order_month, 
    SUM(1) AS rsa_vol 
     
FROM 
    sandpit.renewal_base 
     
WHERE 1=1 
    AND prod_type ='Non-RSA' 
    AND prod_name ='NRMA Blue' 
    AND order_type = 'New' 
    AND YEAR(order_start_dt) IN (2018, 2019, 2020, 2021) 
    AND businessmembernumber IS NULL 
    and item_net_price>0 
 
GROUP BY 
    1 
     
ORDER BY 
    1 ASC 
 
 
 
spark.sql(''' 
SELECT 
    renewal_yyyymm AS order_month, 
    SUM(1) AS rsa_vol 
     
FROM 
    sandpit.renewal_base 
     
WHERE 1=1 
    AND prod_type ='Non-RSA' 
    AND prod_name ='NRMA Blue' 
    AND order_type = 'New' 
    AND YEAR(order_start_dt) IN (2018, 2019, 2020, 2021) 
    AND businessmembernumber IS NULL 
    and item_net_price>=60 
 
GROUP BY 
    1 
     
ORDER BY 
    1 ASC 
''').show(500, False) 
 
 
spark.sql(''' 
SELECT 
    TRUNC(order_start_dt, 'MONTH') AS order_month, 
    member_colour, 
    SUM(1) AS rsa_vol 
     
FROM 
    sandpit.renewal_base 
     
WHERE 
    prod_type = 'RSA' 
    AND order_type = 'New' 
    AND YEAR(order_start_dt) IN (2018, 2019, 2020, 2021) 
    AND businessmembernumber IS NULL 
 
GROUP BY 
    1, 
    2 
     
ORDER BY 
    1 ASC, 
    2 
''').show(500, False) 
spark.sql(''' 
SELECT 
    ordi.contact_id, 
    TRUNC(ord.order_dt, 'MONTH') AS ord_month, 
    MAX(ordi.net_pri) AS mem_fee 
     
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
    AND ordi.net_pri > 0 
 
GROUP BY 
    1, 
    2 
     
ORDER BY 
    3 DESC 
''').createOrReplaceTempView('membership') 
SELECT DISTINCT prod_name FROM 
    sandpit.renewal_base 
 
 
 
spark.sql(''' 
SELECT 
    TRUNC(base.order_start_dt, 'MONTH') AS order_month,base.prod_name 
    name, 
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
        WHEN YEAR(NOW()) - YEAR(con.birth_dt) < 25 THEN 20 
        ELSE 10*INT((YEAR(NOW()) - YEAR(con.birth_dt) - 25)/10) + 25 
    END AS age, 
    CASE 
        WHEN item_promo_name = 'CC_NoJoinFee_RSA_0119' THEN 'Join Offer' 
        WHEN membership.contact_id IS NULL THEN 'No Membership Fee' 
        ELSE 'Membership Fee' 
    END AS mem, 
    SUM(1) AS vol 
     
FROM 
    sandpit.renewal_base AS base 
 
LEFT JOIN 
    membership 
    ON membership.contact_id = base.contact_id 
     
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
    base.prod_type = 'RSA' 
    AND base.order_type = 'New' 
    AND YEAR(base.order_start_dt) IN (2018, 2019, 2020, 2021) 
    AND base.businessmembernumber IS NULL 
     
GROUP BY 
    1, 
    2, 
    3, 
    4, 
    5, 
    6 
''').show(500000, False) 
 
 
 
 
spark.sql(''' 
SELECT 
    base.member_colour = conx.attrib_55, 
    SUM(1) 
     
FROM 
    sandpit.renewal_base AS base 
     
LEFT JOIN 
    gms.s_contact_x AS conx 
    ON conx.par_row_id = base.contact_id 
     
WHERE 
    base.prod_type = 'RSA' 
    AND base.order_type = 'New' 
    AND YEAR(base.order_start_dt) IN (2018, 2019, 2020, 2021) 
    AND base.businessmembernumber IS NULL 
     
GROUP BY 1 
''').show(500, False) 
 
 
 
 
 
spark.sql(''' 
SELECT DISTINCT 
    contact_id, 
    TRUNC(base.order_start_dt, 'MONTH') AS order_month, 
    conx.attrib_55 IS NOT NULL AS colour_plus, 
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
    END != 'UNKNOWN' AS region, 
    con.birth_dt IS NOT NULL AS dob 
     
FROM 
    sandpit.renewal_base AS base 
     
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
    base.prod_type = 'RSA' 
    AND base.order_type = 'New' 
    AND YEAR(base.order_start_dt) IN (2020, 2021) 
    AND base.businessmembernumber IS NULL 
''').createOrReplaceTempView('anomaly') 
 
spark.sql(''' 
SELECT order_month, COUNT(DISTINCT contact_id) FROM anomaly 
WHERE region AND dob AND NOT colour_plus 
GROUP BY 1 ORDER BY 1 
''').show(25, False) 
 
 
spark.sql(''' 
SELECT DISTINCT contact_id FROM anomaly 
WHERE region AND dob AND NOT colour_plus 
AND order_month IN ('2020-12-01', '2021-01-01') 
''').show(25000, False) 
 
 
 
spark.sql(''' 
SELECT * FROM anomaly 
WHERE region AND dob 
AND order_month IN ('2020-12-01', '2021-01-01') 
''').show(25, False) 
