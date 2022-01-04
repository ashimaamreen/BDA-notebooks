import pyspark.sql.functions as f 
 
def load_file(csv, view): 
    spark.read.load(\ 
        '/user/hive/warehouse/campaign_data.db/external_files/NRMAMarine/{csv}'.format(csv = csv),\ 
        format = 'csv',\ 
        sep = ',',\ 
        inferSchema = True,\ 
        header = True\ 
    ).createOrReplaceTempView(view) 
 
load_file('FH-FAN.csv', 'A') 
load_file('FH-MFF.csv', 'B') 
load_file('FH-FAN_NRMA_members_custom_field.csv', 'C') 
load_file('FH-MFF_NRMA_members_custom_field.csv', 'D') 
load_file('Masabi_201801-202006.csv', 'E') 
 
# tables = [['EchidnaProductTable-20181100Prod20-06-17_21-26-11.csv', 'date AS dt'], 
# ['Echidna_Transaction_Table-20181100Trans20-06-17_21-27-40.csv', 'date AS dt'], 
# ['FH-FAN.csv', 'TO_TIMESTAMP(`created at`, "yyyy-MM-dd") AS dt'], 
# ['FH-FAN_NRMA_members_custom_field.csv', 'TO_TIMESTAMP(customers, "yyyy-MM-dd") AS dt'], 
# ['FH-MFF.csv', 'TO_TIMESTAMP(`created at`, "yyyy-MM-dd") AS dt'], 
# ['FH-MFF_NRMA_members_custom_field.csv', 'TO_TIMESTAMP(customers, "yyyy-MM-dd") AS dt'], 
# ['Masabi_201801-202006.csv', 'TO_TIMESTAMP(msgdate, "d/MM/yyyy") AS dt'], 
# ['erg20200602_16-24-23.csv', 'date AS dt']] 
 
# Pulls in the potentially usable matching data 
# Zipcode is not used in the end as name quality is poor so any matching based on name and zipcode would be unreliable 
aleph = spark.sql(''' 
SELECT `booking id` AS booking_id, NULL AS csn, contact, phone, email, NULL AS zipcode, 
TO_TIMESTAMP(`created at`, "yyyy-MM-dd") AS dt, Item AS prod, Total AS price, 'A' AS src FROM A 
UNION ALL 
SELECT `booking id` AS booking_id, NULL AS csn, contact, phone, email, NULL AS zipcode, 
TO_TIMESTAMP(`created at`, "yyyy-MM-dd") AS dt, Item AS prod, Total AS price, 'B' AS src FROM B 
UNION ALL 
SELECT _c0 AS booking_id, _c1 AS csn, _c10 AS contact, _c11 AS phone, _c12 AS email, NULL AS zipcode, 
TO_TIMESTAMP(customers, "yyyy-MM-dd") AS dt, Bookings AS prod, NULL AS price, 'C' AS src FROM C 
UNION ALL 
SELECT _c0 AS booking_id, _c1 AS csn, _c10 AS contact, _c11 AS phone, _c12 AS email, NULL AS zipcode, 
TO_TIMESTAMP(customers, "yyyy-MM-dd") AS dt, Bookings AS prod, NULL AS price, 'D' AS src FROM D 
UNION ALL 
SELECT id AS booking_id, NULL AS csn, cardname AS contact, NULL AS phone, email, zipcode, 
TO_TIMESTAMP(msgdate, "d/MM/yyyy") AS dt, NULL AS prod, totalvalue AS price, 'E' AS src FROM E 
''') 
 
aleph = aleph.withColumn('price', f.regexp_replace('price',  '[$,]', '')) 
 
aleph = aleph.withColumn('prod', f.upper(f.col('prod'))) 
 
# Removes non-numeric characters from NRMA member numbers 
aleph = aleph.withColumn('csn', f.regexp_replace('csn',  '[^0-9]', '')) 
 
# Uppercases email, email quality was very good, didn't even have dummy noreply emails 
aleph = aleph.withColumn('email', f.upper(f.col('email'))) 
 
# Replaces country codes with 0 in phone numbers 
aleph = aleph.withColumn('phone', f.regexp_replace('phone',  '^\+[0-9]+', '0')) 
 
# Removes non-numeric characters from phone numbers  
aleph = aleph.withColumn('phone', f.regexp_replace('phone',  '[^0-9]', '')) 
 
# Uppercases names 
aleph = aleph.withColumn('contact', f.upper(f.col('contact'))) 
 
# For some reason a lot of titles are preceeded by a dot, so we can use that to remove them 
aleph = aleph.withColumn('contact', f.regexp_replace('contact',  '[.][A-Z]+', '')) 
 
# More title removals, gets rid of common titles at the beginning or after a space 
aleph = aleph.withColumn('contact', f.regexp_replace('contact',  '(^| )(MR|MRS|MS|DR|MISS) ', '')) 
 
# Changes substrings and symbols that denote multiple peope into a symbol we can more easily split on 
# and splits based on it 
aleph = aleph.withColumn('contact', f.regexp_replace('contact', ' AND ', '+')) 
aleph = aleph.withColumn('contact', f.regexp_replace('contact', '&', '+')) 
aleph = aleph.withColumn('contact', f.regexp_replace('contact', '/', '+')) 
aleph = aleph.withColumn('contact', f.regexp_replace('contact', ',', '+')) 
aleph = aleph.withColumn('contact', f.explode(f.split('contact', '\+'))) 
 
# Splits based on spaces 
aleph.withColumn('contact', f.explode(f.split('contact', ' '))).createOrReplaceTempView('aleph') 
 
# Trims results and purges anything that is only one letter 
spark.sql(''' 
SELECT 
    booking_id, 
    prod, 
    csn, 
    TRIM(contact) AS contact, 
    phone, 
    email, 
    zipcode, 
    src, 
    COALESCE(FLOAT(price), 0) AS price, 
    DATE(dt) AS dt 
     
FROM 
    aleph 
     
WHERE 
    LENGTH(TRIM(contact)) > 1 
''').createOrReplaceTempView('aleph') 
 
# Prepares the GMS side of the data 
gms = spark.sql(''' 
SELECT 
    con.row_id AS con_id, 
    con.csn, 
    con.con_cd, 
    TRIM(UPPER(con.email_addr)) AS email_addr, 
    TRIM(UPPER(con.alt_email_addr)) AS alt_email_addr, 
    CONCAT_WS(' ', UPPER(con.fst_name), UPPER(con.last_name)) AS name, 
    addr.zipcode AS zipcode, 
    REGEXP_REPLACE(con.home_ph_num, '[^0-9]', '') AS home_ph_num, 
    REGEXP_REPLACE(con.work_ph_num, '[^0-9]', '') AS work_ph_num, 
    REGEXP_REPLACE(con.cell_ph_num, '[^0-9]', '') AS cell_ph_num 
     
FROM 
    gms.s_contact AS con 
 
LEFT JOIN 
    gms.s_asset AS asset 
    ON asset.owner_con_id = con.row_id 
 
LEFT JOIN  
    gms.s_addr_per AS addr 
    ON addr.row_id = con.pr_per_addr_id 
''') 
 
gms.select(\ 
    gms.con_id,\ 
    gms.csn,\ 
    gms.con_cd,\ 
    gms.email_addr,\ 
    gms.alt_email_addr,\ 
    f.posexplode(f.split('name', ' ')).alias('pos', 'name'),\ 
    gms.zipcode,\ 
    gms.home_ph_num,\ 
    gms.work_ph_num,\ 
    gms.cell_ph_num 
).createOrReplaceTempView('gms') 
 
# Condition for matching is that there is at least one name fragment match 
# and one additional match 
template = ''' 
SELECT 
    aleph.src, 
    aleph.booking_id, 
    aleph.prod, 
    aleph.dt, 
    aleph.price, 
    gms.con_id, 
    gms.con_cd 
 
FROM 
    aleph 
 
INNER JOIN 
    gms 
    ON gms.name = aleph.contact 
    AND {cond} 
''' 
 
# Accepted matchings are Member Number, Phone Number, Email 
matchings = ['gms.csn = aleph.csn', 
             'gms.home_ph_num = aleph.phone', 
             'gms.work_ph_num = aleph.phone', 
             'gms.cell_ph_num = aleph.phone', 
             'gms.email_addr = aleph.email', 
             'gms.alt_email_addr = aleph.email'] 
 
# For every condition in "matchings", run "template" with that condition and union it all together 
spark.sql(''' 
UNION ALL 
'''.join([template.format(cond = x) for x in matchings])).createOrReplaceTempView('matchings') 
 
# For every unique booking, rank the matched contacts based on how many matches they produced 
# with Rank 1 being our most confident match 
spark.sql(''' 
SELECT src, booking_id, prod, con_id, con_cd, MIN(dt) AS dt, MAX(price) AS price,  
RANK() OVER (PARTITION BY booking_id ORDER BY COUNT(*) DESC) AS priority FROM matchings GROUP BY 1, 2, 3, 4, 5 
''').createOrReplaceTempView('matchings') 
 
 
 
 
 
 
spark.sql(''' 
SELECT 
    *, 
    CASE 
        WHEN src = 'E' THEN 'Ferry' 
        WHEN prod RLIKE 'RETURN' AND prod NOT RLIKE 'HOPPER' THEN 'Ferry' 
        WHEN prod RLIKE '(SINGLE|RETURN|ONE-WAY) (FERRY )?TICKET' THEN 'Ferry' 
        WHEN prod RLIKE 'ONE WAY' THEN 'Ferry' 
        WHEN prod RLIKE 'NYE' THEN 'Event' 
        WHEN prod RLIKE 'NEW YEAR' THEN 'Event' 
        WHEN prod RLIKE 'BOXING' THEN 'Event' 
        WHEN prod RLIKE 'VIVID' THEN 'Event' 
        WHEN prod RLIKE 'FIREWORKS' THEN 'Event' 
        WHEN prod RLIKE 'AUSTRALIA DAY' THEN 'Event' 
        WHEN prod RLIKE 'GOSFORD RACE' THEN 'Event' 
        WHEN prod RLIKE 'NRL' THEN 'Event' 
        ELSE 'Non-Event' 
    END AS classification 
 
FROM 
    matchings 
     
WHERE 
    priority = 1 
''').createOrReplaceTempView('marine') 
 
spark.sql(''' 
SELECT 
    con_id, 
    MAX(dt) AS dt, 
    FALSE AS src_ams, 
    TRUE AS src_mar, 
    MAX(classification IN ('Ferry')) AS ferry, 
    MAX(classification IN ('Non-Event', 'Event')) AS tourism, 
    MAX(classification IN ('Non-Event')) AS non_event, 
    MAX(classification IN ('Event')) AS event, 
    COUNT(DISTINCT CASE WHEN classification IN ('Event', 'Non-Event') THEN booking_id ELSE 0 END) AS vol, 
    0 AS ams_price, 
    SUM(price) AS marine_price 
 
FROM 
    marine 
     
GROUP BY 
    1 
''').createOrReplaceTempView('marine') 
spark.sql(''' 
SELECT DISTINCT 
    det.trx_detail_id, 
    con.row_id AS con_id, 
    red.time_stamp AS dt, 
    TRUE AS src_ams, 
    FALSE AS src_mar, 
    red.partner, 
    det.product_id, 
    UPPER(det.item_description) AS prod, 
    FLOAT(item_price) AS price 
     
FROM 
    gms.s_contact AS con 
 
INNER JOIN 
    m4m.return_feed_header AS red 
    ON red.member_number = con.csn 
     
INNER JOIN 
    m4m.return_feed_detail AS det 
    ON det.trx_header_id = red.trx_header_id 
 
WHERE 
    partner IN ('My Fast Ferry', 'Fantasea') 
''').createOrReplaceTempView('ams') 
 
spark.sql(''' 
SELECT 
    *, 
    CASE 
        WHEN product_id IS NOT NULL THEN 'Ferry' 
        WHEN prod RLIKE 'NEW YEAR' THEN 'Event' 
        WHEN prod RLIKE 'BOXING' THEN 'Event' 
        WHEN prod RLIKE 'VIVID'  THEN 'Event' 
        WHEN prod RLIKE 'FIREWORKS' THEN 'Event' 
        WHEN prod RLIKE 'AUSTRALIA DAY' THEN 'Event' 
        WHEN prod RLIKE 'GOSFORD RACE' THEN 'Event' 
        ELSE 'Non-Event' 
    END AS classification 
 
FROM 
    ams 
''').createOrReplaceTempView('ams') 
 
spark.sql(''' 
SELECT 
    con_id, 
    DATE(MAX(dt)) AS dt, 
    TRUE AS src_ams, 
    FALSE AS src_mar, 
    MAX(classification IN ('Ferry')) AS ferry, 
    MAX(classification IN ('Non-Event', 'Event')) AS tourism, 
    MAX(classification IN ('Non-Event')) AS non_event, 
    MAX(classification IN ('Event')) AS event, 
    COUNT(DISTINCT CASE WHEN classification IN ('Event', 'Non-Event') THEN trx_detail_id ELSE 0 END) AS vol, 
    SUM(price) AS ams_price, 
    NULL AS marine_price 
 
FROM 
    ams 
 
WHERE 
    partner IN ('My Fast Ferry', 'Fantasea') 
 
GROUP BY 
    1 
''').createOrReplaceTempView('ams') 
 
spark.sql(''' 
SELECT * FROM marine 
UNION ALL  
SELECT * FROM ams 
''').createOrReplaceTempView('users') 
 
spark.sql(''' 
SELECT con_id, MAX(dt) AS dt, MAX(src_ams) AS src_ams, MAX(src_mar) AS src_mar, MAX(ferry) AS ferry, MAX(tourism) AS tourism, MAX(non_event) AS non_event, 
MAX(event) AS event, MAX(vol) AS vol, MAX(ams_price) AS ams_price, MAX(marine_price) AS marine_price 
FROM users GROUP BY 1 
''').createOrReplaceTempView('users') 
 
spark.sql(''' 
SELECT DISTINCT CAST(poa_code16 AS STRING) AS poa FROM geospatial.meshblock2other WHERE lga_code16 = 15990 
''').createOrReplaceTempView('nb') 
 
spark.sql(''' 
SELECT member_number, 
MAX(partner IN ('IAG', 'NRMA Multi Policy Discount', 'NRMA MPD Insurance', 'NRMA Insurance')) AS red_iag, 
MAX(partner IN ('My Fast Ferry', 'Fantasea')) AS red_marine, 
MAX(partner NOT IN ('IAG', 'NRMA Multi Policy Discount', 'NRMA MPD Insurance', 'NRMA Insurance', 'My Fast Ferry', 'Fantasea')) AS red_other 
FROM m4m.return_feed_header 
GROUP BY 1 
''').createOrReplaceTempView('redeemed') 
 
spark.sql(''' 
SELECT DISTINCT 
    con.row_id AS con_id 
     
    FROM 
        gms.s_contact AS con 
     
    INNER JOIN 
        gms.s_contact_x AS conx 
        ON conx.par_row_id = con.row_id 
     
    INNER JOIN 
        gms.s_contact_fnx AS confnx 
        ON confnx.par_row_id = con.row_id 
     
    INNER JOIN 
        gms.s_asset AS asset 
        ON asset.owner_accnt_id = con.pr_dept_ou_id 
     
    INNER JOIN 
        gms.s_prod_int AS prod 
        ON prod.row_id = asset.prod_id 
     
    WHERE 
        asset.status_cd = 'Active' 
        AND prod.type = 'Membership' 
        AND prod.prod_cd = 'Promotion' 
        AND con.cust_stat_cd = 'Active' 
        AND con.csn IS NOT NULL 
        -- AND con.con_cd IN ('Ordinary Member', 'Affiliate Member') 
        AND COALESCE(confnx.deceased_flg,'N') = 'N' 
        AND COALESCE(con.x_nrma_title, '') != 'Estate Of The Late' 
''').createOrReplaceTempView('blue_eligible') 
 
spark.sql(''' 
    SELECT DISTINCT 
        con.row_id AS con_id, 
        prod.name LIKE 'Autoclub%' AS cmo 
     
    FROM 
        gms.s_contact AS con 
     
    INNER JOIN 
        gms.s_asset AS asset 
        ON asset.owner_con_id = con.row_id 
     
    INNER JOIN 
        gms.s_prod_int AS prod 
        ON prod.row_id = asset.prod_id 
     
    WHERE 
        asset.status_cd = 'Active' 
        AND prod.sub_type_cd = 'RSA' 
        AND prod.prod_cd = 'Product' 
        -- AND prod.name NOT LIKE 'Autoclub%' 
        -- AND prod.name NOT LIKE 'Mobility Scooter' 
        AND con.cust_stat_cd = 'Active' 
        AND con.csn IS NOT NULL 
''').createOrReplaceTempView('rsa') 
spark.sql(''' 
SELECT DISTINCT 
    con.csn AS member_id, 
    con.fst_name AS first_name, 
    con.last_name AS last_name, 
    con.email_addr AS email, 
    addr.city AS suburb, 
    addr.zipcode AS postcode, 
    YEAR(NOW()) - YEAR(con.birth_dt) AS age, 
    con.sex_mf AS gender, 
    conx.attrib_55 AS colour_plus, 
    users.dt IS NOT NULL AS marine_user, 
    COALESCE(users.ferry, FALSE) AS ferry_user, 
    COALESCE(users.tourism, FALSE) AS tourism_user, 
    COALESCE(users.non_event, FALSE) AS non_event_user, 
    COALESCE(users.event, FALSE) AS event_user, 
    COALESCE(users.src_ams, FALSE) AS in_ams_dataset, 
    COALESCE(users.src_mar, FALSE) AS in_marine_dataset, 
    COALESCE(redeemed.red_iag, FALSE) AS redeemed_iag, 
    COALESCE(redeemed.red_marine, FALSE) AS redeemed_marine, 
    COALESCE(redeemed.red_other, FALSE) AS redeemed_other, 
    CASE 
        WHEN rsa.con_id IS NOT NULL THEN 'RSA' 
        WHEN blue_eligible.con_id IS NOT NULL THEN 'Blue Eligible' 
        WHEN con_cd = 'Ordinary Member' THEN 'Anomaly' 
        ELSE 'Affiliate' 
    END AS status, 
    conx.attrib_17 AS tenure 
     
    FROM 
        gms.s_contact AS con 
     
    INNER JOIN 
        gms.s_contact_x AS conx 
        ON conx.par_row_id = con.row_id 
     
    INNER JOIN 
        gms.s_contact_fnx AS confnx 
        ON confnx.par_row_id = con.row_id 
     
    INNER JOIN 
        gms.s_asset AS asset 
        ON asset.owner_accnt_id = con.pr_dept_ou_id 
     
    INNER JOIN 
        gms.s_prod_int AS prod 
        ON prod.row_id = asset.prod_id 
     
    INNER JOIN 
        gms.s_addr_per AS addr 
        ON addr.row_id = con.pr_per_addr_id 
 
    INNER JOIN 
        geospatial.geocoded_mem_addr AS mesh 
        ON mesh.addr_id = con.pr_per_addr_id 
         
    INNER JOIN 
        geospatial.meshblock2other AS lga 
        ON CAST(lga.mb_code16 AS STRING) = mesh.gnaf_mb 
 
    INNER JOIN 
        blue_eligible 
        ON blue_eligible.con_id = con.row_id     
         
    LEFT OUTER JOIN 
        redeemed 
        ON redeemed.member_number = con.csn 
 
    LEFT OUTER JOIN 
        rsa 
        ON rsa.con_id = con.row_id         
 
    LEFT OUTER JOIN 
        users 
        ON users.con_id = con.row_id 
     
    WHERE 
        con.cust_stat_cd = 'Active' 
        AND con.csn IS NOT NULL 
        AND con.con_cd IN ('Ordinary Member', 'Affiliate Member') 
        AND COALESCE(confnx.deceased_flg,'N') = 'N' 
        AND COALESCE(con.x_nrma_title, '') != 'Estate Of The Late' 
        AND COALESCE(con.x_inv_email_1, 'N') = 'N' 
        AND con.email_addr IS NOT NULL 
        AND COALESCE(rsa.cmo, FALSE) = FALSE  
        AND ( 
            sa3_code16 IN ( 
            11501, 
            11502, 
            11504, 
            12301, 
            12302, 
            12401, 
            12403, 
            12404, 
            12405 
            ) 
            OR 
            sa4_code16 IN ( 
                116, 
                117, 
                118, 
                119, 
                120, 
                121, 
                122, 
                125, 
                126, 
                127, 
                128 
            ) 
        ) 
''').createOrReplaceTempView('pool') 
# spark.sql(''' 
# SELECT 
#     member_id, 
#     first_name, 
#     last_name, 
#     email, 
#     suburb, 
#     postcode, 
#     age, 
#     gender, 
#     colour_plus, 
#     marine_user, 
#     ferry_user, 
#     tourism_user, 
#     non_event_user, 
#     event_user, 
#     in_ams_dataset, 
#     in_marine_dataset, 
#     redeemed_iag, 
#     redeemed_marine, 
#     redeemed_other, 
#     status, 
#     tenure 
     
# FROM 
#     pool 
     
# WHERE 
#     age >= 16 
# ''').sample(False, 0.019).repartition(1).write.saveAsTable('campaign_data.cc_dmc2074_marineproductexplorationall_research_edm_20200720_adhoc') 
# spark.sql(''' 
# SELECT 
#     member_id, 
#     first_name, 
#     last_name, 
#     email, 
#     suburb, 
#     postcode, 
#     age, 
#     gender, 
#     colour_plus, 
#     marine_user, 
#     ferry_user, 
#     tourism_user, 
#     non_event_user, 
#     event_user, 
#     in_ams_dataset, 
#     in_marine_dataset, 
#     redeemed_iag, 
#     redeemed_marine, 
#     redeemed_other, 
#     status, 
#     tenure 
     
# FROM 
#     pool 
     
# LEFT ANTI JOIN 
#     campaign_data.cc_dmc2074_marineproductexplorationall_research_edm_20200720_adhoc AS all 
#     ON all.member_id = pool.member_id 
     
# WHERE 
#     age >= 16 
#     AND marine_user 
# ''').sample(False, 0.6).repartition(1).write.saveAsTable('campaign_data.cc_dmc2074_marineproductexplorationuser_research_edm_20200720_adhoc')