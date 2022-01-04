spark.sql(""" 
SELECT 
    member_number AS con_csn, 
    CONCAT_WS(',', SORT_ARRAY(COLLECT_LIST(DISTINCT partner))) AS partners 
     
FROM 
    m4m.return_feed_header 
 
WHERE 
    DATE(time_stamp) >= DATE('2021-04-01') 
    AND DATE(time_stamp) < DATE('2021-05-01') 
    AND partner NOT IN ('IAG', 'NRMA Multi Policy Discount', 'NRMA MPD Insurance', 'NRMA Insurance') 
 
GROUP BY 
    member_number 
""").createOrReplaceTempView("recent") 
spark.sql(""" 
SELECT 
    member_number AS con_csn, 
    COUNT(DISTINCT trx_header_id) AS redemptions 
     
FROM 
    m4m.return_feed_header 
 
WHERE 
    partner IN ('Caltex') 
 
GROUP BY 
    member_number 
""").createOrReplaceTempView("caltex") 
spark.sql(""" 
SELECT 
    con.row_id AS con_id, 
    con.fst_name AS first_name, 
    con.last_name AS last_name, 
    con.email_addr AS email_address, 
    con.csn AS member_id, 
    YEAR(NOW()) - YEAR(con.birth_dt) AS age, 
    con.sex_mf AS gender, 
    con.pr_per_addr_id AS addr_id, 
    conx.attrib_55 AS colour_plus, 
    conx.attrib_22 AS geotribe_segment, 
    conx.attrib_17 AS membership_tenure, 
    CASE 
        WHEN COALESCE(conx.attrib_36, '') != 'No' AND COALESCE(confnx.brloc_attrib13, '') != 'Y' THEN 'Y' 
        ELSE 'N' 
    END AS edm_consent 
 
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
 
LEFT ANTI JOIN 
    campaign_data.js_dmc2311_bluewave9seg1_research_edm_20210302_adhoc as seg1 
    ON seg1.member_id = con.csn 
 
LEFT ANTI JOIN 
    (select distinct member_id from campaign_data.campaign_contact_adhoc_staging where table_name='js_dmc2311_bluewave9seg2_research_edm_20210302_adhoc') as seg2 
    ON seg2.member_id = con.csn 
     
LEFT ANTI JOIN 
    (select distinct member_id from campaign_data.campaign_contact_adhoc_staging where table_name='js_dmc2311_bluewave9seg3_research_edm_20210302_adhoc') as seg3 
    ON seg3.member_id = con.csn 
 
WHERE 
    asset.status_cd = 'Active' 
    AND prod.type = 'Membership' 
    AND prod.prod_cd = 'Promotion' 
    AND con.cust_stat_cd = 'Active' 
    AND con.csn IS NOT NULL 
    AND con.con_cd IN ('Ordinary Member', 'Affiliate Member') 
    AND COALESCE(confnx.deceased_flg,'N') = 'N' 
    AND COALESCE(con.x_nrma_title, '') != 'Estate Of The Late' 
    AND COALESCE(con.x_inv_email_1, 'N') = 'N' 
    AND con.email_addr IS NOT NULL 
""").createOrReplaceTempView("eligible") 
spark.sql(""" 
SELECT 
    con.row_id AS con_id 
 
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
    AND prod.prod_cd != 'Promotion' 
    AND prod.sub_type_cd IN ('RSA') 
""").createOrReplaceTempView("rsa") 
spark.sql(""" 
SELECT 
    DISTINCT membernumber 
     
FROM sandpit.ga_appsession as app 
 
INNER JOIN  
    gms.s_contact AS con 
    on con.csn = app.membernumber 
     
WHERE 
    app.membernumber IS NOT NULL 
 
""").createOrReplaceTempView("applogin") 
spark.sql(""" 
SELECT DISTINCT 
    eligible.first_name, 
    eligible.last_name, 
    eligible.email_address, 
    eligible.member_id, 
    CASE 
        WHEN 
            ( 
                address.zipcode >= '2000' AND address.zipcode <= '2082' OR 
                address.zipcode >= '2084' AND address.zipcode <= '2234' OR 
                address.zipcode >= '2555' AND address.zipcode <= '2574' OR 
                address.zipcode >= '2745' AND address.zipcode <= '2770' OR 
                address.zipcode >= '2775' AND address.zipcode <= '2775' 
            ) 
            AND 
            ( 
                UPPER(address.country) = 'AUSTRALIA' OR 
                UPPER(address.country) = 'AU' 
            ) 
        THEN 
            'METROPOLITAN' 
 
        WHEN 
            ( 
                address.zipcode >= '2083' AND address.zipcode <= '2083' OR 
                address.zipcode >= '2250' AND address.zipcode <= '2338' OR 
                address.zipcode >= '2415' AND address.zipcode <= '2423' OR 
                address.zipcode >= '2425' AND address.zipcode <= '2425' OR 
                address.zipcode >= '2428' AND address.zipcode <= '2428' OR 
                address.zipcode >= '2500' AND address.zipcode <= '2535' OR 
                address.zipcode >= '2538' AND address.zipcode <= '2541' OR 
                address.zipcode >= '2575' AND address.zipcode <= '2578' OR 
                address.zipcode >= '2600' AND address.zipcode <= '2617' OR 
                address.zipcode >= '2773' AND address.zipcode <= '2774' OR 
                address.zipcode >= '2776' AND address.zipcode <= '2786' OR 
                address.zipcode >= '2900' AND address.zipcode <= '2914' 
            ) 
            AND 
            ( 
                UPPER(address.country) = 'AUSTRALIA' OR 
                UPPER(address.country) = 'AU' 
            ) 
        THEN 
            'REGIONAL' 
         
        WHEN 
            ( 
                address.zipcode >= '2339' AND address.zipcode <= '2411' OR 
                address.zipcode >= '2424' AND address.zipcode <= '2424' OR 
                address.zipcode >= '2426' AND address.zipcode <= '2427' OR 
                address.zipcode >= '2429' AND address.zipcode <= '2490' OR 
                address.zipcode >= '2536' AND address.zipcode <= '2537' OR 
                address.zipcode >= '2545' AND address.zipcode <= '2551' OR 
                address.zipcode >= '2579' AND address.zipcode <= '2594' OR 
                address.zipcode >= '2618' AND address.zipcode <= '2739' OR 
                address.zipcode >= '2787' AND address.zipcode <= '2898' OR 
                address.zipcode >= '6798' AND address.zipcode <= '6799' 
            ) 
            AND 
            ( 
                UPPER(address.country) = 'AUSTRALIA' OR 
                UPPER(address.country) = 'AU' 
            ) 
        THEN 
            'RURAL' 
         
        WHEN 
            ( 
                address.zipcode >= '0800' AND address.zipcode <= '0886' OR 
                address.zipcode >= '3000' AND address.zipcode <= '6770' OR 
                address.zipcode >= '6907' AND address.zipcode <= '7470' OR 
                address.zipcode >= '7471'  
            ) 
            AND 
            ( 
                UPPER(address.country) = 'AUSTRALIA' OR 
                UPPER(address.country) = 'AU' 
            ) 
        THEN 
            'INTERSTATE' 
 
        ELSE 
            'UNKNOWN' 
    END AS area_definition, 
    eligible.colour_plus, 
    eligible.age, 
    eligible.gender, 
    address.city AS suburb, 
    address.zipcode AS postcode, 
    eligible.geotribe_segment, 
    'Yes' AS app_login, 
    COALESCE(caltex.redemptions, 0) AS caltex_redemptions, 
    CASE WHEN recent.con_csn IS NULL THEN 'N' ELSE 'Y' END AS redeemed_recently, 
    COALESCE(recent.partners, '') AS recent_partners, 
    CASE WHEN rsa.con_id IS NULL THEN 'N' ELSE 'Y' END AS has_rsa, 
    eligible.membership_tenure, 
    eligible.edm_consent 
 
FROM 
    eligible 
 
LEFT OUTER JOIN 
    gms.s_addr_per AS address 
    ON address.row_id = eligible.addr_id 
 
LEFT OUTER JOIN 
    rsa 
    ON rsa.con_id = eligible.con_id 
 
LEFT OUTER JOIN 
    recent 
    ON recent.con_csn = eligible.member_id 
     
LEFT OUTER JOIN 
    caltex 
    ON caltex.con_csn = eligible.member_id 
 
LEFT OUTER JOIN 
    applogin 
    on applogin.membernumber = eligible.member_id 
""").createOrReplaceTempView("population") 
pop = spark.sql(""" 
SELECT * FROM population 
""") 
pop.sample(False, 20100.0/pop.count(), 0).repartition(1).write.saveAsTable("campaign_data.aa_dmc2394_bluewave9seg1_research_edm_20210518")  
SELECT * FROM campaign_data.campaign_contact_adhoc_staging  
where table_name='js_dmc2311_bluewave9seg2_research_edm_20210302_adhoc' 
pop = spark.sql(""" 
SELECT * FROM population  
LEFT ANTI JOIN campaign_data.aa_dmc2394_bluewave9seg1_research_edm_20210518 AS s1 ON s1.member_id = population.member_id 
WHERE redeemed_recently = 'Y' 
""") 
 
pop.sample(False, 10200.0/pop.count(), 0).repartition(1).write.saveAsTable("campaign_data.aa_dmc2394_bluewave9seg2_research_edm_20210518")  
pop = spark.sql(""" 
SELECT * FROM population 
LEFT ANTI JOIN campaign_data.aa_dmc2394_bluewave9seg1_research_edm_20210518 AS s1 ON s1.member_id = population.member_id 
LEFT ANTI JOIN campaign_data.aa_dmc2394_bluewave9seg2_research_edm_20210518 AS s2 ON s2.member_id = population.member_id 
WHERE caltex_redemptions > 0 
""") 
 
pop.sample(False, 10100.0/pop.count(), 0).repartition(1).write.saveAsTable("campaign_data.aa_dmc2394_bluewave9seg3_research_edm_20210518")  
SELECT * from campaign_data.aa_dmc2394_bluewave9seg1_research_edm_20210518