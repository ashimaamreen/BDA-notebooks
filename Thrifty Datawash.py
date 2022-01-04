import pyspark.sql.functions as f 
 
gms = spark.sql(''' 
SELECT 
    con.row_id AS con_id, 
    con.csn, 
    TRIM(UPPER(con.email_addr)) AS email_addr, 
    TRIM(UPPER(con.alt_email_addr)) AS alt_email_addr, 
    UPPER(con.fst_name) AS fst_name, 
    UPPER(con.last_name) AS last_name, 
    DATE(birth_dt) AS birth_dt, 
    addr.zipcode AS zipcode, 
    REGEXP_REPLACE(con.home_ph_num, '[^0-9]', '') AS home_ph_num, 
    REGEXP_REPLACE(con.work_ph_num, '[^0-9]', '') AS work_ph_num, 
    REGEXP_REPLACE(con.cell_ph_num, '[^0-9]', '') AS cell_ph_num, 
    detail.product_id 
 
FROM 
    gms.s_contact AS con 
 
LEFT JOIN  
    gms.s_addr_per AS addr 
    ON addr.row_id = con.pr_per_addr_id 
     
LEFT JOIN 
    m4m.return_feed_header AS header 
    ON header.member_number = con.csn 
    AND header.partner IN ('Thrifty', 'Thrifty Car Rental') 
     
LEFT JOIN 
    m4m.return_feed_detail AS detail 
    ON detail.trx_header_id = header.trx_header_id 
''') 
 
gms.select(\ 
    gms.con_id,\ 
    gms.csn,\ 
    gms.email_addr,\ 
    gms.alt_email_addr,\ 
    f.posexplode(f.split('fst_name', ' ')).alias('pos', 'fst_name'),\ 
    gms.last_name,\ 
    gms.birth_dt,\ 
    gms.zipcode,\ 
    gms.home_ph_num,\ 
    gms.work_ph_num,\ 
    gms.cell_ph_num,\ 
    gms.product_id 
).createOrReplaceTempView('gms') 
 
names = spark.sql(''' 
SELECT 
    driverrenterid AS renterid, 
    eventid, 
    REGEXP_REPLACE(TRIM(UPPER(driverfirstname)), '[^A-Z -]', '') AS firstname, 
    REGEXP_REPLACE(TRIM(UPPER(driverlastname)), '[^A-Z -]', '') AS lastname, 
    rentalagreementno 
 
FROM 
    era.businesseventsnapshot 
     
UNION ALL 
 
SELECT 
    customerrenterid AS renterid, 
    eventid, 
    REGEXP_REPLACE(TRIM(UPPER(customerfirstname)), '[^A-Z -]', '') AS firstname, 
    REGEXP_REPLACE(TRIM(UPPER(customerlastname)), '[^A-Z -]', '') AS lastname, 
    rentalagreementno 
 
FROM 
    era.businesseventsnapshot 
''') 
 
names.select(\ 
    names.renterid, 
    names.eventid, 
    f.posexplode(f.split('firstname', ' ')).alias('pos', 'firstname'),\ 
    names.lastname,\ 
    names.rentalagreementno 
).createOrReplaceTempView('names') 
 
spark.sql(''' 
SELECT DISTINCT 
    ind.eventid, 
    ind.renterid, 
    ind.customertype, 
    ind.primarypostcode, 
    ind.secondarypostcode, 
    ind.autoclubnumber, 
    names.firstname, 
    names.lastname, 
    DATE(ind.dateofbirth) AS dateofbirth, 
    TRIM(UPPER(ind.email)) AS email, 
    REGEXP_REPLACE(REGEXP_REPLACE(ind.telephonenumber, '^+..', '0'), '[^0-9+]', '') AS telephonenumber, 
    names.rentalagreementno 
     
FROM 
    era.individual AS ind 
     
INNER JOIN 
    names 
    ON names.renterid = ind.renterid 
    AND names.eventid = ind.eventid 
''').createOrReplaceTempView('era') 
 
spark.sql(''' 
SELECT 
    eventid, 
    renterid, 
    customertype, 
    primarypostcode, 
    secondarypostcode, 
    autoclubnumber, 
    firstname, 
    lastname, 
    dateofbirth, 
    CASE 
        WHEN email LIKE "%NOEMAIL%" THEN NULL 
        WHEN email LIKE "%NO@EMAIL%" THEN NULL 
        WHEN email LIKE "%NO.EMAIL%" THEN NULL 
        WHEN email LIKE "%NOMAIL%" THEN NULL 
        WHEN email LIKE "NO@%" THEN NULL 
        WHEN email LIKE "UNDISCLOSED%" THEN NULL 
        WHEN email LIKE '%REPLY%' THEN NULL 
        WHEN email LIKE '%@%' THEN email 
        ELSE NULL 
    END AS email, 
    telephonenumber, 
    rentalagreementno 
     
FROM 
    era 
     
WHERE 
    firstname NOT IN ('MS', 'MZ', 'MST', 'MR', 'MRS', 'DR') 
''').createOrReplaceTempView('era') 
conditions = ['gms.email_addr = era.email', 
              'gms.alt_email_addr = era.email', 
              'gms.csn = era.autoclubnumber', 
              'gms.product_id = era.rentalagreementno', 
              'era.firstname = gms.fst_name AND gms.birth_dt = era.dateofbirth', 
              'era.firstname = gms.fst_name AND gms.home_ph_num = era.telephonenumber', 
              'era.firstname = gms.fst_name AND gms.work_ph_num = era.telephonenumber', 
              'era.firstname = gms.fst_name AND gms.cell_ph_num = era.telephonenumber', 
              'era.firstname = gms.fst_name AND gms.zipcode = era.primarypostcode', 
              'era.firstname = gms.fst_name AND gms.zipcode = era.secondarypostcode'] 
               
template = ''' 
SELECT DISTINCT 
    con_id, 
    eventid, 
    renterid 
 
FROM 
    gms 
     
INNER JOIN 
    era 
    ON era.lastname = gms.last_name 
    AND {cond} 
''' 
 
query = '''\nUNION\n'''.join([template.format(cond = cond) for cond in conditions]) 
 
 
spark.sql(query).createOrReplaceTempView('washed') 
 
spark.sql(''' 
SELECT DISTINCT 
    con_id, 
    eventid, 
    renterid 
 
FROM 
    washed 
''').repartition(1).write.saveAsTable('era.member_join') 