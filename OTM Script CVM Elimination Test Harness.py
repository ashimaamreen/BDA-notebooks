Contents
cvm_hrhv = spark.read.load('gms_otm/cvm_hrhv.csv', format = 'csv', header = 'true') 
gms_hrhv = spark.read.load('gms_otm/gms_hrhv.csv', format = 'csv', header = 'true') 
df = cvm_hrhv.join(gms_hrhv, cvm_hrhv.cvm_ord == gms_hrhv.gms_ord, 'left_anti').cache() 
df.createOrReplaceTempView('df') 
 
spark.sql(''' 
SELECT 
    churn.contact_row_id, 
    MAX(CASE WHEN DATE(churn.duedate) = '2019-08-04' AND churn.prediction_probability > 0.21 THEN 1 ELSE 0 END) AS qualifies 
 
FROM 
    df 
     
LEFT OUTER JOIN 
    mlpipelinedb.churn_predictions_history AS churn 
    ON churn.contact_row_id = df.cvm_con  
     
GROUP BY 
    churn.contact_row_id 
     
ORDER BY 
    churn.contact_row_id 
''').show(1000, False) 
df = gms_hrhv.join(cvm_hrhv, cvm_hrhv.cvm_ord == gms_hrhv.gms_ord, 'left_anti').cache() 
df.createOrReplaceTempView('df') 
 
spark.sql(''' 
SELECT 
    df.gms_con, 
    asset.owner_con_id, 
    churn.asset_row_id, 
    churn.churn_score 
 
FROM 
    df 
     
INNER JOIN 
    gms.s_org_ext AS account 
    ON account.pr_con_id = df.gms_con 
     
INNER JOIN 
    gms.s_asset AS asset 
    ON asset.owner_accnt_id = account.par_row_id 
     
INNER JOIN 
    gms.cx_cvm_churn AS churn 
    ON churn.asset_row_id = asset.row_id 
''').show(1000, False) 
cvm_bau = spark.read.load('gms_otm/cvm_bau.csv', format = 'csv', header = 'true') 
gms_bau = spark.read.load('gms_otm/gms_bau.csv', format = 'csv', header = 'true') 
df = cvm_bau.join(gms_bau, cvm_bau.cvm_ord == gms_bau.gms_ord, 'left_anti').cache() 
df.createOrReplaceTempView('df') 
 
spark.sql(''' 
SELECT 
    churn.contact_row_id, 
    MAX(CASE WHEN DATE(churn.duedate) = '2019-07-16' AND churn.prediction_probability > 0.142 THEN 1 ELSE 0 END) AS qualifies 
 
FROM 
    df 
     
LEFT OUTER JOIN 
    mlpipelinedb.churn_predictions_history AS churn 
    ON churn.contact_row_id = df.cvm_con  
     
GROUP BY 
    churn.contact_row_id 
''').show(1000, False) 
df = gms_bau.join(cvm_bau, cvm_bau.cvm_ord == gms_bau.gms_ord, 'left_anti').cache() 
df.createOrReplaceTempView('df') 
 
spark.sql(''' 
SELECT 
    df.gms_con, 
    asset.owner_con_id, 
    churn.asset_row_id, 
    churn.churn_score 
 
FROM 
    df 
     
INNER JOIN 
    gms.s_org_ext AS account 
    ON account.pr_con_id = df.gms_con 
     
INNER JOIN 
    gms.s_asset AS asset 
    ON asset.owner_accnt_id = account.par_row_id 
     
INNER JOIN 
    gms.cx_cvm_churn AS churn 
    ON churn.asset_row_id = asset.row_id 
     
ORDER BY 
    df.gms_con 
''').show(1000, False) 
df.createOrReplaceTempView('df') 
 
spark.sql(''' 
SELECT 
    * 
 
FROM 
    df 
     
ORDER BY 
    df.gms_con 
''').show(1000, False) 
SELECT * FROM gms.cx_cvm_churn WHERE asset_row_id = "1-9351MYT" 
SELECT prediction_probability, contact_row_id, membernumber, assetnumber, duedate, asset_id FROM mlpipelinedb.churn_predictions_history WHERE asset_id = "1-9351MYT" 
SELECT end_dt FROM gms.s_asset WHERE row_id = "1-9351MYT" 