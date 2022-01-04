SELECT DISTINCT  
        con.csn 
        , con.fst_name 
        , con.last_name 
        , con.email_addr 
        , m.trx_header_id 
 
FROM 
    gms.s_contact con 
     
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
    m4m.return_feed_header m 
    on con.csn=m.member_number 
     
WHERE 1=1 
    AND con.cust_stat_cd = 'Active' 
    AND con.con_cd IN ('Ordinary Member', 'Affiliate Member') 
    AND COALESCE(con.x_inv_email_1, 'N') = 'N' 
    AND con.email_addr IS NOT NULL 
    AND COALESCE(conx.attrib_44, '') NOT RLIKE 'Honorary' 
    AND FLOOR(DATEDIFF(NOW(), con.birth_dt)/365) >= 18 
    AND COALESCE(addr.state, '') IN ('NSW', 'ACT') 
     
    AND partner='Caltex' 
    AND to_date(time_stamp) between '2021-06-01' and '2021-06-30' 
Winner_List = spark.sql(""" 
SELECT DISTINCT  
        con.csn 
        , con.fst_name 
        , con.last_name 
        , con.email_addr 
        , m.trx_header_id 
 
FROM 
    gms.s_contact con 
     
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
    m4m.return_feed_header m 
    on con.csn=m.member_number 
     
WHERE 1=1 
    AND con.cust_stat_cd = 'Active' 
    AND con.con_cd IN ('Ordinary Member', 'Affiliate Member') 
    AND COALESCE(con.x_inv_email_1, 'N') = 'N' 
    AND con.email_addr IS NOT NULL 
    AND COALESCE(conx.attrib_44, '') NOT RLIKE 'Honorary' 
    AND FLOOR(DATEDIFF(NOW(), con.birth_dt)/365) >= 18 
    AND COALESCE(addr.state, '') IN ('NSW', 'ACT') 
     
    AND partner='Caltex' 
    AND date(time_stamp) between date('2021-06-01') and date('2021-06-30') 
""") 
Winner_List.createOrReplaceTempView("Winner_List") 
spark.sql("""select * from Winner_List """).show(250000, False) 
spark.sql("""select * from Winner_List """).count() 
spark.sql("""select csn, fst_name, last_name, email_addr 
        , trx_header_id, RAND() from Winner_list  
order by 6 DESC""").show(250000,False) 