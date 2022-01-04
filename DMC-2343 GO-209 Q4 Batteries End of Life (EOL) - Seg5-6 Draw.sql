select  distinct e.campaign_id 
 
from omc.send_level_summary e 
 
inner join omc.campaign_info m 
on m.campaign_id=e.campaign_id 
 
where e.bounce_event_date is null 
and e.send_event_date>'2021-01-01' 
and e.channel='Email' 
and m.campaign_name like '%20210510%' 
--and m.campaign_type='PromotionalCampaign' 
select distinct partner from m4m.return_feed_header where partner like '%atter%' 
SELECT * from m4m.return_feed_header 
where member_number='609511801' 
and partner='NRMA Batteries' 
--trx_header_id='99912705690655754515' 
select member_number,sum(total_amount) from m4m.return_feed_header 
 
where partner='Caltex' 
 
group by 1 
having sum(total_amount)<=0 and count(*)>1 and max(total_amount>0) 
SELECT DISTINCT con.csn, con.row_id 
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
    omc.send_level_summary e 
    on e.customer_id=con.row_id 
 
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
     
    AND e.campaign_id in ('57438262','57438982','57438442') 
    and e.control is false 
     
    And partner='NRMA Batteries' 
    AND to_date(time_stamp) between '2021-05-10' and '2021-06-30' 
SELECT DISTINCT con.csn, con.row_id 
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
    omc.send_level_summary e 
    on e.customer_id=con.row_id 
 
    INNER JOIN  
    m4m.return_feed_header m 
    on con.csn=m.member_number 
 
 
WHERE 1=1 
    AND con.cust_stat_cd = 'Active' 
    AND con.con_cd IN ('Ordinary Member', 'Affiliate Member') 
    AND COALESCE(con.x_inv_email_1, 'N') = 'N' 
    AND con.email_addr IS NOT NULL 
    AND COALESCE(conx.attrib_44, '') NOT RLIKE 'Honorary' 
    AND FLOOR(DATEDIFF(current_timestamp(), con.birth_dt)/365) >= 18 
    AND COALESCE(addr.state, '') IN ('NSW', 'ACT') 
     
    AND e.campaign_id in ('57438262','57438982','57438442') 
    and e.control=false 
     
    And partner='NRMA Batteries' 
    AND date(time_stamp) between date('2021-05-10') and date('2021-06-30') 
Winner_List = spark.sql(""" 
SELECT DISTINCT con.csn 
        , con.row_id 
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
    omc.send_level_summary e 
    on e.customer_id=con.row_id 
 
    INNER JOIN  
    m4m.return_feed_header m 
    on con.csn=m.member_number 
 
 
WHERE 1=1 
    AND con.cust_stat_cd = 'Active' 
    AND con.con_cd IN ('Ordinary Member', 'Affiliate Member') 
    AND COALESCE(con.x_inv_email_1, 'N') = 'N' 
    AND con.email_addr IS NOT NULL 
    AND COALESCE(conx.attrib_44, '') NOT RLIKE 'Honorary' 
    AND FLOOR(DATEDIFF(current_timestamp(), con.birth_dt)/365) >= 18 
    AND COALESCE(addr.state, '') IN ('NSW', 'ACT') 
     
    AND e.campaign_id in ('57438262','57438982','57438442') 
    and e.control=false 
     
    And partner='NRMA Batteries' 
    AND date(time_stamp) between date('2021-05-10') and date('2021-06-30') 
""") 
Winner_List.createOrReplaceTempView("Winner_List") 
spark.sql("""select * from Winner_List """).show(250000, False) 
spark.sql("""select * from Winner_List """).count() 
spark.sql("""create table campaign_data.aa_dmc_2344_Q4_Batteries_EOL as 
select csn, fst_name, last_name, email_addr,trx_header_id, RAND() from Winner_list  
order by 6 DESC""").show(250000,False) 
spark.sql(""" select a.*,c.row_id from campaign_data.aa_dmc_2344_Q4_Batteries_EOL a 
inner join gms.s_contact c 
on c.csn=a.csn 
""").show(1000,False) 