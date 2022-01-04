spark.read.load('/user/hive/warehouse/campaign_data.db/external_files/SE/NRMA_billlable_data_May_June_2021.csv', format = 'csv', header = 'true').createOrReplaceTempView('se') 
con = spark.sql(''' 
SELECT 
    con.row_id AS con_id, 
    con.csn, 
    UPPER(con.fst_name) AS first_name, 
    UPPER(con.last_name) AS last_name, 
    UPPER(con.email_addr) AS email, 
    UPPER(TRIM(CONCAT_WS(' ', COALESCE(addr.addr, ''), COALESCE(addr.addr_line_2, '')))) AS address, 
    UPPER(TRIM(addr.city)) AS suburb, 
    addr.zipcode AS post_code, 
    addr.state AS region 
FROM 
    gms.s_contact AS con 
     
LEFT JOIN 
    gms.s_addr_per AS addr 
    ON addr.row_id = con.pr_per_addr_id 
     
WHERE 
    con.con_cd IN ('Ordinary Member', 'Affiliate Member') 
''') #.cache() 
con.createOrReplaceTempView('con') 
wash = spark.sql(''' 
SELECT 
    se.*, 
    COALESCE(c1.con_id, c2.con_id, c3.con_id) AS con_id, 
    COALESCE(c1.csn, c2.csn, c3.csn) AS csn, 
    TO_TIMESTAMP(contract_signed_date, 'dd-MMM-yy') AS dt 
FROM 
    se 
LEFT JOIN 
    con AS c1 
    ON se.rewards_card_plan_id = c1.csn 
     
LEFT JOIN 
    con AS c2 
    ON UPPER(se.email) = c2.email 
     
LEFT JOIN 
    con AS c3 
    ON UPPER(se.first_name) = c3.first_name 
    AND UPPER(se.last_name) = c3.last_name 
    AND UPPER(se.address) = c3.address 
    AND se.post_code = c3.post_code 
''').cache() 
wash.createOrReplaceTempView('wash') 
wash = spark.sql(''' 
SELECT 
    se.*, 
    COALESCE(c1.con_id, c2.con_id) AS con_id, 
    COALESCE(c1.csn, c2.csn) AS csn, 
    TO_TIMESTAMP(contract_signed_date, 'dd-MMM-yy') AS dt 
FROM 
    se 
LEFT JOIN 
    con AS c1 
    ON se.rewards_card_plan_id = c1.csn 
     
LEFT JOIN 
    con AS c2 
    ON UPPER(se.email) = c2.email 
     
 
''').cache() 
wash.createOrReplaceTempView('wash') 
spark.sql(''' select * from se ''').show(10) 
spark.sql(''' select distinct csn from wash ''').count() 
SELECT DISTINCT partner from m4m.return_feed_header 
where partner rlike 'imply' 
spark.sql(''' 
select  
distinct o.row_id as account_id, member_number as member_number 
 
from m4m.return_feed_header m 
 
inner join gms.s_contact con 
on con.csn = m.member_number 
 
inner join gms.s_org_ext o 
on o.row_id = con.pr_dept_ou_id 
 
where 1=1 
and partner like 'Simply Energy' 
and time_stamp> '2020-09-27' 
 
''').createOrReplaceTempView('ams') 
# 
#spark.sql('select * from ams''').show(10,False) 
 
eligible = spark.sql(""" 
SELECT distinct 
    owner_accnt_id AS account_id, 
    con.row_id AS con_id, 
    con.csn AS member_id, 
    con.fst_name AS first_name, 
    con.last_name AS last_name, 
    con.email_addr AS email_address, 
    case when  
     ((con.cell_ph_num is not null and nvl(con.veteran_flg,'N') <> 'Y')  
         and regexp_replace(con.cell_ph_num, "[^0-9]+", "") not like '0_0000000_'  
         and regexp_replace(con.cell_ph_num, "[^0-9]+", "") not like '041111111_')  
      then regexp_replace(con.cell_ph_num, "[^0-9]+", "")  
      else NULL end cell_ph_num, 
       
      case when  
     ((con.asst_ph_num is not null and nvl(con.ok_to_sample_flg,'N') <> 'Y')  
         and regexp_replace(con.asst_ph_num, "[^0-9]+", "") not like '0_0000000_'  
         and regexp_replace(con.asst_ph_num, "[^0-9]+", "") not like '041111111_')  
      then regexp_replace(con.asst_ph_num, "[^0-9]+", "")  
      else NULL end asst_ph_num,  
       
      case when  
      ((con.home_ph_num is not null  and nvl(con.hard_to_reach,'N') <> 'Y')  
         and regexp_replace(con.home_ph_num, "[^0-9]+", "") not like '0_0000000_'  
         and regexp_replace(con.home_ph_num, "[^0-9]+", "") not like '041111111_')  
      then regexp_replace(con.home_ph_num, "[^0-9]+", "")  
      else NULL end home_ph_num,  
       
         case when  
      ((con.work_ph_num is not null  and nvl(con.speaker_flg,'N') <> 'Y')  
         and regexp_replace(con.work_ph_num, "[^0-9]+", "") not like '0_0000000_'  
         and regexp_replace(con.work_ph_num, "[^0-9]+", "") not like '041111111_')  
      then regexp_replace(con.work_ph_num, "[^0-9]+", "")  
      else NULL end work_ph_num,  
     
    YEAR(NOW()) - YEAR(con.birth_dt) AS age, 
    con.sex_mf AS gender, 
    con.pr_per_addr_id AS addr_id, 
    conx.attrib_55 AS colour_plus, 
    conx.attrib_22 AS geotribe_segment, 
    conx.attrib_17 AS membership_tenure, 
    CASE 
        WHEN  
        COALESCE(conx.attrib_36, '') != 'No'  
        AND COALESCE(confnx.brloc_attrib13, '') != 'Y' THEN 'Y' 
        ELSE 'N' 
    END AS edm_consent, 
    case when  
        COALESCE(con.x_inv_email_1, 'N') = 'N' 
         AND con.email_addr IS NOT NULL then 'Y'  
        ELSE 'N' 
    END AS email_valid 
 
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
LEFT JOIN 
    gms.s_addr_per AS addr 
    ON addr.row_id = con.pr_per_addr_id 
LEFT ANTI JOIN 
    ams  
    ON ams.account_id = con.pr_dept_ou_id       --1815485   
LEFT ANTI JOIN 
    wash 
    on wash.csn=con.csn 
WHERE 
    asset.status_cd = 'Active' 
    AND prod.type = 'Membership' 
    AND prod.prod_cd = 'Promotion' 
    AND con.cust_stat_cd = 'Active' 
    AND con.csn IS NOT NULL 
    AND con.con_cd IN ('Ordinary Member', 'Affiliate Member') 
    AND COALESCE(confnx.deceased_flg,'N') = 'N' 
    AND COALESCE(con.x_nrma_title, '') != 'Estate Of The Late' 
    AND addr.state = 'NSW' 
    AND YEAR(NOW()) - YEAR(con.birth_dt) >= 18 
 
""") 
eligible.createOrReplaceTempView("eligible") 
#not checking for primary 
spark.sql('''select count(distinct member_id) from eligible 
 
''').show(10000,False) 
import pyspark.sql.functions as f 
se = spark.sql(''' 
SELECT 
  distinct first_name, last_name, email, mobile_number, rewards_card_plan_id 
FROM 
    campaign_data.simplyenergy_NRMA_billables_NSW_180521 
''')# 
split_col = f.split(se['first_name'], ' ') 
se = se.withColumn('first_name1',split_col.getItem(0)) 
# se = se.withColumn('first_name2',split_col.getItem(1)) 
 
split_col2 = f.split(se['first_name'], '-') 
se = se.withColumn('first_name2',split_col2.getItem(0)) 
se.createOrReplaceTempView('exploded') 
from pyspark.sql.functions import concat_ws,col 
se=se.select(concat_ws(' ',se.first_name, se.last_name).alias("FullName"),"first_name", "last_name", "email", "mobile_number", "rewards_card_plan_id","first_name1","first_name2") 
se.createOrReplaceTempView('exploded') 
exclusion_account = spark.sql(''' 
select distinct account_id, 0 as id from eligible e 
inner join exploded se 
on regexp_replace(e.cell_ph_num, "[^0-9]+", "") = regexp_replace(regexp_replace(regexp_replace(regexp_replace(se.mobile_number, "[^0-9]+", ""), "^61", "0") , "^00", "0"), '^([1-9])', '0$1')  
 
union all 
select distinct account_id, 1 as id from eligible e 
inner join exploded se 
on regexp_replace(e.ASST_PH_NUM, "[^0-9]+", "") = regexp_replace(regexp_replace(regexp_replace(regexp_replace(se.mobile_number, "[^0-9]+", ""), "^61", "0") , "^00", "0"), '^([1-9])', '0$1')  
 
union all 
select distinct account_id, 2 as id from eligible e 
inner join exploded se 
on regexp_replace(e.HOME_PH_NUM, "[^0-9]+", "") = regexp_replace(regexp_replace(regexp_replace(regexp_replace(se.mobile_number, "[^0-9]+", ""), "^61", "0") , "^00", "0"), '^([1-9])', '0$1')  
 
union all 
select distinct account_id, 3 as id from eligible e 
inner join exploded se 
on regexp_replace(e.WORK_PH_NUM, "[^0-9]+", "") = regexp_replace(regexp_replace(regexp_replace(regexp_replace(se.mobile_number, "[^0-9]+", ""), "^61", "0") , "^00", "0"), '^([1-9])', '0$1')  
 
union all 
select distinct account_id, 4 as id from eligible e 
inner join exploded se2 
on lower(trim(e.member_id)) = lower(trim(se2.rewards_card_plan_id)) 
 
union all 
select distinct account_id, 5 as id from eligible e 
inner join exploded se5 
on lower(trim(concat(e.first_name,' ',e.last_name))) = lower(trim(se5.FullName)) 
 
union all 
select distinct account_id, 6 as id from eligible e 
inner join exploded se6 
on lower(trim(e.email_address)) = lower(trim(se6.email)) 
 
union all 
select distinct account_id, 7 as id from eligible e 
inner join exploded se3 
on lower(trim(e.first_name)) = lower(trim(regexp_replace(se3.first_name1, "[^a-zA-Z]", "") )) 
and lower(trim(e.last_name)) = lower(trim(regexp_replace(se3.last_name, "[^a-zA-Z]", "") )) 
 
union all 
select distinct account_id, 8 as id from eligible e 
inner join exploded se4 
on lower(trim(e.first_name)) = lower(trim(regexp_replace(se4.first_name2, "[^a-zA-Z]", "") ))   
and lower(trim(e.last_name)) = lower(trim(regexp_replace(se4.last_name, "[^a-zA-Z]", "") )) 
 
''') 
 
exclusion_account.createOrReplaceTempView("exclusion_account") 
eligible_2 = spark.sql(''' 
select * from eligible e 
anti join exclusion_account as ex 
on e.account_id = ex.account_id 
 
-- anti join exclusion_contact ex2 
-- on e.member_id = ex2.member_id 
 
''') 
eligible_2.createOrReplaceTempView("eligible_2") 
spark.sql(''' 
select count(distinct account_id) from exclusion_account 
''').show(10,False) 
 
# spark.sql(''' 
# select count(distinct member_id) from exclusion_contact 
# ''').show(10,False) 
 
#Before name exclusion 1,848,559 
#After ams wash 1,830,454 
print eligible_2.select("member_id").distinct().count() 
print eligible_2.count() 
 
# After SE wash 1750604, exluded 97955 
 
 
# All rules: 
# 
#Either full name, first name & last name match, email, phone or member number match 
 
 
control=eligible_2.sample(False,0.092, seed=0) 
control.createOrReplaceTempView("control") 
print control.count() 
spark.sql(''' 
select * from eligible_2 
''').show(1000,False) 
DROP TABLE campaign_data.aa_dmc2521_simplyenergy_campaign_fb_20210927_adhoc 
spark.sql(''' 
create table campaign_data.aa_dmc2521_simplyenergy_campaign_fb_20210927_adhoc as  
select distinct  
    a.account_id, 
    a.con_id, 
    a.member_id, 
    a.first_name, 
    a.last_name, 
    a.email_address, 
    a.cell_ph_num, 
    a.asst_ph_num,  
    a.home_ph_num,  
    a.work_ph_num,  
    a.age, 
    a.gender, 
    a.addr_id, 
    a.colour_plus, 
    a.geotribe_segment, 
    a.membership_tenure, 
    a.edm_consent, 
    a.email_valid, 
    case when c.account_id is not null then 0 else 1 end Target 
 
from eligible_2 a 
 
left join control c 
on c.account_id=a.account_id 
''').show() 
#Target.createOrReplaceTempView("Target") 
--#1736347 
SELECT DISTINCT cell_ph_num as mobile, home_ph_num as home_phone, work_ph_num as work_phone, email_address  
from campaign_data.aa_dmc2521_simplyenergy_campaign_fb_20210927_adhoc 
where target=1 
SELECT count(*) from campaign_data.aa_dmc2521_simplyenergy_campaign_fb_20210927_adhoc 