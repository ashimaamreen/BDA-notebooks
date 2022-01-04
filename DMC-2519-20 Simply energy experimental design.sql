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
spark.sql(''' 
        SELECT distinct 
            customer_id,  
            MAX(B.purpose = 'P') AS promo  
        FROM  
            omc.send_level_summary AS A  
              
        LEFT JOIN  
            omc.campaign_info AS B  
            ON B.campaign_id = A.campaign_id  
        WHERE  
            B.channel = 'Push'  
            AND DATEDIFF(NOW(), A.send_event_date) <= 90  
              
        GROUP BY  
            1  
''').createOrReplaceTempView('push') 
select  
distinct o.row_id as account_id, member_number as member_number 
 
from m4m.return_feed_header m 
 
inner join gms.s_contact con 
on con.csn = m.member_number 
 
inner join gms.s_org_ext o 
on o.row_id = con.pr_dept_ou_id 
 
where 1=1 
and partner like 'Simply Energy' 
and datediff(current_timestamp(),time_stamp) between 0 and 365 
 
# Member who had an redemption within last 12 months 
 
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
and datediff(current_timestamp(),time_stamp) between 0 and 365 
 
''').createOrReplaceTempView('ams') 
 
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
    END AS email_valid, 
    case when push.customer_id is null then 'N' else 'Y' end AS push_consent 
 
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
 
LEFT JOIN 
    push 
    on con.row_id = push.customer_id 
 
LEFT ANTI JOIN                                  --excluding members with redemption in the L12 
    ams  
    ON ams.account_id = con.pr_dept_ou_id 
 
LEFT ANTI JOIN                                  --new simply member list wash 
    wash 
    on wash.csn=con.csn 
 
LEFT anti join                                  --excluding old simply list from Eileens table 
    campaign_data.ek_datawash_20210519 as ex 
    on con.pr_dept_ou_id = ex.account_id 
 
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
spark.sql('''select count(distinct member_id) from eligible 
 
''').show(10000,False) 
# spark.sql(''' 
#  select * from campaign_data.simplyenergy_NRMA_billables_NSW_180521 
# --select count(*) from campaign_data.simplyenergy_NRMA_billables_NSW_180521 
 
# ''').show(50000,False) 
# before excludions from simply file --1811615 
# after exclusions      --1735415 
print eligible.select("member_id").distinct().count() 
print eligible.count() 
# accuracy = spark.sql(''' 
 
 
# select rewards_card_planid is not null as match, member_number is not null as match, count(distinct member_number), count(distinct rewards_card_planid) 
# from ams  
# full outer join simplyenergy.simplyenergy_nrma_customer_data_09042021 se 
# on ams.member_number = se.rewards_card_planid 
# group by 1,2 
 
# ''') 
# accuracy.createOrReplaceTempView("accuracy") 
 
# spark.sql('''select * from accuracy''').show(100,False) 
# import pyspark.sql.functions as f 
# se = spark.sql(''' 
# SELECT 
#   distinct first_name, last_name, email, mobile_number, rewards_card_plan_id 
# FROM 
#     campaign_data.simplyenergy_NRMA_billables_NSW_180521 
# ''')# 
# split_col = f.split(se['first_name'], ' ') 
# se = se.withColumn('first_name1',split_col.getItem(0)) 
# # se = se.withColumn('first_name2',split_col.getItem(1)) 
 
# split_col2 = f.split(se['first_name'], '-') 
# se = se.withColumn('first_name2',split_col2.getItem(0)) 
# se.createOrReplaceTempView('exploded') 
# from pyspark.sql.functions import concat_ws,col 
# se=se.select(concat_ws(' ',se.first_name, se.last_name).alias("FullName"),"first_name", "last_name", "email", "mobile_number", "rewards_card_plan_id","first_name1","first_name2") 
# se.createOrReplaceTempView('exploded') 
# exclusion_account = spark.sql(''' 
# select distinct account_id, 0 as id from eligible e 
# inner join exploded se 
# on regexp_replace(e.cell_ph_num, "[^0-9]+", "") = regexp_replace(regexp_replace(regexp_replace(regexp_replace(se.mobile_number, "[^0-9]+", ""), "^61", "0") , "^00", "0"), '^([1-9])', '0$1')  
 
# union all 
# select distinct account_id, 1 as id from eligible e 
# inner join exploded se 
# on regexp_replace(e.ASST_PH_NUM, "[^0-9]+", "") = regexp_replace(regexp_replace(regexp_replace(regexp_replace(se.mobile_number, "[^0-9]+", ""), "^61", "0") , "^00", "0"), '^([1-9])', '0$1')  
 
# union all 
# select distinct account_id, 2 as id from eligible e 
# inner join exploded se 
# on regexp_replace(e.HOME_PH_NUM, "[^0-9]+", "") = regexp_replace(regexp_replace(regexp_replace(regexp_replace(se.mobile_number, "[^0-9]+", ""), "^61", "0") , "^00", "0"), '^([1-9])', '0$1')  
 
# union all 
# select distinct account_id, 3 as id from eligible e 
# inner join exploded se 
# on regexp_replace(e.WORK_PH_NUM, "[^0-9]+", "") = regexp_replace(regexp_replace(regexp_replace(regexp_replace(se.mobile_number, "[^0-9]+", ""), "^61", "0") , "^00", "0"), '^([1-9])', '0$1')  
 
# union all 
# select distinct account_id, 4 as id from eligible e 
# inner join exploded se2 
# on lower(trim(e.member_id)) = lower(trim(se2.rewards_card_plan_id)) 
 
# union all 
# select distinct account_id, 5 as id from eligible e 
# inner join exploded se5 
# on lower(trim(concat(e.first_name,' ',e.last_name))) = lower(trim(se5.FullName)) 
 
# union all 
# select distinct account_id, 6 as id from eligible e 
# inner join exploded se6 
# on lower(trim(e.email_address)) = lower(trim(se6.email)) 
 
# union all 
# select distinct account_id, 7 as id from eligible e 
# inner join exploded se3 
# on lower(trim(e.first_name)) = lower(trim(regexp_replace(se3.first_name1, "[^a-zA-Z]", "") )) 
# and lower(trim(e.last_name)) = lower(trim(regexp_replace(se3.last_name, "[^a-zA-Z]", "") )) 
 
# union all 
# select distinct account_id, 8 as id from eligible e 
# inner join exploded se4 
# on lower(trim(e.first_name)) = lower(trim(regexp_replace(se4.first_name2, "[^a-zA-Z]", "") ))   
# and lower(trim(e.last_name)) = lower(trim(regexp_replace(se4.last_name, "[^a-zA-Z]", "") )) 
 
# ''') 
 
# exclusion_account.createOrReplaceTempView("exclusion_account") 
spark.sql(''' create table campaign_data.ek_datawash_20210519 as select * from exclusion_account ''') 
# exclusion_contact = spark.sql(''' 
# select distinct member_id from eligible e 
# inner join exploded se3 
# on lower(trim(e.first_name)) = lower(trim(regexp_replace(se3.first_name1, "[^a-zA-Z]", "") )) 
# and lower(trim(e.last_name)) = lower(trim(regexp_replace(se3.last_name, "[^a-zA-Z]", "") )) 
 
# union  
# select distinct member_id from eligible e 
# inner join exploded se4 
# on lower(trim(e.first_name)) = lower(trim(regexp_replace(se4.first_name2, "[^a-zA-Z]", "") ))   
# and lower(trim(e.last_name)) = lower(trim(regexp_replace(se4.last_name, "[^a-zA-Z]", "") )) 
# ''') 
# exclusion_contact.createOrReplaceTempView("exclusion_contact") 
# eligible_2 = spark.sql(''' 
# select * from eligible e 
# anti join campaign_data.ek_datawash_20210519 as ex 
# on e.account_id = ex.account_id 
 
# -- anti join exclusion_contact ex2 
# -- on e.member_id = ex2.member_id 
 
# ''') 
# eligible_2.createOrReplaceTempView("eligible_2") 
# spark.sql(''' 
# select count(distinct account_id) from exclusion_account 
# ''').show(10,False) 
 
# spark.sql(''' 
# select count(distinct member_id) from exclusion_contact 
# ''').show(10,False) 
 
#Before name exclusion 1,848,559 
#After ams wash 1,830,454 
# print eligible_2.select("member_id").distinct().count() 
# print eligible_2.count() 
 
# After SE wash 1750604, exluded 97955 
 
 
# All rules: 
# 
#Either full name, first name & last name match, email, phone or member number match 
 
 
spark.sql(''' 
select email_valid, edm_consent, push_consent, count(distinct member_id) from eligible group by 1,2,3 
''').show(100,False) 
# Members with both consent 
spark.sql(''' 
create table campaign_data.simply_test_20211006 as 
with pool as  
(select *,rand() rnk from eligible where email_valid = 'Y' and edm_consent = 'Y' and push_consent = 'Y') 
select member_id,account_id,con_id,first_name,last_name,email_address,cell_ph_num,age,gender,addr_id,colour_plus,geotribe_segment,membership_tenure,edm_consent,email_valid,push_consent 
        , 
        (case     when rnk between 0 and 0.1      then 'prof1_control' 
                 when rnk between 0.1 and 0.4    then 'prof1_target_both' 
                 when rnk between 0.4 and 0.7    then 'prof1_target_edm' 
                 else 'prof1_target_push' end) segment 
from pool 
 
''') 
# Members with edm consent only 
spark.sql(''' 
create table campaign_data.simply_test2_20211006 as 
with pool as  
(select *,rand() rnk from eligible where email_valid = 'Y' and edm_consent = 'Y' and push_consent = 'N') 
select member_id,account_id,con_id,first_name,last_name,email_address,cell_ph_num,age,gender,addr_id,colour_plus,geotribe_segment,membership_tenure,edm_consent,email_valid,push_consent 
        , 
        (case     when rnk between 0 and 0.1      then 'prof2_control' 
                 else 'prof2_target_edm' end) segment 
from pool 
 
''') 
# Members with push consent only 
spark.sql(''' 
create table campaign_data.simply_test3_20211006 as 
with pool as  
(select *,rand() rnk from eligible where  (email_valid = 'N' or edm_consent = 'N') and push_consent = 'Y') 
select member_id,account_id,con_id,first_name,last_name,email_address,cell_ph_num,age,gender,addr_id,colour_plus,geotribe_segment,membership_tenure,edm_consent,email_valid,push_consent 
        , 
        (case     when rnk between 0 and 0.1      then 'prof3_control' 
                 else 'prof3_target_push' end) segment 
from pool 
''') 
select * from campaign_data.ek_dmc2327_se_20210416_prof1_control2 
drop table campaign_data.ek_dmc2327_se_campaign_edmpush_20210416_2 purge 
create table campaign_data.aa_dmc2520_se_campaign_edmpush_20211006  as 
select member_id,account_id,con_id,first_name,last_name,email_address,cell_ph_num,age,gender,addr_id,colour_plus,geotribe_segment,membership_tenure,edm_consent,email_valid,push_consent,segment from  
campaign_data.simply_test_20211006 union 
select member_id,account_id,con_id,first_name,last_name,email_address,cell_ph_num,age,gender,addr_id,colour_plus,geotribe_segment,membership_tenure,edm_consent,email_valid,push_consent,segment from campaign_data.simply_test2_20211006 union  
select member_id,account_id,con_id,first_name,last_name,email_address,cell_ph_num,age,gender,addr_id,colour_plus,geotribe_segment,membership_tenure,edm_consent,email_valid,push_consent,segment from campaign_data.simply_test3_20211006  
 
SELECT * from campaign_data.aa_dmc2520_se_campaign_edmpush_20211006 
-- select segment, count(distinct member_id) from campaign_data.ek_dmc2327_se_campaign_edmpush_20210416 group by 1 
-- distribution check 
-- select  segment,  
--         trunc(age/5), 
--         -- gender, 
--         -- colour_plus, 
--         -- geotribe_segment, 
--          -- trunc(membership_tenure/5), 
--         count(distinct member_id)  
-- from campaign_data.ek_dmc2327_se_campaign_edmpush_20210416 group by 1,2 
 
-- select count(distinct member_id) from campaign_data.ek_dmc2327_se_campaign_edmpush_20210417_2 
-- select count(distinct con_id) from campaign_data.ek_dmc2327_se_campaign_edmpush_20210417_2 
-- select count(distinct account_id) from campaign_data.ek_dmc2327_se_campaign_edmpush_20210417_2 
 
-- account 950,800 
-- con_id 992,245 
-- member_id 992,245 
 
-- To OMC 
 
-- Solus email list 801293 
 
select  
-- con_id 
--,  
-- segment, 
count(distinct con_id) 
, count(*)  
from  
( 
select distinct con_id, member_id 
        , segment 
        , case  when segment in ('prof1_target_edm','prof1_target_both','prof2_target_edm') then 'T' 
                when segment in ('prof1_control','prof2_control') then 'C' 
          else '' end target_control 
from campaign_data.ek_dmc2386_se_campaign_edmpush_20210519 
where segment in ('prof1_control','prof1_target_edm','prof1_target_both','prof2_target_edm','prof2_control') 
) b 
group by 1 
order by 2 desc 
 
 
----- 299508 
select  
-- con_id 
-- ,  
-- segment, 
-- count(distinct con_id) 
-- ,  
count(*)  
from  
( 
select distinct con_id, member_id 
        , segment 
        , case  when segment in ('prof1_target_push','prof1_target_both','prof3_target_push') then 'T' 
                when segment in ('prof1_control','prof3_control') then 'C' 
          else '' end target_control 
from campaign_data.ek_dmc2386_se_campaign_edmpush_20210519 
where segment in ('prof1_target_push','prof1_target_both','prof3_target_push','prof1_control','prof3_control') 
) b 
group by 1 
order by 2 desc 
-- GO203_SE_solus_eDM_v1.txt 
select distinct con_id, member_id, segment, case  when segment in ('prof1_target_edm','prof1_target_both','prof2_target_edm') then 'T'  when segment in ('prof1_control','prof2_control') then 'C'           else '' end target_control from campaign_data.aa_dmc2520_se_campaign_edmpush_20211006 where segment in ('prof1_control','prof1_target_edm','prof1_target_both','prof2_target_edm','prof2_control') 
 
-- GO203_SE_push_v1.txt 
select target_control,count(*) from  
( 
select distinct con_id, member_id, segment, case  when segment in ('prof1_target_push','prof1_target_both','prof3_target_push') then 'T' when segment in ('prof1_control','prof3_control') then 'C'           else '' end target_control from campaign_data.aa_dmc2520_se_campaign_edmpush_20211006 where segment in ('prof1_target_push','prof1_target_both','prof3_target_push','prof1_control','prof3_control') 
) a 
group by 1 
spark.sql(''' 
SELECT * from sandpit.renewal_base_old 
''').show(100,False) 
select target_control,count(*) from  
( 
select distinct con_id 
                , member_id 
                , segment 
                , case  when segment in ('prof1_target_edm','prof1_target_both','prof2_target_edm') then 'T'   
                        when segment in ('prof1_control','prof2_control') then 'C'            
                        else '' end target_control  
from campaign_data.aa_dmc2520_se_campaign_edmpush_20211006  
where segment in ('prof1_control','prof1_target_edm','prof1_target_both','prof2_target_edm','prof2_control') 
) a 
group by 1 
 
select distinct con_id 
            , member_id 
            , segment 
            , case  when segment in ('prof1_target_push','prof1_target_both','prof3_target_push') then 'T'  
                    when segment in ('prof1_control','prof3_control') then 'C'            
                    else '' end target_control  
from campaign_data.aa_dmc2520_se_campaign_edmpush_20211006  
where segment in ('prof1_target_push','prof1_target_both','prof3_target_push','prof1_control','prof3_control') 