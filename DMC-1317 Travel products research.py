
# load s_contact, s_contact_x, s_contact_fnx, s_contact_addr 
 
# s_contact and s_contact_x contains most of the details required for the task 
s_contact = spark.table("gms.s_contact") 
s_contact.createOrReplaceTempView("s_contact") 
print s_contact.count() 
 
s_contact_x = spark.table("gms.s_contact_x") 
s_contact_x.createOrReplaceTempView("s_contact_x") 
print s_contact_x.count() 
 
# s_contact_fnx contains deceased flag 
s_contact_fnx = spark.table("gms.s_contact_fnx") 
s_contact_fnx.createOrReplaceTempView("s_contact_fnx") 
print s_contact_fnx.count() 
 
# s_addr_per contains zipcode, country 
s_addr_per = spark.table("gms.s_addr_per") 
s_addr_per.createOrReplaceTempView("s_addr_per") 
print s_addr_per.count() 
 
# This is to load m4m table 
 
m4m_rfh = spark.table("m4m.return_feed_header") 
m4m_rfh.createOrReplaceTempView("m4m_rfh") 
# This is to get members who have use travel before 
 
spark.sql(""" 
 
 
    select distinct rfh.member_number 
    from m4m_rfh rfh 
    where 
        rfh.partner = "NRMA Travel" 
    limit 10 
     
""").show() 
# This is to extract the list of members from gms 
# criteria:  
#   1. include members who have valid email address 
#   2. include members who are not deceased 
#   3. include members who fall into ordinary and affiliate buckets 
 
gms_ext_1 = spark.sql(""" 
 
 
    select  
        s.csn as memberid, s.fst_name, s.last_name, s.email_addr, 
        2018 - cast (split(split(s.birth_dt," ")[0],"\\-")[0] as int) as age,  
        s.sex_mf as gender, per.zipcode, per.country, s.con_cd as member_status,  
        x.attrib_17 as tenure, x.attrib_55 as colour_segment   
    from s_contact s 
     
    inner join s_contact_fnx fnx 
    on s.row_id = fnx.par_row_id 
     
    inner join s_addr_per per 
    on s.pr_per_addr_id = per.row_id 
     
    inner join s_contact_x x 
    on s.row_id = x.par_row_id 
     
    where  
        -- only include members who have a valid email 
        ( 
            s.x_inv_email_1 = "N"  
            or s.x_inv_email_1 is null 
        )  
        and s.email_addr is not null 
         
        -- members who are not deceased and who are ordinary or affiliate members 
        and  
        ( 
            NVL(fnx.deceased_flg, "N") = "N"  
            or NVL(s.x_nrma_title,'no title') != 'Estate Of The Late' 
        ) 
        and s.con_cd in ("Ordinary Member", "Affiliate Member") 
 
 
""") 
 
gms_ext_1.show(10, False) 
gms_ext_1.createOrReplaceTempView("gms_ext_1") 
# This is to do the sampling on gms_ext_1 which get from previous section 
# criteria:  
#   1. a random sampling on colour segment 
#   2. a list with 20k members 
 
sample_len = 20000.0 
gms_len = gms_ext_1.count() 
print gms_len 
sample_1 = gms_ext_1.sample(False, sample_len / gms_len, seed=1000) 
sample_1.createOrReplaceTempView("sample_1") 
sample_1.show(10, False) 
# To test on the distribution on colour segment against the total base 
 
from pyspark.sql.functions import * 
 
sample_1.groupBy("colour_segment").count().show() 
gms_ext_1.groupBy("colour_segment").count().show() 
sample_1.groupBy("member_status").count().show() 
gms_ext_1.groupBy("member_status").count().show() 
# sample_2 is to add a calculated field with region breakdown 
 
sample_2 = spark.sql(""" 
 
    select *,  
        case  
            when  
             (ad.zipcode >= '2000' and ad.zipcode <= '2082' or  
              ad.zipcode >= '2000' and ad.zipcode <= '2082' or  
              ad.zipcode >= '2084' and ad.zipcode <= '2234' or  
              ad.zipcode >= '2555' and ad.zipcode <= '2574' or  
              ad.zipcode >= '2745' and ad.zipcode <= '2770' or  
              ad.zipcode >= '2775' and ad.zipcode <= '2775') 
              and (upper(ad.country) = 'AUSTRALIA' or upper(ad.country) = 'AU') 
              then 'METROPOLITAN' 
            when  
             (ad.zipcode >= '2083' and ad.zipcode <= '2083' or  
              ad.zipcode >= '2250' and ad.zipcode <= '2338' or  
              ad.zipcode >= '2415' and ad.zipcode <= '2423' or  
              ad.zipcode >= '2425' and ad.zipcode <= '2425' or  
              ad.zipcode >= '2428' and ad.zipcode <= '2428' or  
              ad.zipcode >= '2500' and ad.zipcode <= '2535' or  
              ad.zipcode >= '2538' and ad.zipcode <= '2541' or  
              ad.zipcode >= '2575' and ad.zipcode <= '2578' or  
              ad.zipcode >= '2600' and ad.zipcode <= '2617' or  
              ad.zipcode >= '2773' and ad.zipcode <= '2774' or  
              ad.zipcode >= '2776' and ad.zipcode <= '2786' or  
              ad.zipcode >= '2900' and ad.zipcode <= '2914') 
              and (upper(ad.country) = 'AUSTRALIA' or upper(ad.country) = 'AU')  
              then  'REGIONAL' 
            when  
              (ad.zipcode >= '2339' and ad.zipcode <= '2411' or  
              ad.zipcode >= '2424' and ad.zipcode <= '2424' or  
              ad.zipcode >= '2426' and ad.zipcode <= '2427' or  
              ad.zipcode >= '2429' and ad.zipcode <= '2490' or  
              ad.zipcode >= '2536' and ad.zipcode <= '2537' or  
              ad.zipcode >= '2545' and ad.zipcode <= '2551' or  
              ad.zipcode >= '2579' and ad.zipcode <= '2594' or  
              ad.zipcode >= '2618' and ad.zipcode <= '2739' or  
              ad.zipcode >= '2787' and ad.zipcode <= '2898' or  
              ad.zipcode >= '6798' and ad.zipcode <= '6799') 
              and (upper(ad.country) = 'AUSTRALIA' or upper(ad.country) = 'AU')  
              then  'RURAL' 
            when  
              (ad.zipcode >= '0800' and ad.zipcode <= '0886' or  
              ad.zipcode >= '3000' and ad.zipcode <= '6770' or  
              ad.zipcode >= '6907' and ad.zipcode <= '7470' or  
              ad.zipcode >= '7471' ) 
              and (upper(ad.country) = 'AUSTRALIA' or upper(ad.country) = 'AU')  
              then  'INTERSTATE' 
            else 'UNKNOWN' 
            end as area 
 
    from sample_1 ad 
     
     
 
""") 
sample_2.createOrReplaceTempView("sample_2") 
sample_2.show(10, False) 
# sample_3 is to add Travel product or not 
 
sample_3 = spark.sql(""" 
     
     
    select *, if (member_number is null, "N", "Y") as purchase_travel 
    from  
    ( 
        select * 
        from sample_2 sample 
        left join ( 
            select distinct rfh.member_number 
            from m4m_rfh rfh 
            where 
                rfh.partner = "NRMA Travel" 
            ) m4m 
        on  
            sample.memberid = m4m.member_number 
    ) 
     
     
 
""") 
sample_3.createOrReplaceTempView("sample_3") 
sample_3.show(10, False) 
sample_4 = sample_3.select( 
    "fst_name", "last_name",  
    "email_addr", "age", "gender", "area", "zipcode", 
    "member_status", "tenure", "purchase_travel", "colour_segment" 
) 
sample_4.show(10, False) 
sample_4.createOrReplaceTempView("sample_4") 
# This is to create the table under campaign_data so that the IT team can use it  
 
spark.sql(""" 
 
 
    create table if not exists campaign_data.yc_20181109_c_dmc1317_travelresearch as  
        select * 
        from sample_4 
 
""") 
sample_4.groupBy("purchase_travel").count().show()