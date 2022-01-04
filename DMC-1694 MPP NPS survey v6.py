# get all pay by term 
lc_mpp_contacts = spark.sql(""" 
 
select sch.contact_id 
    ,to_Date(MIN(sch.scheduled_dt)) as mpp_join_date 
from  gms.cx_pbt_sch sch 
-- gets all pay by term 
where sch.contact_id in (select distinct contact_id from gms.s_order where x_pay_by_term = 'Y') 
and sch.scheduled_dt > '2017-11-01' 
group by 1 
""") 
 
lc_mpp_contacts.createOrReplaceTempView("lc_mpp_contacts") 
lc_mpp_contacts.show(2,False) 
spark.sql("""select count(distinct contact_id) from lc_mpp_contacts""").show() 
excl_3monthsfree = spark.sql(""" 
select sch.contact_id 
    ,sum(sch.pay_amt)  
from  gms.cx_pbt_sch sch 
where sch.scheduled_dt >= add_months(current_timestamp(),-12) --last 12months the total payment amount =0 and the pay status is not scheduled 
and sch.pay_status not in ('Scheduled') 
group by sch.contact_id  
having sum(sch.pay_amt) = 0 
""") 
 
excl_3monthsfree.show(5,False) 
excl_3monthsfree.createOrReplaceTempView("excl_3monthsfree") 
spark.sql("""select count(distinct contact_id) from excl_3monthsfree""").show()  
excl_jor = spark.sql(""" 
select  distinct a.owner_con_id 
from gms.s_asset a 
inner join gms.s_prod_int pi 
  on a.prod_id = pi.row_id 
  and pi.prod_cd ='Promotion' 
  and pi.type = 'Membership' 
  and pi.sub_type_cd IN ('RSA', 'Non-RSA') 
  and a.type_cd = 'Membership' 
  and pi.name like ('%Go%') 
  and a.status_cd in ('Active') 
""") 
excl_jor.show(12,False) 
excl_jor.createOrReplaceTempView("excl_jor") 
spark.sql("""select count(distinct owner_con_id) from excl_jor""").show()  
Main_mpp_table=spark.sql (""" 
select  distinct 
         c.csn as Member_Number 
        ,c.fst_name AS First_Name 
        ,c.last_name AS Last_Name 
        ,c.email_addr AS Email_Address  
        ,cx.attrib_55  AS Colour_Plus 
        ,cx.attrib_17 MembershipTenure 
        ,(year(current_date) - year(c.birth_dt)) AS Age 
        ,ad.city as Suburb 
        ,ad.zipcode as Postcode 
        ,mpp.mpp_join_date 
        ,mpp.contact_id 
 
from gms.s_contact as c 
 
inner join gms.s_contact_x as cx  
on c.row_id = cx.par_row_id 
 
inner join gms.s_org_ext as o  
on c.row_id = o.pr_con_id 
 
 
inner join gms.s_addr_per as ad 
on c.pr_per_addr_id = ad.row_id 
 
inner join lc_mpp_contacts mpp 
on c.row_id = mpp.contact_id 
 
inner join gms.s_contact_fnx as fn  
on fn.par_row_id = c.row_id  
 
 
left anti join excl_3monthsfree 
on c.row_id = excl_3monthsfree.contact_id 
 
-- excludes JOR 
left anti join excl_jor 
on c.row_id = excl_jor.owner_con_id 
 
where c.cust_stat_cd = 'Active' 
        and c.con_cd in ('Ordinary Member' , 'Affiliate Member') 
        and NVL(c.x_nrma_title,'no title') != 'Estate Of The Late' 
        and case when trim(c.x_inv_email_1) = 'Y' or c.email_addr is null then 'N' else 'Y' end = 'Y' 
        and NVL(fn.deceased_flg,'N') = 'N' 
""") 
Main_mpp_table.createOrReplaceTempView("Main_mpp_table") 
spark.sql('select count(distinct member_number) from Main_mpp_table').show() 
# need only one table 
# get all pay by term dishonours with insufficient funds 
# dishonour_tranx > 0 with insufficient funds are cancellations 
lc_mpp_insufficientfunds = spark.sql(""" 
 
select distinct con.*, 'Insufficient funds' as Dishonoured_Reason, "0" as control 
from Main_mpp_table con 
 
inner join gms.cx_pbt_sch sch 
on con.contact_id = sch.contact_id 
  
inner join gms.s_src_payment pmt 
on sch.par_pay_id = pmt.row_id 
 
where pmt.x_nrma_reversal_reason in ('Insufficient Funds', 'Insufficient Fund')  
 
""") 
 
lc_mpp_insufficientfunds.createOrReplaceTempView("lc_mpp_insufficientfunds") 
lc_mpp_insufficientfunds.show(2,False) 
spark.sql("""select count(distinct contact_id) from lc_mpp_insufficientfunds""").show()  
 
 
lc_mpp_otherdishonourreason = spark.sql(""" 
 
select distinct con.*, 'Other reasons' as Dishonoured_Reason, "0" as control 
from Main_mpp_table con 
 
inner join gms.cx_pbt_sch sch 
 on con.contact_id = sch.contact_id 
  
inner join gms.s_src_payment pmt 
 on sch.par_pay_id = pmt.row_id 
 
where 
    sch.pay_status in ('Dishonour')  
and con.contact_id not in (select nf.contact_id from lc_mpp_insufficientfunds nf) 
 
 
""") 
 
lc_mpp_otherdishonourreason.createOrReplaceTempView("lc_mpp_otherdishonourreason") 
lc_mpp_otherdishonourreason.show(2,False) 
spark.sql("""select count(distinct contact_id) from lc_mpp_otherdishonourreason""").show() 
 
 
# get MPP members who have no missed payments 
lc_mpp_nomissedpayments = spark.sql(""" 
 
select  
    distinct con.*, 'No_misses_payment' Dishonoured_Reason, "0" as control 
from Main_mpp_table con 
 
inner join gms.cx_pbt_sch sch 
on con.contact_id = sch.contact_id 
 
left join lc_mpp_insufficientfunds if 
on if.contact_id=con.contact_id 
 
left join lc_mpp_otherdishonourreason dh 
on dh.contact_id=con.contact_id 
 
 
where 
    sch.pay_status not in ('Cancelled','Dishonour') 
and if.contact_id is null 
and dh.contact_id is null 
 
""") 
 
 
 
lc_mpp_nomissedpayments.createOrReplaceTempView("lc_mpp_nomissedpayments") 
lc_mpp_nomissedpayments.show(12,False) 
spark.sql("""select count(distinct contact_id) from lc_mpp_nomissedpayments""").show() 
spark.sql(""" 
select * from lc_mpp_insufficientfunds a 
inner join lc_mpp_otherdishonourreason b 
on a.contact_id = b.contact_id 
 
inner join lc_mpp_nomissedpayments c 
on a.contact_id = c.contact_id 
""").show(1000) 
working_table=spark.sql (""" 
select  distinct mpp.contact_id 
        ,cc.member_number as member_id 
        ,cc.First_name 
        ,cc.Last_Name 
        ,cc.Contact_Email_Addr  
        ,cp.Colour_Plus 
        ,cp.Membership_Tenure 
        ,cp.Age 
        ,cp.Gender 
        ,r.Suburb 
        ,r.Post_Code 
        ,r.Region_name 
        ,mpp.mpp_join_date 
 
     
from lc_mpp_contacts mpp -- contains only MPP contacts with x_pay_by_term = 'Y' 
inner join campaign_data.lc_consumer_contacts_vw  cc 
on mpp.contact_id = cc.contact_id 
 
inner join campaign_data.lc_consumer_profile_vw  cp 
on cc.contact_id = cp.contact_id 
 
inner join campaign_data.lc_regions_vw r 
on cc.contact_id = r.contact_id 
 
-- excluldes 3 months free 
left anti join excl_3monthsfree 
on cc.contact_id = excl_3monthsfree.contact_id 
 
-- excludes JOR 
left anti join excl_jor 
on cc.contact_id = excl_jor.owner_con_id 
 
 
-- other exclusion 
where cc.contact_type in ('Ordinary Member', 'Affiliate Member') 
    and cc.valid_email = 'Y' 
    and cc.deceased_flag <> 'Y' 
""") 
working_table.createOrReplaceTempView("working_table") 
spark.sql('select count(*) from working_table').show() 
spark.sql('select * from working_table').show(5) 
spark.sql("""select 'dup check',contact_id, count(*) from working_table group by 1,2 having count(*) > 1""").show() 
 
# spark.sql("""select contact_id, member_id, membership_tenure, colour_plus from working_table1""").repartition(1).write.saveAsTable("sandpit.lc_mpp_insufficientfunds") 
spark.sql("""select * from sandpit.lc_mpp_insufficientfunds""").show(5,False) 
# spark.sql("""drop table sandpit.lc_mpp_insufficientfunds""").show() 
# spark.sql("""select * from working_table1""").repartition(1).write.saveAsTable("campaign_data.lc_mpp_insufficientfunds") 
# spark.sql("""select contact_id, member_id, membership_tenure, colour_plus from working_table2""").repartition(1).write.saveAsTable("sandpit.lc_mpp_otherdishonourreason") 
spark.sql("""select * from sandpit.lc_mpp_otherdishonourreason""").show(5,False) 
# spark.sql("""drop table sandpit.lc_mpp_otherdishonourreason""").show() 
#spark.sql("""select * from working_table2""").repartition(1).write.saveAsTable("campaign_data.lc_mpp_otherdishonourreason") 
# spark.sql("""select contact_id, member_id, membership_tenure, colour_plus from working_table3""").repartition(1).write.saveAsTable("sandpit.lc_mpp_nomissedpayments") 
spark.sql("""select * from sandpit.lc_mpp_nomissedpayments""").show(5,False) 
# spark.sql("""drop table sandpit.lc_mpp_nomissedpayments""").show() 
# spark.sql("""select * from working_table3""").repartition(1).write.saveAsTable("campaign_data.lc_mpp_nomissedpayments") 
Segment1=Working_table1.sample(False,0.0289752121733278) 
Segment1.createOrReplaceTempView("Segment1") 
spark.sql('select * from Segment1').count() 
spark.sql("""select * from Segment1""").show(10,False) 
spark.sql("""select contact_id, count()*) from Segment1 group by 1 having count(*) = 0 
spark.sql("""select * from Segment1""").repartition(1).write.saveAsTable("campaign_data.lc_dmc1694_mppnpsseg1insufficientfunds_research_edm_20190826_adhoc") 
Segment2=working_table2.sample(False,0.0539036689271431) 
Segment2.createOrReplaceTempView("Segment2") 
 
spark.sql(""" 
select * from Segment2""").count() 
spark.sql("""select * from Segment2""").show(10,False) 
spark.sql("""select contact_id, count()*) from Segment2 group by 1 having count(*) = 0 
spark.sql('select * from Segment2').repartition(1).write.saveAsTable("campaign_data.lc_dmc1694_mppnpsseg2otherreasons_research_edm_20190826_adhoc") 
Segment3=Working_table3.sample(False,0.0712919492262215) 
Segment3.createOrReplaceTempView("Segment3") 
spark.sql('select * from Segment3').count() 
spark.sql("""select * from Segment3""").show(10,False) 
spark.sql("""select contact_id, count()*) from Segment3 group by 1 having count(*) = 0 
spark.sql('select * from Segment3').repartition(1).write.saveAsTable("campaign_data.lc_dmc1694_mppnpsseg3nomissedpayments_research_edm_20190826_adhoc")