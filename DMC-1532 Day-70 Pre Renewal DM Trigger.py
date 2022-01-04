Contents
#Renewal Orders (Order Item Details) 
 
s_asset = spark.table("gms.s_asset") 
s_asset.createOrReplaceTempView("s_asset") 
 
s_prod_int = spark.table("gms.s_prod_int") 
s_prod_int.createOrReplaceTempView("s_prod_int") 
 
s_contact = spark.table("gms.s_contact") 
s_contact.createOrReplaceTempView("s_contact") 
 
s_contact_fnx = spark.table("gms.s_contact_fnx") 
s_contact_fnx.createOrReplaceTempView("s_contact_fnx") 
 
s_contact_x = spark.table("gms.s_contact_x") 
s_contact_x.createOrReplaceTempView("s_contact_x") 
 
s_addr_per = spark.table("gms.s_addr_per") 
s_addr_per.createOrReplaceTempView("s_addr_per") 
Eligible = spark.sql("""   
 
select   distinct c.csn  Member_Number 
        , c.row_id as contact_id 
        , c.CUST_VALUE_CD as Membership_type 
        , c.x_nrma_title as Title 
        , c.fst_name as First_Name 
        , c.MID_NAME as Middle_Name 
        , c.last_name as Last_Name 
        , addr.addr as Address_Line_1 
        , addr.addr_line_2 as Address_Line_2 
        , addr.addr_line_3 as Address_Line_3 
        , addr.addr_line_4 as Address_Line_4 
        , addr.city as Suburb 
        , addr.state as State 
        , addr.zipcode as Postcode 
        , 'Day -70 Pre renewal DM trial' as Campaign_ID 
        , now() as Campaign_Code 
        , '' as Offer_Code 
        , 'DM' as Treatment_Code 
        , '' as Response_Method 
        , c.CON_CD as contact_type 
        , c.email_addr as email_address 
        , a.end_dt as asset_endDT 
        , replace(p.name,'|',',') Product 
        , p.sub_type_cd as Product_type 
         
from s_contact c 
 
inner join s_contact_fnx as fn 
on fn.par_row_id = c.row_id 
         
inner join gms.s_contact_x as cx 
on cx.par_row_id = c.row_id 
 
inner join s_addr_per addr 
on addr.row_id = c.pr_per_addr_id 
     
inner join s_asset a 
on c.row_id = a.owner_con_id 
 
inner join s_prod_int p 
on a.prod_id = p.row_id 
 
 
where c.con_cd in ('Ordinary Member','Affiliate Member') 
    --and p.name in ('Classic Care','Premium Care','Premium Plus','Traveller Care','Free2go','Blue') 
    and c.cust_stat_cd = 'Active' 
    and p.prod_cd = 'Product' 
    and NVL(fn.deceased_flg,'N') = 'N' 
    and NVL(c.x_nrma_title, 'No Title') <> 'Estate Of The Late' 
    and NVL(cx.attrib_44,'No Discount') not RLIKE 'Honorary' 
    and datediff(a.end_dt,now()) between 69 and 82 
    and a.STATUS_CD ='Active' 
    and c.cust_value_cd not in ('Gold Life') 
    and NVL(cx.ATTRIB_35,'Y') != 'No' --post permission 
    and (year(current_date) - year(c.birth_dt))>18 
    and p.sub_type_cd in ('Non-RSA','RSA') 
 
""") 
Eligible.createOrReplaceTempView("Eligible") 
spark.sql('select * from Eligible').count() 
spark.sql('select * from Eligible').show(100) 
# Creating table for all CMO  
CMO_Members = spark.sql (""" select p.name as product,c.row_id as contact_id,c.csn as member_number,c.con_cd as member_type from gms.s_contact as c 
     
    inner join s_asset as a 
        on a.owner_con_id = c.row_id 
         
    inner join s_prod_int as p 
        on a.prod_id = p.row_id 
 
where 
    (p.name like 'Autoclub%') 
    and a.status_cd = 'Active' 
""") 
 
CMO_Members.createOrReplaceTempView("CMO_Members") 
 
spark.sql (""" select * from CMO_Members """).count() 
# selecting contact-able members from contact table 
Exclusion_1 = spark.sql (""" 
 
select c.csn, c.row_id from s_contact c 
 
inner join s_contact_x x 
on x.par_row_id = c.row_id 
 
inner join s_asset a 
on a.owner_con_id = c.row_id 
 
where c.cust_stat_cd = 'Active' 
and case when trim(c.x_inv_email_1) = 'Y' or c.email_addr is null then 'N' else 'Y' end = 'Y' -- all email able customer 
and NVL(x.attrib_36,'Yes')!='No' --with mkt permission 
 
or a.X_RENEWAL_STATUS ='In Renewal' 
 
""") 
 
Exclusion_1.createOrReplaceTempView("Exclusion_1") 
 
spark.sql (""" select * from Exclusion_1 """).count() 
Final_Members=spark.sql(""" 
select e.* from eligible e 
 
left outer join CMO_Members as c 
on e.contact_id = c.contact_id 
 
left outer join Exclusion_1 as ex 
on ex.row_id = e.contact_id 
 
LEFT JOIN campaign_data.aa_20190417_c_dmc1532_prerenewald_70_1 b 
on e.member_number = b.member_number 
  
LEFT JOIN campaign_data.aa_20190506_c_dmc1532_prerenewald_70_2 two  
on e.member_number = two.member_number 
 
where c.contact_id is null 
and ex.row_id is null 
and b.member_number is null 
and two.member_number is null 
""") 
Final_Members.createOrReplaceTempView("Final_Members") 
spark.sql('select * from Final_Members').count() 
 
 
control = Final_Members.sample(False, 0.10, seed=0) 
control.createOrReplaceTempView("control") 
spark.sql('select * from control').count() 
target = spark.sql(""" 
select a.* from Final_Members a 
left join control b 
on a.contact_id=b.contact_id 
where b.contact_id is NULL""") 
target.createOrReplaceTempView("target") 
spark.sql('select * from target').count() 
spark.sql(""" create table campaign_data.aa_20190520_c_dmc1532_control_3 as 
select *,'control' as segment from control 
union  
select *, 'target' as segment from target 
""").count() 
create table campaign_data.aa_20190520_c_dmc1532_prerenewald_70_3 as 
select distinct Member_Number 
        , Membership_type 
        , Title 
        , First_Name 
        , Middle_Name 
        , Last_Name 
        , Address_Line_1 
        , Address_Line_2 
        , Address_Line_3 
        , Address_Line_4 
        , Suburb 
        , State 
        , Postcode 
        , Campaign_ID 
        , Campaign_Code 
        , Offer_Code 
        , Treatment_Code 
        , Response_Method 
         
from campaign_data.aa_20190520_c_dmc1532_control_3 
where segment='target' 
 
select * from campaign_data.aa_20190520_c_dmc1532_prerenewald_70_3