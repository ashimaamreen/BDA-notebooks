all you need to do is change the file source
Contents
s_contact = spark.table("gms.s_contact") 
s_contact.createOrReplaceTempView("s_contact") 
 
s_contact_x = spark.table("gms.s_contact_x") 
s_contact_x.createOrReplaceTempView("s_contact_x") 
GeoCoded_List = spark.read.load("/user/aamreen/Mem_distance/Mem_distance.csv",format="csv", sep=",", inferSchema="true", header="true") 
GeoCoded_List.createOrReplaceTempView("GeoCoded_List") 
spark.sql('select * from GeoCoded_List').count() 
spark.sql("""select * from GeoCoded_List""").show(10) 
blue_eligible = spark.sql(""" 
  
select distinct 
         c.csn as member_number 
        ,c.row_id as contact_id 
        ,o.row_id as account_id 
        ,c.cust_stat_cd as cust_stat 
        ,c.con_cd 
	    ,c.x_nrma_household_flg as hh 
    	,nvl(o.pr_con_id,'N') = 'Y' as primary  
 
    from gms.s_contact as c  
     
	inner join gms.s_org_ext as o  
	on c.pr_dept_ou_id = o.row_id 
 
	inner join gms.s_asset as a  
	on a.owner_accnt_id = o.par_row_id 
 
	inner join gms.s_prod_int as p 
       on a.prod_id = p.row_id 
        
    inner join gms.s_contact_fnx as fn  
       on fn.par_row_id = c.row_id  
     
    where a.status_cd = 'Active' 
    and p.type = 'Membership' 
    and p.prod_cd = 'Promotion' 
    and NVL(fn.deceased_flg,'N') = 'N' 
    and c.csn is not null 
""") 
blue_eligible.createOrReplaceTempView("blue_eligible") 
 
spark.sql ('select * from blue_eligible').count() 
#these fields can be different to the file so please check what fields are available in the file and may be you might need an extra join here or there 
 
Working_Table = spark.sql (""" 
select   g.integration_id as contact_id 
        ,id as not_member_id 
        ,x_con_type as member_type 
        ,address 
        ,g.suburb 
        ,g.postcode 
        ,branch as branch_code 
        ,Name as branch_name 
        ,FromBreak as min_distance 
        ,ToBreak as max_distance 
from GeoCoded_List as g 
 
inner join blue_eligible as b 
on g.integration_id=b.contact_id 
 
inner join gms.s_contact as gmsContact 
on g.integration_id=gmsContact.row_id 
 
inner join s_contact_x as cx  
on g.integration_id = cx.par_row_id 
 
where gmsContact.cust_stat_cd = 'Active' 
        and gmsContact.con_cd in ('Ordinary Member' , 'Affiliate Member') 
        and NVL(gmsContact.x_nrma_title,'no title') != 'Estate Of The Late' 
        and case when trim(gmsContact.x_inv_email_1) = 'Y' or gmsContact.email_addr is null then 'N' else 'Y' end = 'Y' 
        and nvl(cx.attrib_36,'Yes') !='No' 
""") 
 
Working_Table.createOrReplaceTempView("Working_Table") 
 
spark.sql (""" select * from working_table """).count() 
 
spark.sql (""" select * from working_table where postcode=2750""").show(10,False) 