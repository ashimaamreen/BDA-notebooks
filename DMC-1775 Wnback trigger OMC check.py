#MotorSmart Orders (Order Item Details) 
 
s_asset = spark.table("gms.s_asset") 
s_asset.createOrReplaceTempView("s_asset") 
 
s_order = spark.table("gms.s_order") 
s_order.createOrReplaceTempView("s_order") 
 
s_order_type = spark.table("gms.s_order_type") 
s_order_type.createOrReplaceTempView("s_order_type") 
 
s_order_item = spark.table("gms.s_order_item") 
s_order_item.createOrReplaceTempView("s_order_item") 
 
s_order_item_x = spark.table("gms.s_order_item_x") 
s_order_item_x.createOrReplaceTempView("s_order_item_x") 
 
s_prod_int = spark.table("gms.s_prod_int") 
s_prod_int.createOrReplaceTempView("s_prod_int") 
 
s_contact = spark.table("gms.s_contact") 
s_contact.createOrReplaceTempView("s_contact") 
 
s_org_ext = spark.table("gms.s_org_ext") 
s_org_ext.createOrReplaceTempView("s_org_ext") 
 
s_src_payment = spark.table("gms.s_src_payment") 
s_src_payment.createOrReplaceTempView("s_src_payment") 
blue_eligible = spark.sql(""" 
select distinct 
         c.csn as member_number 
        ,c.row_id as contact_id 
    --    ,o.row_id as account_id 
        ,c.cust_stat_cd as cust_stat 
        ,c.con_cd as contact_type 
	    ,c.x_nrma_household_flg as hh 
    --	,nvl(o.pr_con_id,'N') = 'Y' as primary 
    	,c.email_addr as email_address 
 
    from gms.s_contact as c  
     
	--inner join gms.s_org_ext as o  
	--on c.pr_dept_ou_id = o.row_id 
 
	inner join gms.s_asset as a  
	on a.owner_accnt_id = c.pr_dept_ou_id 
 
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
spark.sql('select distinct contact_id from blue_eligible').count() 
Winback_pool= spark.sql("""   
 
select    c.row_id ContactID 
        , c.csn MemberNumber 
        , c.cust_stat_cd as cust_stat 
        , c.CON_CD as contact_type 
        , c.X_NRMA_JOIN_DT as join_date 
 
 
from s_contact c 
 
inner join gms.s_contact_fnx as fn 
on fn.par_row_id = c.row_id 
         
inner join gms.s_contact_x as x 
on x.par_row_id = c.row_id 
 
left join blue_eligible b --exclude all blue eligible 
on b.email_address = c.email_addr 
 
 
where b.contact_id is NULL 
    and c.con_cd = 'Affiliate Member' --lapsed 
    and c.cust_stat_cd = 'Active'  --Active Customer 
    and NVL(fn.deceased_flg,'N') = 'N' --Non deceased 
    and NVL(c.x_nrma_title,'no title') != 'Estate Of The Late'  
    and case when trim(c.x_inv_email_1) = 'Y' or c.email_addr is null then 'N' else 'Y' end = 'Y' --valid email address 
    and nvl(x.attrib_36,'Yes') !='No' --email permission 
 
""") 
Winback_pool.createOrReplaceTempView("Winback_pool") 
spark.sql('select distinct contactid from Winback_pool').count() 
spark.sql(""" create table campaign_data.aa_20190401_c_dmc1502_control as 
select * from control""").show() 
spark.sql(""" create table campaign_data.aa_20190401_c_dmc1502_winback as 
select w.* from Main_table w 
left join control s 
on s.ContactID = w.ContactID 
where s.ContactID is NULL 
""").show() 
lapsed = spark.sql(""" 
Select 
         c.row_id as contact_id 
        ,c.cust_stat_cd as cust_stat 
        ,c.con_cd as con_type 
        ,c.csn as member_number 
        ,fn.deceased_flg as deceased 
       -- ,case when o.pr_con_id = c.row_id then 'Y' else 'N' end as primary 
        ,p.name as product 
        ,p.sub_type_cd as product_type 
        ,a.status_cd as asset_status 
        ,x.attrib_36 as Email_Mktg 
        ,case when c.email_addr is not null then 'Has email' end as email_addr 
        ,c.x_inv_email_1 
        ,a.end_dt as effective_to_date 
 
         
    from  
        gms.s_contact as c 
     
        left join blue_eligible as b 
        on c.row_id=b.contact_id 
     
 
	    inner join gms.s_asset as a  
	    on a.owner_con_id = c.row_id 
 
	    inner join gms.s_prod_int as p 
        on a.prod_id = p.row_id 
        --and p.name != 'Membership' 
        and p.prod_cd = 'Product' 
        and p.sub_type_cd in ('RSA','Non-RSA') 
         
        inner join gms.s_contact_fnx as fn 
        on fn.par_row_id = c.row_id 
         
        inner join gms.s_contact_x as x 
        on x.par_row_id = c.row_id 
 
 
     
    where (fn.deceased_flg = 'N' or fn.deceased_flg is null) 
        and c.con_cd in ('Affiliate Member')  
        and b.contact_id is null 
""") 
spark.sql("""select distinct contact_id from lapsed  
where Email_Mktg ='Yes' 
and (deceased = 'N' or deceased is null) 
and email_addr ='Has email'  
and (x_inv_email_1 = 'N' or x_inv_email_1 is null) 
""").count() 
spark.sql ("""select segment, count(distinct contactID) from control group by segment""").count() 
spark.sql(""" create table campaign_data.aa_20190401_c_dmc1502_control as 
select * from control""").show() 
spark.sql(""" create table campaign_data.aa_20190401_c_dmc1502_winback as 
select w.* from Main_table w 
left join control s 
on s.ContactID = w.ContactID 
where s.ContactID is NULL 
""").show() 
SELECT segment, contactID FROM campaign_data.aa_20190401_c_dmc1502_winback 
select contactid, membernumber, cust_stat, contact_type, product, product_type, redemptions, segment  
from campaign_data.aa_20190401_c_dmc1502_winback where segment=2 
SELECT contact_type FROM campaign_data.aa_20190401_c_dmc1502_control LIMIT 100; 
drop table campaign_data.aa_20190401_c_dmc1502_winback 