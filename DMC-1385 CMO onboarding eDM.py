Contents
from pyspark.sql.functions import * 
 
# how adding affiliate and ordinary and without changing the datacount 
 
s_contact = spark.table("gms.s_contact") 
s_contact.createOrReplaceTempView("s_contact") 
 
s_org_ext = spark.table("gms.s_org_ext") 
s_org_ext.createOrReplaceTempView("s_org_ext") 
 
s_contact_fnx = spark.table("gms.s_contact_fnx") 
s_contact_fnx.createOrReplaceTempView("s_contact_fnx") 
 
s_contact_x = spark.table("gms.s_contact_x") 
s_contact_x.createOrReplaceTempView("s_contact_x") 
 
s_prod_int = spark.table("gms.s_prod_int") 
s_prod_int.createOrReplaceTempView("s_prod_int") 
 
s_asset = spark.table("gms.s_asset") 
s_asset.createOrReplaceTempView("s_asset") 
 
s_addr_per = spark.table("gms.s_addr_per") 
s_addr_per.createOrReplaceTempView("s_addr_per") 
 
 
outcome = spark.sql(""" 
     
    select  
        csn, row_id,  
        datediff(current_date(), start_dt) as date_diff, 
        start_dt, current_date() as today 
    from 
    ( 
     
        select c.csn, c.row_id, min(to_date(split(a.start_dt," ")[0])) as start_dt 
         
        from s_contact as c 
             
            inner join s_contact_fnx as f 
            on c.row_id = f.par_row_id 
         
            inner join s_contact_x as x 
            on c.row_id = x.par_row_id 
             
            inner join s_asset as a 
            on c.row_id = a.owner_con_id 
             
            inner join s_prod_int as p 
            on p.row_id = a.prod_id 
     
         
        where 
            (c.x_nrma_title <> "Estate Of The Late" or c.x_nrma_title is null) 
            and (f.deceased_flg = "N" or f.deceased_flg is null) -- Excluded deceased members 
            and (lower(x.attrib_36) in ("yes", "null") or x.attrib_36 is null) -- Email Consent Yes 
            and (c.x_inv_email_1 = "N" or c.x_inv_email_1 is null) -- Valid email 
            and c.email_addr is not null -- valid email 
            and c.cust_stat_cd = "Active"  -- Active record 
            and lower(p.name) rlike "autoclub"  -- only avaiable to autoclub 
            and not lower(p.name) rlike "bundle"  -- exclude go bundles 
             
            -- This is because if we select only the one with active asset, it will give me the renew date for autoclub 
            -- and a.status_cd = "Active"  -- Active Asset Record  Have to remove this filter.  
            -- and f.brloc_attrib13 is null or f.brloc_attrib13 = "N"  -- Global opt-out 
         
        group by c.csn, c.row_id 
    )  
     
    where  
         datediff(current_date(), start_dt) = 30 
 
""") 
 
print outcome.count() 
outcome.show(100,False) 
autoclub = spark.sql(""" 
     
     
        select c.csn, c.row_id, to_date(split(a.start_dt," ")[0]) as start_dt 
         
        from s_contact as c 
             
            inner join s_contact_fnx as f 
            on c.row_id = f.par_row_id 
         
            inner join s_contact_x as x 
            on c.row_id = x.par_row_id 
             
            inner join s_asset as a 
            on c.row_id = a.owner_con_id 
             
            inner join s_prod_int as p 
            on p.row_id = a.prod_id 
     
         
        where 
            (c.x_nrma_title <> "Estate Of The Late" or c.x_nrma_title is null)  -- no dead people 
            and (f.deceased_flg = "N" or f.deceased_flg is null) -- Excluded deceased members 
            and (lower(x.attrib_36) in ("yes", "null") or x.attrib_36 is null) -- Email Consent Yes 
            and (c.x_inv_email_1 = "N" or c.x_inv_email_1 is null) -- Valid email 
            and c.email_addr is not null -- valid email 
            and c.cust_stat_cd = "Active"  -- Active record 
            and lower(p.name) rlike "autoclub"  -- only avaiable to autoclub 
            and not lower(p.name) rlike "bundle"  -- exclude go bundles 
            and a.status_cd = "Active"  -- Active Asset Record  Have to remove this filter.  
         
 
""") 
 
print autoclub.count() 
autoclub.show(100,False) 
membership = spark.sql(""" 
     
     
        select c.csn, c.row_id, p.name, to_date(split(a.start_dt," ")[0]) as start_dt 
         
        from s_contact as c 
             
            inner join s_contact_fnx as f 
            on c.row_id = f.par_row_id 
         
            inner join s_contact_x as x 
            on c.row_id = x.par_row_id 
             
            inner join s_asset as a 
            on c.row_id = a.owner_con_id 
             
            inner join s_prod_int as p 
            on p.row_id = a.prod_id 
     
         
        where 
            (c.x_nrma_title <> "Estate Of The Late" or c.x_nrma_title is null)  -- no dead people 
            and (f.deceased_flg = "N" or f.deceased_flg is null) -- Excluded deceased members 
            and (lower(x.attrib_36) in ("yes", "null") or x.attrib_36 is null) -- Email Consent Yes 
            and (c.x_inv_email_1 = "N" or c.x_inv_email_1 is null) -- Valid email 
            and c.email_addr is not null -- valid email 
            and c.cust_stat_cd = "Active"  -- Active record 
            and lower(p.name) rlike "^membership"  -- only membership 
            and a.status_cd = "Active"  -- Active Asset Record 
         
 
""") 
 
print membership.count() 
membership.show(100,False) 
autoclub.createOrReplaceTempView("autoclub") 
membership.createOrReplaceTempView("membership") 
 
outcome_2 = spark.sql(""" 
     
    select *, datediff(today, start_dt) as diff_date 
    from 
    ( 
        select distinct autoclub.*, current_date() as today 
        from autoclub, membership 
        where  
                autoclub.start_dt = membership.start_dt 
            and autoclub.row_id = membership.row_id 
    ) 
    where 
        datediff(today, start_dt) = 30 
 
""") 
 
print outcome_2.count() 
outcome_2.show(10,False) 