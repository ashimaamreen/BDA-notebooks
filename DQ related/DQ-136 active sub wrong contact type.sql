select 
     con.row_id 
    ,con.con_cd 
    ,prod.name 
    ,con.x_nrma_join_dt 
    ,asset.end_dt 
 
from gms.s_contact as con 
    inner join gms.s_asset as asset 
        on asset.owner_con_id = con.row_id 
    inner join gms.s_prod_int as prod 
        on asset.prod_id = prod.row_id 
     
where con.cust_stat_cd = "Active" 
    AND asset.status_cd = "Active" 
    AND con.con_cd in ("Affiliate Member","Prospect","Customer") 
    AND prod.prod_cd = "Product" 
    AND prod.sub_type_cd in ("RSA","Non-RSA") 
    AND NOT (con.con_cd = "Affiliate Member" AND prod.name in ("Autoclub Classic","NCO Blue")) 
    AND	(year(now()) - year(from_utc_timestamp(con.birth_dt,'AEST'))  
   + case when month(from_utc_timestamp(con.birth_dt,'AEST')) > month(now()) then -1 
          when month(from_utc_timestamp(con.birth_dt,'AEST')) = month(now())  
               and day(from_utc_timestamp(con.birth_dt,'AEST')) > day(now()) -1 then -1 
          else 0  
     end) >=18 