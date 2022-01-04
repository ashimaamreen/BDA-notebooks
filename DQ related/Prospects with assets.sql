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
     
where  con.con_cd = "Prospect" 
 