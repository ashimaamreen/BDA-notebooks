SELECT  
       ass1.row_id AS parent_asset_id, 
       prod1.name as parent, 
       prod2.name as child, 
       ass1.end_dt AS parent_end_dt, 
       ass2.end_dt as child_end_dt 
  
FROM gms.s_asset as ass1 
    INNER JOIN gms.s_asset as ass2 
        ON ass1.root_asset_id = ass2.root_asset_id 
        and ass1.row_id <> ass2.row_id 
    INNER JOIN gms.s_contact_fnx as fnx 
        ON fnx.par_row_id = ass1.owner_con_id 
    inner join gms.s_prod_int as prod1 
        on ass1.prod_id = prod1.row_id 
    inner join gms.s_prod_int as prod2 
        on ass2.prod_id = prod2.row_id 
 
WHERE prod1.name in ('Autoclub Plus','Autoclub Classic','Autoclub Premium','Classic Care','Free2Go','MVB Premium Care','Premium Care') 
    and prod1.row_id <> prod2.row_id 
    and ass1.row_id <> ass2.row_id 
    and ass1.end_dt <> ass2.end_dt 
    and ass2.end_dt > now() 