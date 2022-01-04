select 
    count(*) 
    ,prod.name 
 
    from gms.s_contact as con 
    inner join gms.s_asset as asset 
	    on asset.owner_con_id = con.row_id 
 
	inner join gms.s_prod_int as prod 
       on asset.prod_id = prod.row_id 
        
    inner join gms.s_contact_fnx as fn  
       on fn.par_row_id = con.row_id  
     
where asset.status_cd = 'Active'  /* Someone in the household has an active sub */ 
    and prod.type = 'Membership'    /* of a type that conveys Membership */ 
    and prod.prod_cd = 'Promotion' 
    and NVL(fn.deceased_flg,'N') = 'N' 
    and con.csn is not null 
    and asset.end_dt is null 
    and prod.name in ('Autoclub Plus','Autoclub Classic','Autoclub Premium','Classic Care','Club Care','Free2Go','Key Plus', 
        'MVB Premium Care','Pet Plus','Premium Care','Tow Plus','Windscreen Plus','NRMA Blue') 
 
group by prod.name