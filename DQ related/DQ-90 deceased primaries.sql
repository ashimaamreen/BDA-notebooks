create table sandpit.cd_deceased_primaries as 
select distinct 
    con.row_id as new_primary_id 
  , con.pr_dept_ou_id as org_row_id 
  , pricon.row_id as deceased_primary_id 
 
from gms.s_contact as con 
    inner join gms.s_org_ext as org 
    on con.pr_dept_ou_id = org.row_id 
     
    inner join gms.s_contact as pricon 
    on org.pr_con_id = pricon.row_id 
     
    inner join gms.s_asset as asset 
    on asset.owner_accnt_id = con.pr_dept_ou_id 
     
	inner join gms.s_prod_int as prod 
       on asset.prod_id = prod.row_id 
     
where pricon.x_nrma_title = "Estate Of The Late" 
    AND nvl(con.x_nrma_title,'No title') <> "Estate Of The Late" 
    and asset.status_cd = 'Active'  /* Someone in the household has an active sub */ 
    and prod.type = 'Membership'    /* of a type that converys Membership */ 
    and prod.prod_cd = 'Promotion' 
    and con.csn is not null 