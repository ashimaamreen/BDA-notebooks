SELECT DISTINCT 
    org.name, 
    org.row_id AS account_id, 
    con.x_nrma_title as title, 
    con.fst_name, 
    con.last_name, 
    con.row_id AS contact_id, 
    fnx.deceased_flg as deceased_flg, 
    con.cust_stat_cd as contact_status, 
    ORG.last_upd, 
    user.login 
 
FROM gms.s_org_ext AS org 
INNER JOIN gms.s_contact AS con ON con.pr_dept_ou_id = org.row_id 
INNER JOIN gms.s_contact_fnx AS fnx ON con.row_id = fnx.par_row_id 
INNER JOIN gms.s_asset AS asset ON asset.owner_con_id = con.row_id 
INNER JOIN gms.s_prod_int AS prod ON prod.row_id = asset.prod_id 
inner join gms.s_user as user on user.row_id = org.last_upd_by 
WHERE (org.name ILIKE '%of the lat%' or org.name ilike "%Estat%") 
  AND fnx.deceased_flg = 'N' 
  AND con.cust_stat_cd = 'Active' 
  AND asset.status_cd = 'Active' 
  AND prod.type = 'Membership' /* of a type that conveys Membership */ 
  AND prod.prod_cd = 'Promotion' 
  AND con.csn is not null 
  AND upper(con.last_name) <> "ESTATHEO" 
  and con.x_nrma_title <> "Estate Of The Late" 