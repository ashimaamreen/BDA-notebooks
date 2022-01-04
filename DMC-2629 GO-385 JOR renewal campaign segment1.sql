with addNgo as (
SELECT distinct membernumber,contact_id,prod_cd,prod_name,item_promo_name, nrp_prod_name, renewed_prod_name, integration_id, x_renewal_status, order_status_cd,item_status_cd
from `mynrma-mm-ds-dw-sandbox-a815.datasci.subs_base`
where item_promo_name like 'Add%'
and date(asset_end_dt)='2021-12-25'
--and x_renewal_count=0
--and renewed_prod_name is null
--and membernumber='971974501'
)
    select a.*
            ,conx.attrib_36 
            ,fn.brloc_attrib13
            ,con.email_addr 
            ,con.x_inv_email_1
            ,con.cust_stat_cd
            ,con.con_cd
            ,fn.deceased_flg
            ,con.x_nrma_title
    from addNgo a
    
    inner join gms.s_contact con
    on a.membernumber = con.csn
    
    inner join gms.s_contact_fnx fn
    on fn.par_row_id = con.row_id
    
    inner join gms.s_contact_x conx
    ON conx.par_row_id = con.row_id  
    
    where 1=1
        and (lower(conx.attrib_36) in ("yes", "null") or conx.attrib_36 is null) 
        and (fn.brloc_attrib13 is null or fn.brloc_attrib13 = 'N') 
        and con.email_addr is not null
        and COALESCE(con.x_inv_email_1,'N')!= 'Y' 
        and con.cust_stat_cd = 'Active'
        and con.con_cd in ('Ordinary Member','Affiliate Member')
        AND COALESCE(fn.deceased_flg, 'N') = 'N'
        AND COALESCE(con.x_nrma_title, '') != 'Estate Of The Late'
        --and con.csn='187629901'

order by 1

--------------------------------------------------------------------------------------------------------------------------
with joinNgo as (
SELECT distinct membernumber,contact_id,prod_cd,prod_name,item_promo_name, nrp_prod_name, renewed_prod_name, integration_id, x_renewal_status, order_status_cd,item_status_cd
from `mynrma-mm-ds-dw-sandbox-a815.datasci.subs_base`
where item_promo_name like 'Join%'
and date(asset_end_dt)='2021-12-25'
--and x_renewal_count=0
--and renewed_prod_name is null
--and membernumber='971974501'
)
    select a.*
            ,conx.attrib_36 
            ,fn.brloc_attrib13
            ,con.email_addr 
            ,con.x_inv_email_1
            ,con.cust_stat_cd
            ,con.con_cd
            ,fn.deceased_flg
            ,con.x_nrma_title
    from joinNgo a
    
    inner join gms.s_contact con
    on a.membernumber = con.csn
    
    inner join gms.s_contact_fnx fn
    on fn.par_row_id = con.row_id
    
    inner join gms.s_contact_x conx
    ON conx.par_row_id = con.row_id  
    
    where 1=1
        and (lower(conx.attrib_36) in ("yes", "null") or conx.attrib_36 is null) 
        and (fn.brloc_attrib13 is null or fn.brloc_attrib13 = 'N') 
        and con.email_addr is not null
        and COALESCE(con.x_inv_email_1,'N')!= 'Y' 
        and con.cust_stat_cd = 'Active'
        and con.con_cd in ('Ordinary Member','Affiliate Member')
        AND COALESCE(fn.deceased_flg, 'N') = 'N'
        AND COALESCE(con.x_nrma_title, '') != 'Estate Of The Late'
        --and con.csn='187629901'

order by 1