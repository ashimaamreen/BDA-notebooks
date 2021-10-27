select  count(distinct a.owner_con_id) ContactID
        -- , x_nrma_promo_code as join_offer
        -- , p.name as product_name
        , to_date(from_utc_timestamp(o.order_dt,'AEST')) as RenewalDueDate
        , to_date(date_sub(from_utc_timestamp(o.order_dt,'AEST'),42)) as campaign_sent
        , to_date(date_sub(from_utc_timestamp(o.order_dt,'AEST'),43)) as omc_date

from gms.s_order o

inner join gms.s_order_type ot
on o.order_type_id = ot.row_id

inner join gms.s_order_item oi
on o.row_id = oi.order_id
--and oi.ACTION_CD in ('Update','Add') --when they renew only update line but add is upgrade/downgrade and new

inner join gms.s_prod_int p
on oi.prod_id = p.row_id
and p.sub_type_cd in ('Non-RSA','RSA')
and p.prod_cd = 'Promotion'

INNER JOIN gms.s_asset a
on oi.asset_integ_id=a.integration_id
and a.status_cd='Active'

inner join campaign_data.aa_dmc2546_free2freeseg2_campaign_edm_20210927_adhoc cam
on cam.con_id=a.owner_con_id

INNER JOIN gms.s_contact c
on c.row_id=a.owner_con_id

inner join gms.s_contact_x conx
ON conx.par_row_id = c.row_id 

inner join gms.s_contact_fnx as fn
on fn.par_row_id = c.row_id


where c.cust_stat_cd = 'Active'
and c.con_cd in ('Ordinary Member' , 'Affiliate Member')
and NVL(c.x_nrma_title,'no title') != 'Estate Of The Late'
and NVL(fn.deceased_flg,'Yes') != 'No'
and case when trim(c.x_inv_email_1) = 'Y' or c.email_addr is null then 'N' else 'Y' end = 'Y'
and NVL(conx.attrib_36,'Yes') != 'No'
and (fn.brloc_attrib13 is null or fn.brloc_attrib13 = 'N') 

and ot.name = 'Renew'
and x_nrma_promo_code='CVMFREEBLUE'
and p.name='NB_Simply_Energy_Blue_Offer_0320'
and to_date(from_utc_timestamp(o.order_dt,'AEST'))>'2021-10-25'
--and c.row_id='1-EHR434J'

group by 2,3,4
order by 2,3,4 