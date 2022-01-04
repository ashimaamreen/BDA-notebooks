select distinct x_nrma_promo_code 
--, p.x_paybyterm  
 
from gms.s_order 
where x_nrma_promo_code is not null 
--where 1=1 
select  distinct  c.row_id ContactID 
        , o.order_num OrderNumber 
        --, oi.order_id as OrderLineItemID 
        , to_date(o.ORDER_DT) as RenewalDueDate 
        --, to_date(o.X_STAT_CMPLTD_DT) as OrderCompletionDate  
        --, oi.action_cd 
        --, o.X_RENEWAL_RELATED 
        , ot.name order_type 
        , a.row_id product_asset_row_id 
        , x_nrma_promo_code as join_offer 
        , p.name 
 
 
from gms.s_order o 
 
inner join gms.s_order_type ot 
on o.order_type_id = ot.row_id 
 
inner join gms.s_order_item oi 
on o.row_id = oi.order_id 
and oi.ACTION_CD in ('Update','Add') --when they renew only update line but add is upgrade/downgrade and new 
 
inner join gms.s_prod_int p 
on oi.prod_id = p.row_id 
and p.sub_type_cd in ('Non-RSA','RSA') 
and p.prod_cd = 'Product' 
 
INNER JOIN gms.s_asset a 
on oi.asset_integ_id=a.integration_id 
and a.status_cd='Active' 
 
inner join gms.s_contact c 
on o.contact_id = c.row_id 
 
 
where ot.name = 'New' 
and o.status_cd != 'Revised' 
and o.ORDER_DT between '2019-12-20' and '2020-10-20' 
and x_nrma_promo_code is not null 
--and c.row_id='1-E1TK2JB' 
--and c.csn='990353369' 
--and o.order_num='17824240826' 
-- group by c.row_id  
--         , o.order_num  
--         , oi.order_id  
--         , to_date(o.ORDER_DT) 
--         , to_date(o.X_STAT_CMPLTD_DT) 
spark.sql(''' 
select  min(o.order_dt),max(o.order_dt) 
 
 
from gms.s_order o 
 
inner join gms.s_order_type ot 
on o.order_type_id = ot.row_id 
 
inner join gms.s_order_item oi 
on o.row_id = oi.order_id 
and oi.ACTION_CD in ('Update','Add') --when they renew only update line but add is upgrade/downgrade and new 
 
inner join gms.s_prod_int p 
on oi.prod_id = p.row_id 
and p.sub_type_cd in ('Non-RSA','RSA') 
and p.prod_cd = 'Product' 
 
INNER JOIN gms.s_asset a 
on oi.asset_integ_id=a.integration_id 
and a.status_cd='Active' 
 
inner join gms.s_contact c 
on o.contact_id = c.row_id 
 
 
where ot.name = 'New' 
and o.status_cd != 'Revised' 
and o.ORDER_DT between '2019-12-20' and '2020-10-25' 
and x_nrma_promo_code is not null 
''' 
).show(100000,False) 
spark.sql(''' 
select  distinct a.row_id 
        , x_nrma_promo_code 
 
 
from gms.s_order o 
 
inner join gms.s_order_type ot 
on o.order_type_id = ot.row_id 
 
inner join gms.s_order_item oi 
on o.row_id = oi.order_id 
and oi.ACTION_CD in ('Update','Add') --when they renew only update line but add is upgrade/downgrade and new 
 
inner join gms.s_prod_int p 
on oi.prod_id = p.row_id 
and p.sub_type_cd in ('Non-RSA','RSA') 
and p.prod_cd = 'Product' 
 
INNER JOIN gms.s_asset a 
on oi.asset_integ_id=a.integration_id 
and a.status_cd='Active' 
 
inner join gms.s_contact c 
on o.contact_id = c.row_id 
 
 
where ot.name = 'New' 
and o.status_cd != 'Revised' 
and o.ORDER_DT between '2019-12-20' and '2020-10-25' 
and x_nrma_promo_code is not null 
''' 
).show(100000,False) 
spark.sql(''' 
select  distinct a.row_id 
        , x_nrma_promo_code 
 
 
from gms.s_order o 
 
inner join gms.s_order_type ot 
on o.order_type_id = ot.row_id 
 
inner join gms.s_order_item oi 
on o.row_id = oi.order_id 
and oi.ACTION_CD in ('Update','Add') --when they renew only update line but add is upgrade/downgrade and new 
 
inner join gms.s_prod_int p 
on oi.prod_id = p.row_id 
and p.sub_type_cd in ('Non-RSA','RSA') 
and p.prod_cd = 'Product' 
 
INNER JOIN gms.s_asset a 
on oi.asset_integ_id=a.integration_id 
and a.status_cd='Active' 
 
inner join gms.s_contact c 
on o.contact_id = c.row_id 
 
 
where ot.name = 'New' 
and o.status_cd != 'Revised' 
and o.ORDER_DT between '2019-12-20' and '2020-10-25' 
and x_nrma_promo_code is not null 
''' 
).count() 
spark.sql(''' 
select  min(o.order_dt), max(o.order_dt) 
 
 
from gms.s_order o 
 
inner join gms.s_order_type ot 
on o.order_type_id = ot.row_id 
 
inner join gms.s_order_item oi 
on o.row_id = oi.order_id 
and oi.ACTION_CD in ('Update','Add') --when they renew only update line but add is upgrade/downgrade and new 
 
inner join gms.s_prod_int p 
on oi.prod_id = p.row_id 
and p.sub_type_cd in ('Non-RSA','RSA') 
and p.prod_cd = 'Product' 
 
INNER JOIN gms.s_asset a 
on oi.asset_integ_id=a.integration_id 
and a.status_cd='Active' 
 
inner join gms.s_contact c 
on o.contact_id = c.row_id 
 
 
where ot.name = 'New' 
and o.status_cd != 'Revised' 
and o.ORDER_DT between '2020-10-25' and '2020-11-11' 
and x_nrma_promo_code is not null 
--order by 3 desc 
''' 
).show(100000,False) 
spark.sql(''' 
select  distinct a.row_id 
        , x_nrma_promo_code 
 
 
from gms.s_order o 
 
inner join gms.s_order_type ot 
on o.order_type_id = ot.row_id 
 
inner join gms.s_order_item oi 
on o.row_id = oi.order_id 
and oi.ACTION_CD in ('Update','Add') --when they renew only update line but add is upgrade/downgrade and new 
 
inner join gms.s_prod_int p 
on oi.prod_id = p.row_id 
and p.sub_type_cd in ('Non-RSA','RSA') 
and p.prod_cd = 'Product' 
 
INNER JOIN gms.s_asset a 
on oi.asset_integ_id=a.integration_id 
and a.status_cd='Active' 
 
inner join gms.s_contact c 
on o.contact_id = c.row_id 
 
 
where ot.name = 'New' 
and o.status_cd != 'Revised' 
and o.ORDER_DT between '2020-10-25' and '2020-11-11' 
and x_nrma_promo_code is not null 
''' 
).count() 
spark.sql(''' 
select  distinct a.row_id 
        , x_nrma_promo_code 
 
 
from gms.s_order o 
 
inner join gms.s_order_type ot 
on o.order_type_id = ot.row_id 
 
inner join gms.s_order_item oi 
on o.row_id = oi.order_id 
and oi.ACTION_CD in ('Update','Add') --when they renew only update line but add is upgrade/downgrade and new 
 
inner join gms.s_prod_int p 
on oi.prod_id = p.row_id 
and p.sub_type_cd in ('Non-RSA','RSA') 
and p.prod_cd = 'Product' 
 
INNER JOIN gms.s_asset a 
on oi.asset_integ_id=a.integration_id 
and a.status_cd='Active' 
 
inner join gms.s_contact c 
on o.contact_id = c.row_id 
 
 
where ot.name = 'New' 
and o.status_cd != 'Revised' 
and o.ORDER_DT between '2020-10-25' and '2020-11-11' 
and x_nrma_promo_code is not null 
''' 
).show(10000,False) 
select p.x_nrma_offer_id as offer_id, p.name, p.row_id, p.desc_text, p.x_type_1, p.x_type_2, p.x_paybyterm  
 
from gms.s_prod_int p 
where 1=1 
--and p.x_type_1='DIGFUELCOMPNOV19' 
--p.row_id in ('1-DKHX3RO','1-DKHX3PS') 
--where p.x_type_1='JOINOFFER' 
--and p.sub_type_cd in ('Non-RSA','Add-on','RSA') 
--and p.prod_cd = 'Product' 
--on c.row_id = a.owner_con_id 
 
--inner join gms.s_prod_int p 
--on a.prod_id = p.row_id 
 
--where 1=1 
--and p.name in ('$20k travelcomp') 
and x_nrma_offer_id in ('1-DFU82FD','1-DFU82FH','1-DFU82F8') 
--and a.owner_con_id='1-HO4-3755' 
select o.contact_id 
        --count(distinct o.contact_id) 
        , o.prev_order_rev_id 
        , o.x_portal_pay_option 
        , o.x_digital_offer_id 
        , o.row_id 
        , o.par_order_id 
        , p.name 
from gms.s_order o 
 
 
--inner join  
where o.x_digital_offer_id in ('1-DFU82FD','1-DFU82FH','1-DFU82F8') 
--and o.contact_id='1-FWU-1998' 
select row_id from gms.s_contact where csn='750725501' 
--spark.sql(''' 
select  x_nrma_promo_code as join_offer_code, o.order_dt 
 
 
from gms.s_order o 
 
inner join gms.s_order_type ot 
on o.order_type_id = ot.row_id 
 
inner join gms.s_order_item oi 
on o.row_id = oi.order_id 
and oi.ACTION_CD in ('Update','Add') --when they renew only update line but add is upgrade/downgrade and new 
 
inner join gms.s_prod_int p 
on oi.prod_id = p.row_id 
and p.sub_type_cd in ('Non-RSA','RSA') 
and p.prod_cd = 'Product' 
 
INNER JOIN gms.s_asset a 
on oi.asset_integ_id=a.integration_id 
and a.status_cd='Active' 
 
inner join gms.s_contact c 
on o.contact_id = c.row_id 
 
 
where ot.name = 'New' 
and o.status_cd != 'Revised' 
and o.ORDER_DT between '2019-11-01' and '2020-11-10' 
and x_nrma_promo_code is not null 
--and p.name='NRMA Blue' 
--group by 2  
order by 2 desc 
select  distinct a.row_id 
        , to_date(o.order_dt) 
 
 
from gms.s_order o 
 
inner join gms.s_order_type ot 
on o.order_type_id = ot.row_id 
 
inner join gms.s_order_item oi 
on o.row_id = oi.order_id 
and oi.ACTION_CD in ('Update','Add') --when they renew only update line but add is upgrade/downgrade and new 
 
inner join gms.s_prod_int p 
on oi.prod_id = p.row_id 
and p.sub_type_cd in ('Non-RSA','RSA') 
and p.prod_cd = 'Product' 
 
INNER JOIN gms.s_asset a 
on oi.asset_integ_id=a.integration_id 
and a.status_cd='Active' 
 
inner join gms.s_contact c 
on o.contact_id = c.row_id 
 
 
where ot.name = 'New' 
and o.status_cd != 'Revised' 
and o.ORDER_DT between '2020-10-26' and '2020-11-11' 
and x_nrma_promo_code is not null 
 
order by 2 desc 
spark.sql(''' 
select  distinct a.row_id 
        , x_nrma_promo_code 
 
 
from gms.s_order o 
 
inner join gms.s_order_type ot 
on o.order_type_id = ot.row_id 
 
inner join gms.s_order_item oi 
on o.row_id = oi.order_id 
and oi.ACTION_CD in ('Update','Add') --when they renew only update line but add is upgrade/downgrade and new 
 
inner join gms.s_prod_int p 
on oi.prod_id = p.row_id 
and p.sub_type_cd in ('Non-RSA','RSA') 
and p.prod_cd = 'Product' 
 
INNER JOIN gms.s_asset a 
on oi.asset_integ_id=a.integration_id 
and a.status_cd='Active' 
 
inner join gms.s_contact c 
on o.contact_id = c.row_id 
 
 
where ot.name = 'New' 
and o.status_cd != 'Revised' 
and o.ORDER_DT between '2020-10-26' and '2020-11-10' 
and x_nrma_promo_code is not null 
''' 
).show(100000,False) 
select  year(o.order_dt),month(o.order_dt) 
        , case when x_nrma_promo_code='THRIFTYBLUE19' then 'Thrifty' else 'Parks and Resorts' end offer 
        , count(distinct a.row_id) 
 
 
from gms.s_order o 
 
inner join gms.s_order_type ot 
on o.order_type_id = ot.row_id 
 
inner join gms.s_order_item oi 
on o.row_id = oi.order_id 
and oi.ACTION_CD in ('Update','Add') --when they renew only update line but add is upgrade/downgrade and new 
 
inner join gms.s_prod_int p 
on oi.prod_id = p.row_id 
and p.sub_type_cd in ('Non-RSA','RSA') 
and p.prod_cd = 'Product' 
 
INNER JOIN gms.s_asset a 
on oi.asset_integ_id=a.integration_id 
and a.status_cd='Active' 
 
inner join gms.s_contact c 
on o.contact_id = c.row_id 
 
 
where ot.name = 'New' 
and o.status_cd != 'Revised' 
and o.ORDER_DT between '2019-11-01' and '2020-11-11' 
and x_nrma_promo_code in ('THRIFTYBLUE19','PARBLUE19','PARBLUE20') 
 
group by 1,2,3 
order by 1,2,3 
 
select c.csn as membernumber 
 
 
from gms.s_order o 
 
inner join gms.s_order_type ot 
on o.order_type_id = ot.row_id 
 
inner join gms.s_order_item oi 
on o.row_id = oi.order_id 
and oi.ACTION_CD in ('Update','Add') --when they renew only update line but add is upgrade/downgrade and new 
 
inner join gms.s_prod_int p 
on oi.prod_id = p.row_id 
and p.sub_type_cd in ('Non-RSA','RSA') 
and p.prod_cd = 'Product' 
 
INNER JOIN gms.s_asset a 
on oi.asset_integ_id=a.integration_id 
and a.status_cd='Active' 
 
inner join gms.s_contact c 
on o.contact_id = c.row_id 
 
 
where ot.name = 'New' 
and o.status_cd != 'Revised' 
and o.ORDER_DT between '2020-01-01' and '2020-01-03' 
and x_nrma_promo_code is not null 
and x_nrma_promo_code in ('THRIFTYBLUE19','PARBLUE19','PARBLUE20') 
-- group by 1,2,3 
-- order by 1,2,3 
 
spark.sql(''' 
select distinct a.row_id asset_row_id 
        , x_nrma_promo_code as Join_offer_code 
        , c.csn as Member_number 
        , to_date(o.order_dt) as renewal_due_date 
 
 
from gms.s_order o 
 
inner join gms.s_order_type ot 
on o.order_type_id = ot.row_id 
 
inner join gms.s_order_item oi 
on o.row_id = oi.order_id 
and oi.ACTION_CD in ('Update','Add') --when they renew only update line but add is upgrade/downgrade and new 
 
inner join gms.s_prod_int p 
on oi.prod_id = p.row_id 
and p.sub_type_cd in ('Non-RSA','RSA') 
and p.prod_cd = 'Product' 
 
INNER JOIN gms.s_asset a 
on oi.asset_integ_id=a.integration_id 
and a.status_cd='Active' 
 
inner join gms.s_contact c 
on o.contact_id = c.row_id 
 
 
where ot.name = 'New' 
and o.status_cd != 'Revised' 
and to_date(o.ORDER_DT) between '2020-01-20' and '2020-10-25' 
and x_nrma_promo_code is not null 
 
order by 4 
''').show(1000000,False) 
spark.sql(''' 
select distinct a.row_id asset_row_id 
        , x_nrma_promo_code as Join_offer_code 
       -- , c.csn as Member_number 
       -- , to_date(o.order_dt) as renewal_due_date 
 
 
from gms.s_order o 
 
inner join gms.s_order_type ot 
on o.order_type_id = ot.row_id 
 
inner join gms.s_order_item oi 
on o.row_id = oi.order_id 
and oi.ACTION_CD in ('Update','Add') --when they renew only update line but add is upgrade/downgrade and new 
 
inner join gms.s_prod_int p 
on oi.prod_id = p.row_id 
and p.sub_type_cd in ('Non-RSA','RSA') 
and p.prod_cd = 'Product' 
 
INNER JOIN gms.s_asset a 
on oi.asset_integ_id=a.integration_id 
and a.status_cd='Active' 
 
inner join gms.s_contact c 
on o.contact_id = c.row_id 
 
 
where ot.name = 'New' 
and o.status_cd != 'Revised' 
and to_date(o.ORDER_DT) between '2020-06-20' and '2020-10-25' 
and x_nrma_promo_code is not null 
''').count() 