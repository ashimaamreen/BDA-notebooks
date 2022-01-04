with idp as (
select distinct o.row_id as order_id from gms.s_contact c

INNER JOIN gms.s_order o
on o.contact_id=c.row_id

INNER JOIN gms.s_order_item oi
on oi.order_id=o.row_id

inner JOIN gms.s_prod_int p
on p.row_id=oi.prod_id

where name='International Driving Permit (IDP)'
)

SELECT distinct o.order_num,c.row_id as contact_id, c.csn as member_number
        ,case when o.created>='2021-08-15' then 'since Brumy' else 'pre Brumby' end brumby
        -- , c.con_cd
        -- , case when lower(p.name)='membership' then 1 else 0 end effected
        -- , count(DISTINCT c.row_id) 
        
from gms.s_contact c

INNER JOIN gms.s_order o
on o.contact_id=c.row_id


INNER JOIN gms.s_order_item oi
on oi.order_id=o.row_id

inner JOIN gms.s_prod_int p
on p.row_id=oi.prod_id

inner join idp i
on o.row_id=i.order_id

inner join gms.s_order_type ot 
on o.order_type_id = ot.row_id

where 1=1
and o.created>'2021-01-01'
and lower(ot.name)='new'
--and case when o.created>='2021-08-15' then 'since Brumy' else 'pre Brumby' end='pre Brumby'
and lower(p.name)='membership'
and c.con_cd='Affiliate Member'

-- GROUP BY 1,2,3
-- ORDER BY 1,3,2