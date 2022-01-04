spark.sql(''' 
select  
    b.renewal_yyyymm 
,   sum(1)                      AS renewals 
,   sum(c.renewal_cd)           as renewed 
,   sum(case when  b.renewed_completed_dt <= date_add(order_end_dt,2) then 1 else 0 end  ) as renewed_pot 
from sandpit.renewal_base b 
left join  sandpit.util_prod_budget  a  
    on b.prod_budget = a.prod_budget 
    and case when b.order_payment_term = "Y" then 'MPP' else 'Annual' end = a.payment_plan 
inner join sandpit.util_renew_summ c  
    on b.match_rnk = c.match_rnk  
where    
        date_add(b.order_end_dt,2) >= "2018-06-01 00:00:00" 
    and  COALESCE(c.type_rnk, 0)  <> 1  
    and COALESCE(b.member_staff, 0)  = 0  
    AND a.removeID = 0  
    and (date_add(b.order_end_dt,2) >= a.dt_min  OR a.dt_min IS NULL) 
    AND b.contact_cd IN ('Ordinary Member', 'Affiliate Member', 'Customer') 
    AND b.nrp_prod_name = 'F2G' 
group by  
b.renewal_yyyymm 
 
order by 
b.renewal_yyyymm 
''').show(200, False) 
 
spark.sql(''' 
SELECT 
    account_id, 
    order_id, 
    order_start_dt, 
    -- match_rnk, 
    prod_budget, 
    item_net_price, 
    -- renewed_completed_dt, 
    -- order_end_dt, 
    order_payment_term, 
    renewal_yyyymm, 
    prod_name, 
    nrp_prod_name 
 
FROM 
    sandpit.renewal_base 
     
WHERE 
    nrp_prod_name = 'F2G' 
    AND item_net_price = '96.0' 
''').show(250, False) 
 
spark.sql(''' 
SELECT 
    item_net_price, 
    COUNT(*) 
 
FROM 
    sandpit.renewal_base 
     
WHERE 
    nrp_prod_name = 'F2G' 
     
GROUP BY 
    item_net_price 
''').show(250, False) 
# order_channel in renewal_base approximates payment channel 