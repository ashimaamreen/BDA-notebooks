select * from gms.s_order where order_num = '30923957778' 
-- select order_channel from sandpit.renewal_base limit 100 
select py.* from gms.s_order ord 
left join gms.s_src_payment py 
        on ord.row_id = py.order_id 
        AND py.pay_stat_cd in ('Payment Taken','Reconciled') 
 
where ord.order_num = '30889016479' 
     
spark.sql(''' 
select    
        --b.channel_name 
        --, b.channel_group 
        --, base.order_channel 
         prod.name 
        , date_format(ord.X_STAT_CMPLTD_DT,'yyyy-MM') order_month 
        --, ord.X_PAY_BY_TERM as MPP_flag 
        , count(distinct a.row_id) 
 
from gms.s_order_item a 
 
inner join gms.s_order         as ord 
        on a.order_id = ord.row_id  
        and ord.status_cd = 'Complete' 
         
inner join gms.s_order_type ot 
        on ord.order_type_id = ot.row_id 
         
inner join gms.s_contact c 
        on ord.contact_id = c.row_id 
 
inner join gms.s_prod_int           as prod  
        on a.prod_id = prod.row_id  
        and prod.sub_type_cd in ('Non-RSA','Add-on','RSA') 
        and prod.prod_cd = 'Product' 
         
inner join gms.dw_order_item b 
        on a.row_id = b.order_item_id 
 
--inner join sandpit.renewal_base base 
        --on base.item_row_id = b.order_item_id 
 
left join gms.s_src_payment py 
        on ord.row_id = py.order_id 
         
        --AND py.pay_stat_cd != 'Cancelled' -- same number of records 
 
where date_format(ord.X_STAT_CMPLTD_DT,'yyyy-MM') in ('2020-07','2020-08') 
        and trim(prod.name) in ('Basic Bundle','Basic Care','Classic Care','Classic Care Plan','Essential Bundle','Free2go','Key Plus','MVB Classic Care','MVB Premium Care','NRMA Blue','Pet Plus','Premium Bundle' 
        ,'Premium Care','Premium Care Plan','Premium Plus','Premium Plus Plan','Tow Plus','Traveller Care','Windscreen Plus') 
        and a.ACTION_CD in ('Update','Add') 
        AND py.pay_stat_cd in ('Payment Taken','Reconciled') 
        and ord.status_cd != 'Revised' 
        and con_cd != 'Business Contact' 
        -- and net_pri > 0 
         
group by 1,2 
 
''').show(100000) 
spark.sql(''' 
select    
        b.channel_name 
        , b.channel_group 
        , base.order_channel 
         --prod.name 
        , date_format(ord.X_STAT_CMPLTD_DT,'yyyy-MM') order_month 
        --, ord.X_PAY_BY_TERM as MPP_flag 
        , count(distinct a.row_id) 
 
from gms.s_order_item a 
 
inner join gms.s_order         as ord 
        on a.order_id = ord.row_id  
        and ord.status_cd = 'Complete' 
         
inner join gms.s_order_type ot 
        on ord.order_type_id = ot.row_id 
         
inner join gms.s_contact c 
        on ord.contact_id = c.row_id 
 
inner join gms.s_prod_int           as prod  
        on a.prod_id = prod.row_id  
        and prod.sub_type_cd in ('Non-RSA','Add-on','RSA') 
        and prod.prod_cd = 'Product' 
         
inner join gms.dw_order_item b 
        on a.row_id = b.order_item_id 
 
inner join sandpit.renewal_base base 
       on base.item_row_id = b.order_item_id 
 
left join gms.s_src_payment py 
        on ord.row_id = py.order_id 
     
 
where date_format(ord.X_STAT_CMPLTD_DT,'yyyy-MM') in ('2020-07','2020-08') 
        and trim(prod.name) in ('Basic Bundle','Basic Care','Classic Care','Classic Care Plan','Essential Bundle','Free2go','Key Plus','MVB Classic Care','MVB Premium Care','NRMA Blue','Pet Plus','Premium Bundle' 
        ,'Premium Care','Premium Care Plan','Premium Plus','Premium Plus Plan','Tow Plus','Traveller Care','Windscreen Plus') 
        and a.ACTION_CD in ('Update','Add') 
        AND py.pay_stat_cd in ('Payment Taken','Reconciled') 
        and ord.status_cd != 'Revised' 
        and con_cd != 'Business Contact' 
        -- and net_pri > 0 
         
group by 1,2,3,4 
 
''').show(100000) 
select * from gms.s_order limit 100 
-- select count(*) from gms.dw_order_item  
-- 5,0798,642 
 
-- select count(*) from gms.s_order 
-- 18,253,196 
 
-- select count(distinct order_num) from gms.s_order 
-- 17883358 
-- select count(distinct order_num) from gms.s_order where status_cd = 'Complete' 
-- -- 13560177 
 
-- select year(from_timestamp(X_STAT_CMPLTD_DT,'yyyy-MM-dd')), count(distinct order_num) from gms.s_order where status_cd = 'Complete' group by 1 order by 1 
 
-- select year(from_timestamp(X_STAT_CMPLTD_DT,'yyyy-MM-dd')), count(distinct order_num) from gms.s_order  
 
 
select year(from_timestamp(X_STAT_CMPLTD_DT,'yyyy-MM-dd')), count(distinct order_num) from gms.s_order c 
inner join gms.s_order_item a 
        on a.order_id = c.row_id  
        and c.status_cd = 'Complete' 
         
         
inner join gms.dw_order_item b 
on a.row_id = b.order_item_id 
 
group by 1 order by 1 
select * from gms.dw_order_item limit 100 