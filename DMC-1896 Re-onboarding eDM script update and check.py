Contents
#Renewal Orders (Order Item Details) 
 
s_asset = spark.table("gms.s_asset") 
s_asset.createOrReplaceTempView("s_asset") 
 
s_order = spark.table("gms.s_order") 
s_order.createOrReplaceTempView("s_order") 
 
s_order_type = spark.table("gms.s_order_type") 
s_order_type.createOrReplaceTempView("s_order_type") 
 
s_order_item = spark.table("gms.s_order_item") 
s_order_item.createOrReplaceTempView("s_order_item") 
 
s_order_item_x = spark.table("gms.s_order_item_x") 
s_order_item_x.createOrReplaceTempView("s_order_item_x") 
 
s_prod_int = spark.table("gms.s_prod_int") 
s_prod_int.createOrReplaceTempView("s_prod_int") 
 
s_contact = spark.table("gms.s_contact") 
s_contact.createOrReplaceTempView("s_contact") 
 
s_org_ext = spark.table("gms.s_org_ext") 
s_org_ext.createOrReplaceTempView("s_org_ext") 
 
s_src_payment = spark.table("gms.s_src_payment") 
s_src_payment.createOrReplaceTempView("s_src_payment") 
Renewal_Order = spark.sql("""   
 
select    distinct o.contact_id 
        --cam.contact_id 
        --, cam.holdoutgroup 
        --, cam.opentoclick 
        --, to_date(cam.sendtime) as send_date 
        , to_date(o.ORDER_DT) as RenewalDueDate 
        , to_date(py.txn_dt) as PaymentDate 
        , to_date(o.X_STAT_CMPLTD_DT) as OrderCompletionDate 
       -- , datediff(to_date(cam.sendtime),to_date(py.txn_dt)) as date_diff 
        , py.payment_req_num 
 
 
from s_order o 
 
inner join s_order_type ot 
on o.order_type_id = ot.row_id 
 
inner join s_order_item oi 
on o.row_id = oi.order_id 
and oi.ACTION_CD in ('Update','Add') --when they renew only update line but add is upgrade/downgrade and new 
 
--left join s_order_item_x ox 
--on oi.row_id = ox.par_row_id 
 
inner join s_prod_int p 
on oi.prod_id = p.row_id 
and p.sub_type_cd in ('Non-RSA','RSA') 
and p.prod_cd = 'Product' 
 
inner join s_src_payment py 
on o.row_id = py.order_id 
 
--inner join omc.member_campaign_summary_table cam 
--on o.contact_id=cam.contact_id 
--and campaign_name='Consumer_Lifecycle_Post_renewal_BenefitsCheckEDM_20191213' 
 
where py.txn_dt='2020-01-28 00:00:00' 
and o.X_PAYMENT_STATUS in ('Payment Taken', 'Reconciled') 
and py.pay_stat_cd in ('Payment Taken', 'Reconciled') 
and o.X_PAY_BY_TERM='N' 
""") 
Renewal_Order.createOrReplaceTempView("Renewal_Order") 
spark.sql('select * from Renewal_Order').count() 
select    distinct o.contact_id 
        , o.order_num 
        --cam.contact_id 
        --, cam.holdoutgroup 
        --, cam.opentoclick 
        --, to_date(cam.sendtime) as send_date 
        , to_date(o.ORDER_DT) as Renewal_Due_Date 
        , to_date(py.txn_dt) as Payment_Date 
        , py.pay_stat_cd 
     --   , to_date(o.X_STAT_CMPLTD_DT) as Order_Completion_Date 
        , o.status_cd 
        , p.PART_NUM 
       -- , datediff(to_date(cam.sendtime),to_date(py.txn_dt)) as date_diff 
       -- , py.payment_req_num 
 
 
from gms.s_order o 
 
inner join gms.s_order_type ot 
on o.order_type_id = ot.row_id 
 
inner join gms.s_order_item oi 
on o.row_id = oi.order_id 
and oi.ACTION_CD in ('Update','Add') --when they renew only update line but add is upgrade/downgrade and new 
 
--left join s_order_item_x ox 
--on oi.row_id = ox.par_row_id 
 
inner join gms.s_prod_int p 
on oi.prod_id = p.row_id 
and p.sub_type_cd in ('Non-RSA','RSA') 
--and p.prod_cd = 'Product' 
 
inner join gms.s_src_payment py 
on o.row_id = py.order_id 
 
--inner join omc.member_campaign_summary_table cam 
--on o.contact_id=cam.contact_id 
--and campaign_name='Consumer_Lifecycle_Post_renewal_BenefitsCheckEDM_20191213' 
 
where to_date(py.txn_dt)>'2020-01-15' 
and o.X_PAYMENT_STATUS in ('Payment Taken', 'Reconciled') 
and py.pay_stat_cd in ('Payment Taken', 'Reconciled') 
and o.X_PAY_BY_TERM='N' 
and o.contact_id in ('1-BB4V40H') 
--('1-HQN-430','1-DO6-3803','1-G1E-3125','1-222WFB0','1-D8P-14','1-BUJYSNP','1-FY7-3097','1-BB4V40H','1-HV0-4060','1-BNPKZC0','1-HSF-1713','1-BIA58RC' 
 --                 ,'1-DWD-2416','1-FTQ-3989','1-DHL-2240') 
and p.part_num not like '%PROM%' 
AND p.part_num NOT LIKE 'JG%' 
AND p.part_num NOT LIKE 'AUTOCLUB%' 
AND p.part_num NOT LIKE 'ADDGO%' 
--and o.status_cd in ('Submitted','Complete') 
select contact_id, order_dt,x_stat_cmpltd_dt, status_cd from gms.s_order 
where order_num='30229728076' 
eligible = spark.read.load("/user/aamreen/Consumer_Post_renewal_benefits_20200217.csv",format="csv", sep=",", inferSchema="true", header="true") 
eligible.createOrReplaceTempView("eligible") 
spark.sql('select * from eligible').count() 
spark.sql('select * from eligible').show(10000,False) 
spark.sql(""" 
 
select  distinct o.contact_id 
        , to_date(o.ORDER_DT) as RenewalDueDate 
        , to_date(py.txn_dt) as PaymentDate 
        , to_date(o.X_STAT_CMPLTD_DT) as OrderCompletionDate 
        , o.status_cd 
        , e.* 
 
from s_order o 
 
inner join s_order_type ot 
on o.order_type_id = ot.row_id 
 
inner join s_src_payment py 
on o.row_id = py.order_id 
 
inner join eligible e 
on e.CUSTOMER_ID_=o.contact_id 
 
where (NVL(o.X_RENEWAL_RELATED, 'N') = 'Y' or ot.name = 'Renew') 
and o.X_PAYMENT_STATUS in ('Payment Taken', 'Reconciled') 
and py.pay_stat_cd in ('Payment Taken', 'Reconciled') 
and o.X_PAY_BY_TERM='N' 
and o.status_cd in ('Submitted','Complete')  
and to_Date(py.txn_dt) ='2020-02-03' 
""").count() 
spark.sql(""" 
select contact_id, count(*) from 
(select  distinct o.contact_id 
        , to_date(o.ORDER_DT) as RenewalDueDate 
        , to_date(py.txn_dt) as PaymentDate 
        , py.payment_req_num 
        , o.status_cd 
        , e.* 
 
from s_order o 
 
inner join s_order_type ot 
on o.order_type_id = ot.row_id 
 
inner join s_src_payment py 
on o.row_id = py.order_id 
 
inner join eligible e 
on e.CUSTOMER_ID_=o.contact_id 
 
where (NVL(o.X_RENEWAL_RELATED, 'N') = 'Y' or ot.name = 'Renew') 
and o.X_PAYMENT_STATUS in ('Payment Taken', 'Reconciled') 
and py.pay_stat_cd in ('Payment Taken', 'Reconciled') 
and o.X_PAY_BY_TERM='N' 
and to_Date(py.txn_dt) ='2020-02-03' 
--and o.status_cd not in ('Submitted','Complete')  
order by 3 )aa 
group by contact_id 
order by 2 desc 
""").show(2000,False) 
Renewal_Order = spark.sql("""   
 
select    distinct c.row_id as contact_id 
        , o.X_PAY_BY_TERM as MPP_flag 
        , to_date(o.ORDER_DT) as RenewalDueDate 
        , to_date(py.txn_dt) as PaymentDate 
        , to_date(o.X_STAT_CMPLTD_DT) as OrderCompletionDate 
        , py.payment_req_num 
 
 
from s_order o 
 
inner join s_order_type ot 
on o.order_type_id = ot.row_id 
 
inner join s_order_item oi 
on o.row_id = oi.order_id 
and oi.ACTION_CD in ('Update','Add') --when they renew only update line but add is upgrade/downgrade and new 
 
--left join s_order_item_x ox 
--on oi.row_id = ox.par_row_id 
 
inner join s_prod_int p 
on oi.prod_id = p.row_id 
and p.sub_type_cd in ('Non-RSA','RSA') 
and p.prod_cd = 'Product' 
 
inner join s_src_payment py 
on o.row_id = py.order_id 
 
inner join s_contact c 
on o.contact_id=c.row_id 
 
where datediff(to_date(now()),to_date(py.txn_dt))=19 
and (NVL(o.X_RENEWAL_RELATED, 'N') = 'Y' or ot.name = 'Renew') 
and o.status_cd != 'Revised' 
and o.X_PAYMENT_STATUS in ('Payment Taken', 'Reconciled') 
and py.pay_stat_cd in ('Payment Taken', 'Reconciled') 
 
""") 
Renewal_Order.createOrReplaceTempView("Renewal_Order") 
spark.sql('select * from Renewal_Order').count() 
spark.sql(""" 
 
select * from Renewal_Order ro 
inner join eligible e 
on e.customer_id_=ro.contact_id 
 
""").show(10000,False) 
spark.sql(""" 
 
select * from eligible e 
left join Renewal_Order ro 
on e.customer_id_=ro.contact_id 
 
""").count() 
spark.sql("""select * from Renewal_Order """).show(1000,False) 
spark.sql("""select MPP_flag,count(distinct contact_id) from Renewal_Order 
where 1=1 
--date_diff between 0 and 15 
and holdoutgroup='N' 
group by MPP_flag 
""").show(200,False) 
spark.sql("""select * from Renewal_Order 
where MPP_flag='Y' 
and holdoutgroup='N' 
--where contact_id='1-HFR-2848' 
""").count() 
select * from gms.s_asset 