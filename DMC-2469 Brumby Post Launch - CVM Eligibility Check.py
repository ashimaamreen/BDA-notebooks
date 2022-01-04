
--Sales reporting always uses created date 
#[11:14 AM] Genevieve Lao 
#   (1) receipt dt - will miss those who paid on time but not reconciled yet (2) trx dt - depending on when we run it, it can exclude those who paid on time closer to due date as it will reflect 'late' 
# 
#    versus 2) min(payment creation dt) for lines with payment taken or reconciled status 
 
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
select    c.row_id ContactID 
        , o.order_num OrderNumber 
        , oi.order_id as OrderLineItemID 
        , to_date(o.ORDER_DT) as RenewalDueDate 
        , min(to_date(COALESCE(py.received_dt,py.txn_dt))) as PaymentDate 
        , to_date(o.X_STAT_CMPLTD_DT) as OrderCompletionDate  
        , o.X_PAYMENT_STATUS as PaymentStatus 
 
 
from gms.s_order o 
 
inner join gms.s_order_type ot 
on o.order_type_id = ot.row_id 
 
inner join gms.s_order_item oi 
on o.row_id = oi.order_id 
and oi.ACTION_CD in ('Update','Add') --when they renew only update line but add is upgrade/downgrade and new 
 
inner join gms.s_prod_int p 
on oi.prod_id = p.row_id 
and p.sub_type_cd in ('Non-RSA','Add-on','RSA') 
and p.prod_cd = 'Product' 
 
left join gms.s_src_payment py 
on o.row_id = py.order_id 
AND py.pay_stat_cd != 'Cancelled' 
 
inner join gms.s_contact c 
on o.contact_id = c.row_id 
 
 
where (NVL(o.X_RENEWAL_RELATED, 'N') = 'Y' or ot.name = 'Renew') 
and o.status_cd != 'Revised' 
 
group by c.row_id  
        , o.order_num  
        , oi.order_id  
        , to_date(o.ORDER_DT) 
        , to_date(o.X_STAT_CMPLTD_DT) 
        , o.X_PAYMENT_STATUS 
SELECT DISTINCT  
         from_utc_timestamp(received_dt,'AEST') as receipt_dt--09/02/2021 
        , from_utc_timestamp(txn_dt,'AEST') as txn_dt --10/02/2021 03:27:57 AM 
        , from_utc_timestamp(py.created,'AEST') as created_dt --10/02/2021 03:27:59 AM 
        , min(to_date(COALESCE(py.received_dt,py.txn_dt))) as PaymentDate 
        , from_utc_timestamp(o.X_STAT_CMPLTD_DT,'AEST') as order_completion_dt 
        , o.X_PAYMENT_STATUS as PaymentStatus 
        --, o.x_stat_cmpltd_dt --04/03/2021 07:16:28 PM 
        , o.order_num 
        , o.order_dt 
 
from gms.s_src_payment py 
 
INNER JOIN gms.s_order o 
on o.row_id = py.order_id 
 
 
where 1=1 
and o.order_num='32039377836' 
--'31825144559' 
AND py.pay_stat_cd != 'Cancelled' 
and o.x_payment_status='Reconciled' 
and o.X_STAT_CMPLTD_DT is null 
Order_Payment = spark.sql("""   
 
select    c.row_id ContactID 
        , o.order_num OrderNumber 
        , oi.order_id as OrderLineItemID 
        , to_date(o.ORDER_DT) as RenewalDueDate 
        , min(to_date(py.txn_dt)) as PaymentDate 
        , to_date(o.X_STAT_CMPLTD_DT) as OrderCompletionDate  
 
 
from s_order o 
 
inner join s_order_type ot 
on o.order_type_id = ot.row_id 
 
inner join s_order_item oi 
on o.row_id = oi.order_id 
--and oi.ACTION_CD in ('Update','Add') --when they renew only update line but add is upgrade/downgrade and new 
 
inner join s_prod_int p 
on oi.prod_id = p.row_id 
and p.sub_type_cd in ('Non-RSA','Add-on','RSA') 
and p.prod_cd = 'Product' 
 
left join s_src_payment py 
on o.row_id = py.order_id 
AND py.pay_stat_cd != 'Cancelled' 
 
inner join s_contact c 
on o.contact_id = c.row_id 
 
 
where (NVL(o.X_RENEWAL_RELATED, 'N') = 'Y' or ot.name = 'Renew') 
and o.status_cd != 'Revised' 
 
group by c.row_id  
        , o.order_num  
        , oi.order_id  
        , to_date(o.ORDER_DT) 
        , to_date(o.X_STAT_CMPLTD_DT) 
 
""") 
Order_Payment.createOrReplaceTempView("Order_Payment") 
#spark.sql('select * from Order_Payment').show(10000,False) 
Renewal_Order = spark.sql("""   
 
select    c.row_id ContactID 
        , case when c.csn is null then org.ou_num else c.csn end MemberNumber 
        , c.CON_CD as contact_type 
        , o.order_num OrderNumber 
        , oi.row_id as OrderLineItemID 
        , ot.name OrderType 
        , replace(p.name,'|',',') Product 
        , p.sub_type_cd as Product_type 
        , o.status_cd OrderStatus 
        , o.X_RENEWAL_RELATED RenewalRelated_flag 
        , o.X_PAY_BY_TERM as MPP_flag 
        , o.X_PAYMENT_STATUS as PaymentStatus 
        , oi.ACTION_CD as Action --if =add then upgrade/dwngrade/new 
        , ox.ATTRIB_04 as UpgradeDowngrade_flag 
        , to_date(o.ORDER_DT) as RenewalDueDate 
        , py.PaymentDate 
        , to_date(o.X_STAT_CMPLTD_DT) as OrderCompletionDate  
 
 
from s_order o 
 
inner join s_order_type ot 
on o.order_type_id = ot.row_id 
 
inner join s_order_item oi 
on o.row_id = oi.order_id 
--and oi.ACTION_CD in ('Update','Add') --when they renew only update line but add is upgrade/downgrade and new 
 
left join s_order_item_x ox 
on oi.row_id = ox.par_row_id 
 
inner join s_prod_int p 
on oi.prod_id = p.row_id 
and p.sub_type_cd in ('Non-RSA','Add-on','RSA') 
and p.prod_cd = 'Product' 
 
left join Order_Payment py 
on o.order_num = py.OrderNumber 
 
inner join s_contact c 
on o.contact_id = c.row_id 
 
left join s_org_ext org 
on o.accnt_id = org.row_id 
 
where (NVL(o.X_RENEWAL_RELATED, 'N') = 'Y' or ot.name = 'Renew') 
and o.status_cd != 'Revised' 
and o.ORDER_DT between '2021-09-01' and '2021-09-30' 
--and p.name like 'day' 
 
""") 
Renewal_Order.createOrReplaceTempView("Renewal_Order") 
#spark.sql('select * from Renewal_Order').show(100000,False) 
Renewals_summary = spark.sql(""" 
select    ContactID 
        , MemberNumber 
        , contact_type 
        , OrderNumber 
        , OrderLineItemID 
        , OrderType 
        , Product 
        , Product_type 
        , OrderStatus 
        , PaymentStatus 
        , Action --if =add then upgrade/dwngrade/new 
        , UpgradeDowngrade_flag 
        , RenewalDueDate 
        , PaymentDate 
        , OrderCompletionDate 
        , RenewalRelated_flag 
        , MPP_flag 
         
from Renewal_Order 
where contact_type!='Business Contact' 
--where RenewalDueDate ='2019-02-01' 
""") 
Renewals_summary.createOrReplaceTempView("Renewals_summary") 
spark.sql('select * from Renewals_summary').show(10000) 
spark.sql(''' 
select * from renewals_summary where ordernumber='32674673162' 
''').show() 
spark.sql("""select RenewalDueDate, MPP_flag 
                    , count(distinct OrderLineItemID) RenewalsDue 
                    , count(distinct(case when POT = 'Y' then OrderLineItemID end)) as POT 
                    , count(distinct(case when Renewed = 'Y' then OrderLineItemID end)) as TotalRenewed 
                    , count(distinct(case when NewAdded = 'Y' then OrderLineItemID end)) as NewSubsAdded 
                    , count(distinct MemberNumber) MembersDue 
                    , count(distinct(case when POT = 'Y' then MemberNumber end)) as POT_Members 
                    , count(distinct(case when Renewed = 'Y' then MemberNumber end)) as TotalRenewed_Members 
                    -- , count(distinct(case when MPP_flag = 'Y' then MemberNumber end)) as MPP_Members 
            from Renewals_summary  
            where contact_type !='Business Contact' 
            -- and RenewalDueDate = '' 
            group by RenewalDueDate, MPP_flag 
""").show(1000,False) 
spark.sql("""select RenewalDueDate 
                    , count(distinct OrderLineItemID) RenewalsDue 
                    , sum(case when POT = 'Y' then 1 else 0 end) POT 
                    , sum(case when Renewed = 'Y' then 1 else 0 end) TotalRenewed 
                    , sum(case when MPP_flag = 'Y' then 1 else 0 end) MPP 
                    ,sum(case when NewAdded = 'Y' then 1 else 0 end) NewAdded  
            from Renewals_summary where contact_type !='Business Contact' 
            group by RenewalDueDate 
""").show(1000,False) 
Renewal_Orders = spark.sql("""   
 
select    c.row_id ContactID 
        , case when c.csn is null then org.ou_num else c.csn end MemberNumber 
        , c.CON_CD as contact_type 
        , o.order_num OrderNumber 
        , oi.row_id as OrderLineItemID 
        , ot.name OrderType 
        , replace(p.name,'|',',') Product 
        , p.sub_type_cd as Product_type 
        , o.status_cd OrderStatus 
        , o.X_RENEWAL_RELATED RenewalRelated_flag 
        , o.X_PAY_BY_TERM as MPP_flag 
        , o.X_PAYMENT_STATUS as PaymentStatus 
        , oi.ACTION_CD as Action --if =add then upgrade/dwngrade/new 
        , ox.ATTRIB_04 as UpgradeDowngrade_flag 
        , to_date(o.ORDER_DT) as RenewalDueDate 
        , py.PaymentDate 
        , to_date(o.X_STAT_CMPLTD_DT) as OrderCompletionDate  
 
 
from s_order o 
 
inner join s_order_type ot 
on o.order_type_id = ot.row_id 
 
inner join s_order_item oi 
on o.row_id = oi.order_id 
and oi.ACTION_CD in ('Update','Add') --when they renew only update line but add is upgrade/downgrade and new 
 
left join s_order_item_x ox 
on oi.row_id = ox.par_row_id 
 
inner join s_prod_int p 
on oi.prod_id = p.row_id 
and p.sub_type_cd in ('Non-RSA','Add-on','RSA') 
and p.prod_cd = 'Product' 
 
left join Order_Payment py 
on o.order_num = py.OrderNumber 
 
inner join s_contact c 
on o.contact_id = c.row_id 
 
left join s_org_ext org 
on o.accnt_id = org.row_id 
 
where (NVL(o.X_RENEWAL_RELATED, 'N') = 'Y' or ot.name = 'Renew') 
and o.status_cd != 'Revised' 
and o.ORDER_DT between '2021-07-01' and '2021-08-18' 
--and p.name like 'day' 
 
""") 
Renewal_Orders.createOrReplaceTempView("Renewal_Orders") 
#spark.sql('select * from Renewal_Orders').show(100000,False) 
#Renewals_summary =  
spark.sql(""" 
select   RenewalDueDate 
        , count(OrderLineItemID) 
         
from Renewal_Order 
group by 1 
order by 1 
""").show(10000) 
#Renewals_summary.createOrReplaceTempView("Renewals_summary") 
#spark.sql('select * from Renewals_summary').show(10000) 
SELECT max(Created) from gms.s_order 
spark.sql(''' 
select to_date(order_dt) ,count(*) from gms.s_order 
where order_dt between '2021-09-01' and '2021-09-30' 
group by 1 
order by 1 
''').show(1000,False) 
spark.sql(''' 
select to_date(created) ,count(*) from gms.s_order 
where to_date(created) between '2021-08-01' and '2021-08-30' 
group by 1 
order by 1 
''').show(10000,False) 
The acquisition needs to look at the product they lapsed from and also how long ago they lapsed 
SELECT * FROM gms.s_order 
where order_num = '23879158989' 
 
--|1-FU5-3159|   823056101|23879158989|      1-B4BFMGH|    Renew|        Classic Care|         RSA|     Complete|   Reconciled|Update|                 null|    2019-02-01| 2019-02-05|        Renew|       N|                  Y|       Y| 
select from_utc_timestamp(actl_pay_dt,'AEST') actl_pay_dt_conv , actl_pay_dt , from_utc_timestamp(txn_dt,'AEST') txn_dt_conv , txn_dt  
from gms.s_src_payment 
where order_id = '1-B4BFMGH' 
 
spark.sql(""" 
 
select actl_pay_dt , txn_dt  
from gms.s_src_payment 
where order_id = '1-B4BFMGH' 
 
""").show() 
spark.sql("""select * from Renewal_Order where PaymentDate < OrderCompletionDate """).count() 