SELECT min(c.send_event_date) 
 
from omc.send_level_summary c 
 
where c.campaign_id in ('57441282','57438802') 
--and c.bounce_event_date is null 
--and (c.open_event_date is not null or c.click_event_date is not null) 
--and c.click_event_date is not null 
--and c.unsubbed_event_date is null 
-- GROUP BY 1,2 
-- ORDER BY 1,2 
--ORDER BY 3 DESC 
SELECT case when c.campaign_id='57441282' then 'QBR' 
            when c.campaign_id='57438802' then 'Non QBR' 
            else 'Other' end Segment 
        , c.control 
        , count(DISTINCT c.customer_id) 
 
from omc.send_level_summary c 
 
where c.campaign_id in ('57441282','57438802') 
--and c.bounce_event_date is null 
--and (c.open_event_date is not null or c.click_event_date is not null) 
--and c.click_event_date is not null 
--and c.unsubbed_event_date is null 
GROUP BY 1,2 
ORDER BY 1,2 
--ORDER BY 3 DESC 
SELECT case when c.campaign_id='57441282' then 'QBR' 
            when c.campaign_id='57438802' then 'Non QBR' 
            else 'Other' end Segment 
        --, c.clicked_elements 
        , count(DISTINCT c.customer_id) 
 
from omc.send_level_summary c 
 
where c.campaign_id in ('57441282','57438802') 
and c.bounce_event_date is null 
--and (c.open_event_date is not null or c.click_event_date is not null) 
and c.click_event_date is not null 
and c.unsubbed_event_date is null 
GROUP BY 1 
ORDER BY 1 
--ORDER BY 3 DESC 
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
Order_Payment = spark.sql("""   
 
select    c.row_id ContactID 
        , o.order_num OrderNumber 
        , oi.order_id as OrderLineItemID 
        , to_date(o.ORDER_DT) as RenewalDueDate 
        , min(to_date(COALESCE(py.received_dt,py.txn_dt))) as PaymentDate 
        , to_date(o.X_STAT_CMPLTD_DT) as OrderCompletionDate  
 
 
from s_order o 
 
inner join s_order_type ot 
on o.order_type_id = ot.row_id 
 
inner join s_order_item oi 
on o.row_id = oi.order_id 
and oi.ACTION_CD in ('Update','Add') --when they renew only update line but add is upgrade/downgrade and new 
 
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
spark.sql('select * from Order_Payment').show(10000,False) 
SELECT to_date(order_dt) from gms.s_order limit 100; 
 
--2021-05-11 
-- SELECT DISTINCT to_date(e.send_event_date) 
-- from omc.campaign_info c 
-- INNER JOIN omc.send_level_summary e 
-- on e.campaign_id=c.campaign_id 
-- where c.campaign_id in ('57441282','57438802') 
SELECT DISTINCT p.sub_type_cd from gms.s_prod_int p 
Renewal_Order_working = spark.sql("""   
 
select distinct c.row_id ContactID 
        , org.row_id AS ACCOUNTID 
        , case when c.csn is null then org.ou_num else c.csn end MemberNumber 
        , c.CON_CD as contact_type 
        , o.order_num OrderNumber 
        , oi.order_id as OrderLineItemID 
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
        , e.campaign_id 
        , e.control 
 
 
from omc.send_level_summary e 
 
inner join s_contact c 
on e.customer_id=c.row_id 
 
left JOIN s_order o 
on o.contact_id = c.row_id 
and o.status_cd != 'Revised' 
and o.X_STAT_CMPLTD_DT between e.send_event_date and '2021-07-05' 
--and order_dt>e.send_event_date 
 
left join s_org_ext org 
on o.accnt_id = org.row_id 
 
left join s_order_type ot 
on o.order_type_id = ot.row_id 
--and (NVL(o.X_RENEWAL_RELATED, 'N') = 'Y' or ot.name = 'Renew') 
 
left join s_order_item oi 
on o.row_id = oi.order_id 
and oi.ACTION_CD in ('Update','Add') --when they renew only update line but add is upgrade/downgrade and new 
 
left join s_order_item_x ox 
on oi.row_id = ox.par_row_id 
 
left join s_prod_int p 
on oi.prod_id = p.row_id 
and p.sub_type_cd in ('Non-RSA','Add-on','RSA') 
and p.prod_cd = 'Product' 
 
left join Order_Payment py 
on o.order_num = py.OrderNumber 
 
where 1=1 
and e.campaign_id in ('57441282','57438802') 
 
""") 
Renewal_Order_working.createOrReplaceTempView("Renewal_Order_working") 
spark.sql('select * from Renewal_Order_working').count() 
spark.sql(""" 
select  case when c.campaign_id='57441282' then 'QBR' 
            when c.campaign_id='57438802' then 'Non QBR' 
            else 'Other' end Segment 
        , c.control 
        , count(DISTINCT c.ContactID) 
 
from Renewal_Order_working c 
group by 1,2 
order by 1,2 
""").show(1000,False) 
Renewals_summary_working = spark.sql(""" 
select    ContactID 
        , MemberNumber 
        , campaign_id 
        , control 
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
        , case when PaymentStatus in ('Reconciled','Payment Taken') and PaymentDate<=RenewalDueDate then 'Y' else 'N' end POT 
        , case when PaymentStatus in ('Reconciled','Payment Taken') then 'Y' else 'N' end Renewed 
        , case when Action = 'Add' and UpgradeDowngrade_flag is NULL then 'Y' else 'N' end NewAdded 
        , RenewalRelated_flag 
         
from Renewal_Order_working 
 
""") 
Renewals_summary_working.createOrReplaceTempView("Renewals_summary_working") 
spark.sql('select * from Renewals_summary_working').show(10000) 
spark.sql("""select campaign_id 
                    , control 
                    , count(distinct contactid) Members 
                    , count(distinct OrderLineItemID) RenewalsDue 
                    , count(distinct(case when POT = 'Y' then OrderLineItemID end)) as POT 
                    , count(distinct(case when Renewed = 'Y' then OrderLineItemID end)) as TotalRenewed 
                    , count(distinct(case when NewAdded = 'Y' then OrderLineItemID end)) as NewSubsAdded 
                     
            from Renewals_summary_working  
            group by 1,2 
""").show(1000,False) 
SELECT * from sandpit.renewal_base 
where contact_cd='Business Contact' 
and prod_sub_type='Annual Fee' 
and account_id='1-17C-1353' 
Renewal_Order = spark.sql("""   
 
select distinct c.row_id ContactID 
        , org.row_id AS ACCOUNTID 
        , case when c.csn is null then org.ou_num else c.csn end MemberNumber 
        , c.CON_CD as contact_type 
        , o.order_num OrderNumber 
        , oi.order_id as OrderLineItemID 
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
        , e.campaign_id 
        , e.control 
 
 
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
 
INNER JOIN omc.send_level_summary e 
on e.customer_id=c.row_id 
and e.campaign_id in ('57441282','57438802') 
and order_dt>e.send_event_date 
 
where (NVL(o.X_RENEWAL_RELATED, 'N') = 'Y' or ot.name = 'Renew') 
and o.status_cd != 'Revised' 
and o.ORDER_DT between e.send_event_date and '2021-06-30' 
 
""") 
Renewal_Order.createOrReplaceTempView("Renewal_Order") 
spark.sql('select * from Renewal_Order').count() 
spark.sql(""" 
select  case when c.campaign_id='57441282' then 'QBR' 
            when c.campaign_id='57438802' then 'Non QBR' 
            else 'Other' end Segment 
        , c.control 
        , count(DISTINCT c.ContactID) 
 
from Renewal_Order c 
group by 1,2 
order by 1,2 
""").show(1000,False) 
spark.sql(""" 
select  case when c.campaign_id='57441282' then 'QBR' 
            when c.campaign_id='57438802' then 'Non QBR' 
            else 'Other' end Segment 
        , c.control 
        , count(DISTINCT c.ContactID) 
 
from Renewal_Order_2 c 
group by 1,2 
order by 1,2 
""").show(1000,False) 
Renewals_summary = spark.sql(""" 
select    ContactID 
        , MemberNumber 
        , campaign_id 
        , control 
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
        , case when PaymentStatus in ('Reconciled','Payment Taken') and PaymentDate<=RenewalDueDate then 'Y' else 'N' end POT 
        , case when PaymentStatus in ('Reconciled','Payment Taken') then 'Y' else 'N' end Renewed 
        , case when Action = 'Add' and UpgradeDowngrade_flag is NULL then 'Y' else 'N' end NewAdded 
        , RenewalRelated_flag 
        , MPP_flag 
         
from Renewal_Order 
 
""") 
Renewals_summary.createOrReplaceTempView("Renewals_summary") 
spark.sql('select * from Renewals_summary').show(10000) 
 
spark.sql('select * from Renewals_summary where UpgradeDowngrade_flag is not null ').show(10000) 
spark.sql("""select campaign_id 
                    , control 
                    , count(distinct OrderLineItemID) RenewalsDue 
                    , count(distinct(case when POT = 'Y' then OrderLineItemID end)) as POT 
                    , count(distinct(case when Renewed = 'Y' then OrderLineItemID end)) as TotalRenewed 
                    , count(distinct(case when NewAdded = 'Y' then OrderLineItemID end)) as NewSubsAdded 
                    , count(distinct MemberNumber) MembersDue 
                    , count(distinct(case when POT = 'Y' then MemberNumber end)) as POT_Members 
                    , count(distinct(case when Renewed = 'Y' then MemberNumber end)) as TotalRenewed_Members 
                    -- , count(distinct(case when MPP_flag = 'Y' then MemberNumber end)) as MPP_Members 
            from Renewals_summary  
            group by 1,2 
""").show(1000,False) 
Renewal_Order_2 = spark.sql("""   
 
select    c.row_id ContactID 
        , case when c.csn is null then org.ou_num else c.csn end MemberNumber 
        , c.CON_CD as contact_type 
        , o.order_num OrderNumber 
        , oi.order_id as OrderLineItemID 
        , ot.name OrderType 
        , replace(p.name,'|',',') Product 
        , p.sub_type_cd as Product_type 
        , o.status_cd OrderStatus 
        , o.X_RENEWAL_RELATED RenewalRelated_flag 
        , o.X_PAYMENT_STATUS as PaymentStatus 
        , to_date(o.ORDER_DT) as RenewalDueDate 
        , py.PaymentDate 
        , to_date(o.X_STAT_CMPLTD_DT) as OrderCompletionDate  
        , e.campaign_id 
        , e.control 
 
 
from s_order o 
 
left anti join Renewal_Order ro 
on ro.OrderNumber=o.order_num 
and ro.ACCOUNTID=o.accnt_id 
 
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
 
INNER JOIN omc.send_level_summary e 
on e.customer_id=c.row_id 
and e.campaign_id in ('57441282','57438802') 
 
where oi.ACTION_CD='Add' 
and lower(ot.name) = 'new' 
and lower(o.status_cd) like '%complete%' 
and o.X_STAT_CMPLTD_DT between send_event_date and '2021-07-05' 
and ox.ATTRIB_04 is null 
 
""") 
Renewal_Order_2.createOrReplaceTempView("Renewal_Order_2") 
spark.sql('select * from Renewal_Order_2').count() 
spark.sql('select * from Renewal_Order_2').show(10000,False) 
Renewals_summary_2 = spark.sql(""" 
select    ContactID 
        , MemberNumber 
        , campaign_id 
        , control 
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
        , case when PaymentStatus in ('Reconciled','Payment Taken') and PaymentDate<=RenewalDueDate then 'Y' else 'N' end POT 
        , case when PaymentStatus in ('Reconciled','Payment Taken') then 'Y' else 'N' end Renewed 
        , case when Action = 'Add' and UpgradeDowngrade_flag is NULL then 'Y' else 'N' end NewAdded 
        , RenewalRelated_flag 
         
from Renewal_Order_2 
 
""") 
Renewals_summary_2.createOrReplaceTempView("Renewals_summary_2") 
spark.sql('select * from Renewals_summary_2').show(10000) 
spark.sql("""select * from Renewals_summary_2 where RenewalRelated_flag='N' """).show(10000) 
spark.sql("""select campaign_id 
                    , control 
                    , count(distinct OrderLineItemID) RenewalsDue 
                    --, count(distinct(case when POT = 'Y' then OrderLineItemID end)) as POT 
                    --, count(distinct(case when Renewed = 'Y' then OrderLineItemID end)) as TotalRenewed 
                    , count(distinct(case when NewAdded = 'Y' then OrderLineItemID end)) as NewSubsAdded 
                    , count(distinct MemberNumber) MembersDue 
                    --, count(distinct(case when POT = 'Y' then MemberNumber end)) as POT_Members 
                    --, count(distinct(case when Renewed = 'Y' then MemberNumber end)) as TotalRenewed_Members 
                    -- , count(distinct(case when MPP_flag = 'Y' then MemberNumber end)) as MPP_Members 
            from Renewals_summary_2  
            --where RenewalRelated_flag='N' 
            group by 1,2 
""").show(1000,False) 
spark.sql(""" 
select  case when c.campaign_id='57441282' then 'QBR' 
            when c.campaign_id='57438802' then 'Non QBR' 
            else 'Other' end Segment 
        , c.control 
        , count(DISTINCT c.ContactID) 
 
from Renewal_Order c 
group by 1,2 
order by 1,2 
""").show(1000,False) 
spark.sql(''' 
WITH 
    join_orders AS ( 
        SELECT 
            org.row_id AS acc_id, 
            ord.order_dt, 
            RANK() OVER(PARTITION BY org.row_id ORDER BY ord.order_dt DESC) AS idx 
         
        FROM 
            gms.s_order AS ord 
         
        LEFT JOIN 
            gms.s_order_type AS ordt 
            ON ord.order_type_id = ordt.row_id 
         
        LEFT JOIN 
            gms.s_order_item AS ordi 
            ON ord.row_id = ordi.order_id 
         
        LEFT JOIN 
            gms.s_prod_int AS prod 
            ON ordi.prod_id = prod.row_id 
         
        LEFT JOIN 
            gms.s_org_ext AS org 
            ON ord.accnt_id = org.row_id 
         
        WHERE 1=1 
            AND ordt.name IN ('New') 
            AND ordi.action_cd IN ('Add') 
            AND ord.status_cd = 'Complete' 
            AND org.ou_type_cd IN ('Member Organisation') 
            AND prod.name IN ('Membership') 
            AND ord.x_payment_status IN ('Reconciled', 'Payment Taken', 'Not Required') 
            AND COALESCE(market_class_cd, '') NOT LIKE 'ADM%' 
            AND COALESCE(market_class_cd, '') NOT LIKE '50+%' 
    ) 
 
SELECT DISTINCT 
    join_orders.acc_id, 
    MIN(join_orders.order_dt) AS order_dt 
 
FROM 
    join_orders 
     
WHERE 
    idx = 1 
     
GROUP BY 
    1 
''').createOrReplaceTempView('joined') 
spark.sql(""" 
select year(order_dt), month(order_dt), count(distinct acc_id) from joined 
group by 1,2 
order by 1,2 
""").show(10000,False)