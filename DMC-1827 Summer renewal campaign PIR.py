SELECT * FROM aggregator.campaign_summary_table 
where (campaign_name) = 'Consumer_Renewal_Summer_EarlyPayers_wave1and2_20191202' 
--and lower(campaign_name) like '%renewal%' 
SELECT contact_id,count(*) FROM omc.member_campaign_summary_table 
where (campaign_name) = 'Consumer_Renewal_Summer_EarlyPayers_wave1and2_20191202' 
and holdoutgroup='Y' 
--and lower(campaign_name) like '%renewal%' 
GROUP BY contact_id 
order by 2 desc 
SELECT  DISTINCT  to_date(sendtime) 
--max(sendtime), min(sendtime)  
 
FROM omc.member_campaign_summary_table 
where (campaign_name) = 'Consumer_Renewal_Summer_EarlyPayers_wave1and2_20191202' 
and holdoutgroup='Y' 
--and lower(campaign_name) like '%renewal%' 
--and contact_id='1-HTY-1943' 
SELECT holdoutgroup, count(DISTINCT contact_id) FROM omc.member_campaign_summary_table 
where (campaign_name) = 'Consumer_Renewal_Summer_EarlyPayers_wave1and2_20191202' 
--and holdoutgroup='N' 
and to_date(sendtime)='2019-12-01' 
GROUP BY holdoutgroup 
SELECT contact_id 
        , account_id 
        , holdoutgroup 
        , opentoclick, case when opentoclick like 'Opened%' then True ELSE false end open_rate 
        , to_date(sendtime) as send_time 
         
--count(*), count(riid), count(contact_id)  
FROM omc.member_campaign_summary_table 
 
where (campaign_name) = 'Consumer_Renewal_Summer_EarlyPayers_wave1and2_20191202' 
and to_date(sendtime)='2019-12-01' 
select distinct status_cd from gms.s_order 
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
 
select    c.row_id ContactID 
        , c.csn as MemberNumber 
        , c.CON_CD as contact_type 
        , c.CUST_VALUE_CD as loyaltycolor 
        , cx.attrib_55  AS Colour_Plus 
        , o.order_num OrderNumber 
        , oi.order_id as OrderLineItemID 
        , ot.name OrderType 
        , replace(p.name,'|',',') Product 
        , p.sub_type_cd as Product_type 
        , o.status_cd OrderStatus 
        , o.X_RENEWAL_RELATED RenewalRelated_flag 
        , o.X_PAY_BY_TERM as MPP_flag 
        , o.X_PAYMENT_STATUS as PaymentStatus 
       -- , oi.ACTION_CD as Action --if =add then upgrade/dwngrade/new 
       -- , ox.ATTRIB_04 as UpgradeDowngrade_flag 
        , to_date(o.ORDER_DT) as RenewalDueDate 
        , to_date(py.txn_dt) as PaymentDate 
        , to_date(o.X_STAT_CMPLTD_DT) as OrderCompletionDate 
        , cam.* 
 
 
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
and p.sub_type_cd in ('Non-RSA','Add-on','RSA') 
and p.prod_cd = 'Product' 
 
left join s_src_payment py 
on o.row_id = py.order_id 
 
inner join s_contact c 
on o.contact_id = c.row_id 
 
inner join gms.s_contact_x cx 
on c.row_id = cx.par_row_id 
 
inner join (SELECT contact_id 
        , account_id 
        , holdoutgroup 
        , opentoclick, case when opentoclick like 'Opened%' then True ELSE false end open_rate 
        , to_date(sendtime) as send_time 
         
FROM omc.member_campaign_summary_table 
 
where (campaign_name) = 'Consumer_Renewal_Summer_EarlyPayers_wave1and2_20191202' 
and to_date(sendtime)='2019-12-01') cam 
on cam.contact_id=c.row_id 
 
 
where (NVL(o.X_RENEWAL_RELATED, 'N') = 'Y' 
or ot.name = 'Renew') 
and o.status_cd != 'Revised' 
 
""") 
Renewal_Order.createOrReplaceTempView("Renewal_Order") 
spark.sql('select * from Renewal_Order').count() 
spark.sql('select * from Renewal_Order').show(10,False) 
Renewals_summary = spark.sql(""" 
select  distinct  ContactID 
        , MemberNumber 
        , contact_type 
        , loyaltycolor 
        , Colour_Plus 
        , OrderNumber 
        , OrderLineItemID 
        , OrderType 
        , Product 
        , Product_type 
        , OrderStatus 
        , PaymentStatus 
        , RenewalDueDate 
        , PaymentDate 
        , OrderCompletionDate 
        , case when OrderCompletionDate<=date_add(RenewalDueDate,2) then 'Y' else 'N' end POT  
        , case when OrderStatus = 'Complete' then 'Y' else 'N' end Renewed 
        , RenewalRelated_flag 
        , holdoutgroup 
        , opentoclick 
        , open_rate 
        , send_time 
         
from Renewal_Order  
where RenewalDueDate between '2019-11-26' and '2019-12-29' 
and orderstatus!='Cancelled' 
and MPP_flag='N' 
""") 
Renewals_summary.createOrReplaceTempView("Renewals_summary") 
spark.sql("""select * from Renewals_summary""").show(10000) 
Renewals_summary_1 = spark.sql(""" 
select  distinct  ContactID 
        , MemberNumber 
        , contact_type 
        , loyaltycolor 
        , Colour_Plus 
        , OrderNumber 
        , OrderLineItemID 
        , OrderType 
        , Product 
        , Product_type 
        , OrderStatus 
        , PaymentStatus 
        , RenewalDueDate 
        , PaymentDate 
        , OrderCompletionDate 
        , case when PaymentStatus in ('Reconciled','Payment Taken')and PaymentDate<=RenewalDueDate then 'Y' else 'N' end POT  
        , case when OrderStatus = 'Complete' then 'Y' else 'N' end Renewed 
        , RenewalRelated_flag 
        , holdoutgroup 
        , opentoclick 
        , open_rate 
         
from Renewal_Order  
where RenewalDueDate between '2019-11-26' and '2019-12-29' 
and orderstatus!='Cancelled' 
and MPP_flag='N' 
""") 
Renewals_summary_1.createOrReplaceTempView("Renewals_summary_1") 
spark.sql("""select * from Renewals_summary_1""").show(10000) 
spark.sql('select holdoutgroup, count(distinct contactid) from Renewals_summary group by holdoutgroup').show() 
spark.sql("""select holdoutgroup 
                    , count(distinct OrderLineItemID) RenewalsDue 
                    , count(distinct(case when POT = 'Y' then OrderLineItemID end)) as POT 
                    , count(distinct(case when Renewed = 'Y' then OrderLineItemID end)) as TotalRenewed 
                    , count(distinct MemberNumber) MembersDue 
                    , count(distinct(case when POT = 'Y' then MemberNumber end)) as POT_Members 
                    , count(distinct(case when Renewed = 'Y' then MemberNumber end)) as TotalRenewed_Members 
                    --, count(distinct(case when MPP_flag = 'Y' then MemberNumber end)) as MPP_Members 
            from Renewals_summary  
            where contact_type !='Business Contact' 
            -- and RenewalDueDate = '' 
            group by holdoutgroup 
""").show(1000,False) 
spark.sql("""select holdoutgroup 
                    , count(distinct OrderLineItemID) RenewalsDue 
                    , count(distinct(case when POT = 'Y' then OrderLineItemID end)) as POT 
                    , count(distinct(case when Renewed = 'Y' then OrderLineItemID end)) as TotalRenewed 
                    , count(distinct MemberNumber) MembersDue 
                    , count(distinct(case when POT = 'Y' then MemberNumber end)) as POT_Members 
                    , count(distinct(case when Renewed = 'Y' then MemberNumber end)) as TotalRenewed_Members 
                    --, count(distinct(case when MPP_flag = 'Y' then MemberNumber end)) as MPP_Members 
            from Renewals_summary_1  
            where contact_type !='Business Contact' 
            --and MPP_flag='N' 
            -- and RenewalDueDate = '' 
            group by holdoutgroup 
""").show(1000,False) 
spark.sql("""select loyaltycolor 
                    , count(distinct OrderLineItemID) RenewalsDue 
                    , count(distinct(case when POT = 'Y' then OrderLineItemID end)) as POT 
                    , count(distinct(case when Renewed = 'Y' then OrderLineItemID end)) as TotalRenewed 
                    , count(distinct MemberNumber) MembersDue 
                    , count(distinct(case when POT = 'Y' then MemberNumber end)) as POT_Members 
                    , count(distinct(case when Renewed = 'Y' then MemberNumber end)) as TotalRenewed_Members 
                    --, count(distinct(case when MPP_flag = 'Y' then MemberNumber end)) as MPP_Members 
            from Renewals_summary  
            where contact_type !='Business Contact' 
            and holdoutgroup='N' 
            group by loyaltycolor 
""").show(1000,False) 
spark.sql("""select loyaltycolor 
                    , count(distinct OrderLineItemID) RenewalsDue 
                    , count(distinct(case when POT = 'Y' then OrderLineItemID end)) as POT 
                    , count(distinct(case when Renewed = 'Y' then OrderLineItemID end)) as TotalRenewed 
                    , count(distinct MemberNumber) MembersDue 
                    , count(distinct(case when POT = 'Y' then MemberNumber end)) as POT_Members 
                    , count(distinct(case when Renewed = 'Y' then MemberNumber end)) as TotalRenewed_Members 
                    --, count(distinct(case when MPP_flag = 'Y' then MemberNumber end)) as MPP_Members 
            from Renewals_summary  
            where contact_type !='Business Contact' 
            and holdoutgroup='Y' 
            group by loyaltycolor 
""").show(1000,False) 
spark.sql("""select POT, Colour_Plus,count(distinct membernumber) 
            from Renewals_summary  
            where holdoutgroup='N' 
            and contact_type !='Business Contact' 
            group by POT, Colour_Plus 
            order by 1,2 
            
""").show(1000,False) 
spark.sql("""select POT, loyaltycolor,count(distinct membernumber) 
            from Renewals_summary  
            where holdoutgroup='N' 
            group by POT, loyaltycolor 
            order by 1,2 
            
""").show(1000,False) 
spark.sql(""" 
 
select case when m.partner like 'NRMA Parks and Resorts%' or m.partner like 'NRMA Holiday Park%' then 'NRMA Parks and Resorts'  
             when m.partner in ('IAG','NRMA Multi Policy Discount','NRMA MPD Insurance','NRMA Insurance','NRMA Insurance ControlPro','NRMA Insurance Branch','NRMA Branch') then 'IAG' 
             else m.partner end NRMA_benefit, count(distinct membernumber) from Renewals_summary a 
 
inner join m4m.return_feed_header m 
on m.member_number=a.membernumber 
and a.send_time<to_date(m.time_stamp) 
 
where holdoutgroup='N' 
and POT='Y' 
 
group by case when m.partner like 'NRMA Parks and Resorts%' or m.partner like 'NRMA Holiday Park%' then 'NRMA Parks and Resorts'  
             when m.partner in ('IAG','NRMA Multi Policy Discount','NRMA MPD Insurance','NRMA Insurance','NRMA Insurance ControlPro','NRMA Insurance Branch','NRMA Branch') then 'IAG' 
             else m.partner end 
order by 2 desc 
""").show(10000, False) 
spark.sql(""" 
 
select case when m.partner like 'NRMA Parks and Resorts%' or m.partner like 'NRMA Holiday Park%' then 'NRMA Parks and Resorts'  
             when m.partner in ('IAG','NRMA Multi Policy Discount','NRMA MPD Insurance','NRMA Insurance','NRMA Insurance ControlPro','NRMA Insurance Branch','NRMA Branch') then 'IAG' 
             else m.partner end NRMA_benefit, count(distinct membernumber) from Renewals_summary a 
 
inner join m4m.return_feed_header m 
on m.member_number=a.membernumber 
and a.send_time<to_date(m.time_stamp) 
 
where holdoutgroup='N' 
and POT='N' 
 
group by case when m.partner like 'NRMA Parks and Resorts%' or m.partner like 'NRMA Holiday Park%' then 'NRMA Parks and Resorts'  
             when m.partner in ('IAG','NRMA Multi Policy Discount','NRMA MPD Insurance','NRMA Insurance','NRMA Insurance ControlPro','NRMA Insurance Branch','NRMA Branch') then 'IAG' 
             else m.partner end 
order by 2 desc 
""").show(10000, False) 
spark.sql(""" 
 
select case when m.partner like 'NRMA Parks and Resorts%' or m.partner like 'NRMA Holiday Park%' then 'NRMA Parks and Resorts'  
             when m.partner in ('IAG','NRMA Multi Policy Discount','NRMA MPD Insurance','NRMA Insurance','NRMA Insurance ControlPro','NRMA Insurance Branch','NRMA Branch') then 'IAG' 
             else m.partner end NRMA_benefit, count(distinct membernumber) from Renewals_summary a 
 
inner join m4m.return_feed_header m 
on m.member_number=a.membernumber 
and a.send_time<to_date(m.time_stamp) 
 
where holdoutgroup='N' 
--and POT='N' 
 
group by case when m.partner like 'NRMA Parks and Resorts%' or m.partner like 'NRMA Holiday Park%' then 'NRMA Parks and Resorts'  
             when m.partner in ('IAG','NRMA Multi Policy Discount','NRMA MPD Insurance','NRMA Insurance','NRMA Insurance ControlPro','NRMA Insurance Branch','NRMA Branch') then 'IAG' 
             else m.partner end 
order by 2 desc 
""").show(10000, False) 
spark.sql(""" 
 
select count(distinct membernumber) from Renewals_summary a 
 
inner join m4m.return_feed_header m 
on m.member_number=a.membernumber 
and a.send_time<to_date(m.time_stamp) 
 
where holdoutgroup='N' 
and POT='Y' 
and m.partner not in ('IAG','NRMA Multi Policy Discount','NRMA MPD Insurance','NRMA Insurance','NRMA Insurance ControlPro','NRMA Insurance Branch','NRMA Branch')  
""").show(10000, False) 
spark.sql(""" 
 
select count(distinct membernumber) from Renewals_summary a 
 
inner join m4m.return_feed_header m 
on m.member_number=a.membernumber 
and a.send_time<to_date(m.time_stamp) 
 
where holdoutgroup='N' 
and POT='N' 
and m.partner not in ('IAG','NRMA Multi Policy Discount','NRMA MPD Insurance','NRMA Insurance','NRMA Insurance ControlPro','NRMA Insurance Branch','NRMA Branch')  
""").show(10000, False) 
spark.sql(""" 
 
select count(distinct membernumber) from Renewals_summary a 
 
inner join m4m.return_feed_header m 
on m.member_number=a.membernumber 
and a.send_time<to_date(m.time_stamp) 
 
where holdoutgroup='N' 
--and POT='N' 
and m.partner not in ('IAG','NRMA Multi Policy Discount','NRMA MPD Insurance','NRMA Insurance','NRMA Insurance ControlPro','NRMA Insurance Branch','NRMA Branch')  
""").show(10000, False) 
spark.sql(""" 
 
select count(distinct membernumber) from Renewals_summary a 
 
inner join m4m.return_feed_header m 
on m.member_number=a.membernumber 
and a.send_time<to_date(m.time_stamp) 
 
where holdoutgroup='Y' 
--and POT='N' 
and m.partner not in ('IAG','NRMA Multi Policy Discount','NRMA MPD Insurance','NRMA Insurance','NRMA Insurance ControlPro','NRMA Insurance Branch','NRMA Branch')  
""").show(10000, False) 
select case when m.partner like 'NRMA Parks and Resorts%' or m.partner like 'NRMA Holiday Park%' then 'NRMA Parks and Resorts'  
             when m.partner in ('IAG','NRMA Multi Policy Discount','NRMA MPD Insurance','NRMA Insurance','NRMA Insurance ControlPro','NRMA Insurance Branch','NRMA Branch') then 'IAG' 
             else m.partner end NRMA_benefit 
        , count(a.contact_id)  
 
from omc.member_campaign_summary_table a 
 
inner join gms.s_contact c 
on c.row_id=a.contact_id 
 
inner join m4m.return_feed_header m 
on m.member_number=c.csn 
 
where (campaign_name) = 'Consumer_Renewal_Summer_EarlyPayers_wave1and2_20191202' 
and holdoutgroup='N' 
and a.sendtime<m.time_stamp 
and to_date(sendtime)='2019-12-01' 
 
group by case when m.partner like 'NRMA Parks and Resorts%' or m.partner like 'NRMA Holiday Park%' then 'NRMA Parks and Resorts'  
             when m.partner in ('IAG','NRMA Multi Policy Discount','NRMA MPD Insurance','NRMA Insurance','NRMA Insurance ControlPro','NRMA Insurance Branch','NRMA Branch') then 'IAG' 
             else m.partner end 
order by 2 desc 
select count(a.contact_id), a.sendtime, m.time_stamp 
 
from omc.member_campaign_summary_table a 
 
inner join gms.s_contact c 
on c.row_id=a.contact_id 
 
inner join m4m.return_feed_header m 
on m.member_number=c.csn 
 
where (campaign_name) = 'Consumer_Renewal_Summer_EarlyPayers_wave1and2_20191202' 
and holdoutgroup='N' 
and to_date(sendtime)='2019-12-01' 
and a.sendtime<m.time_stamp 
select open_rate, count(distinct contactid) 
from aa_dmc1827_summerholiday_pot_20200302 
where POT='Y' 
and holdoutgroup='N' 
 
group by open_rate 
select open_rate, count(distinct contactid) 
from aa_dmc1827_summerholiday_pot_20200302 
where holdoutgroup='N' 
 
group by open_rate 
select open_rate, count(distinct contactid) 
from aa_dmc1827_summerholiday_pot_20200302 
where POT='Y' 
and holdoutgroup='N' 
 
group by open_rate 
spark.sql(""" 
 
select count(distinct membernumber) from campaign_data.aa_dmc1827_summerholiday_pot_20200302 a 
 
inner join m4m.return_feed_header m 
on m.member_number=a.membernumber 
and a.send_time<to_date(m.time_stamp) 
 
where holdoutgroup='Y' 
and POT='N' 
and m.partner not in ('IAG','NRMA Multi Policy Discount','NRMA MPD Insurance','NRMA Insurance','NRMA Insurance ControlPro','NRMA Insurance Branch','NRMA Branch')  
""").show(10000, False) 
spark.sql(""" 
 
select count(distinct membernumber) from campaign_data.aa_dmc1827_summerholiday_pot_20200302 a 
 
inner join m4m.return_feed_header m 
on m.member_number=a.membernumber 
and a.send_time<to_date(m.time_stamp) 
 
where holdoutgroup='N' 
and POT='N' 
and m.partner not in ('IAG','NRMA Multi Policy Discount','NRMA MPD Insurance','NRMA Insurance','NRMA Insurance ControlPro','NRMA Insurance Branch','NRMA Branch')  
""").show(10000, False)