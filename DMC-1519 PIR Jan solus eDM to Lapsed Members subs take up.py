#MotorSmart Orders (Order Item Details) 
 
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
New_Order = spark.sql("""   
 
select    c.row_id ContactID 
        , case when c.csn is null then org.ou_num else c.csn end MemberNumber 
        , c.CON_CD as contact_type 
        , o.order_num OrderNumber 
        , oi.order_id as OrderLineItemID 
        , ot.name OrderType 
        , replace(p.name,'|',',') Product 
        , p.sub_type_cd as Product_type 
        , o.status_cd OrderStatus 
        , o.X_PAY_BY_TERM as MPP_flag 
        , o.X_PAYMENT_STATUS as PaymentStatus 
        , oi.ACTION_CD as Action --if =add then upgrade/dwngrade/new 
        , ox.ATTRIB_04 as UpgradeDowngrade_flag 
        , to_date(o.ORDER_DT) as RenewalDueDate 
        , to_date(o.X_STAT_CMPLTD_DT) as OrderCompletionDate 
        , c.X_NRMA_JOIN_DT as join_date 
 
 
from s_order o 
 
inner join s_order_type ot 
on o.order_type_id = ot.row_id 
 
inner join s_order_item oi 
on o.row_id = oi.order_id 
 
left join s_order_item_x ox 
on oi.row_id = ox.par_row_id 
 
inner join s_prod_int p 
on oi.prod_id = p.row_id 
and p.sub_type_cd in ('Non-RSA','RSA') 
and p.prod_cd = 'Product' 
 
inner join s_contact c 
on o.contact_id = c.row_id 
 
left join s_org_ext org 
on o.accnt_id = org.row_id 
 
where ot.name = 'New' 
or oi.ACTION_CD in ('Add') 
 
""") 
New_Order.createOrReplaceTempView("New_Order") 
spark.sql('select * from New_Order').count() 
spark.sql("""select * from new_order n 
inner join campaign_data.yc_20190116_c_dmc1384_jansolus_lapsedmembers l 
on l.row_id = n.contactID 
where OrderCompletionDate <'2019-01-22' """).show(100) 
New_Summary = spark.sql(""" 
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
        , RenewalDueDate 
        , OrderCompletionDate 
        , MPP_flag 
        , join_date 
        , segment 
         
         
from New_Order n 
inner join campaign_data.yc_20190116_c_dmc1384_jansolus_lapsedmembers l 
on l.row_id = n.contactID 
where UpgradeDowngrade_flag is NULL 
and OrderCompletionDate > '2019-01-22' 
""") 
New_Summary.createOrReplaceTempView("New_Summary") 
spark.sql('select * from New_Summary').show(10000) 
spark.sql("""select segment, contact_type, product 
                    , count(distinct OrderNumber) Renewed_Orders 
                    , count(distinct MemberNumber) New_Members 
            from New_Summary  
            where contact_type not in ('Business Contact','Customer') 
           -- and contactid ='1-D9D-784' 
            group by segment, contact_type, product 
            order by 1,2,3,4 
""").show(1000,False) 
The acquisition needs to look at the product they lapsed from and also how long ago they lapsed 
#look at rejoin date 
#add the asset effective from date and check join date 
#look at openeded member numbers 
#number of comms receveived post 
select segment, count( distinct l.row_id) as winback 
from campaign_data.yc_20190116_c_dmc1384_jansolus_lapsedmembers l 
 
inner join gms.s_contact c 
on c.row_id=l.row_id 
where c.CON_CD = 'Ordinary Member' 
and l.segment in ('segment8','segment7') 
group by segment 
select l.row_id, c.csn, c.con_cd as winback 
from campaign_data.yc_20190116_c_dmc1384_jansolus_lapsedmembers l 
 
inner join gms.s_contact c 
on c.row_id=l.row_id 
where c.CON_CD = 'Ordinary Member' 
and l.segment in ('segment8','segment7') 
--group by segment 
SELECT count(row_id) FROM campaign_data.yc_20190116_c_dmc1384_jansolus_lapsedmembers  
where segment = 'segment7' 
and row_id in (select row_id from campaign_data.yc_20190116_c_dmc1384_jansolus_lapsedmembers  
where segment = 'segment8') 
--group by segment 
SELECT DISTINCT campaign_id, campaign_name, to_date(sendtime) FROM omc.member_campaign_summary_table  
where to_date(sendtime) between '2019-08-05' and '2019-08-31' 
and campaign_name like '%winback%' 
order by 3 
select distinct signature_id 
from omc.member_campaign_summary_table 
where program_id='37135582' 
--campaign_name='Consumer_Acquisition_winback_90_20190807' 
--and holdoutgroup='Y' 
SELECT DISTINCT signature_id, rule_name FROM omc.email_dynamic_content  
where signature_id IN ('"1503514929"','"-1301653346"','"1041487889"','"-1538547244"','"1211164154"','"539186792"','"2055748349"','"1658048944"','"1566476068"') 
limit 100 
SELECT DISTINCT substring(a.customer_id_, 2, length(a.customer_id_)-2) as contact_id 
        , case when b.holdout_id like '%Family Blue Offer%' then 1  
                when b.holdout_id like '%Mature Blue Offer%' then 2 
                when b.holdout_id like '%Blue Offer%' then 3 
                when b.holdout_id like '%Family RSA Offer%' then 4 
                when b.holdout_id like '%RSA Offer' then 5 
                when b.holdout_id like '%RSA Offer' then 6 
                when b.holdout_id like '% No offer%' then 7 
                when b.holdout_id like '% No offer%' then 8 
                when b.holdout_id like '% No offer%' then 9 
        ,substring(b.holdout_id, 2, length(b.holdout_id)-2) as Segment FROM omc.riid_mapping a 
inner join omc.audit_hold_out b 
on a.contact_riid=b.riid 
where b.program_id='"37135582"' 