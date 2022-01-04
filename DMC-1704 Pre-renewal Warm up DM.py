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
 
select    cam.* 
        , o.order_num OrderNumber 
        , oi.order_id as OrderLineItemID 
        , ot.name OrderType 
        , replace(p.name,'|',',') Product_new 
        , p.sub_type_cd as Product_type 
        , o.status_cd OrderStatus 
        , o.X_RENEWAL_RELATED RenewalRelated_flag 
        , o.X_PAY_BY_TERM as MPP_flag 
        , o.X_PAYMENT_STATUS as PaymentStatus 
        , oi.ACTION_CD as Action --if =add then upgrade/dwngrade/new 
        , ox.ATTRIB_04 as UpgradeDowngrade_flag 
        , to_date(o.ORDER_DT) as RenewalDueDate 
        , to_date(py.txn_dt) as PaymentDate 
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
 
left join s_src_payment py 
on o.row_id = py.order_id 
 
inner join (SELECT * FROM campaign_data.cc_20190508_c_dmc1532_dm_trigger 
where batch in (1,2,3,4)) cam 
on o.contact_id=cam.contact_id 
 
where (NVL(o.X_RENEWAL_RELATED, 'N') = 'Y' or ot.name = 'Renew') 
and o.status_cd != 'Revised' 
 
""") 
Renewal_Order.createOrReplaceTempView("Renewal_Order") 
spark.sql('select * from Renewal_Order').count() 
spark.sql('select * from Renewal_Order').show(10,False) 
Renewals_summary = spark.sql(""" 
select    batch 
        , Contact_ID 
        , Member_Number 
        , membership_type 
        , contact_type 
        , asset_enddt 
        , product as old_product 
        , segment 
        , OrderNumber 
        , OrderLineItemID 
        , OrderType 
        , Product 
        , OrderStatus 
        , PaymentStatus 
        , Action --if =add then upgrade/dwngrade/new 
        , UpgradeDowngrade_flag 
        , RenewalDueDate 
        , PaymentDate 
        , OrderCompletionDate 
        , case when PaymentDate<=RenewalDueDate then 'Y' else 'N' end POT 
        , case when OrderStatus = 'Complete' then 'Y' else 'N' end Renewed 
        , case when Action = 'Add' and UpgradeDowngrade_flag is NULL then 'Y' else 'N' end NewAdded 
        , RenewalRelated_flag 
         
from Renewal_Order 
where RenewalDueDate between '2019-06-29' and '2019-08-24' 
 
 
""") 
Renewals_summary.createOrReplaceTempView("Renewals_summary") 
spark.sql('select * from Renewals_summary').show(10000) 
spark.sql("""select segment 
                    , count(distinct OrderLineItemID) RenewalsDue 
                    , count(distinct(case when POT = 'Y' then OrderLineItemID end)) as POT 
                    , count(distinct(case when Renewed = 'Y' then OrderLineItemID end)) as TotalRenewed 
                    , count(distinct Member_Number) MembersDue 
                    , count(distinct(case when POT = 'Y' then Member_Number end)) as POT_Members 
                    , count(distinct(case when Renewed = 'Y' then Member_Number end)) as TotalRenewed_Members 
            from Renewals_summary  
            group by segment 
            order by 1 
""").show(1000,False) 
spark.sql("""select segment , count(distinct Member_Number) MembersDue 
            from Renewals_summary  
            group by segment 
            order by 1 
""").show(1000,False) 
 
spark.sql("""select segment , count(distinct Member_Number) POT_Members 
            from Renewals_summary where POT='Y' 
            group by segment 
            order by 1 
""").show(1000,False) 
 
spark.sql("""select segment , count(distinct Member_Number) TotalRenewed_Members 
            from Renewals_summary where Renewed='Y' 
            group by segment 
            order by 1 
""").show(1000,False) 
Redemption_summary_target = spark.sql (""" 
SELECT    distinct m.MemberNumber 
        , count(distinct TransID) as Total_Redemption 
        , sum(case when partner in ('IAG','NRMA Multi Policy Discount','NRMA MPD Insurance','NRMA Insurance') then 1 else 0 end) IAG_Patner 
        , sum(case when partner in ('NRMA car servicing') then 1 else 0 end) Car_Servicing 
        , sum(case when partner in ('Caltex') then 1 else 0 end) Caltex 
        , count (distinct Partner) as Total_Partners 
        --, sum(Savings) as Total_Savings 
 
from M4M_Redemption_during m 
 
inner join campaign_data.aa_20190313_c_dmc1464_marchsolus_repart a 
on a.member_number = m.membernumber 
 
where partner not in ('IAG','NRMA Multi Policy Discount','NRMA MPD Insurance','NRMA Insurance') 
group by m.MemberNumber 
""") 
Redemption_summary_target.createOrReplaceTempView("Redemption_summary_target") 
spark.sql('select count(distinct membernumber), sum(total_redemption), sum(IAG_Patner), sum(Car_Servicing), sum(Caltex), sum(Batteries), sum (Total_Partners) from Redemption_summary_target').show(5) 
M4M_Redemption_during = spark.sql(""" 
SELECT    segment,m.member_number 
        , count(distinct trx_header_id) as Total_Redemption 
        , count(distinct partner) as Total_Partners 
        , sum(case when partner in ('NRMA car servicing') then 1 else 0 end) Car_Servicing 
        , sum(case when partner in ('Caltex') then 1 else 0 end) Caltex 
        , sum(case when (m.partner like 'NRMA Parks and Resorts%' or m.partner like '%Holiday%' ) then 1 else 0 end) NRMA_Parks_Resorts 
        , sum(case when m.partner in ('Frequent Values') then 1 else 0 end) Dining 
 
from m4m.return_feed_header m 
 
inner join (SELECT * FROM campaign_data.cc_20190508_c_dmc1532_dm_trigger 
where batch in (1,2,3,4)) cam 
on m.member_number=cam.member_number 
 
where to_date(time_stamp)>='2019-04-30' 
and partner not in ('IAG','NRMA Multi Policy Discount','NRMA MPD Insurance','NRMA Insurance') 
 
group by segment, m.Member_Number 
""") 
 
M4M_Redemption_during.createOrReplaceTempView("M4M_Redemption_during") 
spark.sql('select segment, count(distinct member_number),sum(total_redemption),sum(total_partners), sum(Car_Servicing),sum(Caltex),sum(NRMA_Parks_Resorts),sum(Dining) from M4M_Redemption_during group by segment').show(5) 
Redemption_summary_target = spark.sql (""" 
SELECT   * 
        
from M4M_Redemption_during m 
 
inner join (SELECT * FROM campaign_data.cc_20190508_c_dmc1532_dm_trigger 
where batch in (1,2,3,4)) cam 
on m.membernumber=cam.member_number 
 
group by batch, segment, m.MemberNumber 
""") 
Redemption_summary_target.createOrReplaceTempView("Redemption_summary_target") 
spark.sql('select batch, segment, count(distinct membernumber),sum(total_redemption),sum(total_partners) from Redemption_summary_target group by batch, segment').show(100,False) 
Redemption_summary_target_1 = spark.sql (""" 
SELECT a.member_number 
        , a.contact_id 
        , a.batch 
        , a.segment 
        , to_date(min(m.time_stamp)) as first_redemption 
         
FROM campaign_data.cc_20190508_c_dmc1532_dm_trigger a 
LEFT JOIN m4m.return_feed_header m 
on m.member_number=a.member_number 
where batch in (1,2,3,4) 
and partner not in ('IAG','NRMA Multi Policy Discount','NRMA MPD Insurance','NRMA Insurance') 
 
group by a.member_number 
        , a.contact_id 
        , a.batch 
        , a.segment 
""") 
Redemption_summary_target_1.createOrReplaceTempView("Redemption_summary_target_1") 
spark.sql('select * from Redemption_summary_target_1').show(100,False) 
spark.sql(""" 
SELECT segment, count(contact_id) 
from Redemption_summary_target_1 
where to_date(first_redemption) > '2019-04-30' 
group by segment 
""").show(10,False) 
SELECT dm.segment, count(DISTINCT dm.member_number) FROM campaign_data.bn_appusers_010119_to_130619 a 
 
inner join campaign_data.cc_20190508_c_dmc1532_dm_trigger dm 
on dm.member_number = a.membernumber 
 
where dm.batch in (1,2,3,4) 
group by dm.segment 
select dm.segment, count(dm.member_number) from sandpit.appdata_01012019_20082019 a 
inner join campaign_data.cc_20190508_c_dmc1532_dm_trigger dm 
on dm.member_number = a.membernumber 
where dm.batch in (1,2,3,4) 
group by dm.segment 
select count (distinct c.contact_Id) 
        , CUST_VALUE_CD as loyaltycolor 
        --,cx.attrib_55  AS Colour_Plus 
 
from campaign_data.cc_20190508_c_dmc1532_dm_trigger c 
 
inner join gms.s_contact co 
on co.row_id = c.contact_Id 
 
inner join gms.s_contact_x as cx  
on co.row_id = cx.par_row_id 
 
where c.batch in (1,2,3,4) 
and c.segment='target' 
group by CUST_VALUE_CD 
--cx.attrib_55 
select count (distinct c.contact_Id) 
        --, CUST_VALUE_CD as loyaltycolor 
        ,cx.attrib_55  AS Colour_Plus 
 
from campaign_data.cc_20190508_c_dmc1532_dm_trigger c 
 
inner join gms.s_contact co 
on co.row_id = c.contact_Id 
 
inner join gms.s_contact_x as cx  
on co.row_id = cx.par_row_id 
 
where c.batch in (1,2,3,4) 
and c.segment='target' 
group by  
--CUST_VALUE_CD 
cx.attrib_55 
spark.sql(""" 
select count (distinct c.contact_Id) 
        , CUST_VALUE_CD as loyaltycolor 
        --,cx.attrib_55  AS Colour_Plus 
 
from Renewals_summary c 
 
inner join s_contact co 
on co.row_id = c.contact_Id 
 
inner join gms.s_contact_x as cx  
on co.row_id = cx.par_row_id 
 
where POT='Y' 
group by CUST_VALUE_CD 
--cx.attrib_55 
 
""").show(1000) 
spark.sql(""" 
select count (distinct c.contact_Id) 
        ,cx.attrib_55  AS Colour_Plus 
 
from Renewals_summary c 
 
inner join s_contact co 
on co.row_id = c.contact_Id 
 
inner join gms.s_contact_x as cx  
on co.row_id = cx.par_row_id 
 
where POT='Y' 
group by cx.attrib_55 
 
""").show(1000) 
spark.sql(""" 
select count (distinct c.member_number) 
        , CUST_VALUE_CD as loyaltycolor 
        --,cx.attrib_55  AS Colour_Plus 
 
from M4M_Redemption_during c 
 
inner join gms.s_contact co 
on co.csn = c.member_number 
 
inner join gms.s_contact_x as cx  
on co.row_id = cx.par_row_id 
 
group by CUST_VALUE_CD 
--cx.attrib_55 
 
""").show(1000) 
spark.sql(""" 
select count (distinct c.member_number) 
        ,cx.attrib_55  AS Colour_Plus 
 
from M4M_Redemption_during c 
 
inner join gms.s_contact co 
on co.csn = c.member_number 
 
inner join gms.s_contact_x as cx  
on co.row_id = cx.par_row_id 
 
group by cx.attrib_55 
 
""").show(1000) 