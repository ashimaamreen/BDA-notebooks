#Contents
 
#quick analysis to understand data 
 
select t1.new_date, count(t1.row_id) 
from  
( 
    select from_timestamp(c.event_date, "yyyy-MM-dd") as new_date, c.* 
    from gms.cx_cam_response as c 
    where  
        c.campaign_id = "32947442" 
) as t1 
group by t1.new_date 
order by t1.new_date 
select * 
from gms.cx_cam_response as c 
where  
    c.campaign_id = "32947442" 
-- table manually refreshed 
-- with omc response summary table 
 
SELECT *  
FROM sandpit.email_campaigns_summary  
where  
    campaign_id = "32947442"  
LIMIT 100; 
-- table manually refreshed 
-- with control group  
 
SELECT from_timestamp(sendtime, "yyyy-MM-dd") as sendtime_str, count(distinct contact_id) as number 
FROM sandpit.member_campaign_summary_table  
where  
    campaign_id = "32947442"  
    and UPPER(holdoutgroup) <> "Y" 
group by from_timestamp(sendtime, "yyyy-MM-dd") 
order by sendtime_str 
""" 
Parameter setup 
 
# campaign_id 
# campaign start and end date 
# sent and control group from OMC table 
 
""" 
 
campaign_start_dt = "2019-02-11" 
campaign_end_dt = "2019-03-31" 
 
campaign_response_tbl = spark.table("sandpit.member_campaign_summary_table") 
campaign_response_tbl.filter("campaign_id='32947442' and upper(holdoutgroup)<>'Y'").createOrReplaceTempView("campaign_sent") 
campaign_response_tbl.filter("campaign_id='32947442' and upper(holdoutgroup)='Y'").createOrReplaceTempView("campaign_control") 
campaign_response_tbl.printSchema() 
 
spark.sql("""select * from campaign_control""").groupby("campaign_name", "holdoutgroup").count().show(10,False) 
spark.sql("""select * from campaign_sent""").groupby("campaign_name", "holdoutgroup").count().show(10,False) 
 
print spark.sql("""select distinct contact_id from campaign_control""").count() 
print spark.sql("""select distinct contact_id from campaign_sent""").count() 
# Table creates info of members who made a booking in the given time period regardless of booking status 
# Change dates as per PIR requirement 
 
s_evt_act_x = spark.table("gms.s_evt_act_x") 
s_evt_act_x.createOrReplaceTempView("s_evt_act_x") 
 
s_evt_act = spark.table("gms.s_evt_act") 
s_evt_act.createOrReplaceTempView("s_evt_act") 
 
s_srv_regn = spark.table("gms.s_srv_regn") 
s_srv_regn.createOrReplaceTempView("s_srv_regn") 
 
s_contact = spark.table("gms.s_contact") 
s_contact.createOrReplaceTempView("s_contact") 
 
s_org_ext = spark.table("gms.s_org_ext") 
s_org_ext.createOrReplaceTempView("s_org_ext") 
 
Booking_Details = spark.sql("""   
 
select    c.row_id ContactID 
        , case when c.csn is null then o.ou_num else c.csn end MemberNumber --to get member number of business member too 
        , c.con_cd ContactType 
        , actx.attrib_44 BookingNumber 
        , act.created BookingCreatedDate 
        , srv.name Branch 
        , act.todo_plan_start_dt PlannedServiceDate 
        , act.x_booking_service1 BookingService1 
        , act.x_booking_service2 BookingService2 
        , act.x_booking_service3 BookingService3 
        , act.evt_stat_cd BookingStatus 
        , act.x_cancel_reason CancellationReason 
         
from s_evt_act act 
 
inner join s_evt_act_x actx 
on actx.par_row_id = act.row_id 
 
inner join s_srv_regn srv 
on srv.row_id = act.srv_regn_id 
 
inner join s_contact c 
on c.row_id = act.target_per_id 
 
inner join s_org_ext o 
on c.pr_dept_ou_id = o.row_id 
 
where date_format(act.created,'yyyy-MM-dd') between '%s' and '%s' 
""" % (campaign_start_dt, campaign_end_dt)  
) 
Booking_Details.createOrReplaceTempView("Booking_Details") 
spark.sql("""select * from Booking_Details""").show(10,False) 
#MotorSmart Orders (Order Item Details) 
 
s_asset = spark.table("gms.s_asset") 
s_asset.createOrReplaceTempView("s_asset") 
 
s_order = spark.table("gms.s_order") 
s_order.createOrReplaceTempView("s_order") 
 
s_order_type = spark.table("gms.s_order_type") 
s_order_type.createOrReplaceTempView("s_order_type") 
 
s_order_item = spark.table("gms.s_order_item") 
s_order_item.createOrReplaceTempView("s_order_item") 
 
s_prod_int = spark.table("gms.s_prod_int") 
s_prod_int.createOrReplaceTempView("s_prod_int") 
 
Repair_Order = spark.sql("""   
 
select    c.row_id ContactID 
        , case when c.csn is null then org.ou_num else c.csn end MemberNumber 
        , o.order_num OrderNumber 
        , ot.name OrderType 
        , replace(p.name,'|',',') Product 
        , o.created OrderCreateDate 
        , o.status_cd OrderStatus 
        , o.status_dt StatusDate 
        , a.LCNS_NUM Rego 
        , oi.net_pri NetPrice_incGST 
        , oi.extd_qty as QTY 
        , oi.base_unit_pri RRP 
        , oi.net_pri*oi.extd_qty as GrossPrice 
        , (oi.net_pri*oi.extd_qty)/1.1 as NetPrice 
        , case when oi.net_pri<0 then round(oi.net_pri) else 0 end OfferDiscount 
 
 
from s_order o 
 
inner join s_order_type ot 
on o.order_type_id = ot.row_id 
and ot.name = 'Repair Order' 
 
inner join s_order_item oi -- product line item filter too 
on o.row_id = oi.order_id 
 
inner join s_prod_int p 
on oi.prod_id = p.row_id 
 
left join s_asset a 
on a.integration_id = oi.asset_integ_id 
 
left join s_contact c 
on o.contact_id = c.row_id 
 
left join s_org_ext org 
on o.accnt_id = org.row_id 
 
where o.created between '%s' and '%s' -- USE SAME TIME DURATION AS BOOKING CREATED DATE/BAU 
""" % (campaign_start_dt, campaign_end_dt)) 
Repair_Order.createOrReplaceTempView("Repair_Order") 
spark.sql('select * from Repair_Order').show(10,False) 
RO_Details = spark.sql("""  
select  ContactID 
                        ,MemberNumber 
                        ,OrderNumber 
                        ,OrderType 
                        ,OrderCreateDate 
                        ,OrderStatus 
                        ,Round(sum(GrossPrice)) TotalRevenue 
                        ,Round(sum(NetPrice)) TotalNetPrice --always use netprice for $ calculation 
                        ,sum (OfferDiscount) DiscountAmount 
                        ,count(OrderNumber) NoOfLineItems 
from Repair_Order 
group by ContactID 
                        ,MemberNumber 
                        ,OrderNumber 
                        ,OrderType 
                        ,OrderCreateDate 
                        ,OrderStatus 
 
""") 
RO_Details.createOrReplaceTempView("RO_Details") 
spark.sql("""select * from RO_Details""").show(10,False) 
Target_Converison = spark.sql(""" 
 
select  
    count (distinct b.ContactID) as uniqueContacts, count (distinct BookingNumber) as NoOfBookings  
from Booking_Details b 
    inner join ( 
    select * from campaign_sent where split(sendtime," ")[0] >= '%s' and split(sendtime, " ")[0] <= '%s' 
    ) as t 
    on t.contact_id = b.contactid 
 
""" % (campaign_start_dt, campaign_end_dt) 
) 
Target_Converison.createOrReplaceTempView("Target_Converison") 
spark.sql("""select * from Target_Converison""").show(100000,False) 
Control_Converison = spark.sql(""" 
 
select  
    count (distinct b.ContactID) as uniqueContacts, count (distinct BookingNumber) as NoOfBookings  
from Booking_Details b 
    inner join ( 
    select * from campaign_control where split(sendtime," ")[0] >= '%s' and split(sendtime, " ")[0] <= '%s' 
    ) as t 
    on t.contact_id = b.contactid 
 
""" % (campaign_start_dt, campaign_end_dt) 
) 
Control_Converison.createOrReplaceTempView("Control_Converison") 
spark.sql("""select * from Control_Converison""").show(100000,False) 
TargetRO_Converison = spark.sql(""" 
 
select count (distinct ro.ContactID) as uniqueContacts, count (distinct ro.OrderNumber) as NoOfRO , sum(ro.TotalRevenue) as RevenueGenerated  
from RO_Details ro 
 
    inner join ( 
    select * from campaign_sent where split(sendtime," ")[0] >= '%s' and split(sendtime, " ")[0] <= '%s' 
    ) t 
    on ro.contactid = t.contact_id 
 
    inner join booking_details b 
    on t.contact_id = b.contactid 
 
where  
    ro.orderStatus = 'Complete' 
    and ro.TotalRevenue > 0 
 
"""% (campaign_start_dt, campaign_end_dt) 
) 
TargetRO_Converison.createOrReplaceTempView("TargetRO_Converison") 
spark.sql("""select * from TargetRO_Converison""").show(100000,False) 
ControlRO_Converison = spark.sql(""" 
 
select count (distinct ro.ContactID) as uniqueContacts, count (distinct ro.OrderNumber) as NoOfRO , sum(ro.TotalRevenue) as RevenueGenerated  
from RO_Details ro 
 
    inner join ( 
    select * from campaign_control where split(sendtime," ")[0] >= '%s' and split(sendtime, " ")[0] <= '%s' 
    ) as t 
    on ro.contactid = t.contact_id 
 
    inner join booking_details b 
    on t.contact_id = b.contactid 
 
where  
    ro.orderStatus = 'Complete' 
    and ro.TotalRevenue > 0 
 
"""% (campaign_start_dt, campaign_end_dt) 
) 
ControlRO_Converison.createOrReplaceTempView("ControlRO_Converison") 
spark.sql("""select * from ControlRO_Converison""").show(100000,False)