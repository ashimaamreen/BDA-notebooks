SELECT event_date 
        ,contact_id 
        , 'OLD' segment 
FROM gms.cx_cam_response  
where campaign_id='8767302' 
and event_date>='2019-09-07 00:00:00' 
and event_date<='2020-03-07 00:00:00' 
CREATE table campaign_data.aa_dmc1727_carServicingSMS_campaign_analysis_20200408 as 
SELECT event_date 
        ,contact_id 
        , 'NEW' segment 
FROM gms.cx_cam_response  
where campaign_id='37172662' 
and event_date>='2019-09-07 00:00:00' 
and event_date<='2020-03-07 00:00:00' 
 
UNION 
 
SELECT event_date 
        ,contact_id 
        , 'NEW_2' segment 
FROM gms.cx_cam_response  
where campaign_id='37172762' 
and event_date>='2019-09-07 00:00:00' 
and event_date<='2020-03-07 00:00:00' 
 
union 
 
SELECT event_date 
        ,contact_id 
        , 'OLD' segment 
FROM gms.cx_cam_response  
where campaign_id='8767302' 
and event_date>='2019-09-07 00:00:00' 
and event_date<='2020-03-07 00:00:00' 
select segment, count(distinct contact_id) from campaign_data.aa_dmc1727_carServicingSMS_campaign_analysis_20200322 
group by 1 
WITH reminder as (  --11540 
SELECT event_date 
        ,contact_id 
        , 'NEW_2' segment 
FROM gms.cx_cam_response  
where campaign_id='37172762' 
and event_date>='2019-09-07 00:00:00' 
and event_date<='2020-03-07 00:00:00' 
), confirmation as (    --12082 
 
SELECT event_date 
        ,contact_id 
        , 'NEW' segment 
FROM gms.cx_cam_response  
where campaign_id='37172662' 
and event_date>='2019-09-07 00:00:00' 
and event_date<='2020-03-07 00:00:00' 
) 
select count(DISTINCT contact_id) from confirmation 
where contact_id in (SELECT contact_id from reminder) 
#RUN 1st 
 
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
        , a.event_date as SMS_sent 
        , act.created BookingCreatedDate 
        , srv.name Branch 
        , act.todo_plan_start_dt PlannedServiceDate 
        , act.x_booking_service1 BookingService1 
        , act.x_booking_service2 BookingService2 
        , act.x_booking_service3 BookingService3 
        , act.evt_stat_cd BookingStatus 
        , act.x_cancel_reason CancellationReason 
        , act.row_id 
        , a.event_date 
        , a.segment 
         
from s_evt_act act 
 
inner join s_evt_act_x actx 
on actx.par_row_id = act.row_id 
 
inner join s_srv_regn srv 
on srv.row_id = act.srv_regn_id 
 
inner join s_contact c 
on c.row_id = act.target_per_id 
 
inner join s_org_ext o 
on c.pr_dept_ou_id = o.row_id 
 
inner join campaign_data.aa_dmc1727_carServicingSMS_campaign_analysis_20200408 a 
on a.contact_id = c.row_id 
and datediff(act.todo_plan_start_dt,a.event_date) between 2 and 5 
 
where date_format(act.created,'yyyy-MM-dd') between '2019-09-07 00:00:00' and'2020-03-07 00:00:00' 
""") 
Booking_Details.createOrReplaceTempView("Booking_Details") 
spark.sql("""select * from Booking_Details""").show(10,False) 
spark.sql(""" 
select * from Booking_Details  
where ContactID='1-2M9QH37' 
""").show(10,False) 
select segment , count(distinct contact_id) as noOfContacts 
        --, count (distinct BookingNumber) as NoOfBookings  
 
from campaign_data.aa_dmc1727_carServicingSMS_campaign_analysis_20200408 a 
group by 1 
#RUN 6th Booking converison 
spark.sql(""" 
 
select segment , count(distinct contactid) as noOfContacts 
        , count (distinct BookingNumber) as NoOfBookings  
 
from Booking_Details b 
 
inner join campaign_data.aa_dmc1727_carServicingSMS_campaign_analysis_20200408 a 
on a.contact_id = b.contactid 
 
--where BookingCreatedDate>=event_date 
group by a.segment 
""").show(100000,False) 
#RUN 2nd 
#MotorSmart Orders (Order Item Details) selects members who have made a RO and had a booking in the above previous table 
 
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
 
S_SRV_REQ = spark.table("gms.S_SRV_REQ") 
S_SRV_REQ.createOrReplaceTempView("S_SRV_REQ") 
 
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
 
inner join Booking_Details act --gets all RO generated from the above booking period 
on act.row_id = o.x_activity_id 
 
""") 
Repair_Order.createOrReplaceTempView("Repair_Order") 
spark.sql('select * from Repair_Order').show(1000,False) 
#RUN 3rd 
RO_Details = spark.sql("""  
select distinct ContactID 
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
order by 4 desc 
""") 
RO_Details.createOrReplaceTempView("RO_Details") 
spark.sql("""select * from RO_Details""").show(10,False) 
#RUN 8th RepairORDER converison 
RO_Converison = spark.sql(""" 
 
select segment, count (distinct ro.ContactID) as uniqueContacts, count (distinct ro.OrderNumber) as NoOfRO , sum(ro.TotalRevenue) as RevenueGenerated from RO_Details ro 
 
 
inner join campaign_data.aa_dmc1727_carServicingSMS_campaign_analysis_20200408 t 
on t.contact_id = ro.contactid 
 
where ro.orderStatus = 'Complete' 
and ro.TotalRevenue>0 
group by segment 
""") 
RO_Converison.createOrReplaceTempView("RO_Converison") 
spark.sql("""select * from RO_Converison""").show(100000,False) 
SELECT riid, customer_id, event_captured_dt, personalization_dt FROM omc.email_sent  
where campaign_id ='"8767502"' 
limit 10 
 
SELECT launch_id, campaign_id, campaign_name, launch_started_dt  
FROM omc.audit_launch_state 
where campaign_id in ('"8767302"') 
SELECT  DISTINCT riid, contact_id, campaign_id, campaign_name, channel, sendtime 
--program_id, count(DISTINCT riid)  
 
from omc.member_campaign_summary_table 
where 1=1 
--and program_id in ('616422') 
and campaign_id  in ('8767302','8767502','615622') 
and sendtime>='2019-09-07 00:00:00' 
and channel='Email' 
--ORDER BY sendtime 
--group by program_id, campaign_id 
 
SELECT DISTINCT riid, contact_id, campaign_id, campaign_name, campaign_type, channel, sendtime 
--program_id, count(DISTINCT riid)  
 
from omc.member_campaign_summary_table 
where 1=1 
--and program_id in ('616422') 
and campaign_id  in ('37176022','37172762','37172662','8767222') 
and sendtime>='2019-09-07 00:00:00' 
--and riid='5952706062' 
--ORDER BY sendtime 
--group by program_id, campaign_id 
 
SELECT rm.customer_id_ 
--, campaign_id, campaign_name, campaign_type, channel, sendtime 
 
from omc.sms_sent a 
 
inner join omc.riid_mapping rm 
on rm.booking_riid=a.riid 
 
where 1=1 
--and program_id in ('616422') 
and campaign_id  in ('"37176022"','"37172762"','"37172662"','"8767222"') 
--and sendtime>='2019-09-07 00:00:00' 
SELECT * FROM gms.cx_cam_response  
where campaign_id in ('8767302','37172762','37172662')  
LIMIT 100; 
--its not mandatory othave contact id in riid mapping table 
--comms sent at booking level  
select * from omc.member_campaign_summary_table 
where campaign_id='8767302' 
select program_id, * from aggregator.sms_campaigns_summary 
where program_id in ('616682') 
order by 1 
select program_id, * from aggregator.sms_campaigns_summary 
where program_id in ('37172842','616682') 
order by 1 
select program_id, campaign_id from aggregator.sms_campaigns_summary 
where program_id in ('37172842','616682') 
---OLD CAMPAIGN 
select distinct campaign_type from omc.member_campaign_summary_table 
where program_id in ('37172842') 
select * from m4m.return_feed_header m 
inner join gms.s_contact c 
on c.csn=m.member_number 
 
where c.row_id in ('1-DDN-1497') 
select * from m4m.return_feed_header m 
inner join gms.s_contact c 
on c.csn=m.member_number 
 
where c.row_id in ('1-CW42JMB') 
--('1-HGV-2046') 
and partner not in ('IAG','NRMA Multi Policy Discount','NRMA MPD Insurance','NRMA Insurance') 
select x_legal_entity_name from gms.s_org_ext  
where x_legal_entity_name is not null 