Contents
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
        , act.created BookingCreatedDate 
        , srv.name Branch 
        , act.todo_plan_start_dt PlannedServiceDate 
        , act.x_booking_service1 BookingService1 
        , act.x_booking_service2 BookingService2 
        , act.x_booking_service3 BookingService3 
        , act.evt_stat_cd BookingStatus 
        , act.x_cancel_reason CancellationReason 
        , act.row_id 
         
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
""" % (begin_date, end_date)) 
Booking_Details.createOrReplaceTempView("Booking_Details") 
spark.sql("""select * from Booking_Details""").show(10,False) 
#RUN 2nd 
#MotorSmart Orders (Order Item Details) selects members who have made a RO and had a booking in the above previous table 
 
s_contact = spark.table("gms.s_contact") 
s_contact.createOrReplaceTempView("s_contact") 
 
s_org_ext = spark.table("gms.s_org_ext") 
s_org_ext.createOrReplaceTempView("s_org_ext") 
 
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
 
select    distinct c.row_id ContactID 
        --, case when c.csn is null then org.ou_num else c.csn end MemberNumber 
        --, o.order_num OrderNumber 
        --, ot.name OrderType 
        --, replace(p.name,'|',',') Product 
        --, o.created OrderCreateDate 
        --, o.status_cd OrderStatus 
        --, o.status_dt StatusDate 
        --, a.LCNS_NUM Rego 
        --, oi.net_pri NetPrice_incGST 
        --, oi.extd_qty as QTY 
        --, oi.base_unit_pri RRP 
        --, oi.net_pri*oi.extd_qty as GrossPrice 
        --, (oi.net_pri*oi.extd_qty)/1.1 as NetPrice 
        --, case when oi.net_pri<0 then round(oi.net_pri) else 0 end OfferDiscount 
        , a.X_MOTOSMART_REGO_EXPIRY_DT as Rego_end_dt 
        , a.STATUS_CD 
 
 
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
 
--inner join booking_details act --gets all RO generated from the above booking period 
--on act.row_id = o.x_activity_id 
 
where date_format(o.created,'yyyy-MM-dd') between '2019-05-01' and '2019-05-30'  
 
""") 
Repair_Order.createOrReplaceTempView("Repair_Order") 
spark.sql('select * from Repair_Order').show(1000,False) 
select count(distinct csn), code from campaign_data.aa_20190523_c_dmc1565_dm_edm_2 group by code 
select distinct 
X_LAST_SERVICE_BRANCH from gms.s_Asset 
where X_MOTOSMART_REGO_EXPIRY_DT is not null 
SELECT DISTINCT 
         c.row_id as Contact_id 
        , c.last_name 
        --,dm.DM_Sent 
        --,dm.Letter_Name 
        , a.lcns_num  
        , a.branch_id 
        , a.x_last_inspection_date  Last_Inspection_Date 
        , a.x_last_service_date     Last_Service_Date 
        , a.x_last_service_branch   Last_Service_Branch   
        , a.X_LaST_SeRVCE_BRNCH_CDE 
        , a.x_last_repair_date      Last_Repair_Date 
        , act.created booking_dt 
        , a.STATUS_CD 
       -- ,datediff('2018-10-11 00:00:00',a.x_last_service_date) as days_dif 
        --,srv.SR_STAT_ID 
         
      
    FROM gms.s_contact c 
       
     
    -- get event/booking info     
     INNER JOIN gms.s_evt_act act 
     ON c.row_id = act.target_per_id  
    
    -- get booking number/date  
    INNER JOIN  gms.s_evt_act_x actx 
    ON act.row_id = actx.par_row_id  
  
    -- get branch and rego details 
    INNER JOIN gms.s_srv_regn srv 
    ON srv.row_id = act.srv_regn_id 
     
    INNER JOIN gms.s_contact_x cx    
    on cx.par_row_id = c.row_id 
     
    -- get deceased flag 
    INNER JOIN gms.s_contact_fnx  cf 
    ON c.row_id = cf.par_row_id 
     
    -- get RTS flag 
    inner join gms.s_addr_per addr 
    on c.pr_per_addr_id = addr.row_id 
     
    -- get business member details 
    INNER JOIN gms.s_org_ext o 
    ON c.pr_dept_ou_id = o.row_id 
 
    -- get retail member details 
    INNER JOIN gms.s_contact_x contx 
    ON c.row_id=contx.par_row_id 
     
    -- get car service details 
    INNER JOIN gms.s_asset a  
    ON c.row_id = a.owner_con_id 
 
 
where a.x_last_service_branch IS NOT NULL  
and a.x_last_service_date between '2019-05-01 00:00:00' and '2019-05-30 00:00:00' 
--AND dm.letter_name='S6'  
--AND datediff(a.x_last_service_date,'2018-10-11 00:00:00')<=180 AND srv.name<>'MOBILE MECHANIC' 
SELECT x_expiry_date,X_LST_SRVCE_BRNCH_CDE FROM gms.s_srv_req LIMIT 100; 
select attrib_41 from gms.s_contact_x 
select LCNS_NUM as Rego 
from gms.S_VEHICLE 
select  DISTINCT 
         c.row_id as asset_id 
        , c.last_name 
        --,dm.DM_Sent 
        --,dm.Letter_Name 
        , a.lcns_num  
        , a.branch_id 
        , a.x_last_inspection_date  Last_Inspection_Date 
        , a.x_last_service_date     Last_Service_Date 
        , a.x_last_service_branch   Last_Service_Branch   
        , a.X_LaST_SeRVCE_BRNCH_CDE 
        , a.x_last_repair_date      Last_Repair_Date 
        , act.created booking_dt 
        , a.STATUS_CD 
         
       -- ,datediff('2018-10-11 00:00:00',a.x_last_service_date) as days_dif 
        --,srv.SR_STAT_ID 
         
from gms.s_asset 
select  DISTINCT 
         row_id as asset_id 
        --, a.owner_con_id  
        ---CAR DETAILS--- 
        , trim(upper(a.lcns_num)) as Rego 
        , to_date(a.X_MOTOSMART_REGO_EXPIRY_DT) as Rego_Expiry_Date 
        , a.MAKE_CD as Make 
        , a.model_cd as Model 
        --year 
        ---SERVICE DETAILS--- 
        , to_date(a.x_last_inspection_date)  Last_Inspection_Date 
        , to_date(a.x_last_repair_date)      Last_Repair_Date 
        , to_date(a.x_last_service_date)     Last_Service_Date 
        , a.X_SERVICE_CYCLE Service_Cycle 
        , to_date(a.X_MOTOSMART_NXT_SERV_DT) Next_Service_Date 
        , a.x_last_service_branch   Last_Service_Branch 
        ---CALCULATED FIELDS--- 
        , case when  a.X_SERVICE_CYCLE is null  
                then to_date(add_months(x_last_service_date,9))  
                else to_date(add_months(x_last_service_date,cast(X_SERVICE_CYCLE as INT))) end S9_calc 
        , case when  a.X_SERVICE_CYCLE is null  
                then to_date(add_months(x_last_service_date,6))  
                else to_date(add_months(x_last_service_date,cast(X_SERVICE_CYCLE as INT))) end S6_calc 
        , case when  a.X_SERVICE_CYCLE is null  
                then to_date(add_months(x_last_service_date,12))  
                else to_date(add_months(x_last_service_date,cast(X_SERVICE_CYCLE as INT))) end S12_calc 
        , case when  a.X_SERVICE_CYCLE is null  
                then to_date(add_months(x_last_service_date,8))  
                else to_date(add_months(x_last_service_date,cast(X_SERVICE_CYCLE as INT)-1)) end DM_send_calc 
        , case when  a.X_MOTOSMART_REGO_EXPIRY_DT is null  
                then 'Get Rego Expiry'  
                else to_date(add_months(X_MOTOSMART_REGO_EXPIRY_DT,-1)) end R_DM_send_calc 
         
        --, a.X_LaST_SeRVCE_BRNCH_CDE 
        , a.branch_id 
        --, act.created booking_dt 
        , a.STATUS_CD 
       -- ,datediff('2018-10-11 00:00:00',a.x_last_service_date) as days_dif 
        --,srv.SR_STAT_ID 
         
from gms.s_asset a 
where type_cd = 'Vehicle' 
and a.x_last_service_date is not null 
and case when (case when  a.X_SERVICE_CYCLE is null  
                then to_date(add_months(x_last_service_date,8))  
                else to_date(add_months(x_last_service_date,cast(X_SERVICE_CYCLE as INT)-1)) end = case when  a.X_MOTOSMART_REGO_EXPIRY_DT is null  
                then 'Get Rego Expiry'  
                else to_date(add_months(X_MOTOSMART_REGO_EXPIRY_DT,-1)) end) then 1 else 0 end =1 
select LAST_INSP_DT from gms.S_ASSET_X 
--* from gms.s_asset_x 
select distinct X_SERVICE_CYCLE from gms.s_asset 
check this field and frequency 
select distinct c.csn as Membership_Number 
        ,c.row_id as contact_id 
        ,c.x_nrma_title as title 
        ,c.fst_name as firstname 
        ,c.last_name as lastname 
        ,a.addr as address1 
        ,a.addr_line_2 as address2 
        ,a.city as suburb 
        ,a.state as state 
        ,a.zipcode as postcode 
        ,a.country as country 
        ,regexp_replace(c.home_ph_num, "[^0-9]+", "") as home_phone 
        ,regexp_replace(c.work_ph_num, "[^0-9]+", "") as work_phone 
        ,regexp_replace(c.cell_ph_num, "[^0-9]+", "") as mobile1 
        ,regexp_replace(c.asst_ph_num, "[^0-9]+", "") as mobile2 
        ,c.email_addr as email1 
        ,c.alt_email_addr as email2 
        ,c.birth_dt as DOB 
        ,x.attrib_55 as colour_segment 
        ,c.cust_value_cd as loyalty_group 
        ,x.attrib_43 as phone_perm 
        ,x.attrib_35 as post_perm 
        ,x.attrib_36 as email_perm 
        ,case when trim(c.x_inv_email_1) = 'Y' or c.email_addr is null then 'N' else 'Y' end valid_email 
        ,X_MOTOSMART_CUST_FLG as motorsmart_mkt_Perm 
         
from  
  gms.s_contact as c 
         
    inner join gms.s_contact_fnx as fn 
        on fn.par_row_id = c.row_id 
         
    inner join gms.s_contact_x as x 
        on x.par_row_id = c.row_id 
         
    inner join gms.s_addr_per as a 
        on a.row_id = c.pr_per_addr_id 
         
where c.cust_stat_cd = 'Active'   
    and NVL(fn.deceased_flg,'N') = 'N' 
    --and NVL(x.attrib_43,'Yes') <> 'No' 
    and c.con_cd in ('Ordinary Member' , 'Affiliate Member') 
    and NVL(c.x_nrma_title,'no title') != 'Estate Of The Late' 
    --and case when trim(x_inv_email_1) = 'Y' or email_addr is null then 'N' else 'Y' end = 'Y' 
SELECT * FROM gms.s_camp_con  
where con_per_id ='1-DMT-4161' 
SELECT row_id 
        , db_last_upd 
        , event_date 
        , campaign_id 
        , contact_id 
        , status_cd 
        , omc_campaign_code 
        , email_addr 
FROM gms.cx_cam_response  
where campaign_id in ('CISR','354822') 
and status_cd IN ('DMSENT','Sent') 
ORDER BY event_date DESC 
--contact_id ='1-DMT-4161' 
--where channel = 'DM' 
SELECT * FROM gms.cx_iras_launch  
where campaign_name ='Service Reminder eDM' 
SELECT DISTINCT status_cd 
FROM gms.cx_cam_response  
where campaign_id in ('354822') 
--and status_cd ='DMSENT' 
--ORDER BY event_date DESC 
--contact_id ='1-DMT-4161' 
--where channel = 'DM' 
S_CONTACT X_MOTOSMART_CUST_FLG  
Join : S_ASSET.OWNER_CON_ID = S_CONTACT.ROW_ID 
 
select X_MOTOSMART_CUST_FLG from gms.s_contact 
select  DISTINCT 
         row_id as asset_id 
        --, a.owner_con_id  
        ---CAR DETAILS--- 
        , trim(upper(a.lcns_num)) as Rego 
        , to_date(a.X_MOTOSMART_REGO_EXPIRY_DT) as Rego_Expiry_Date 
        , a.MAKE_CD as Make 
        , a.model_cd as Model 
        --year 
        ---SERVICE DETAILS--- 
        , to_date(a.x_last_inspection_date)  Last_Inspection_Date 
        , to_date(a.x_last_repair_date)      Last_Repair_Date 
        , to_date(a.x_last_service_date)     Last_Service_Date 
        , a.X_SERVICE_CYCLE Service_Cycle 
        , to_date(a.X_MOTOSMART_NXT_SERV_DT) Next_Service_Date 
        , a.x_last_service_branch   Last_Service_Branch 
        , a.branch_id 
        --, act.created booking_dt 
        , a.STATUS_CD 
       -- ,datediff('2018-10-11 00:00:00',a.x_last_service_date) as days_dif 
        --,srv.SR_STAT_ID 
         
from gms.s_asset a 
where type_cd = 'Vehicle' 
and a.x_last_service_date is not null 
and to_date(a.x_last_service_date) between '2018-08-01' and '2019-01-01' 
select distinct x_last_service_branch from gms.s_asset 
--where x_last_service_date> current_timestamp()