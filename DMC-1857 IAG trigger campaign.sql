DROP TABLE campaign_data.aa_dmc1857_iagtrigger_20200206; 
create table campaign_data.aa_DMC1857_iagTrigger_20200218 as 
SELECT substring(form.riid,2,length(form.riid)-2) riid, 
        substring(form.customer_id,2,length(form.customer_id)-2) as Contact_id, 
        substring(form.member_number,2,length(form.member_number)-2) member_number,  
        substring(form.nrma_account_id,2,length(form.nrma_account_id)-2) Account_id,  
        substring(form.event_captured_dt,2,length(form.event_captured_dt)-2) event_captured_dt, 
        substring(form.event_stored_dt,2,length(form.event_stored_dt)-2) event_stored_dt, 
        --substring(form.event_type_id,2,length(form.event_type_id)-2) event_type_id, 
        substring(form.iag_lead_follow_up,2,length(form.iag_lead_follow_up)-2) iag_lead_follow_up, 
        substring(form.iag_lead_product,2,length(form.iag_lead_product)-2) iag_lead_product, 
        substring(form.iag_lead_sub_product,2,length(form.iag_lead_sub_product)-2) iag_lead_sub_product, 
        substring(form.lead_modified_by_user,2,length(form.lead_modified_by_user)-2) lead_channel, 
        substring(form.send_email,2,length(form.send_email)-2) email_perm, 
        substring(form.campaign_id,2,length(form.campaign_id)-2) campaign_id, 
        substring(form.launch_id,2,length(form.launch_id)-2) launch_id, 
        substring(form.form_id,2,length(form.form_id)-2) form_id, 
        substring(form.form_name,2,length(form.form_name)-2) form_name 
 
FROM omc.audit_form form 
where form_id like '"IAG_FORM%' 
--and form.event_type_id <>'"15"' 
select * from campaign_data.aa_dmc1857_iagtrigger_20200218; 
Select count(distinct contact_id) from ( 
select   distinct c.csn  Member_Number 
        , c.row_id as contact_id 
        , c.CUST_VALUE_CD as Membership_type 
        , c.CON_CD as contact_type 
        , c.email_addr as email_address 
         
from gms.s_contact c 
 
inner join gms.s_contact_fnx as fn 
on fn.par_row_id = c.row_id 
         
inner join gms.s_contact_x as cx 
on cx.par_row_id = c.row_id 
 
inner join gms.s_asset a 
on c.pr_dept_ou_id = a.owner_accnt_id 
 
inner join gms.s_prod_int p 
on a.prod_id = p.row_id 
 
inner join campaign_data.aa_dmc1857_iagtrigger_20200218 iag 
on iag.contact_id=c.row_id 
 
where c.con_cd in ('Ordinary Member','Affiliate Member') 
--and c.csn is not null 
and c.cust_stat_cd = 'Active' 
and NVL(fn.deceased_flg,'N') = 'N' 
and NVL(c.x_nrma_title, 'No Title') <> 'Estate Of The Late' 
and NVL(cx.attrib_44,'No Discount') not RLIKE 'Honorary' 
and case when trim(c.x_inv_email_1) = 'Y' or c.email_addr is null then 'N' else 'Y' end = 'Y' 
--and NVL(cx.ATTRIB_36,'Y') != 'No' --Email permission 
 
and a.STATUS_CD ='Active' 
and p.prod_cd = 'Promotion' 
and p.sub_type_cd in ('Non-RSA','RSA') 
and p.type='Membership' 
and iag.iag_lead_follow_up='March 2020' 
and iag.email_perm='Y' 
) aa 
Select count(distinct contact_id) from ( 
select   distinct c.csn  Member_Number 
        , c.row_id as contact_id 
        , c.CUST_VALUE_CD as Membership_type 
        , c.CON_CD as contact_type 
        , c.email_addr as email_address 
        --, iag.* 
         
from gms.s_contact c 
 
inner join gms.s_contact_fnx as fn 
on fn.par_row_id = c.row_id 
         
inner join gms.s_contact_x as cx 
on cx.par_row_id = c.row_id 
 
inner join gms.s_asset a 
on c.pr_dept_ou_id = a.owner_accnt_id 
 
inner join gms.s_prod_int p 
on a.prod_id = p.row_id 
 
inner join campaign_data.aa_dmc1857_iagtrigger_20200218 iag 
on iag.contact_id=c.row_id 
 
where c.con_cd in ('Ordinary Member','Affiliate Member') 
and c.csn is not null 
and c.cust_stat_cd = 'Active' 
and NVL(fn.deceased_flg,'N') = 'N' 
and NVL(c.x_nrma_title, 'No Title') <> 'Estate Of The Late' 
and NVL(cx.attrib_44,'No Discount') not RLIKE 'Honorary' 
and case when trim(c.x_inv_email_1) = 'Y' or c.email_addr is null then 'N' else 'Y' end = 'Y' 
--and NVL(cx.ATTRIB_36,'Y') != 'No' --Email permission 
 
and a.STATUS_CD ='Active' 
and p.prod_cd = 'Promotion' 
and p.sub_type_cd in ('Non-RSA','RSA') 
and p.type='Membership' 
and iag.iag_lead_follow_up='February 2020' 
and iag.email_perm='Y' 
) aa 
select distinct type from gms.s_prod_int 
--IAG_Member_Leads_monthly_eligible_202020200218 
XSell_Predictions = spark.read.load("/user/aamreen/IAG_Member_Leads_monthly_eligible_202020200218.csv",format="csv", sep=",", inferSchema="true", header="true") 
XSell_Predictions.createOrReplaceTempView("XSell_Predictions") 
spark.sql (""" select * from XSell_Predictions""").count() 
spark.sql (""" select * from XSell_Predictions a 
 
""").show() 
spark.sql(""" 
create table campaign_data.aa_dmc1857_iagtrigger_20200218_test as 
select   c.CUST_VALUE_CD as Membership_type 
        , c.CON_CD as contact_type 
        , c.email_addr as email_address 
        , iag.* 
         
from gms.s_contact c 
 
inner join gms.s_contact_fnx as fn 
on fn.par_row_id = c.row_id 
         
inner join gms.s_contact_x as cx 
on cx.par_row_id = c.row_id 
 
inner join gms.s_asset a 
on c.pr_dept_ou_id = a.owner_accnt_id 
 
inner join gms.s_prod_int p 
on a.prod_id = p.row_id 
 
inner join campaign_data.aa_dmc1857_iagtrigger_20200218 iag 
on iag.contact_id=c.row_id 
 
left anti join XSell_Predictions xs 
on xs.CUSTOMER_ID_=iag.contact_id 
 
where c.con_cd in ('Ordinary Member','Affiliate Member') 
and c.csn is not null 
and c.cust_stat_cd = 'Active' 
and NVL(fn.deceased_flg,'N') = 'N' 
and NVL(c.x_nrma_title, 'No Title') <> 'Estate Of The Late' 
and NVL(cx.attrib_44,'No Discount') not RLIKE 'Honorary' 
and case when trim(c.x_inv_email_1) = 'Y' or c.email_addr is null then 'N' else 'Y' end = 'Y' 
--and NVL(cx.ATTRIB_36,'Y') != 'No' --Email permission 
 
and a.STATUS_CD ='Active' 
and p.prod_cd = 'Promotion' 
and p.sub_type_cd in ('Non-RSA','RSA') 
and p.type='Membership' 
and iag.iag_lead_follow_up='February 2020' 
and iag.email_perm='Y' 
""").show(1000,False) 
SELECT contact_id 
        , m.member_number 
        , iag_lead_product 
        --, event_stored_dt 
        , iag_lead_product 
        , iag_lead_sub_product 
        , email_perm  
        , iag_lead_follow_up 
        --, to_timestamp('event_captured_dt','yyyy-MM-dd HH:mm:ss') 
        , event_captured_dt 
        , m.time_stamp FROM campaign_data.aa_dmc1857_iagtrigger_20200218_test a 
inner join m4m.return_feed_header m 
on m.member_number=a.member_number 
and form_name like 'PROD%' 
 
where to_date(m.time_stamp)>'2019-12-01' 
 
--GROUP BY m.time_stamp 
select * from m4m.return_feed_detail a 
inner join m4m.return_feed_header h 
on h.trx_header_id=a.trx_header_id 
where h.member_number='990682537'