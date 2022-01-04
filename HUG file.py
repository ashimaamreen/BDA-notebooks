39501 Avaya
Contents
insert into campaign_data.avaya_late_payers 
 
select 
         ord.order_num                                                          as ID 
        ,'39501'                                                                as campaign_code 
        ,'Late Payers HUG'                                                     as campaign_name 
        ,con.csn                                                                as Membership_Number 
        ,con.row_id                                                             as customer_id 
        ,con.x_nrma_title                                                       as title 
        ,con.fst_name                                                           as firstname 
        ,con.last_name                                                          as lastname 
        ,''                                                                     as organisation_name 
        ,''                                                                     as ABN 
        ,addr.addr                                                              as address1 
        ,addr.addr_line_2                                                       as address2 
        ,addr.city                                                              as suburb 
        ,addr.state                                                             as state 
        ,addr.zipcode                                                           as postcode 
        ,addr.country                                                           as country 
        ,case when  
            (con.home_ph_num is not null and nvl(con.hard_to_reach,'N') <> 'Y') 
             then regexp_replace(con.home_ph_num, "[^0-9]+", "") 
             else NULL end                                                      as home_phone 
        ,case when 
            (con.work_ph_num is not null and nvl(con.speaker_flg,'N') <> 'Y') 
             then regexp_replace(con.work_ph_num, "[^0-9]+", "") 
             else NULL end                                                      as work_phone 
        ,case when 
            ((con.cell_ph_num is not null and nvl(con.veteran_flg,'N') <> 'Y') 
                and regexp_replace(con.cell_ph_num, "[^0-9]+", "") not like '0_0000000_' 
                and regexp_replace(con.cell_ph_num, "[^0-9]+", "") not like '041111111_') 
             then regexp_replace(con.cell_ph_num, "[^0-9]+", "") 
             else NULL end                                                      as mobile1 
        ,case when 
            ((con.asst_ph_num is not null and nvl(con.ok_to_sample_flg,'N') <> 'Y') 
            and regexp_replace(con.asst_ph_num, "[^0-9]+", "") <> '0000000000') 
             then regexp_replace(con.asst_ph_num, "[^0-9]+", "") 
             else NULL end                                                      as mobile2 
        ,con.email_addr                                                         as email1 
        ,con.alt_email_addr                                                     as email2 
        ,''                                                                     as dpid 
        ,from_unixtime(unix_timestamp(con.birth_dt),'yyyy-MM-dd HH:mm:ss')      as DOB 
        ,conx.attrib_55                                                         as nrma_colour_plus 
        ,con.cust_value_cd                                                      as loyalty_group 
        ,ord.order_num                                                          as order_number 
        ,ord.row_id                                                             as order_ID 
        ,FROM_UNIXTIME(UNIX_TIMESTAMP(),'yyyy-MM-dd HH:mm:ss')                  as file_created_date 
        ,''                                                                     as priority_segment 
        ,from_utc_timestamp(ord.order_dt,'AEST')                                as customised_field1 
        ,NULL                                                                   as customised_field2 
        ,NULL                                                                   as customised_field3 
        ,''                                                                     as customised_field4 
        ,''                                                                     as customised_field5 
        ,''                                                                     as customised_field6 
        ,''                                                                     as customised_field7 
        ,''                                                                     as customised_field8 
        
         
from  
    gms.s_contact as con 
    inner join gms.s_order              as ord 
        on con.row_id = ord.contact_id 
         
    inner join gms.s_org_ext            as org 
        on org.row_id = ord.accnt_id 
         
    inner join gms.s_org_ext_x          as orgx 
        on org.row_id = orgx.par_row_id 
     
    inner join gms.s_order_item         as ordi 
        on ordi.order_id = ord.row_id 
         
    inner join gms.s_order_type         as ordt 
        on ordt.row_id = ord.order_type_id 
 
    inner join gms.s_contact_fnx        as fn 
        on fn.par_row_id = con.row_id 
         
    inner join gms.s_contact_x          as conx 
        on conx.par_row_id = con.row_id 
         
    inner join gms.s_addr_per           as addr 
        on addr.row_id = con.pr_per_addr_id 
         
    inner join gms.s_asset              as asset 
        on asset.integration_id = ordi.asset_integ_id 
         
    inner join gms.s_prod_int           as prod 
        on ordi.prod_id = prod.row_id 
         
    left outer join ( 
        SELECT distinct 
            pimcon.user_contact_id as order_num 
 
        FROM 
            avayaoutbound.pim_contact as pimcon 
            inner join avayaoutbound.pim_contact_store as store 
                on store.pim_contact_store_id = pimcon.pim_contact_store_id 
 
            inner join avayaoutbound.pim_completion_code as code 
                on code.completion_code_id = pimcon.last_completion_code_id 
 
        WHERE 
                store.store_name in ('47891_AVAYA_NRMAOTM','OutboundSalesDailyv0.1') 
            AND pimcon.last_attempt_time > subdate(now(),50) 
            AND code.code in ('Do Not Call Register','Deceased','Fax Number','Gone With Competitor','Membership Cancelled', 
                              'Moved Interstate/Overseas','NAPS','No Valid Number','No English','Poor Experience with NRMA', 
                              'Vehicle Covered Under Another Membership','AlreadyRenewed','Not Interested')  
        ) as avayawash 
        on avayawash.order_num = ord.order_num 
 
where  
        con.cust_stat_cd = 'Active'   
    and NVL(fn.deceased_flg,'N') = 'N' 
    and NVL(con.x_nrma_title, '') <> 'Estate Of The Late' 
    and ordt.name = 'Renew' 
    and ord.status_cd = 'Submitted' 
    and ord.x_payment_status = 'Required' 
    and asset.status_cd = 'Active' 
    and asset.x_renewal_status = 'In Renewal' 
-- exclude staff discounts 
    and NVL(conx.attrib_44,'') not rlike 'Staff' 
    and con.con_cd in ('Ordinary Member','Affiliate Member') 
-- stop renewal order 
    and nvl(orgx.attrib_56, 'N') <> 'Y' 
    and addr.state in ('NSW','ACT') 
-- at least one phone number must be valid 
    and ( 
           (con.home_ph_num is not null and nvl(con.hard_to_reach,'N') <> 'Y') 
        or (con.work_ph_num is not null and nvl(con.speaker_flg,'N') <> 'Y') 
        or ((con.cell_ph_num is not null and nvl(con.veteran_flg,'N') <> 'Y') 
            and regexp_replace(con.cell_ph_num, "[^0-9]+", "") not like '0_0000000_' 
            and regexp_replace(con.cell_ph_num, "[^0-9]+", "") not like '041111111_') 
        or (con.asst_ph_num is not null and nvl(con.ok_to_sample_flg,'N') <> 'Y') 
        ) 
 
 
-- due date 49 days ago 
    and datediff(now(),from_utc_timestamp(ord.order_dt,'AEST')) = 49 
 
-- Wash out undesireable avaya outcomes from past 50 days 
    and avayawash.order_num is null 
     
group by ord.order_num 
        ,con.csn                                                     
        ,con.row_id                                                   
        ,con.x_nrma_title                                                
        ,con.fst_name                                                    
        ,con.last_name                                                    
        ,addr.addr                                                          
        ,addr.addr_line_2                                                 
        ,addr.city                                                           
        ,addr.state                                                         
        ,addr.zipcode                                                     
        ,addr.country                                                  
        ,case when  
            (con.home_ph_num is not null and nvl(con.hard_to_reach,'N') <> 'Y') 
             then regexp_replace(con.home_ph_num, "[^0-9]+", "") 
             else NULL end                                          
        ,case when 
            (con.work_ph_num is not null and nvl(con.speaker_flg,'N') <> 'Y') 
             then regexp_replace(con.work_ph_num, "[^0-9]+", "") 
             else NULL end 
        ,case when 
            ((con.cell_ph_num is not null and nvl(con.veteran_flg,'N') <> 'Y') 
                and regexp_replace(con.cell_ph_num, "[^0-9]+", "") not like '0_0000000_' 
                and regexp_replace(con.cell_ph_num, "[^0-9]+", "") not like '041111111_') 
             then regexp_replace(con.cell_ph_num, "[^0-9]+", "") 
             else NULL end 
        ,case when 
            ((con.asst_ph_num is not null and nvl(con.ok_to_sample_flg,'N') <> 'Y') 
                and regexp_replace(con.asst_ph_num, "[^0-9]+", "") <> '0000000000') 
             then regexp_replace(con.asst_ph_num, "[^0-9]+", "") 
             else NULL end 
        ,con.email_addr 
        ,con.alt_email_addr 
        ,from_unixtime(unix_timestamp(con.birth_dt),'yyyy-MM-dd HH:mm:ss') 
        ,conx.attrib_55 
        ,con.cust_value_cd 
        ,ord.order_num 
        ,ord.row_id 
        ,FROM_UNIXTIME(UNIX_TIMESTAMP(),'yyyy-MM-dd HH:mm:ss') 
        ,from_utc_timestamp(ord.order_dt,'AEST') 
         
having sum(case when prod.name like 'Autoclub%' or prod.name like "Free%" then 1 else 0 end) = 0 