Contents
spark.sql(""" 
select distinct 
         c.csn as ID 
        ,'50500' as campaign_code 
        ,'MPP_dishonour_winback' as campaign_name 
        ,c.csn as Membership_Number 
        ,c.row_id as customer_id 
        ,c.x_nrma_title as title 
        ,c.fst_name as firstname 
        ,c.last_name as lastname 
        ,'' as organisation_name 
        ,'' as ABN 
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
        ,'' as dpid 
        ,c.birth_dt as DOB 
        ,x.attrib_55 as colour_segment 
        ,c.cust_value_cd as loyalty_group 
        ,'' as order_number 
        ,'' as order_ID 
        ,from_unixtime(unix_timestamp(current_timestamp(),'yyyy-MM-dd hh:mm:ss.s'),'yyyy-MM-dd hh:mm:ss') as file_created_date 
        ,'' as priority_segment 
        ,'' as customised_field1 
        ,'' as customised_field2 
        ,'' as customised_field3 
        ,'' as customised_field4 
        ,'' as customised_field5 
        ,'' as customised_field6 
        ,'' as customised_field7 
        ,'' as customised_field8 
         
from  
  gms.s_contact as c 
         
    inner join gms.s_contact_fnx as fn 
        on fn.par_row_id = c.row_id 
         
    inner join gms.s_contact_x as x 
        on x.par_row_id = c.row_id 
         
    inner join gms.s_addr_per as a 
        on a.row_id = c.pr_per_addr_id 
         
where  
    c.con_cd = 'Affiliate Member' 
    and c.cust_stat_cd = 'Active'   
    and NVL(fn.deceased_flg,'N') = 'N' 
    and NVL(x.attrib_43,'Yes') <> 'No' 
""").show(10) 
create table sandpit.aa_dmc_1297_sentfile as 
SELECT a.* 
FROM gms.cx_cam_response cam 
inner join gms.s_contact c 
on c.row_id = cam.contact_id 
 
INNER JOIN sandpit.aa_dmc_1297 a 
on a.customer_id = cam.contact_id 
 
where cam.campaign_id='32814242' 
and cam.launch_id='80644182' 
and cam.status_cd='Sent' 
and c.con_cd ='Affiliate Member' 
and c.CUST_STAT_CD ='Active' 
 
create table sandpit.aa_dmc_1297_final as 
select * from 
 
( 
 
        SELECT    c.* 
                , value.latestmonth 
                , value.latestvalue 
                , rank() over(order by latestvalue desc)  RankValue 
        FROM sandpit.aa_dmc_1297_sentfile c 
        
         
        left join 
        ( 
                select    val.monthid latestmonth 
                        , val.membernumber  
                        , val.total_value latestvalue 
                from mlpipelinedb.rank_value_member val 
                 
                inner join  
                ( 
                    select    membernumber  
                            , max(monthid) monthid 
                    from mlpipelinedb.rank_value_member 
                    group by membernumber 
                ) latest 
                on val.membernumber = latest.membernumber 
                and val.monthid = latest.monthid 
        )  value 
        on c.membership_number = value.membernumber 
         
         
        where latestvalue is not null 
 
) granular 
 
where RankValue <= 5000 
 
order by RankValue 
select distinct 
         c.csn as ID 
        ,'50500' as campaign_code 
        ,'MPP_dishonour_winback' as campaign_name 
        ,c.csn as Membership_Number 
        ,c.row_id as customer_id 
        ,c.x_nrma_title as title 
        ,c.fst_name as firstname 
        ,c.last_name as lastname 
        ,'' as organisation_name 
        ,'' as ABN 
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
        ,'' as dpid 
        ,c.birth_dt as DOB 
        ,x.attrib_55 as colour_segment 
        ,c.cust_value_cd as loyalty_group 
        ,'' as order_number 
        ,'' as order_ID 
        , cast(current_timestamp() as timestamp) as file_created_date 
        ,'' as priority_segment 
        ,'' as customised_field1 
        ,'' as customised_field2 
        ,'' as customised_field3 
        ,'' as customised_field4 
        ,'' as customised_field5 
        ,'' as customised_field6 
        ,'' as customised_field7 
        ,'' as customised_field8 
         
from  
  gms.s_contact as c 
         
    inner join gms.s_contact_fnx as fn 
        on fn.par_row_id = c.row_id 
         
    inner join gms.s_contact_x as x 
        on x.par_row_id = c.row_id 
         
    inner join gms.s_addr_per as a 
        on a.row_id = c.pr_per_addr_id 
         
where  
    c.con_cd = 'Affiliate Member' 
    and c.cust_stat_cd = 'Active'   
    and NVL(fn.deceased_flg,'N') = 'N' 
    and NVL(x.attrib_43,'Yes') <> 'No' 
    limit 100 
drop table sandpit.aa_dmc_1297_2 
create table campaign_data.aa_20181116_c_dmc1297_winbakOTM as 
select   ID 
        ,campaign_code 
        ,campaign_name 
        ,Membership_Number 
        ,customer_id 
        ,title 
        ,firstname 
        ,lastname 
        ,organisation_name 
        ,ABN 
        ,address1 
        ,address2 
        ,suburb 
        ,state 
        ,postcode 
        ,country 
        ,home_phone 
        ,work_phone 
        ,mobile1 
        ,mobile2 
        ,email1 
        ,email2 
        ,dpid 
        ,DOB 
        ,colour_segment 
        ,loyalty_group 
        ,order_number 
        ,order_ID 
        ,file_created_date 
        ,priority_segment 
        ,customised_field1 
        ,customised_field2 
        ,customised_field3 
        ,customised_field4 
        ,customised_field5 
        ,customised_field6 
        ,customised_field7 
        ,customised_field8 from sandpit.aa_dmc_1297_final