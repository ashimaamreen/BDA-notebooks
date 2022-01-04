--create table campaign_data.aa_cad_10NovIssue_20211112 as
with renewal_base as (
select distinct membernumber, contact_id, vehicle_rego from sandpit.renewal_base 
)
, rta as (
SELECT job_number
        , rego
        , max(to_utc_timestamp(at_scene_time,'AEST')) as at_scene_time
        , min(to_utc_timestamp(activation_time,'AEST')) as activation_time
        , max((unix_timestamp(at_scene_time) - unix_timestamp(activation_time))/60) as rta_mins

from cad.cad_jobs cad

where completion_code not in ('DALLOC', 'JOBCANCL')
and to_date(to_utc_timestamp(activation_time,'AEST')) between '2021-11-09' and '2021-11-11'
GROUP BY 1,2
)
select srx.attrib_04 as cad_num
    , s.cst_ou_id account_id -- member's organisation id
    , coalesce(s.cst_con_id,b.contact_id, b2.contact_id) member_contact_id-- member contact id
    , case when rta_mins<60 then 'RTA60'
            when rta_mins<120 then 'RTA120'
            when rta_mins>=120 then 'RTA120+'
            else 'other' end RTA
    , rta_mins
    , activation_time
    , at_scene_time
    , c.con_cd as contact_Type
    , group_concat(distinct s.x_bus_type,' | ') as job_type
    , group_concat(s.root_cause_cd,' | ') as bkdn_reason
    , group_concat(s.sr_stat_id,' | ') as job_status
    ,regexp_replace(c.home_ph_num, "[^0-9]+", "") as home_phone 
    ,regexp_replace(c.work_ph_num, "[^0-9]+", "") as work_phone 
    ,regexp_replace(c.cell_ph_num, "[^0-9]+", "") as mobile1 
    ,regexp_replace(c.asst_ph_num, "[^0-9]+", "") as mobile2 
    ,c.email_addr as email1 
    ,case when trim(c.x_inv_email_1) = 'Y' or c.email_addr is null then 'N' else 'Y' end
    ,case when NVL(fn.deceased_flg,'N')='Y' or NVL(gmsContact.x_nrma_title,'no title') = 'Estate Of The Late' then 'Y' else 'N' end deceased_flag

from gms.s_srv_req_x as srx 
    
inner join gms.s_srv_req as s
on srx.row_id = s.row_id
and srx.attrib_04 is not null

left join renewal_base b
on b.vehicle_rego = srx.attrib_05

left join renewal_base b2
on b2.contact_id = s.cst_con_id

left join gms.s_contact c
on c.row_id=coalesce(s.cst_con_id,b.contact_id,b2.contact_id)

left join rta as r
on r.job_number=srx.attrib_04

left join gms.s_contact_fnx as fn 
on fn.par_row_id = c.row_id 


where
    1=1
    and s.x_bulk_source = "CAD" 
    and to_date(to_utc_timestamp(srx.attrib_27,'AEST'))='2021-11-10'
    and (s.cst_ou_id account_id is not null or coalesce(s.cst_con_id,b.contact_id, b2.contact_id) is not null)
    --and s.sr_stat_id not in ('ABORTED','CANCELLED')
 
 group by 1,2,3,4,5,6,7,8

 --------------------------------------------------------------------------------------------------------------------------------
 CREATE table campaign_data.aa_cad_10novissue_20211112_phonecall as
SELECT a.*
    , regexp_replace(c.home_ph_num, "[^0-9]+", "") as home_phone 
    , regexp_replace(c.work_ph_num, "[^0-9]+", "") as work_phone 
    , regexp_replace(c.cell_ph_num, "[^0-9]+", "") as mobile1 
    , regexp_replace(c.asst_ph_num, "[^0-9]+", "") as mobile2 
    , case when trim(c.x_inv_email_1) = 'Y' or c.email_addr is null then 'N' else 'Y' end valid_email
    , case when NVL(fn.deceased_flg,'N')='Y' or NVL(c.x_nrma_title,'no title') = 'Estate Of The Late' then 'Y' else 'N' end deceased_flag

FROM campaign_data.aa_cad_10novissue_20211112 a 

left join gms.s_contact c
on c.row_id=a.member_contact_id

left join gms.s_contact_fnx as fn 
on fn.par_row_id = c.row_id 
--------------------------------------------------------------------------------------------------------------