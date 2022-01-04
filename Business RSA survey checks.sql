/* 
 
Notes: 
1. s_evt_act.sra_sr_id = s_srv_req.row_id 
2. x_bus_type has "PATROL", "NULL", "AUTOELEC", "OTHER", "CARELEC", "BATTERY", "TOW" 
3. await ring patrol in "resolution_cd" means that a job was hold in CAD for 14 days 
    in case the member phoned back 
4. cad_num = "20200115002197" --> flat battery, await ring battery 
5. cad_num = "20200107002495" --> FLPVE, business members 
6. background of CAD job process 
    --> receive time, the first time when someone called IVR and asked for a job. If there are multiple jobs under one SR number, it would be the earliest one.  
    --> receipt time, sc1: the first time when someone called IVR and asked for a job; sc2: if the IVR called, and the job didn't complete, receipt time will be different from the first time called 
    --> allocation time, the time when a job is allocated to a service provider (in system) 
    --> despatch time, the time when a job and its info sent to the service provider's terminal 
    --> acknowledge time, the time when the service provider clicks the acknowledge button in the terminal 
    --> enroute time, the time when the service provider clicks the button indicating he / she is going to the destination 
    --> at scene time, the time when the service provider is on the spot 
    --> completion time, the time when the service provider completes the job 
 
*/ 
 
with job_mapping as ( 
    select distinct  
        s.x_bus_type as job_type 
        , s.sr_category_cd as bkdn_cd 
        , s.root_cause_cd 
    from gms.s_srv_req as s 
    where 
        1=1 
        and s.x_bulk_source = "CAD" 
) 
 
select distinct 
    srx.attrib_04 as cad_num 
    , s.sr_num -- service number 
    , s.x_bus_type as job_type 
    , s.sr_subtype_cd as job_subtype 
    , s.sr_category_cd as bkdn_cd 
    , s.root_cause_cd as bkdn_reason 
    , s.sr_prio_cd as job_priority 
    , s.sr_stat_id as job_status 
    , s.x_bulk_source as source 
    , s.resolution_cd -- full name of completion cd  
    , s.asset_id 
    , s.ou_addr_id 
    , s.per_addr_id 
    , s.cst_ou_id -- member's organisation id 
    , s.cst_con_id -- member contact id 
    , srx.attrib_27 as receive_dt 
    , srx.attrib_28 as receipt_dt 
    , srx.attrib_26 as ack_dt -- accurate 
    , srx.attrib_13 as enroute_dt -- accurate 
    , srx.attrib_12 as at_scene_dt -- accurate 
    , srx.attrib_30 as completion_dt -- accurate 
from gms.s_srv_req_x as srx  
    inner join gms.s_srv_req as s 
    on srx.row_id = s.row_id 
    and srx.attrib_04 is not null 
where 
    1=1 
    --and to_date(s.created) >= "2020-01-01" 
    and s.x_bulk_source = "CAD"  
    and s.x_bus_type = "PATROL"  
    and srx.attrib_04 = "20170109002755"  -- for testing purpose 
 
 
 
 
 
 
/* 
for comparison between cad and gms in bda 
 
*/ 
 
-- with gms as ( 
 
-- select  
--     srx.attrib_04 as cad_num 
--     , s.sr_num -- service number 
--     , s.x_bus_type as job_type 
--     , s.sr_subtype_cd as job_subtype 
--     , s.sr_category_cd as bkdn_cd 
--     , s.root_cause_cd as bkdn_reason 
--     , s.sr_prio_cd as job_priority 
--     , s.sr_stat_id as job_status 
--     , s.x_bulk_source as source 
--     , s.resolution_cd -- full name of completion cd  
--     , s.asset_id 
--     , s.ou_addr_id 
--     , s.per_addr_id 
--     , s.cst_ou_id -- member's organisation id 
--     , s.cst_con_id -- member contact id 
--     , srx.attrib_27 as receive_dt 
--     , srx.attrib_28 as receipt_dt 
--     , srx.attrib_26 as ack_dt -- accurate 
--     , srx.attrib_13 as enroute_dt -- accurate 
--     , srx.attrib_12 as at_scene_dt -- accurate 
--     , srx.attrib_30 as completion_dt -- accurate 
--     , row_number() over (partition by srx.attrib_04 order by srx.attrib_28) as index 
-- from gms.s_srv_req_x as srx  
--     inner join gms.s_srv_req as s 
--     on srx.row_id = s.row_id 
--     and srx.attrib_04 is not null 
-- where 
--     1=1 
--     --and to_date(s.created) >= "2020-01-01" 
--     and s.x_bulk_source = "CAD"  
--     -- and s.x_bus_type = "PATROL"  
--     -- and srx.attrib_04 = "20170109002755"  -- for testing purpose 
-- ), cad as ( 
     
--     select job_number 
--         ,  breakdown_code_1 
--         , recieved_time 
--         , activation_time 
--         , allocation_time 
--         , acknowledge_time 
--         , enroute_time 
--         , at_scene_time 
--         , completion_time  
--         , row_number() over (partition by job_number order by activation_time) as index 
--     from cad.cad_jobs 
--     where 
--         1=1 
--         -- and job_number = "20170109002755" 
 
-- ), comp as ( 
 
-- select gms.cad_num 
--     , gms.job_type 
--     , hours_add(cad.allocation_time, 11) as cad_alc 
--     , gms.receipt_dt as gms_rpt 
--     , unix_timestamp(hours_add(cad.allocation_time,11)) - unix_timestamp(gms.receipt_dt) as alc_diff-- down to seconds 
--     , cad.completion_time as cad_cpl 
--     , gms.completion_dt as gms_cpl 
--     , unix_timestamp(cad.completion_time) - unix_timestamp(gms.completion_dt) as cpl_diff -- down to seconds 
-- from cad 
--     inner join gms 
--     on cast(gms.cad_num as string) = cast(cad.job_number as string) 
--     and cad.index = gms.index 
-- where 
--     1=1 
--     and to_date(gms.receipt_dt) between "2019-11-01" and "2020-03-31" -- only AEST for comparison 
-- ) 
 
-- select case when abs(cpl_diff) <= 3600 then "<=1hr" else ">1hr" end as diff 
--     , comp.job_type 
--     , count(distinct cad_num) as vol 
-- from comp 
-- group by 1,2 
-- order by 2,1 
 
with job_mapping as ( 
    select distinct  
        s.x_bus_type as job_type 
        , s.sr_category_cd as bkdn_cd 
        , s.root_cause_cd 
    from gms.s_srv_req as s 
    where 
        1=1 
        and s.x_bulk_source = "CAD" 
) 
 
select distinct 
    srx.attrib_04 as cad_num 
    , s.sr_num -- service number 
    , s.x_bus_type as job_type 
    , s.sr_subtype_cd as job_subtype 
    , s.sr_category_cd as bkdn_cd 
    , s.root_cause_cd as bkdn_reason 
    , s.sr_prio_cd as job_priority 
    , s.sr_stat_id as job_status 
    , s.x_bulk_source as source 
    , s.resolution_cd -- full name of completion cd  
    , s.asset_id 
    , s.ou_addr_id 
    , s.per_addr_id 
    , s.cst_ou_id -- member's organisation id 
    , s.cst_con_id -- member contact id 
    , srx.attrib_27 as receive_dt 
    , srx.attrib_28 as receipt_dt 
    , srx.attrib_26 as ack_dt -- accurate 
    , srx.attrib_13 as enroute_dt -- accurate 
    , srx.attrib_12 as at_scene_dt -- accurate 
    , srx.attrib_30 as completion_dt -- accurate 
from gms.s_srv_req_x as srx  
    inner join gms.s_srv_req as s 
    on srx.row_id = s.row_id 
    and srx.attrib_04 is not null 
where 
    1=1 
    --and to_date(s.created) >= "2020-01-01" 
    --and s.x_bulk_source = "CAD"  
    --and s.x_bus_type = "PATROL"  
    and srx.attrib_04 = "20200609000798"  -- for testing purpose 