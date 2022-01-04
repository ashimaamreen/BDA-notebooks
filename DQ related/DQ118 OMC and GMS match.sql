select count(*), count(distinct customer_id_) from sandpit.cd_dq118_omclist_20200730    --3771917   
with gms_contacts as ( 
select distinct c.row_id as gms_contact_id 
        , c.con_cd as gms_contact_type 
        , c.cust_stat_cd as gms_contact_status 
        , fnx.deceased_flg as gms_deceased_flag 
        , case when cx.attrib_36='No' then 'O' else 'I' end gms_email_permission_status_ 
 
from gms.s_contact c 
 
inner join gms.s_contact_x cx 
on cx.par_row_id=c.row_id 
 
inner join gms.s_contact_fnx fnx 
on fnx.par_row_id=c.row_id 
 
) 
 
select count(*) from sandpit.cd_dq118_omclist_20200730 omc 
 
left anti join gms_contacts gms 
on gms.gms_contact_id=omc.customer_id_ 
 
where omc.contact_type='Business Contact' 
with gms_contacts as ( 
select distinct c.row_id as gms_contact_id 
        , c.con_cd as gms_contact_type 
        , c.cust_stat_cd as gms_contact_status 
        , fnx.deceased_flg as gms_deceased_flag 
        , case when cx.attrib_36='No' then 'O' else 'I' end gms_email_permission_status_ 
        , omc.* 
 
from gms.s_contact c 
 
inner join gms.s_contact_x cx 
on cx.par_row_id=c.row_id 
 
inner join gms.s_contact_fnx fnx 
on fnx.par_row_id=c.row_id 
 
left outer join sandpit.cd_dq118_omclist_20200730 omc  
on c.row_id=omc.customer_id_ 
 
) 
 
select gms_contact_type, contact_type, count(*) from gms_contacts 
 
-- where contact_type='Customer' 
-- and gms_contact_type !='Customer' 
group by 1,2 