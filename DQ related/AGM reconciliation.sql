with link_channels as 
( 
select 
    substring(hrn,4) as mn 
    ,case  
        when are = "EEN" and flag = "OR" then "Email+TOR" 
        when are = "EEN" then "Email" 
        when flag = "OR" then "TOR" 
        when are = "NNA" then "Post" 
        when are = "ONN" then "SMS" 
        when mobile = "EEN" then "Email" 
        else "Other" end as channel 
    ,are 
    ,flag 
    ,`group` 
from link_2021_distribution 
), 
differences as 
( 
select agm.member_number as amn 
        ,agm.household_flg 
        ,agm.contact_type 
        ,agm.channel as Cherie_channel 
        ,link_channels.channel as Link_channel 
        ,case when agm.mobile_num is null then 0 else 1 end as has_mobile 
        ,case when agm.email is null then 0 else 1 end as has_email 
        ,case when tor.member_number is null then 0 else 1 end as got_tor 
        ,agm.agm_nom_del_method 
 
from agm_21_complete_20210812 as agm 
    inner join link_channels 
    on agm.member_number = link_channels.mn 
    and agm.channel <> link_channels.channel 
     
    left join wc_open_road_20210810 as tor 
    on agm.member_number = tor.member_number 
) 
select * 
--count(*),achan,lchan 
     
from differences 
    left anti join joint_members 
    on differences.amn = joint_members.member_number 
     
where Cherie_channel <> "Joint Secondary" 
and not ( 
    (Cherie_channel = "Email+TOR" and Link_channel = "TOR") 
    or 
    (Cherie_channel = "Email" and Link_channel = "Email+TOR") 
    ) 
--and contact_type = "Ordinary Member" 
--group by 2,3 
order by 2,3 
 