with active_org_ids as 
( 
    select distinct 
         con.csn                as member_number 
        ,con.row_id             as contact_id 
        ,con.cust_stat_cd       as contact_status 
        ,con.con_cd             as contact_type 
	    ,con.x_nrma_household_flg as household 
	    ,asset.owner_accnt_id   as org_id 
 
    from gms.s_contact as con 
 
	inner join gms.s_asset as asset 
	on asset.owner_accnt_id = con.pr_dept_ou_id  /* contact and asset are under the same account */ 
 
	inner join gms.s_prod_int as prod 
       on asset.prod_id = prod.row_id 
        
     
where asset.status_cd = 'Active'  /* Someone in the household has an active sub */ 
    and prod.type = 'Membership'    /* of a type that converys Membership */ 
    and prod.prod_cd = 'Promotion' 
), 
 
/* Joint accounts (JA) were only legal prior to GMS. This block filters members who are not deceased, joined prior to GMS,  
    and have 0 as second-to-last digit in member number, as Bonus migration appended -01 -02 etc to JA numbers */ 
members_with_zero as 
( 
select 
    distinct member_number 
    ,contact_id 
    ,contact_type 
    ,org_id 
    ,substring(member_number,1,length(member_number)-2) as root_member_number 
    ,fnx.bk_filed_dt as original_join_date 
    ,con.x_nrma_join_dt as join_date 
     
from gms.s_contact as con 
    inner join gms.s_contact_fnx as fnx 
        on fnx.par_row_id = con.row_id 
     
    inner join gms.s_org_ext as org 
        on con.pr_dept_ou_id = org.row_id 
         
    inner join active_org_ids 
        on org.row_id = active_org_ids.org_id 
         
where 1=1  
    AND	FROM_UTC_TIMESTAMP(con.x_nrma_join_dt, 'AEST') < '2015-12-07' /* original join date. Ts and Cs reference this date - no JAs permitted after this */ 
    AND NVL(fnx.deceased_flg,'N') <>'Y' /* we don't include dead people */ 
    AND substring(member_number,length(member_number)-1,2) in ('01','02') /* JMs were migrated from BONUS by adding -01 or -02 to shared JA numbers */ 
    AND contact_status = 'Active' 
), 
 
/* This next block is redundant with the one following, but I kept it here for reference. 
Adding: 
    count of contacts per org, 
    member number without the last two digits, which is what it would have been in BONUS if JA  
    count of contacts with same root member number */ 
-- members_in_households as  
-- ( 
-- select 
--     * 
--     ,count(members_with_zero.member_number) over (partition by members_with_zero.org_id)    as num_members_in_org 
--     ,count(members_with_zero.member_number) over (partition by members_with_zero.root_member_number)    as num_members_in_joint_account 
-- from members_with_zero 
-- ), 
 
/* Filtering only the JMs: 
        more then one in account (if they split the account they now each have a vote) 
        more than one with same root (if they joined post-GMS, root will be different and they each get a vote) 
   Only the first registered Member gets a Notice of Meeting, which is the lowest member number. JA_comms tells which. */ 
members_in_joint_accounts as ( 
select distinct 
    mz1.* 
    ,case when substring(mz1.member_number,length(mz1.member_number),1) = '1' then 'First Joint Member' else 'Joint Secondary' end as JA_comm 
 
from members_with_zero as mz1 
    inner join members_with_zero as mz2 
        on mz1.org_id = mz2.org_id  /* same GMS account */ 
        and mz1.root_member_number = mz2.root_member_number /* same BONUS account */ 
        and mz1.member_number <> mz2.member_number /* Different contact */ 
) 
 
SELECT DISTINCT 
      con.csn 		    	as member_number 
    , case when pricon.csn is not null then pricon.csn else con.csn end 
                            as primary_mem_no 
    , con.x_nrma_title 	    as title 
    , con.fst_name	    	as fst_name 
    , con.last_name	    	as last_name 
    , ''                    as company_name 
    , addr.row_id			as addr_id 
    , addr.addr 	    	as addr_line1 
    , addr.addr_line_2  	as addr_line2 
    , addr.city		    	as city 
    , addr.state	    	as state 
    , addr.zipcode 		    as post_code 
    , addr.country 	    	as country_code 
    , addr.msa 			    as dpid 
    , 'CT'                  as meshblock 
    , 'CT'                  as lga 
    , 'CT'                  as electorate 
    , case when con.email_addr is not null  
                and (NVL(con.x_inv_email_1,'N')<>'Y')  
                and substring(con.email_addr,1,7) not in ('noemail', 'donotre') 
        then con.email_addr end 
 	                        as email 
    , case when con.cell_ph_num is not null  
                and (NVL(con.veteran_flg,'N')<>'Y') 
                and substr(con.cell_ph_num,1,2) = '04' /* must be a mobile num starting with 04 */ 
                and regexp_replace(con.cell_ph_num, "[^0-9]+", "") not in /* common placeholders */ 
                    ('0400000000','0411111111','0412345678','0410000000','0404040404') 
	    then con.cell_ph_num end 
                            as mobile_num 
    , con.con_cd 			as contact_type 
    , org.ou_type_cd 		as account_type 
    , con.cust_value_cd		as loyalty_colour 
    , con.x_nrma_household_flg	as household_flg 
    , con.x_nrma_join_dt	as join_date 
    , fnx.bk_filed_dt		as original_join_dt 
    , conx.attrib_17		as membership_tenure 
    , conx.attrib_19		as accrued_tenure 
    , conx.attrib_39 		as AGM_NOM_Del_Method 
    , conx.attrib_40 		as Annual_Rep_Del_Method 
    , conx.attrib_38 		as Member_Review_Del_Method 
    , conx.attrib_35		as prom_channel_post 
    , conx.attrib_36		as prom_channel_email 
    , conx.attrib_42		as prom_channel_sms 
    , conx.attrib_43		as prom_channel_phone 
    , case  
        when jm.JA_comm = 'Joint Secondary'         then jm.JA_comm /* Joint secondary doesn't get a NOM */ 
        when con.email_addr is not null  
            and NVL(con.x_inv_email_1,'N') <> 'Y' 
            and substring(con.email_addr,1,7) not in ('noemail', 'donotre') 
            and NVL(conx.attrib_39,'Email') = "Email"       
            and tor.member_number is not null          then "Email+TOR"/* People who get both TOR and email*/ 
        when con.email_addr is not null  
            and NVL(con.x_inv_email_1,'N') <> 'Y'  
            and substring(con.email_addr,1,7) not in ('noemail', 'donotre') 
            and NVL(conx.attrib_39,'Email') = "Email"  then "Email"    /* If they have a valid email and the pref is NULL or Email, that's what they get */ 
        when tor.member_number is not null          then "TOR"      /* If they got TOR, they got the printed NOM inside it. */ 
        when con.email_addr is not null  
         and NVL(con.x_inv_email_1,'N') <> 'Y'  
         and conx.attrib_39 = "Post"                then "Post"     /* If they have a valid email and the pref is Post, they set it there themselves, so we honour the preference */ 
        when con.cell_ph_num is not null 
         and NVL(con.veteran_flg,'N') <> 'Y' 
         and substr(con.cell_ph_num,1,2) = '04' 
         and regexp_replace(con.cell_ph_num, "[^0-9]+", "")  
            not in ('0400000000','0411111111','0412345678','0410000000','0404040404') 
                                                    then "SMS"      /* If they have a mobile number and they are not in the groups above */ 
                                                    else "Post"     /* Everyone else gets post. */ 
        end                                                      as channel 
 
FROM 
    gms.s_contact AS con 
    inner join gms.s_contact_x AS conx 
    on con.par_row_id = conx.row_id 
 
    inner join gms.s_org_ext AS org 
    on org.row_id = con.pr_dept_ou_id 
		 
    inner join gms.s_addr_per  AS addr 
    on con.pr_per_addr_id = addr.row_id 
 
    inner join gms.s_contact_fnx as fnx 
	on con.row_id = fnx.par_row_id 
 
    inner join gms.s_contact as pricon 
    on org.pr_con_id = pricon.row_id 
     
    left join agm.wc_open_road_20210810 as tor 
    on tor.member_number = con.csn 
    and tor.extrcat_dt = '2021-07-30' 
     
    left join members_in_joint_accounts as jm 
    on jm.member_number = con.csn 
 
WHERE 
	( 
	con.con_cd = 'Ordinary Member' 
    AND	con.cust_stat_cd = 'Active' 
    AND	org.accnt_type_cd='Customer' 
    AND	org.ou_type_cd='Member Account' 
    AND NVL(fnx.deceased_flg,'N') <>'Y' 
    AND (con.x_nrma_title <>'Estate Of The Late' or con.x_nrma_title is null) 
    AND	(year(now()) - year(from_utc_timestamp(con.birth_dt,'AEST'))  
        + case when month(from_utc_timestamp(con.birth_dt,'AEST')) > month(now()) then -1 
          when month(from_utc_timestamp(con.birth_dt,'AEST')) = month(now())  
               and day(from_utc_timestamp(con.birth_dt,'AEST')) > day(now()) then -1 
          else 0  
        end) >=18 
    AND	con.csn IS NOT NULL 
-- The only null countries are test records 
    AND addr.country is not null 
-- Close of Members register 
    and (from_utc_timestamp(con.x_nrma_join_dt,'AEST') < '2021-10-14' or con.x_nrma_join_dt is null) 
    AND CON.fst_name not like '%ESTATE%' 
    ) 
OR jm.member_number is not null /* Everyone who qualifies as an Ordinary Members PLUS everyone in JMs */ 
 
/* Add in the business members */ 
union 
SELECT DISTINCT 
      org.ou_num 		   	as member_number 
    , org.ou_num        	as primary_mem_no 
    , con.x_nrma_title 	    as title 
    , con.fst_name	    	as fst_name 
    , con.last_name	    	as last_name 
    , org1.name			    as company_name 
    , addr.row_id           as addr_id 
    , addr.addr 	    	as addr_line1 
    , addr.addr_line_2  	as addr_line2 
    , addr.city		    	as city 
    , addr.state	    	as state 
    , addr.zipcode 		    as post_code 
    , addr.country 	    	as country_code 
    , addr.msa 			    as dpid 
    , 'CT'                  as meshblock 
    , 'CT'                  as lga 
    , 'CT'                  as electorate 
    , case when ((org.main_email_addr is not null) and (nvl(org.x_invld_email_addr,'N') <> 'Y'))  
            then org.main_email_addr  
           when (con.email_addr is not null) and NVL(con.x_inv_email_1,'N')<>'Y'  
            then con.email_addr end 
 	                        as email 
    , case when (con.cell_ph_num is not null 
                and NVL(con.veteran_flg,'N')='N' 
                and regexp_replace(con.cell_ph_num, "[^0-9]+", "") <> '0000000000') 
	    then con.cell_ph_num end 
                            as mobile_num 
    , con.con_cd 			as contact_type 
    , org.ou_type_cd 		as Account_type 
    , orgx.attrib_35    	as loyalty_colour 
    , ''				    as household_flg 
    , org.x_nrma_join_dt	as join_date 
    , ''			    	as original_join_dt 
    , orgx.attrib_25	    as membership_tenure 
    , orgx.attrib_59	    as accrued_tenure 
    , orgx.attrib_49    	as AGM_NOM_Del_Method 
    , orgx.attrib_51    	as Annual_Rep_Del_Method 
    , orgx.attrib_50    	as Member_Review_Del_Method 
    , ''	        	    as prom_channel_post 
    , ''	        	    as prom_channel_email 
    , ''	            	as prom_channel_sms 
    , ''	        	    as prom_channel_phone 
    , case when  
            (( 
                org.main_email_addr is not null 
                and (nvl(org.x_invld_email_addr,'N') <> 'Y') 
                and substring(org.main_email_addr,1,7) not in ('noemail', 'donotre') 
            )  
            or  
            ( 
                con.email_addr is not null 
                and (NVL(con.x_inv_email_1,'N')<>'Y') 
                and substring(con.email_addr,1,7) not in ('noemail', 'donotre') 
            )) 
            and NVL(orgx.attrib_49,"Email") <> "Post"           then "Email" /* if we've got an email on the account or contact, use it unless they asked for post */ 
                                                                else "Post" 
      end                                                       as channel 
 
FROM 
    gms.s_contact AS con 
 
    inner join gms.s_org_ext AS org 
    on org.pr_con_id = con.row_id 
 
    inner join gms.s_org_ext as org1 
    on org.row_id = org1.par_ou_id 
 
    inner join gms.s_org_ext_x as orgx 
    on org.row_id = orgx.par_row_id 
		 
    inner join gms.s_addr_per  AS addr 
    on org1.pr_addr_id = addr.row_id 
     
WHERE 
	org.ou_type_cd = 'Member Organisation' 
AND	org.cust_stat_cd = 'Active' 
AND	org.accnt_type_cd='Customer' 
AND org1.ou_type_cd = 'Member Organisation' 
AND	org1.accnt_type_cd ='Billing' 
AND org1.cust_stat_cd = 'Active' 
-- Close of Members register 22 Sept 2021 
and (from_utc_timestamp(con.x_nrma_join_dt,'AEST') < '2021-10-14' or con.x_nrma_join_dt is null) 