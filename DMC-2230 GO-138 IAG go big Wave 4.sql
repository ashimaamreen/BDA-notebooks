with main as( 
select iag.membernumber,max(iag.probability) as prob 
from sandpit.iag_mot_base_20200715_ma_gbtclassifier_pred iag            --should this be max or recent score 
group by 1 
) 
 
select count(*), count(distinct iag.membernumber) from main iag 
 
left anti join m4m.return_feed_header m 
on m.member_number=iag.membernumber 
and partner in ('IAG','NRMA Multi Policy Discount','NRMA MPD Insurance','NRMA Insurance') 
select distinct v_score from campaign_data.aa_dmc2189_iagGoBig_iagscoring 
iag_scoring = spark.read.load("/user/aamreen/IAG/IAG_Scoring_v2.csv",format="csv", sep=",", inferSchema="true", header="true") 
iag_scoring.createOrReplaceTempView("iag_scoring") 
spark.sql('''create table campaign_data.aa_dmc2189_iagGoBig_iagscoring as select * from iag_scoring''').show(1000,False) 
--drop table campaign_data.aa_dmc2189_iagGoBig_iagscoring 
SELECT * from campaign_data.aa_dmc2189_iagGoBig_iagscoring 
with iag_prob as ( 
select iag.membernumber,max(iag.probability) as prob 
from sandpit.iag_mot_base_20200715_ma_gbtclassifier_pred iag 
group by 1 
) 
, main_Table as( 
select  
case when trim(c.x_inv_email_1) = 'Y' or c.email_addr is null or cx.attrib_36='No' then 'N'  else 'Y' end email_able 
        , case when trim(ad.premise_flg) = 'Y' or ad.addr_name is null or cx.attrib_35='No' then 'N' else 'Y' end DM_able  
        , rank() over (order by cast(iag.prob as float) desc) as ranking 
        , ntile(10) over (order by cast(iag.prob as float) desc) as decile 
        , iag.prob 
        , c.row_id as contact_id 
        , c.csn as member_number 
 
 
from iag_prob iag 
 
left anti join m4m.return_feed_header m 
on m.member_number=iag.membernumber 
and partner in ('IAG','NRMA Multi Policy Discount','NRMA MPD Insurance','NRMA Insurance') 
 
inner join gms.s_contact c 
on c.csn=iag.membernumber 
 
inner join gms.s_contact_x cx 
on cx.par_row_id=c.row_id 
 
inner join gms.s_contact_fnx as fn  
on fn.par_row_id = c.row_id 
 
inner join gms.s_addr_per as ad         --813620 
on c.pr_per_addr_id = ad.row_id 
 
where 1=1 
and c.cust_stat_cd = 'Active' 
and c.con_cd in ('Ordinary Member' , 'Affiliate Member') 
and NVL(c.x_nrma_title,'no title') != 'Estate Of The Late' 
and NVL(fn.deceased_flg,'N') = 'N' 
and c.csn is not null 
) 
select * from main_Table 
--count(distinct contact_id)  from main_Table 
--where email_able='Y' 
-- group by 1 
-- order by 1 
 
 
 
-- with vehicle_make as( 
-- SELECT distinct b.membernumber 
--         , b.contact_id 
--         , b.vehicle_make 
-- from sandpit.renewal_base b 
 
-- where 1=1 
-- and b.contact_cd in ('Ordinary Member' , 'Affiliate Member') 
-- and b.asset_cancel_dt is null 
-- and b.asset_status_cd='Active' 
-- ) 
select count(distinct c.row_id)  
from  gms.s_contact c  
 
left anti join m4m.return_feed_header m 
on m.member_number=c.csn 
and partner in ('IAG','NRMA Multi Policy Discount','NRMA MPD Insurance','NRMA Insurance') 
 
left join sandpit.renewal_base b 
on c.csn=b.membernumber 
 
inner join gms.s_contact_x cx 
on cx.par_row_id=c.row_id 
 
inner join gms.s_contact_fnx as fn  
on fn.par_row_id = c.row_id 
 
inner join gms.s_addr_per as ad         --813620 
on c.pr_per_addr_id = ad.row_id 
 
 
select distinct v.vehicle_make as iag_vehicle_make ,v_score from campaign_data.aa_dmc2189_iagGoBig_iagscoring 
drop table campaign_data.aa_dmc2230_IAGGoBig_campaign_eDM_20201204_adhoc 
--create table campaign_data.aa_dmc2230_IAGGoBig_campaign_eDM_20201204_adhoc as   
with iag_prob as ( 
select iag.membernumber,max(iag.probability) as prob 
from sandpit.iag_mot_base_20200715_ma_gbtclassifier_pred iag 
group by 1 
) 
, main_Table as( 
select c.row_id as contact_id 
        , case when trim(c.x_inv_email_1) = 'Y' or c.email_addr is null or cx.attrib_36='No' then 'N'  else 'Y' end email_able 
        , case when trim(ad.premise_flg) = 'Y' or ad.addr_name is null or cx.attrib_35='No' then 'N' else 'Y' end DM_able  
        , ntile(10) over (order by cast(iag.prob as float) desc) as decile 
        , iag.prob 
        --, c.row_id as contact_id 
        , c.csn as member_number 
        , (year(now()) - year(c.birth_dt)) AS Age 
        , b.vehicle_make 
        , ad.zipcode as postcode 
 
 
from gms.s_contact c 
 
left join iag_prob iag 
on c.csn=iag.membernumber 
 
left anti join m4m.return_feed_header m 
on m.member_number=c.csn 
and partner in ('IAG','NRMA Multi Policy Discount','NRMA MPD Insurance','NRMA Insurance') 
 
inner join gms.s_contact_x cx 
on cx.par_row_id=c.row_id 
 
inner join gms.s_contact_fnx as fn  
on fn.par_row_id = c.row_id 
 
inner join gms.s_addr_per as ad         --813620 
on c.pr_per_addr_id = ad.row_id 
 
left join sandpit.renewal_base b 
on c.csn=b.membernumber 
 
INNER JOIN campaign_data.aa_dmc2230_iagGoBig_matched i 
on cast(i.nrma_member_id as STRING)=c.csn 
 
-- left join campaign_data.aa_dmc2189_iagGoBig_iagscoring s 
-- on s.`vehicle make`=b.vehicle_make 
 
where 1=1 
and c.cust_stat_cd = 'Active' 
and c.con_cd in ('Ordinary Member' , 'Affiliate Member') 
and NVL(c.x_nrma_title,'no title') != 'Estate Of The Late' 
and NVL(fn.deceased_flg,'N') = 'N' 
and c.csn is not null 
), final as ( 
select distinct m.member_number, m.contact_id 
                , m.vehicle_make 
                , m.age 
                , m.postcode 
                , NVL(v.v_score,99) v1 
                , nvl(a.a_score,99) a1 
                , nvl(p.p_score,99) p1 
                , m.email_able 
                , m.decile 
                , m.prob 
         
from main_Table m       --736607 
 
left join (select distinct upper(vehicle_make) as iag_vehicle_make ,v_score from campaign_data.aa_dmc2189_iagGoBig_iagscoring)  v         
on v.iag_vehicle_make=m.vehicle_make 
 
left join (select distinct driver_age as iag_driver_age, a_score from campaign_data.aa_dmc2189_iagGoBig_iagscoring) a         
on a.iag_driver_age=m.age 
 
left join (select distinct cast(postcode as string) as iag_postcode, p_score from campaign_data.aa_dmc2189_iagGoBig_iagscoring) p        
on p.iag_postcode=m.postcode 
) 
select *, v1+a1+p1 as iag_score from final 
--having sum(v1,a1,p1) 
 
--where m.member_number='469600301' 
select * from m4m.return_feed_header 
where member_number='990699753' 
SELECT count(DISTINCT member_number) from campaign_data.aa_dmc2230_IAGGoBig_campaign_eDM_20201204_adhoc 
where iag_score between 2 and 5 
and email_able='Y' 
 
select count(distinct member_number), count(*) from campaign_data.aa_dmc2189_IAGGoBig_campaign_eDM_20200915 
where email_able='Y' 
spark.sql(''' 
with score as( 
select distinct member_number, decile, DM_able, email_able 
        , v1+a1+p as iag_score 
     
from campaign_data.aa_dmc2189_IAGGoBig_campaign_eDM_20200916 
) 
select decile,iag_score,count(distinct member_number) 
from score 
--where iag_score<10 
--where email_able='Y' 
group by 1,2 
order by 1,2 
''').show(1000) 
select distinct vehicle_make, iag_vehicle_make from aa_dmc2189_iaggobig_campaign_edm_20200914_adhoc 
where vehicle_make ='ISUZUUTE' 
--like '%ISUZU%' 
select distinct vehicle_make from sandpit.renewal_base 
where vehicle_make like '%ISUZU%' 
select distinct a.contact_id, a.col1, iag.nrma_member_id from campaign_data.campaign_contact_adhoc a 
 
INNER JOIN campaign_data.aa_dmc2230_iagGoBig_matched iag 
on cast(iag.nrma_member_id as STRING)=a.col1 
 
where table_id='97' 
--like '%dmc2230%' 
--aa_dmc2230_IAGGoBig_campaign_eDM_20201204_adhoc 
--campaign_contact_adhoc 
--where table_name='aa_dmc2230_IAGGoBig_campaign_eDM_20201204_adhoc' 
SELECT * from campaign_data.aa_dmc2189_IAGGoBig_campaign_eDM_20200916 
where member_number='852225601' 
 
SELECT DISTINCT csn as member_number, m.time_stamp 
--, m.member_number, m.partner, m.time_stamp, m.created_date,   
 
from gms.s_contact c 
         
inner JOIN m4m.return_feed_header m 
on m.member_number=c.csn 
and m.partner in ('IAG','NRMA Multi Policy Discount','NRMA MPD Insurance','NRMA Insurance') 
 
INNER JOIN m4m.return_feed_detail f 
on f.trx_header_id=m.trx_header_id 
 
INNER JOIN campaign_data.aa_dmc2230_iagGoBig_matched iag 
on cast(iag.nrma_member_id as STRING)=c.csn 
 
where c.csn is not null                                           --#5779 
and c.cust_stat_cd = 'Active'            
SELECT DISTINCT m.member_number, m.partner, m.time_stamp, m.created_date, m.total_amount 
 
from gms.s_contact c 
        
inner JOIN m4m.return_feed_header m 
on m.member_number=c.csn 
and m.partner in ('IAG','NRMA Multi Policy Discount','NRMA MPD Insurance','NRMA Insurance') 
 
INNER JOIN campaign_data.aa_dmc2230_iagGoBig_matched iag 
on cast(iag.nrma_member_id as STRING)=c.csn 
 
where c.csn is not null                                           --#5779 
and c.cust_stat_cd = 'Active'    
-- --and c.csn='990366292' 
 
ORDER BY m.total_amount DESC 
iag_matched = spark.read.load("/user/aamreen/IAG/IAG_Matched_records.csv",format="csv", sep=",", inferSchema="true", header="true") 
iag_matched.createOrReplaceTempView("iag_matched") 
spark.sql('''create table campaign_data.aa_dmc2230_iagGoBig_matched as select * from iag_matched''').show(1000,False) 
select * from campaign_data.aa_dmc2230_iagGoBig_matched 
SELECT * from campaign_data.aa_dmc2230_IAGGoBig_campaign_eDM_20201204_adhoc 
SELECT c.clientmembershipid, c.membernumber, c.headerclientmembershipid, c.headermembernumber, iag.nrma_member_id from campaign_data.aa_dmc2230_iagGoBig_unmatched c 
 
INNER JOIN campaign_data.aa_dmc2230_iagGoBig_matched iag 
on cast(iag.nrma_member_id as STRING)=c.membernumber 
 
-- inner join gms.s_contact con 
-- on con.csn=c.membernumber 
 
 
 
where partnername in ('IAG','NRMA Multi Policy Discount','NRMA MPD Insurance','NRMA Insurance') 
 
spark.sql(''' 
SELECT DISTINCT iag.nrma_member_id 
        , case WHEN c.membernumber is null then 'Present in IAG but not in AMS' else 'Present in unmatched file and IAG but not AMS' end Flag  
        --, sum(case WHEN m.member_number is null then 0 else 1 end)  
from campaign_data.aa_dmc2230_iagGoBig_matched iag 
 
 
left JOIN campaign_data.aa_dmc2230_iagGoBig_unmatched c 
on cast(iag.nrma_member_id as STRING)=c.membernumber 
 
LEFT anti JOIN m4m.return_feed_header m 
on m.member_number=cast(iag.nrma_member_id as STRING) 
and m.partner in ('IAG','NRMA Multi Policy Discount','NRMA MPD Insurance','NRMA Insurance') 
 
-- inner join gms.s_contact con 
-- on con.csn=c.membernumber 
''').show(10000,False) 
unmatched = spark.read.load("/user/hive/warehouse/m4m.db/externalFiles/AMS/*.csv", format="csv", sep="|", inferSchema="true", header="true") 
unmatched.createOrReplaceTempView("unmatched") 
spark.sql(""" create table campaign_data.aa_dmc2230_iagGoBig_unmatched select * from unmatched  """).show(10,False) 
select * from campaign_data.aa_dmc2230_iagGoBig_unmatched 
create table campaign_data.aa_dmc2230_IAGGoBig_campaign_eDM_20201211 as   
with iag_prob as ( 
select iag.membernumber,max(iag.probability) as prob 
from sandpit.iag_mot_base_20200715_ma_gbtclassifier_pred iag 
group by 1 
) 
, main_Table as( 
select c.row_id as contact_id 
        , case when trim(c.x_inv_email_1) = 'Y' or c.email_addr is null or cx.attrib_36='No' then 'N'  else 'Y' end email_able 
        , case when trim(ad.premise_flg) = 'Y' or ad.addr_name is null or cx.attrib_35='No' then 'N' else 'Y' end DM_able  
        , ntile(10) over (order by cast(iag.prob as float) desc) as decile 
        , iag.prob 
        --, c.row_id as contact_id 
        , c.csn as member_number 
        , (year(now()) - year(c.birth_dt)) AS Age 
        , b.vehicle_make 
        , ad.zipcode as postcode 
 
 
from gms.s_contact c 
 
left join iag_prob iag 
on c.csn=iag.membernumber 
 
inner join gms.s_addr_per as ad         --813620 
on c.pr_per_addr_id = ad.row_id 
 
left anti join m4m.return_feed_header m 
on m.member_number=c.csn 
and partner in ('IAG','NRMA Multi Policy Discount','NRMA MPD Insurance','NRMA Insurance') 
 
left anti join campaign_data.aa_dmc2230_iagGoBig_unmatched u 
on u.membernumber=c.csn 
and partnername in ('IAG','NRMA Multi Policy Discount','NRMA MPD Insurance','NRMA Insurance') 
 
inner join gms.s_contact_x cx 
on cx.par_row_id=c.row_id 
 
inner join gms.s_contact_fnx as fn  
on fn.par_row_id = c.row_id 
 
left join sandpit.renewal_base b 
on c.csn=b.membernumber 
 
where 1=1 
and c.cust_stat_cd = 'Active' 
and c.con_cd in ('Ordinary Member' , 'Affiliate Member') 
and NVL(c.x_nrma_title,'no title') != 'Estate Of The Late' 
and NVL(fn.deceased_flg,'N') = 'N' 
and c.csn is not null 
), final as ( 
select distinct m.member_number, m.contact_id 
                , m.vehicle_make 
                , m.age 
                , m.postcode 
                , NVL(v.v_score,99) v1 
                , nvl(a.a_score,99) a1 
                , nvl(p.p_score,99) p1 
                , m.email_able 
                , m.decile 
                , m.prob 
         
from main_Table m       --736607 
 
left join (select distinct upper(vehicle_make) as iag_vehicle_make ,v_score from campaign_data.aa_dmc2189_iagGoBig_iagscoring)  v         
on v.iag_vehicle_make=m.vehicle_make 
 
left join (select distinct driver_age as iag_driver_age, a_score from campaign_data.aa_dmc2189_iagGoBig_iagscoring) a         
on a.iag_driver_age=m.age 
 
left join (select distinct cast(postcode as string) as iag_postcode, p_score from campaign_data.aa_dmc2189_iagGoBig_iagscoring) p        
on p.iag_postcode=m.postcode 
) 
select *, v1+a1+p1 as iag_score from final 
--having sum(v1,a1,p1) 
 
--where m.member_number='469600301' 
select count(distinct member_number) 
--,contact_id  
from campaign_data.aa_dmc2230_IAGGoBig_campaign_eDM_20201211 
select * 
from campaign_data.aa_dmc2230_IAGGoBig_campaign_eDM_20201211 
where member_number='872104202' 