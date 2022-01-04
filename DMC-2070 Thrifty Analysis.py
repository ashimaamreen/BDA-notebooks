select 
suburb, 
state, 
postcode, 
country, 
count(1) cont 
from 
( 
select 
eventid, 
upper(trim(primarycity)) suburb, 
upper(trim(primarystate)) state, 
upper(trim(primarypostcode)) postcode, 
upper(trim(primarycountrycode)) country 
from era.individual 
where 
primaryaddresstype<>'NULL' and 
upper(trim(primarycountrycode)) in ('AUS','NZL') and 
customertype='Driver' and 
primarycity not in ('NULL','[CITY]') 
group by 
eventid, 
upper(trim(primarycity)), 
upper(trim(primarystate)), 
upper(trim(primarypostcode)), 
upper(trim(primarycountrycode)) 
) a 
group by  
suburb, 
state, 
postcode, 
country  
--create table campaign_data.aa_dmc2071_ThriftyRDA_research_otheres_20200610_adhoc as 
SELECT DISTINCT i.eventid as RECORDID 
        , i.primarycountrycode as ISO 
        , upper(i.primaryaddressline1) as ADDRESSLINE1 
        , case when i.primaryaddressline2 is null then upper(i.primarycity) else upper(i.primaryaddressline2) end ADDRESSLINE2 
        , case when i.primaryaddressline2 is null and i.primaryaddressline3 is null then concat(i.primarystate,' ',i.primarypostcode) else i.primaryaddressline3 end ADDRESSLINE3 
        , (year(now()) - year(i.dateofbirth)) as Age 
        , i.primarycity as suburb 
        , i.primarystate as state 
        , i.primarypostcode as zipcode 
 
FROM era.postedtransaction a 
--count(DISTINCT a.eventid) 
INNER JOIN era.individual i 
on i.eventid=a.eventid 
 
where to_Date(a.checkindate) between  '2019-06-01' and '2020-05-31'  
and i.primarycountrycode is not NULL 
and i.primaryaddressline1 is not NULL 
select case when m.eventid is null then 'N' else 'Y' end member_flag 
,count(DISTINCT a.renterid)  
 
FROM era.postedtransaction a 
--count(DISTINCT a.eventid) 
 
INNER JOIN era.individual i 
on i.eventid=a.eventid 
 
left join era.member_join m 
on m.eventid = i.eventid 
 
where to_date(a.checkindate) between  '2019-06-01' and '2020-05-31'  
group by 1 
 
-- 
select count(distinct eventid), NDV(renterid), count(*) from era.postedtransaction 
where to_date(checkindate) between  '2019-06-01' and '2020-05-31'  
RDA_tagging = spark.read.option("delimiter", "|").csv("/user/aamreen/Thrifty_RDA/THRIFTY_14064_Output.txt", inferSchema="true", header="true") 
RDA_tagging.createOrReplaceTempView("RDA_tagging") 
spark.sql (""" select * from RDA_tagging""").count() 
spark.sql (""" create table campaign_data.aa_dmc2071_thriftyRDAtagging_research_others_20200615_adhoc as select * from RDA_tagging""").show(10) 
SELECT in_recordid 
        , in_iso 
        , custsegment2_tag 
        , geolochi_mkey as geotribe 
        , geoloclo_mkey as geosmart 
FROM campaign_data.aa_dmc2071_thriftyrdatagging_research_others_20200615_adhoc LIMIT 100; 
--143102 
--799078 
select count(distinct a.renterid) 
        -- ,m.con_id 
        -- ,case when m.eventid is null then 'N' else 'Y' end member_flag 
        -- ,case when m.eventid is null then t.custsegment2_tag else cx.attrib_55 end ColourPlus 
        -- ,(year(now()) - year(i.dateofbirth)) as Age 
         
 
FROM era.postedtransaction a 
--count(DISTINCT a.eventid) 
 
INNER JOIN era.individual i 
on i.eventid=a.eventid 
 
left join era.member_join m 
on m.eventid = i.eventid 
 
left join campaign_data.aa_dmc2071_thriftyrdatagging_research_others_20200615_adhoc t 
on t.in_recordid = a.eventid 
 
left join gms.s_contact c 
on c.row_id=m.con_id 
 
left JOIN gms.s_contact_x cx 
on cx.par_row_id=c.row_id 
 
left JOIN gms.s_contact_fnx fn 
on fn.par_row_id=c.row_id 
 
left join gms.s_addr_per as ad         --813620 
on c.pr_per_addr_id = ad.row_id 
 
where to_date(a.checkindate) between  '2019-06-01' and '2020-05-31'     -- 
and i.dateofbirth is not null 
--and i.eventid=5886871 
--group by 1 
select * from era.individual  
where eventid=5886871 
select distinct vehiclegroup from era.postedtransaction 
--where eventid=3662253 
SELECT * FROM era.businessevent LIMIT 100; 
SELECT * 
FROM era.postedtransaction  
--where transactionid=27250897 
SELECT DISTINCT eventid,transactionid, datediff(vehicledatein,vehicledateout)  
FROM era.postedtransaction  
where transactionid=27250897 
SELECT * FROM era.vehicle LIMIT 100; 
select distinct franchisee, franchiseecode from businesseventsnapshot 
where franchiseecode like 'NZ%' 
--spark.sql(""" 
 
with age_group as ( 
select distinct a.renterid 
        ,(year(now()) - year(i.dateofbirth)) as Age 
         
 
FROM era.postedtransaction a 
 
INNER JOIN era.individual i 
on i.eventid=a.eventid 
 
left join era.member_join m 
on m.eventid = i.eventid 
 
inner join campaign_data.aa_dmc2071_thriftyrdatagging_research_others_20200615_adhoc t 
on t.in_recordid = a.eventid 
 
left join gms.s_contact c 
on c.row_id=m.con_id 
 
left JOIN gms.s_contact_x cx 
on cx.par_row_id=c.row_id 
 
where to_date(a.checkindate) between  '2019-06-01' and '2020-05-31'     -- 
and i.dateofbirth is not null 
--and i.eventid=5886871 
) 
select case when age<18 then 'under 18' 
            when age<25 then 'under 25' 
            when age<35 then 'under 35' 
            when age<45 then 'under 45' 
            when age<55 then 'under 55' 
            when age<65 then 'under 65' 
            when age<75 then 'under 75' 
            when age<85 then 'under 85' 
        else 'above 85' end Age_bracket 
        , count(distinct renterid) 
from age_group 
group by 1 
order by 1 
--""").show(1000) 
--143102 
--799078 
select case when m.renterid is null then 'N' else 'Y' end member_flag 
        ,case when m.renterid is null then t.custsegment2_tag else cx.attrib_55 end ColourPlus 
        , count(distinct a.renterid) 
        --,m.con_id 
        --,(year(now()) - year(i.dateofbirth)) as Age 
         
 
FROM era.postedtransaction a 
--count(DISTINCT a.eventid) 
 
INNER JOIN era.individual i 
on i.renterid=a.renterid 
 
left join era.member_join m 
on m.renterid = i.renterid 
 
inner join campaign_data.aa_dmc2071_thriftyrdatagging_research_others_20200615_adhoc t 
on t.in_recordid = a.eventid 
 
left join gms.s_contact c 
on c.row_id=m.con_id 
 
left JOIN gms.s_contact_x cx 
on cx.par_row_id=c.row_id 
select 
suburb, 
state, 
postcode, 
country, 
count(1) cont 
from 
( 
select 
eventid, 
upper(trim(primarycity)) suburb, 
upper(trim(primarystate)) state, 
upper(trim(primarypostcode)) postcode, 
upper(trim(primarycountrycode)) country 
from era.individual 
where 
primaryaddresstype<>'NULL' and 
upper(trim(primarycountrycode)) in ('AUS','NZL') and 
customertype='Driver' and 
primarycity not in ('NULL','[CITY]') 
group by 
eventid, 
upper(trim(primarycity)), 
upper(trim(primarystate)), 
upper(trim(primarypostcode)), 
upper(trim(primarycountrycode)) 
) a 
group by  
suburb, 
state, 
postcode, 
country  
where to_date(a.checkindate) between  '2019-06-01' and '2020-05-31'     -- 
and i.dateofbirth is not null 
--and i.eventid=5886871 
group by 1,2 
order by 1,2 
select cx.attrib_55  ColourPlus 
        , count(distinct c.row_id) 
         
 
FROM gms.s_contact c 
--on c.row_id=m.con_id 
 
inner JOIN gms.s_contact_x cx 
on cx.par_row_id=c.row_id 
 
where c.cust_stat_cd = 'Active' 
and c.con_cd in ('Ordinary Member' , 'Affiliate Member') 
and NVL(c.x_nrma_title,'no title') != 'Estate Of The Late' 
group by 1 
 
with frequency as ( 
select a.renterid 
        , case when m.renterid is null then 'N' else 'Y' end member_flag 
        , count(distinct a.transactionid) as freq 
        --,m.con_id 
        --,(year(now()) - year(i.dateofbirth)) as Age 
         
 
FROM era.postedtransaction a 
--count(DISTINCT a.eventid) 
 
INNER JOIN era.individual i 
on i.eventid=a.eventid 
 
left join era.member_join m 
on m.eventid = i.eventid 
 
where to_date(a.checkindate) between  '2019-06-01' and '2020-05-31'     -- 
and i.dateofbirth is not null 
--and i.eventid=5886871 
group by 1,2 
order by 1,2 
)  
select case when freq=1 then 'Single' else 'Multiple' end loyalty, count(distinct renterid) 
from frequency 
group by 1 
with duration as( 
SELECT DISTINCT renterid,transactionid,a.vehicleid, datediff(checkindate,checkoutdate) as duration 
FROM era.postedtransaction a 
 
inner join campaign_data.aa_dmc2071_thriftyrdatagging_research_others_20200615_adhoc c 
on c.in_recordid = a.eventid 
 
inner join era.vehicle v 
on v.vehicleid=a.vehicleid 
 
where to_date(a.checkindate) between  '2019-06-01' and '2020-05-31' 
) 
 
select  v.vehicletype ,d.duration, count(distinct d.transactionid) 
from duration d 
 
inner join era.vehicle v 
on v.vehicleid=d.vehicleid 
 
where d.duration<20 
 
group by 1,2 
order by 1,2 
with duration as( 
SELECT DISTINCT renterid,transactionid,a.vehicleid, datediff(checkindate,checkoutdate) as duration 
FROM era.postedtransaction a 
 
inner join campaign_data.aa_dmc2071_thriftyrdatagging_research_others_20200615_adhoc c 
on c.in_recordid = a.eventid 
 
inner join era.vehicle v 
on v.vehicleid=a.vehicleid 
 
where to_date(a.checkindate) between  '2019-06-01' and '2020-05-31' 
) 
 
select v.vehicletype, count(distinct d.transactionid) 
from duration d 
 
inner join era.vehicle v 
on v.vehicleid=d.vehicleid 
 
group by 1 
order by 1 
--with average as( 
SELECT renterid, count(distinct a.transactionid) as total_bookings 
FROM era.postedtransaction a 
 
inner join campaign_data.aa_dmc2071_thriftyrdatagging_research_others_20200615_adhoc c 
on c.in_recordid = a.eventid 
 
inner join era.vehicle v 
on v.vehicleid=a.vehicleid 
 
where to_date(a.checkindate) between  '2019-06-01' and '2020-05-31' 
group by 1 
-- ) 
 
-- select ARL, count(distinct d.renterid) 
-- from average d 
 
-- --where d.duration<61 
 
-- group by 1 
-- order by 1 
with duration as( 
SELECT DISTINCT renterid,transactionid,a.vehicleid, datediff(checkindate,checkoutdate) as duration 
FROM era.postedtransaction a 
 
inner join campaign_data.aa_dmc2071_thriftyrdatagging_research_others_20200615_adhoc c 
on c.in_recordid = a.eventid 
 
inner join era.vehicle v 
on v.vehicleid=a.vehicleid 
 
where to_date(a.checkindate) between  '2019-06-01' and '2020-05-31' 
) 
 
select  avg(d.duration) 
from duration d 
 
--where d.duration<61 
 
-- group by 1 
-- order by 1 
with duration as( 
SELECT DISTINCT renterid,transactionid,a.vehicleid, datediff(checkindate,checkoutdate) as duration 
FROM era.postedtransaction a 
 
inner join campaign_data.aa_dmc2071_thriftyrdatagging_research_others_20200615_adhoc c 
on c.in_recordid = a.eventid 
 
inner join era.vehicle v 
on v.vehicleid=a.vehicleid 
 
where to_date(a.checkindate) between  '2019-06-01' and '2020-05-31' 
) 
 
select  count(distinct d.transactionid) 
from duration d 
spark.sql(""" 
 
select  v.vehicletype, count(distinct d.transactionid) 
 
from era.postedtransaction d 
 
inner join era.vehicle v 
on v.vehicleid=d.vehicleid 
 
group by 1 
order by 1 
""").show(10000) 
with frequency as ( 
select a.renterid 
        , case when m.renterid is null then 'N' else 'Y' end member_flag 
        --,m.con_id 
        ,(year(now()) - year(i.dateofbirth)) as Age 
        , count(distinct a.transactionid) as freq 
         
 
FROM era.postedtransaction a 
 
INNER JOIN era.individual i 
on i.renterid=a.renterid 
 
left join era.member_join m 
on m.renterid = i.renterid 
 
where to_date(a.checkindate) between  '2019-06-01' and '2020-05-31'     -- 
and i.dateofbirth is not null 
group by 1,2,3 
order by 1,2,3 
) 
select  case when age<18 then 'under 18' 
            when age<25 then 'under 25' 
            when age<35 then 'under 35' 
            when age<45 then 'under 45' 
            when age<55 then 'under 55' 
            when age<65 then 'under 65' 
            when age<75 then 'under 75' 
            when age<85 then 'under 85' 
        else 'above 85' end Age_bracket 
         
        , count(distinct renterid) 
 
from frequency 
 
where freq>1 
group by 1 
order by 1 
with frequency as ( 
select a.renterid 
        , case when m.renterid is null then 'N' else 'Y' end member_flag 
        --,m.con_id 
        ,(year(now()) - year(i.dateofbirth)) as Age 
        , count(distinct a.transactionid) as freq 
         
 
FROM era.postedtransaction a 
 
INNER JOIN era.individual i 
on i.renterid=a.renterid 
 
left join era.member_join m 
on m.renterid = i.renterid 
 
where to_date(a.checkindate) between  '2019-06-01' and '2020-05-31'     -- 
and i.dateofbirth is not null 
group by 1,2,3 
order by 1,2,3 
) 
select  case when age<18 then 'under 18' 
            when age<25 then 'under 25' 
            when age<35 then 'under 35' 
            when age<45 then 'under 45' 
            when age<55 then 'under 55' 
            when age<65 then 'under 65' 
            when age<75 then 'under 75' 
            when age<85 then 'under 85' 
        else 'above 85' end Age_bracket 
         
        , count(distinct renterid) 
 
from frequency 
 
where freq<=1 
group by 1 
order by 1 
with age_group as ( 
select distinct c.row_id as contact_id 
            , (year(now()) - year(c.birth_dt)) as Age 
         
 
FROM gms.s_contact c 
 
where c.cust_stat_cd = 'Active' 
and c.con_cd in ('Ordinary Member' , 'Affiliate Member') 
and NVL(c.x_nrma_title,'no title') != 'Estate Of The Late' 
) 
select  case when age<18 then 'under 18' 
            when age<25 then 'under 25' 
            when age<35 then 'under 35' 
            when age<45 then 'under 45' 
            when age<55 then 'under 55' 
            when age<65 then 'under 65' 
            when age<75 then 'under 75' 
            when age<85 then 'under 85' 
        else 'above 85' end Age_bracket 
         
        , count(distinct contact_id) 
 
from age_group 
 
group by 1 
order by 1 
SELECT case when lower(checkinlocation) like '%airport%' then 'Airport' else 'non-Airport' end checkin_location,count(DISTINCT eventid) 
FROM era.postedtransaction  
where to_date(checkindate) between  '2019-06-01' and '2020-05-31' 
 
group by 1 
SELECT case when lower(checkinlocation) like '%airport%' then 'Airport' else 'non-Airport' end checkin_location,* 
FROM era.postedtransaction  
where to_date(checkindate) between  '2019-06-01' and '2020-05-31' 
and checkoutowninglocationcode like 'AU%' 
and checkoutowninglocationcode like 'NZ%' 
SELECT checkinlocation, count(DISTINCT eventid) 
FROM era.postedtransaction  
where to_date(checkindate) between  '2019-06-01' and '2020-05-31' 
and lower(checkinlocation) like '%airport%'  
group by 1 
order by 1 
SELECT count(DISTINCT eventid) 
FROM era.postedtransaction  
where to_date(checkindate) between  '2019-06-01' and '2020-05-31' 
and lower(checkinlocation) not like '%airport%'  
--group by 1 
--order by 2 DESC 
SELECT i.primarycountrycode, count(DISTINCT a.renterid) 
FROM era.postedtransaction a 
 
INNER JOIN era.individual i 
on i.renterid=a.renterid 
 
INNER JOIN era.`location` l 
on l.locationid=a.paymentlocationid 
 
where to_date(checkindate) between  '2019-06-01' and '2020-05-31' 
--and checkoutowninglocationcode like 'NZ%' 
and l.countrycode='NZL' 
  
group by 1 
order by 1 DESC 
SELECT case when UPPER(trim(i.primarycountrycode))='NZL' then 'NZ' else i.primarycountrycode end country, count(DISTINCT a.eventid) 
FROM era.postedtransaction a 
 
INNER JOIN era.individual i 
on i.eventid=a.eventid 
 
where to_date(checkindate) between  '2019-06-01' and '2020-05-31' 
and checkinowninglocationcode like 'NZ%' 
and i.primarycountrycode is not NULL 
and i.primaryaddressline1 is not NULL 
  
group by 1 
order by 1 
SELECT case when UPPER(trim(i.primarycountrycode))='AUS' then 'AU' 
            when UPPER(trim(i.primarycountrycode))='NZL' then 'NZ' 
            else 'Overseas' end country, count(DISTINCT a.eventid) 
FROM era.postedtransaction a 
 
INNER JOIN era.individual i 
on i.eventid=a.eventid 
 
where to_date(checkindate) between  '2019-06-01' and '2020-05-31' 
and checkinowninglocationcode like 'AU%' 
and i.primarycountrycode is not NULL 
and i.primaryaddressline1 is not NULL 
  
group by 1 
order by 1 
spark.sql(""" 
select case when m.eventid is null then t.custsegment2_tag else cx.attrib_55 end ColourPlus 
        , upper(v.manufacturer) 
        , count(distinct a.eventid) 
         
 
FROM era.postedtransaction a 
--count(DISTINCT a.eventid) 
 
INNER JOIN era.individual i 
on i.eventid=a.eventid 
 
inner join era.vehicle v 
on v.vehicleid=a.vehicleid 
 
left join era.member_join m 
on m.eventid = i.eventid 
 
inner join campaign_data.aa_dmc2071_thriftyrdatagging_research_others_20200615_adhoc t 
on t.in_recordid = a.eventid 
 
left join gms.s_contact c 
on c.row_id=m.con_id 
 
left JOIN gms.s_contact_x cx 
on cx.par_row_id=c.row_id 
 
left JOIN gms.s_contact_fnx fn 
on fn.par_row_id=c.row_id 
 
left join gms.s_addr_per as ad         --813620 
on c.pr_per_addr_id = ad.row_id 
 
where to_date(a.checkindate) between  '2019-06-01' and '2020-05-31'     -- 
and i.dateofbirth is not null 
and v.vehicletype='Car' 
and upper(v.manufacturer) in ('HOLDEN','HYUNDAI','KIA','MERCEDES','MITSUBISHI','NISSAN','SUBARU','SUZUKI','TOYOTA') 
group by 1,2 
order by 1,2 
""").show(1000) 
--792510 
--799070 
select case when lower(checkinlocation) like '%airport%' then 'Airport' else 'non-Airport' end checkin_location 
        , v.bodystyle 
        , count(distinct a.eventid) 
         
 
FROM era.postedtransaction a 
--count(DISTINCT a.eventid) 
 
INNER JOIN era.individual i 
on i.eventid=a.eventid 
 
inner join era.vehicle v 
on v.vehicleid=a.vehicleid  
 
where to_date(a.checkindate) between  '2019-06-01' and '2020-05-31' 
and i.dateofbirth is not null 
and v.vehicletype='Car' 
 
group by 1,2 
order by 1,2 
--792510 
--799070 
select  
case when lower(checkinlocation) like '%airport%' then 'Airport' else 'non-Airport' end checkin_location, 
         upper(v.manufacturer) 
        , count(distinct a.eventid) 
         
 
FROM era.postedtransaction a 
--count(DISTINCT a.eventid) 
 
INNER JOIN era.individual i 
on i.eventid=a.eventid 
 
inner join era.vehicle v 
on v.vehicleid=a.vehicleid  
 
where to_date(a.checkindate) between  '2019-06-01' and '2020-05-31'     -- 
and i.dateofbirth is not null 
and v.vehicletype='Car' 
 
 
group by 1,2 
order by 1,2 
with frequency as ( 
select a.renterid 
        , a.vehicleid 
        , count(distinct a.transactionid) as freq 
         
 
FROM era.postedtransaction a 
--count(DISTINCT a.eventid) 
 
INNER JOIN era.individual i 
on i.renterid=a.renterid 
 
 
where to_date(a.checkindate) between  '2019-06-01' and '2020-05-31'     -- 
and i.dateofbirth is not null 
--and i.eventid=5886871 
group by 1,2 
--order by 1,2 
) 
select  upper(v.bodystyle) 
        , count(distinct renterid) 
 
from frequency f 
 
inner join era.vehicle v 
on v.vehicleid=f.vehicleid  
 
--where freq<=1 
group by 1 
order by 1 
with frequency as ( 
select a.renterid 
        , a.vehicleid 
        , count(distinct a.transactionid) as freq 
         
 
FROM era.postedtransaction a 
--count(DISTINCT a.eventid) 
 
INNER JOIN era.individual i 
on i.renterid=a.renterid 
 
 
where to_date(a.checkindate) between  '2019-06-01' and '2020-05-31'     -- 
group by 1,2 
) 
select  upper(v.manufacturer) 
        , count(distinct renterid) 
 
from frequency f 
 
inner join era.vehicle v 
on v.vehicleid=f.vehicleid  
 
where freq>1 
group by 1 
order by 1 
with frequency as ( 
select a.renterid 
        , case when lower(a.checkinlocation) like '%airport%' then 'Airport' else 'non-Airport' end checkin_location 
        , count(distinct a.transactionid) as freq 
         
 
FROM era.postedtransaction a 
--count(DISTINCT a.eventid) 
 
INNER JOIN era.individual i 
on i.renterid=a.renterid 
 
 
where to_date(a.checkindate) between  '2019-06-01' and '2020-05-31'     -- 
group by 1,2 
) 
select  f.checkin_location 
        , case when f.freq>1 then 'Frequent' else 'One-time' end Type_renter 
        , count(distinct renterid) 
 
from frequency f 
group by 1,2 
order by 1,2 
 
 
with vehicle as ( 
select distinct a.eventid 
        , upper(v.manufacturer) as manufacturer 
        ,(year(now()) - year(i.dateofbirth)) as Age 
         
 
FROM era.postedtransaction a 
--count(DISTINCT a.eventid) 
 
INNER JOIN era.individual i 
on i.eventid=a.eventid 
 
inner join era.vehicle v 
on v.vehicleid=a.vehicleid 
 
where to_date(a.checkindate) between  '2019-06-01' and '2020-05-31'     -- 
and i.dateofbirth is not null 
and upper(v.manufacturer) in ('HOLDEN','HYUNDAI','KIA','MERCEDES','MITSUBISHI','NISSAN','SUBARU','SUZUKI','TOYOTA') 
--and i.eventid=5886871 
--group by 1,2 
--order by 1,2 
) 
select  f.manufacturer 
        ,case when age<18 then 'under 18' 
            when age<25 then 'under 25' 
            when age<35 then 'under 35' 
            when age<45 then 'under 45' 
            when age<55 then 'under 55' 
            when age<65 then 'under 65' 
            when age<75 then 'under 75' 
            when age<85 then 'under 85' 
        else 'above 85' end Age_bracket 
        , count(distinct f.eventid) 
 
from vehicle f 
group by 1,2 
order by 1,2 
SELECT * FROM era.businessevent LIMIT 100; 
SELECT count(DISTINCT i.eventid) as RECORDID 
 
FROM era.postedtransaction a 
--count(DISTINCT a.eventid) 
INNER JOIN era.individual i 
on i.eventid=a.eventid 
 
inner join businessevent b 
on b.eventid=i.eventid 
 
where to_Date(a.checkindate) between  '2019-06-01' and '2020-05-31'  
and i.primarycountrycode is not NULL 
and i.primaryaddressline1 is not NULL 
select * from era.individual limit 10; 
SELECT COUNT(DISTINCT eventid) FROM era.postedtransaction  
WHERE checkinowninglocationcode RLIKE 'NZ'  
and to_Date(checkindate) between  '2017-06-01' and '2020-05-31' 
select  
case when lower(checkinlocation) like '%airport%' then 'Airport' else 'non-Airport' end checkin_location, 
         upper(v.manufacturer) 
        , count(distinct a.eventid) 
         
 
FROM era.postedtransaction a 
--count(DISTINCT a.eventid) 
 
INNER JOIN era.individual i 
on i.eventid=a.eventid 
 
inner join era.vehicle v 
on v.vehicleid=a.vehicleid  
 
where to_date(a.checkindate) between  '2019-06-01' and '2020-05-31'     -- 
and i.dateofbirth is not null 
and v.vehicletype='Car' 
 
 
group by 1,2 
order by 1,2 
select a.eventid 
        , l.locationname 
        , i.primarypostcode 
        , i.primarystate 
        , l.locationtypedescription 
        --, i.* 
 
from era.postedtransaction a 
 
INNER JOIN era.individual i 
on i.eventid=a.eventid 
 
inner join era.`location` l 
on l.locationid=a.paymentlocationid 
 
where lower(checkinlocation) not like '%airport%' 
and locationid=32326 
SELECT bookingchannelgroup, count(DISTINCT a.eventid) 
--bookingchannelgroup, bookingchannel  
FROM era.businesseventsnapshot b 
 
INNER JOIN era.postedtransaction a 
on a.eventid=b.eventid 
 
where to_date(a.checkindate) between  '2019-06-01' and '2020-05-31'  
group by 1 
SELECT *  
FROM era.businesseventsnapshot b 
 
INNER JOIN era.postedtransaction a 
on a.eventid=b.eventid 
 
where to_date(a.checkindate) between  '2019-06-01' and '2020-05-31'  
and b.bookingchannelgroup='GDS' 
--group by 1 
select  
case when companyrenterid is not null then 'Corporate' 
            when insurancerenterid is not null then 'Insurance' 
            when brokerrenterid is not null then 'Broker' 
            when agentrenterid is not null then 'Agent' 
            else 'Customer' end type 
        ,    count(distinct a.renterid) 
             
from businesseventsnapshot b 
INNER JOIN era.postedtransaction a 
on a.eventid=b.eventid 
 
where to_date(a.checkindate) between '2019-06-01' and '2020-05-31'  
and DriverRenterId is not null 
group by 1 
spark.sql(""" 
with booking_lead as ( 
select distinct a.eventid 
        , a.transactionid 
        , datediff(b.pickupdate,b.bookingdate) as booking_lead 
 
from era.businesseventsnapshot b 
 
INNER JOIN era.postedtransaction a 
on a.eventid=b.eventid 
 
where to_date(a.checkindate) between '2019-06-01' and '2020-05-31'  
) 
select booking_lead, count(distinct transactionid) from booking_lead  
group by 1 
order by 1 
""").show(1000) 
spark.sql(""" 
with booking_lead as ( 
select distinct a.eventid 
        , a.transactionid 
        , case when companyrenterid is not null then 'Corporate' 
            when insurancerenterid is not null then 'Insurance' 
            when brokerrenterid is not null then 'Broker' 
            when agentrenterid is not null then 'Agent' 
            else 'Customer' end type 
        , datediff(b.pickupdate,b.bookingdate) as booking_lead 
 
from era.businesseventsnapshot b 
 
INNER JOIN era.postedtransaction a 
on a.eventid=b.eventid 
 
where to_date(a.checkindate) between '2019-06-01' and '2020-05-31'  
) 
select booking_lead, count(distinct transactionid) from booking_lead  
where type='Corporate' 
group by 1 
order by 1 
""").show(1000) 
 
spark.sql(""" 
with booking_lead as ( 
select distinct a.eventid 
        , a.transactionid 
        , case when companyrenterid is not null then 'Corporate' 
            when insurancerenterid is not null then 'Insurance' 
            when brokerrenterid is not null then 'Broker' 
            when agentrenterid is not null then 'Agent' 
            else 'Customer' end type 
        , datediff(b.pickupdate,b.bookingdate) as booking_lead 
 
from era.businesseventsnapshot b 
 
INNER JOIN era.postedtransaction a 
on a.eventid=b.eventid 
 
where to_date(a.checkindate) between '2019-06-01' and '2020-05-31'  
) 
select booking_lead, count(distinct transactionid) from booking_lead  
where type='Customer' 
group by 1 
order by 1 
""").show(1000) 
 
with booking_lead as ( 
select distinct a.eventid 
        , a.transactionid 
        , case when companyrenterid is not null then 'Corporate' 
            when insurancerenterid is not null then 'Insurance' 
            when brokerrenterid is not null then 'Broker' 
            when agentrenterid is not null then 'Agent' 
            else 'Customer' end type 
        , datediff(b.pickupdate,b.bookingdate) as booking_lead 
 
from era.businesseventsnapshot b 
 
INNER JOIN era.postedtransaction a 
on a.eventid=b.eventid 
 
where to_date(a.checkindate) between '2019-06-01' and '2020-05-31'  
) 
select custsegment2_tag, count(a.eventid) from booking_lead a 
inner join campaign_data.aa_dmc2071_thriftyrdatagging_research_others_20200615_adhoc r 
on r.in_recordid=a.eventid 
 
where type='Customer' 
and a.booking_lead<2 
group by 1 
order by 1 
with booking_lead as ( 
select distinct a.eventid 
        , a.transactionid 
        , case when companyrenterid is not null then 'Corporate' 
            when insurancerenterid is not null then 'Insurance' 
            when brokerrenterid is not null then 'Broker' 
            when agentrenterid is not null then 'Agent' 
            else 'Customer' end type 
        , datediff(b.pickupdate,b.bookingdate) as booking_lead 
 
from era.businesseventsnapshot b 
 
INNER JOIN era.postedtransaction a 
on a.eventid=b.eventid 
 
where to_date(a.checkindate) between '2019-06-01' and '2020-05-31'  
) 
select custsegment2_tag, count(a.eventid) from booking_lead a 
inner join campaign_data.aa_dmc2071_thriftyrdatagging_research_others_20200615_adhoc r 
on r.in_recordid=a.eventid 
 
where type='Customer' 
and a.booking_lead<2 
group by 1 
order by 1 
select case when p.partner  in ('IAG','NRMA Multi Policy Discount','NRMA MPD Insurance','NRMA Insurance','NRMA Insurance ControlPro','NRMA Insurance Branch') then 'IAG' 
            when (p.partner like 'NRMA Parks and Resorts%' or p.partner like '%Holiday Park') then 'NRMA Parks and Resorts' 
            else p.partner end member_benefit 
, count(distinct con_id) 
from era.postedtransaction a 
 
inner join era.member_join m 
on m.renterid=a.renterid 
 
inner join gms.s_contact c 
on c.row_id=m.con_id 
 
inner join m4m.return_feed_header p 
on p.member_number=c.csn 
 
where to_date(a.checkindate) between '2019-06-01' and '2020-05-31'  
and p.time_stamp between '2019-06-01' and '2020-05-31'  
group by 1 
order by 2 desc 
 
SELECT manufacturer, count(vehicleid) FROM era.vehicle 
where vehicletype='Car' 
group by 1 
SELECT * FROM era.postedtransaction LIMIT 100; 
SELECT chargegroup, count(DISTINCT eventid) FROM era.postedtransaction  
where to_date(checkindate) between '2019-06-01' and '2020-05-31'  
GROUP BY 1 
ORDER BY 2 DESC 
SELECT chargegroup, count(DISTINCT eventid) FROM era.postedtransaction  
where to_date(checkindate) between '2019-06-01' and '2020-05-31'  
and lower(checkinlocation) like '%airport%' 
GROUP BY 1 
ORDER BY 2 DESC 
SELECT chargegroup, count(DISTINCT eventid) FROM era.postedtransaction  
where to_date(checkindate) between '2019-06-01' and '2020-05-31'  
and lower(checkinlocation) not like '%airport%' 
GROUP BY 1 
ORDER BY 2 DESC 
SELECT sum(chargeamountgross),sum(rateadjustmentamountgross) FROM era.postedtransaction  
where to_date(checkindate) between '2019-06-01' and '2020-05-31'  
--and lower(checkinlocation) like '%airport%' 
select cx.attrib_55  AS Colour_Plus 
, count(distinct con_id) 
from era.postedtransaction a 
 
inner join era.member_join m 
on m.renterid=a.renterid 
 
inner join gms.s_contact c 
on c.row_id=m.con_id 
 
inner join gms.s_contact_x cx 
on cx.par_row_id=c.row_id 
 
where to_date(a.checkindate) between '2019-06-01' and '2020-05-31'  
group by 1 
order by 1 
 
select c.cust_value_cd as loyalty_colour 
, count(distinct con_id) 
from era.postedtransaction a 
 
inner join era.member_join m 
on m.renterid=a.renterid 
 
inner join gms.s_contact c 
on c.row_id=m.con_id 
 
where to_date(a.checkindate) between '2019-06-01' and '2020-05-31'  
group by 1 
order by 1 
 
SELECT  case when lower(a.checkinlocation) like '%airport%' then 'Airport' else 'non-Airport' end checkin_location 
        ,count(DISTINCT i.bluechipnumber)  
 
FROM era.postedtransaction a 
 
INNER JOIN era.individual i 
on i.eventid=a.eventid 
 
INNER join campaign_data.aa_dmc2071_thriftyrdatagging_research_others_20200615_adhoc t 
on t.in_recordid = a.eventid 
 
where to_date(a.checkindate) between '2019-06-01' and '2020-05-31'  
 
group by 1 
order by 1 
SELECT  case when lower(a.checkinlocation) like '%airport%' then 'Airport' else 'non-Airport' end checkin_location 
        ,count(DISTINCT i.renterid)  
 
FROM era.postedtransaction a 
 
INNER JOIN era.individual i 
on i.eventid=a.eventid 
 
INNER join campaign_data.aa_dmc2071_thriftyrdatagging_research_others_20200615_adhoc t 
on t.in_recordid = a.eventid 
 
where to_date(a.checkindate) between '2019-06-01' and '2020-05-31'  
 
group by 1 
order by 2 DESC 
with cust_type as ( 
select distinct a.eventid 
        , a.transactionid 
        , case when companyrenterid is not null then 'Corporate' 
            when insurancerenterid is not null then 'Insurance' 
            when brokerrenterid is not null then 'Broker' 
            when agentrenterid is not null then 'Agent' 
            else 'Customer' end type 
        , a.renterid 
 
from era.businesseventsnapshot b 
 
INNER JOIN era.postedtransaction a 
on a.eventid=b.eventid 
 
where to_date(a.checkindate) between '2019-06-01' and '2020-05-31'  
) 
select a.type, count(distinct a.eventid) from cust_type a 
 
inner join campaign_data.aa_dmc2071_thriftyrdatagging_research_others_20200615_adhoc r 
on r.in_recordid=a.eventid 
 
--where type='Corporate' 
group by 1 
order by 1 
with cust_type as ( 
select distinct a.eventid 
        , a.transactionid 
        , case when companyrenterid is not null then 'Corporate' 
            when insurancerenterid is not null then 'Insurance' 
            when brokerrenterid is not null then 'Broker' 
            when agentrenterid is not null then 'Agent' 
            else 'Customer' end type 
        , a.renterid 
 
from era.businesseventsnapshot b 
 
INNER JOIN era.postedtransaction a 
on a.eventid=b.eventid 
 
where to_date(a.checkindate) between '2019-06-01' and '2020-05-31'  
and a.renterid is not null 
) 
select custsegment2_tag, count(distinct a.renterid) from cust_type a 
 
inner join campaign_data.aa_dmc2071_thriftyrdatagging_research_others_20200615_adhoc r 
on r.in_recordid=a.eventid 
 
where type='Customer' 
group by 1 
order by 1 
with cust_type as ( 
select distinct a.eventid 
        , a.transactionid 
        , case when companyrenterid is not null then 'Corporate' 
            when insurancerenterid is not null then 'Insurance' 
            when brokerrenterid is not null then 'Broker' 
            when agentrenterid is not null then 'Agent' 
            else 'Customer' end type 
        , a.renterid 
 
from era.businesseventsnapshot b 
 
INNER JOIN era.postedtransaction a 
on a.eventid=b.eventid 
 
where to_date(a.checkindate) between '2019-06-01' and '2020-05-31'  
and a.renterid is not null 
) 
select custsegment2_tag, count(distinct a.renterid) from cust_type a 
 
inner join campaign_data.aa_dmc2071_thriftyrdatagging_research_others_20200615_adhoc r 
on r.in_recordid=a.eventid 
 
where type='Corporate' 
group by 1 
order by 1 
with cust_type as ( 
select distinct a.eventid 
        , a.transactionid 
        , case when companyrenterid is not null then 'Corporate' 
            when insurancerenterid is not null then 'Insurance' 
            when brokerrenterid is not null then 'Broker' 
            when agentrenterid is not null then 'Agent' 
            else 'Customer' end type 
        , a.renterid 
        , case when b.bookingchannelgroup='Yet To Be Mapped' OR b.bookingchannelgroup IS NULL then 'Unkown' else b.bookingchannelgroup end booking_channel 
 
from era.businesseventsnapshot b 
 
INNER JOIN era.postedtransaction a 
on a.eventid=b.eventid 
 
where to_date(a.checkindate) between '2019-06-01' and '2020-05-31'  
and a.renterid is not null 
) 
select a.booking_channel, count(distinct a.eventid) from cust_type a 
 
where type='Corporate' 
group by 1 
order by 1 
with cust_type as ( 
select distinct a.eventid 
        , a.transactionid 
        , case when companyrenterid is not null then 'Corporate' 
            when insurancerenterid is not null then 'Insurance' 
            when brokerrenterid is not null then 'Broker' 
            when agentrenterid is not null then 'Agent' 
            else 'Customer' end type 
        , a.renterid 
        , case when b.bookingchannelgroup='Yet To Be Mapped' OR b.bookingchannelgroup IS NULL then 'Unkown' else b.bookingchannelgroup end booking_channel 
 
from era.businesseventsnapshot b 
 
INNER JOIN era.postedtransaction a 
on a.eventid=b.eventid 
 
where to_date(a.checkindate) between '2019-06-01' and '2020-05-31'  
and a.renterid is not null 
) 
select a.booking_channel, count(distinct a.eventid) from cust_type a 
 
where type='Customer' 
group by 1 
order by 1 
with frequency as ( 
select a.renterid 
        , count(distinct a.transactionid) as freq 
        --,m.con_id 
        --,(year(now()) - year(i.dateofbirth)) as Age 
         
 
FROM era.postedtransaction a 
 
where to_date(a.checkindate) between  '2019-06-01' and '2020-05-31'  
group by 1 
order by 1 
)  
select case when freq=1 then 'Single' else 'Multiple' end loyalty 
        ,case when v.bodystyle in ('SUV WAGON','SUV','Wagon') then 'Wagon' 
                when v.bodystyle in ('Hatchback','HATCH') then 'Hatchback' else v.bodystyle end car_type 
        , count(f.renterid) from frequency f 
inner join era.postedtransaction a 
on a.renterid=f.renterid 
 
inner join era.vehicle v 
on v.vehicleid=a.vehicleid 
 
where v.vehicletype='Car' 
and to_date(a.checkindate) between  '2019-06-01' and '2020-05-31'  
 
group by 1,2 
order by 1,2 