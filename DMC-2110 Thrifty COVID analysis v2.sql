select year(pickupdate),month(pickupdate), count(bes.driverrenterid) from era.businesseventsnapshot bes 
 
where to_date(bes.pickupdate) between '2015-07-01' and '2020-06-30' 
and bes.originaltransactionstatus not in ('Void', 'No Show', 'Cancelled') 
 
group by 1,2 
order by 1,2 
select count(distinct bes.renterid) from era.postedtransaction bes 
 
where to_date(bes.checkindate) between '2019-03-01' and '2019-06-30' 
--and bes.transactionstatus='Posted' 
--not in ('Void', 'No Show', 'Cancelled') 
 
-- group by 1,2 
-- order by 1,2 
select to_Date(bes.bookingdate), count(bes.eventid) from era.businesseventsnapshot bes 
 
where to_date(bes.bookingdate) between '2015-07-01' and '2020-06-30' 
 
group by 1 
order by 1 
 
-- inner join customer c 
-- on c.renterid=bes.driverrenterid 
spark.sql(''' 
select to_date(checkindate), count(distinct renterid) 
 
from era.postedtransaction 
where to_date(checkindate) between '2015-07-01' and '2020-06-30' 
group by 1 
order by 1 
''').show(10000) 
RDA_tagging = spark.read.option("delimiter", "|").csv("/user/aamreen/Thrifty_RDA/THRIFTY_3RD_FILE_14184_Output.txt", inferSchema="true", header="true") 
RDA_tagging.createOrReplaceTempView("RDA_tagging") 
spark.sql (""" select * from RDA_tagging""").count() 
spark.sql (""" create table campaign_data.aa_dmc2071_thriftyRDAtagging_research_others_20200715_adhoc as select * from RDA_tagging""").show(10) 
 
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
 
inner join campaign_data.aa_dmc2071_thriftyrdatagging_research_others_20200715_adhoc t 
on t.in_recordid = a.eventid 
 
left join gms.s_contact c 
on c.row_id=m.con_id 
 
left JOIN gms.s_contact_x cx 
on cx.par_row_id=c.row_id 
 
left JOIN gms.s_contact_fnx fn 
on fn.par_row_id=c.row_id 
 
left join gms.s_addr_per as ad         --813620 
on c.pr_per_addr_id = ad.row_id 
 
where to_date(a.checkindate) between  '2019-07-01' and '2020-06-30'     -- 
--and i.dateofbirth is not null 
--and i.eventid=5886871 
--group by 1 
spark.sql(''' 
select case when UPPER(trim(i.primarycountrycode))='AUS' then 'AU' 
            else 'Overseas' end country 
        ,to_date(a.checkindate), count(distinct a.renterid) 
 
from era.postedtransaction a 
 
INNER JOIN era.individual i 
on i.eventid=a.eventid 
 
where to_date(a.checkindate) between  '2015-07-01' and '2020-06-30' 
and checkinowninglocationcode like 'AU%' 
and i.primarycountrycode is not NULL 
and i.primaryaddressline1 is not NULL 
 
group by 1,2 
order by 1,2 
''').show(10000) 
SELECT year(bes.checkindate) 
        ,month(bes.checkindate) 
        , case when lower(bes.checkinlocation) like '%airport%' then 'Airport' else 'non-Airport' end checkin_location 
        , count(DISTINCT bes.renterid) 
 
FROM era.postedtransaction bes 
where to_date(bes.checkindate) between '2018-07-01' and '2020-06-30' 
--and bes.transactionstatus not in ( 'Void', 'No Show', 'Cancelled') 
 
group by 1,2,3 
order by 1,2,3 
select case when lower(locationname) like '%airport%' then 'Airport' else 'non-Airport' end locationname_type 
        , count(DISTINCT locationid)  
from era.`location` 
where countrycode='AUS' 
group by 1 
select case when lower(locationname) like '%airport%' then 'Airport' else 'non-Airport' end checkin_location 
        , sum(grossrevenue)  
from era.revenuesnapshotbyinvoice 
where to_date(checkindate) between '2019-03-01' and '2019-06-30' 
group by 1 
select case when lower(locationname) like '%airport%' then 'Airport' else 'non-Airport' end checkin_location 
        , sum(grossrevenue)  
from era.revenuesnapshotbyinvoice 
where to_date(checkindate) between '2020-03-01' and '2020-06-30' 
group by 1 
SELECT case when lower(bes.checkinlocation) like '%airport%' then 'Airport' else 'non-Airport' end checkin_location 
        , count(DISTINCT bes.eventid) 
 
FROM era.postedtransaction bes 
where to_date(bes.checkindate) between '2019-03-01' and '2019-06-30' 
--and bes.transactionstatus not in ( 'Void', 'No Show', 'Cancelled') 
 
group by 1 
order by 1 
SELECT case when lower(bes.checkinlocation) like '%airport%' then 'Airport' else 'non-Airport' end checkin_location 
        , count(DISTINCT bes.eventid) 
 
FROM era.postedtransaction bes 
where to_date(bes.checkindate) between '2020-03-01' and '2020-06-30' 
--and bes.transactionstatus not in ( 'Void', 'No Show', 'Cancelled') 
 
group by 1 
order by 1 
spark.sql(''' 
SELECT to_date(bes.pickupdate)SELECT to_date(bes.pickupdate) 
        , case when lower(bes.pickuplocation) like '%airport%' then 'Airport' else 'non-Airport' end checkin_location 
        , count(DISTINCT bes.driverrenterid) 
 
FROM era.businesseventsnapshot bes 
where to_date(bes.pickupdate) between '2018-07-01' and '2020-06-30' 
and bes.transactionstatus not in ('Open, Reservation', 'Void', 'No Show', 'Cancelled') 
 
group by 1,2 
order by 1,2 
''').show(10000) 
select * from era.businesseventsnapshot 
where driverrenterid=2118876 
and to_date(pickupdate) between '2018-07-01' and '2020-06-30' 
order by pickupdate 
 
with freq as ( 
select bes.driverrenterid 
        , case when count(distinct bes.maxtransactionid)<2 then 'once' 
                when count(distinct bes.maxtransactionid)<10 then '2-10' 
                when count(distinct bes.maxtransactionid)<20 then '11-20' 
                when count(distinct bes.maxtransactionid)>20 then '20+' 
                else 'none' end freq_member 
 
from era.businesseventsnapshot bes 
where to_date(bes.pickupdate) between '2018-07-01' and '2020-06-30' 
and bes.transactionstatus not in ('Open', 'Reservation', 'Void', 'No Show', 'Cancelled') 
group by 1 
) 
select year(pickupdate),month(pickupdate), count(distinct f.driverrenterid)  
 
from freq f 
 
inner join era.businesseventsnapshot b 
on b.driverrenterid=f.driverrenterid 
 
where to_date(b.pickupdate) between '2018-07-01' and '2020-06-30' 
and f.freq_member='once' 
 
group by 1,2 
order by 1,2 
with freq as ( 
select bes.driverrenterid 
        , case when count(distinct bes.maxtransactionid)<2 then 'once' 
                when count(distinct bes.maxtransactionid)<10 then '2-10' 
                when count(distinct bes.maxtransactionid)<20 then '11-20' 
                when count(distinct bes.maxtransactionid)>20 then '20+' 
                else 'none' end freq_member 
 
from era.businesseventsnapshot bes 
where to_date(bes.pickupdate) between '2018-07-01' and '2020-06-30' 
group by 1 
) 
select year(pickupdate),month(pickupdate), count(distinct b.driverrenterid)  
 
from era.businesseventsnapshot b 
--on b.driverrenterid=f.driverrenterid 
 
where to_date(b.pickupdate) between '2018-07-01' and '2020-06-30' 
--and f.freq_member<>'once' 
 
group by 1,2 
order by 1,2 
with freq as ( 
select bes.driverrenterid 
        , case when count(distinct bes.maxtransactionid)<2 then 'once' 
                when count(distinct bes.maxtransactionid)<10 then '2-10' 
                when count(distinct bes.maxtransactionid)<20 then '11-20' 
                when count(distinct bes.maxtransactionid)>20 then '20+' 
                else 'none' end freq_member 
 
from era.businesseventsnapshot bes 
where to_date(bes.pickupdate) between '2018-07-01' and '2020-06-30' 
group by 1 
) 
select freq, count(distinct f.driverrenterid)  
 
from freq f 
 
group by 1 
order by 1,2 
SELECT eventid, count(DISTINCT marketsegment) from era.revenuesnapshotbyinvoice 
where to_date(checkindate) between '2020-01-01' and '2020-06-30' 
--11:00:00' 
GROUP BY 1 
order by 2 DESC 
select year(bes.pickupdate) 
        ,month(bes.pickupdate) 
        ,case when r.marketsegment like '%Auto Club%' then 'Auto Club' 
            when r.marketsegment like '%Corporate%' then 'Corporate' 
            when r.marketsegment like '%Discretionary%' then 'Discretionary' 
            when r.marketsegment like '%Third Party%' then 'Third Party' 
            else r.marketsegment end segment 
        , count(bes.driverrenterid) total_renters 
 
 
from era.businesseventsnapshot bes     --511150 
 
inner join era.revenuesnapshotbyinvoice r 
on r.eventid=bes.eventid 
 
where to_date(bes.pickupdate) between '2018-07-01' and '2020-06-30' 
 
group by year(bes.pickupdate) 
        ,month(bes.pickupdate) 
        ,case when r.marketsegment like '%Auto Club%' then 'Auto Club' 
            when r.marketsegment like '%Corporate%' then 'Corporate' 
            when r.marketsegment like '%Discretionary%' then 'Discretionary' 
            when r.marketsegment like '%Third Party%' then 'Third Party' 
            else r.marketsegment end 
order by 1,2,3 
select year(bes.pickupdate) 
        ,month(bes.pickupdate) 
        ,case when r.marketsegment like '%Auto Club%' then 'Auto Club' 
            when r.marketsegment like '%Corporate%' then 'Corporate' 
            when r.marketsegment like '%Discretionary%' then 'Discretionary' 
            when r.marketsegment like '%Third Party%' then 'Third Party' 
            else r.marketsegment end segment 
        , count(bes.driverrenterid) total_events  
 
 
from era.businesseventsnapshot bes     --511150 
 
inner join era.revenuesnapshotbyinvoice r 
on r.eventid=bes.eventid 
 
where to_date(bes.pickupdate) between '2018-07-01' and '2020-06-30' 
--and bes.originaltransactionstatus not in ('Void', 'No Show', 'Cancelled') 
 
group by 1,2,3 
order by 1,2,3 
select month(bes.checkindate),t.custsegment2_tag as colour_plus, count(bes.renterid) pre_covid from era.postedtransaction bes     --511150 
 
inner join campaign_data.aa_dmc2071_thriftyrdatagging_research_others_20200715_adhoc t 
on t.in_recordid = bes.renterid 
 
where to_date(bes.checkindate) between '2019-07-01' and '2020-06-30' 
--and bes.transactionstatus not in ('Open', 'Reservation', 'Void', 'No Show', 'Cancelled') 
 
group by 1,2 
order by 1,2 
select t.custsegment2_tag as colour_plus, count(distinct bes.renterid) from era.postedtransaction bes     --511150 
 
inner join campaign_data.aa_dmc2071_thriftyrdatagging_research_others_20200715_adhoc t 
on t.in_recordid = bes.renterid 
 
where to_date(bes.checkindate) between '2019-07-01' and '2020-06-30' 
and lower(bes.checkinlocation) like '%airport%' 
 
group by 1 
order by 1 
select t.custsegment2_tag as colour_plus, count(distinct bes.renterid) from era.postedtransaction bes     --511150 
 
inner join campaign_data.aa_dmc2071_thriftyrdatagging_research_others_20200715_adhoc t 
on t.in_recordid = bes.renterid 
 
where to_date(bes.checkindate) between '2019-07-01' and '2020-06-30' 
and lower(bes.checkinlocation) not like '%airport%' 
 
group by 1 
order by 1 
select t.custsegment2_tag as colour_plus, count(bes.renterid) pre_covid from era.postedtransaction bes     --511150 
 
inner join campaign_data.aa_dmc2071_thriftyrdatagging_research_others_20200715_adhoc t 
on t.in_recordid = bes.renterid 
 
where to_date(bes.checkindate) between '2019-03-01' and '2019-06-30' 
--and bes.transactionstatus not in ('Open', 'Reservation', 'Void', 'No Show', 'Cancelled') 
 
group by 1 
order by 1 
select t.custsegment2_tag as colour_plus, count(bes.renterid) as post_covid from era.postedtransaction bes     --511150 
 
inner join campaign_data.aa_dmc2071_thriftyrdatagging_research_others_20200715_adhoc t 
on t.in_recordid = bes.renterid 
 
where to_date(bes.checkindate) between '2020-03-01' and '2020-06-30' 
--and bes.transactionstatus not in ('Open', 'Reservation', 'Void', 'No Show', 'Cancelled') 
 
group by 1 
order by 1 
select t.GEOLOCHI_MKEY  as Geo_tribe, count(bes.renterid) as pre_covid from era.postedtransaction bes     --511150 
 
inner join campaign_data.aa_dmc2071_thriftyrdatagging_research_others_20200715_adhoc t 
on t.in_recordid = bes.renterid 
 
where to_date(bes.checkindate) between '2019-03-01' and '2019-06-30' 
--and bes.transactionstatus not in ('Open', 'Reservation', 'Void', 'No Show', 'Cancelled') 
 
group by 1 
order by 1 
select t.geolochi_mkey as Geo_tribe, count(bes.renterid) as post_covid from era.postedtransaction bes     --511150 
 
inner join campaign_data.aa_dmc2071_thriftyrdatagging_research_others_20200715_adhoc t 
on t.in_recordid = bes.renterid 
 
where to_date(bes.checkindate) between '2020-03-01' and '2020-06-30' 
--and bes.transactionstatus='Posted' 
--not in ('Open', 'Reservation', 'Void', 'No Show', 'Cancelled') 
 
group by 1 
order by 1 
spark.sql(''' 
select bes.leadtime, count(distinct bes.driverrenterid) as pre_covid from era.businesseventsnapshot bes 
 
where to_date(bes.pickupdate) between '2018-03-01' and '2020-02-29' 
group by 1 
order by 1 
''').show(10000) 
spark.sql(''' 
select bes.leadtime, count(distinct bes.driverrenterid) as post_covid from era.businesseventsnapshot bes 
 
where to_date(bes.pickupdate) between '2020-03-01' and '2020-06-30' 
group by 1 
order by 1 
''').show(10000) 
spark.sql(''' 
select bes.rentaldays, count(distinct bes.driverrenterid) as pre_covid from era.businesseventsnapshot bes 
 
where to_date(bes.pickupdate) between '2018-03-01' and '2020-02-29' 
group by 1 
order by 1 
''').show(10000) 
spark.sql(''' 
select bes.rentaldays, count(distinct bes.driverrenterid) as post_covid from era.businesseventsnapshot bes 
 
where to_date(bes.pickupdate) between '2020-03-01' and '2020-06-30' 
group by 1 
order by 1 
''').show(10000) 
select distinct vehicleclassbookedopen from era.businesseventsnapshot 
select case when bes.rentaldays<3 then 'Short' 
            when bes.rentaldays<5 then 'Medium' 
            when bes.rentaldays<8 then 'Week' 
            when bes.rentaldays>7 then 'Long' 
            else 'none' end Booking_type 
, count(distinct bes.driverrenterid) as pre_covid  
 
from era.businesseventsnapshot bes 
 
where to_date(bes.pickupdate) between '2018-03-01' and '2019-06-30' 
group by 1 
order by 1 
select case when bes.rentaldays<3 then 'Short' 
            when bes.rentaldays<5 then 'Medium' 
            when bes.rentaldays<7 then 'Week' 
            when bes.rentaldays>=7 then 'Long' 
            else 'none' end Booking_type 
, count(distinct bes.driverrenterid) as post_covid  
 
from era.businesseventsnapshot bes 
 
where to_date(bes.pickupdate) between '2019-03-01' and '2020-06-30' 
group by 1 
order by 1 
with age_group as ( 
select distinct a.renterid 
        , a.checkindate 
        ,(year(now()) - year(i.dateofbirth)) as Age 
         
 
FROM era.postedtransaction a 
 
INNER JOIN era.individual i 
on i.eventid=a.eventid 
 
where to_date(a.checkindate) between  '2018-07-01' and '2020-06-30'     -- 
and i.dateofbirth is not null 
) 
select    year(checkindate) 
        , month(checkindate) 
        , case when age<18 then 'under 18' 
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
group by 1,2,3 
order by 1,2,3 
--""").show(1000) 
with eligible as ( 
select renterid, min(checkindate) as aqusition_date from era.postedtransaction 
group by 1) 
 
select year(aqusition_date) 
        , month(aqusition_date) 
        , count(distinct renterid) from eligible 
where to_date(aqusition_date) between  '2018-07-01' and '2020-06-30'     
group by 1,2 
order by 1,2 
 
with acq as( 
select renterid from era.postedtransaction 
group by renterid 
having to_date(min(checkindate)) between  '2018-07-01' and '2020-06-30' ) 
 
select year(checkindate) 
        , month(checkindate) 
        , count(distinct  t.renterid) from  
era.postedtransaction t 
 
left anti join acq a 
on a.renterid=t.renterid 
 
where to_date(t.checkindate) between  '2018-07-01' and '2020-06-30'  
group by 1,2 
order by 1,2 
 
 
with eligibe as( 
select renterid, eventid 
        , row_number() over (partition by renterid order by checkindate asc) as rn 
from era.postedtransaction ) 
 
, acqusition as ( 
select eventid from eligibe 
where rn=1) 
 
select year(checkindate) 
        , month(checkindate) 
        , case when a.eventid is not null then 'Acquisition'  
                else 'Rentention' end type 
        ,count(distinct  t.renterid) from  
 
era.postedtransaction t 
 
left join acqusition a 
on a.eventid=t.eventid 
 
where to_date(t.checkindate) between  '2018-07-01' and '2020-06-30'  
group by 1,2,3 
order by 1,2,3 
with eligibe as( 
select renterid, eventid 
        , row_number() over (partition by renterid order by checkindate asc) as rn 
from era.postedtransaction ) 
 
, acqusition as ( 
select eventid from eligibe 
where rn=1) 
 
select t.custsegment2_tag as colour_plus, count(distinct bes.renterid) from era.postedtransaction bes     --511150 
 
inner join campaign_data.aa_dmc2071_thriftyrdatagging_research_others_20200715_adhoc t 
on t.in_recordid = bes.renterid 
 
left join acqusition a 
on a.eventid=bes.eventid 
 
where to_date(bes.checkindate) between '2019-07-01' and '2020-06-30' 
and a.eventid is not null  
group by 1 
order by 1 
with eligibe as( 
select renterid, eventid 
        , row_number() over (partition by renterid order by checkindate asc) as rn 
from era.postedtransaction ) 
 
, acqusition as ( 
select eventid from eligibe 
where rn=1) 
 
select t.custsegment2_tag as colour_plus, count(distinct bes.renterid) from era.postedtransaction bes     --511150 
 
inner join campaign_data.aa_dmc2071_thriftyrdatagging_research_others_20200715_adhoc t 
on t.in_recordid = bes.renterid 
 
left join acqusition a 
on a.eventid=bes.eventid 
 
where to_date(bes.checkindate) between '2019-07-01' and '2020-06-30' 
and a.eventid is null  
group by 1 
order by 1 
select * from era.vehicle 
select distinct vehiclegroup from era.postedtransaction 
select year(t.checkindate) 
        , month(t.checkindate) 
        , v.vehicletype 
        , count(distinct t.renterid) 
 
from era.postedtransaction t 
 
inner join vehicle v 
on v.vehicleid=t.vehicleid 
 
where to_date(t.checkindate) between '2019-07-01' and '2020-06-30' 
and  
 
group by 1,2,3 
order by 1,2,3 
select year(t.checkindate) 
        , month(t.checkindate) 
        , case when v.bodystyle in ('SUV WAGON','SUV','Wagon') then 'Wagon' 
                when v.bodystyle in ('Hatchback','HATCH') then 'Hatchback'  
                when v.bodystyle in ('Tray', 'Tray Ute','Ute') then 'Ute' 
                when v.bodystyle ='Sedan' then 'Sedan' 
                when v.bodystyle='Bus' then 'Bus' 
                else 'other' end car_type 
        , count(distinct t.renterid) 
 
from era.postedtransaction t 
 
inner join vehicle v 
on v.vehicleid=t.vehicleid 
 
where to_date(t.checkindate) between '2019-07-01' and '2020-06-30' 
and vehicletype='Car' 
 
group by 1,2,3 
order by 1,2,3 
create table campaign_data.aa_dmc2116_thriftyneeds_research_otheres_20200724_adhoc as 
select  distinct bes.eventid --1653147 
        , bes.driverrenterid 
        ,case when bes.rentaldays<3 then 'Short' 
            when bes.rentaldays<5 then 'Medium' 
            when bes.rentaldays<8 then 'Week' 
            when bes.rentaldays>7 then 'Long' 
            else 'none' end secondary 
        , case when bes.vehicleclassbooked in ('CCAH','CCAR','CCMR','CFAR','CNAR','ECAR','ECMR','ESAR','MCAR','MDAR') then 'Small Affordable Car' 
                when bes.vehicleclassbooked in ('FCAH','FCAR','XXXX') then 'Large Affordable Car' 
                when bes.vehicleclassbooked in ('FSAR','GDAR','LCAR','LDAR','PCAR','PDAR','XDAE') then 'Luxury Car' 
                when bes.vehicleclassbooked in ('ICAR','ICMR') then 'Medium Affordable Car' 
                when bes.vehicleclassbooked in ('IVAR') then 'Large Affordable Mini Van' 
                when bes.vehicleclassbooked in ('JFAR','LFAD','LWAR','FWAR','LXFR','PFAR','PWAR','UWAR','XPAD') then 'Luxury SUV' 
                when bes.vehicleclassbooked in ('CWAR','IWAR','SWAR','SCAR','SDAR','SWFB') then 'Affordable SUV' 
                else 'Commercial' end Tertiary 
 
        , bes.odometerin - bes.odometerout as distance 
        , bes.leadtime 
        , bes.rentaldays 
        , case when bes.bookingchannelgroup='Yet To Be Mapped' OR bes.bookingchannelgroup IS NULL then 'Unkown' else bes.bookingchannelgroup end booking_channel 
        --, bes.bookingchannelgroup 
        , (year(current_timestamp()) - year(i.dateofbirth)) as Age 
        , t.custsegment2_tag as colour_plus 
        , t.geolochi_mkey as affluent_group 
        --, v.bodystyle 
        , v.vehicletype 
        , case when v.bodystyle in ('SUV WAGON','SUV','Wagon') then 'Wagon' 
                when v.bodystyle in ('Hatchback','HATCH') then 'Hatchback'  
                when v.bodystyle in ('Tray', 'Tray Ute','Ute') then 'Ute'   
                when v.bodystyle ='Sedan' then 'Sedan' 
                when v.bodystyle='Bus' then 'Bus' 
                else 'other' end car_type 
        , bes.bookingdate 
        , bes.pickupdate 
        , bes.returndate 
        , bes.maxtransactionid 
        , case when r.marketsegment like '%Auto Club%' then 'Auto Club' 
            when r.marketsegment like '%Corporate%' then 'Corporate' 
            when r.marketsegment like '%Discretionary%' then 'Discretionary' 
            when r.marketsegment like '%Third Party%' then 'Third Party' 
            else r.marketsegment end segment 
        , bes.agentrenterid 
        , bes.brokerrenterid 
        , bes.companyrenterid 
        , bes.customerrenterid 
        , bes.insurancerenterid 
        , bes.payorrenterid 
--bes.chargekms, bes.km_col, bes.km_del,  bes.odometerout,bes.odometerin 
--,* 
from era.businesseventsnapshot bes 
 
inner join era.individual i --1653146 
on i.eventid=bes.eventid 
 
left join campaign_data.aa_dmc2071_thriftyrdatagging_research_others_20200715_adhoc t      --1078123 
on t.in_recordid = bes.driverrenterid 
 
left join era.vehicle v 
on v.vehicleid=bes.vehicleid 
 
left join era.revenuesnapshotbyinvoice r 
on r.eventid=bes.eventid 
 
where to_date(bes.pickupdate) between '2018-07-01' and '2020-06-30' 
and bes.transactionstatus='Posted' 
select distinct marketsegment from era.revenuesnapshotbyinvoice 
select year(pickupdate),month(pickupdate),segment, count(distinct driverrenterid) 
from campaign_data.aa_dmc2116_thriftyneeds_research_otheres_20200724_adhoc  
 
group by 1,2,3 
order by 1,2,3 
select colour_plus, count(distinct driverrenterid) 
from campaign_data.aa_dmc2116_thriftyneeds_research_otheres_20200724_adhoc  
where segment in ('Corporate','Discretionary') 
group by 1 
order by 1 
select colour_plus, count(distinct driverrenterid) 
from campaign_data.aa_dmc2116_thriftyneeds_research_otheres_20200724_adhoc  
where segment in ('Corporate','Discretionary','Other') 
and segment is not null 
group by 1 
order by 1 
select year(pickupdate),month(pickupdate),secondary, count(distinct driverrenterid) 
from campaign_data.aa_dmc2116_thriftyneeds_research_otheres_20200724_adhoc  
 
group by 1,2,3 
order by 1,2,3 
select secondary,segment, count(distinct driverrenterid) 
from campaign_data.aa_dmc2116_thriftyneeds_research_otheres_20200724_adhoc  
where to_date(pickupdate) between '2019-07-01' and '2020-06-30' 
--and secondary='Long' 
 
group by 1,2 
order by 1,2 
select secondary 
,case when age<18 then 'under 18' 
            when age<25 then 'under 25' 
            when age<35 then 'under 35' 
            when age<45 then 'under 45' 
            when age<55 then 'under 55' 
            when age<65 then 'under 65' 
            when age<75 then 'under 75' 
            when age<85 then 'under 85' 
        else 'age_unavailable' end Age_bracket 
        , count(distinct driverrenterid) 
from campaign_data.aa_dmc2116_thriftyneeds_research_otheres_20200724_adhoc  
where to_date(pickupdate) between '2018-07-01' and '2020-06-30' 
 
group by 1,2 
order by 1,2 
 
         
select secondary,case when distance<100 then 'Less than 100' 
            when distance<200 then '100 - 200' 
            when distance<300 then '200 - 300' 
            when distance<400 then '300 - 400' 
            when distance<500 then '400 - 500' 
            when distance<1000 then '500 - 1000' 
            when distance<2001 then '1000 - 2001' 
            when distance>2000 then '2000+' 
            else 'none' end travel_distance 
        , count(distinct driverrenterid) 
from campaign_data.aa_dmc2116_thriftyneeds_research_otheres_20200724_adhoc  
--where to_date(pickupdate) between '2019-07-01' and '2020-06-30' 
--and secondary='Long' 
 
group by 1,2 
order by 1,2 
spark.sql(''' 
select distance 
        , count(distinct driverrenterid) 
from campaign_data.aa_dmc2116_thriftyneeds_research_otheres_20200724_adhoc  
--where to_date(pickupdate) between '2019-07-01' and '2020-06-30' 
--and secondary='Long' 
 
group by 1 
order by 1 
''').show(10000) 
select year(pickupdate),month(pickupdate),tertiary, count(distinct driverrenterid) 
from campaign_data.aa_dmc2116_thriftyneeds_research_otheres_20200724_adhoc  
where to_date(pickupdate) between '2018-07-01' and '2020-06-30' 
 
group by 1,2,3 
order by 1,2,3 
select secondary,tertiary, count(distinct driverrenterid) 
from campaign_data.aa_dmc2116_thriftyneeds_research_otheres_20200724_adhoc  
where to_date(pickupdate) between '2018-07-01' and '2020-06-30' 
 
group by 1,2 
order by 1,2 
select distinct leadtime from campaign_data.aa_dmc2116_thriftyneeds_research_otheres_20200724_adhoc 
where booking_channel='WALK UP' 
select segment 
             
 
, count(distinct driverrenterid) from campaign_data.aa_dmc2116_thriftyneeds_research_otheres_20200724_adhoc  
where leadtime >30 
or leadtime is null 
--and to_date(pickupdate) between '2019-07-01' and '2020-06-30' 
group by 1 
order by 1 
select  segment 
 
, count(distinct driverrenterid) from campaign_data.aa_dmc2116_thriftyneeds_research_otheres_20200724_adhoc  
where (leadtime <30 
and leadtime>0) 
or leadtime is null 
--and to_date(pickupdate) between '2019-07-01' and '2020-06-30' 
group by 1 
order by 1 
spark.sql(''' 
select leadtime, count(distinct driverrenterid) from campaign_data.aa_dmc2116_thriftyneeds_research_otheres_20200724_adhoc  
group by 1 
order by 1  
''').show(100000) 
select affluent_group, tertiary, count(distinct driverrenterid) 
from campaign_data.aa_dmc2116_thriftyneeds_research_otheres_20200724_adhoc  
-- where to_date(pickupdate) between '2018-07-01' and '2020-06-30' 
 
group by 1,2 
order by 1,2 
select a.car_type,case when lower(bes.pickuplocation) like '%airport%' then 'Airport' else 'non-Airport' end checkin_location 
        ,count(a.driverrenterid) 
from campaign_data.aa_dmc2116_thriftyneeds_research_otheres_20200724_adhoc a 
 
inner join era.businesseventsnapshot bes 
on bes.eventid=a.eventid 
 
group by 1,2 
order by 1,2 
--where to_date(pickupdate) between '2018-07-01' and '2020-06-30' 
select a.segment, case when to_date(pickupdate) between '2019-03-01' and '2019-06-30' then 'LY (Mar-Jun)' 
                        when to_date(pickupdate) between '2020-03-01' and '2020-06-30' then 'TY COVID' 
                        else 'other' end period 
        ,count(a.driverrenterid) 
from campaign_data.aa_dmc2116_thriftyneeds_research_otheres_20200724_adhoc a 
 
 
group by 1,2 
order by 1,2 
--where to_date(pickupdate) between '2018-07-01' and '2020-06-30' 
select count(distinct driverrenterid) from era.businesseventsnapshot 
where to_date(pickupdate) between '2018-07-01' and '2019-06-30' 
--and bluechipmembershipnumber is not null 
select a.segment, case when to_date(pickupdate) between '2019-03-01' and '2019-06-30' then 'LY (Mar-Jun)' 
                        when to_date(pickupdate) between '2020-03-01' and '2020-06-30' then 'TY COVID' 
                        else 'other' end period 
        ,count(a.driverrenterid) 
from campaign_data.aa_dmc2116_thriftyneeds_research_otheres_20200724_adhoc a 
select * 
from campaign_data.aa_dmc2116_thriftyneeds_research_otheres_20200724_adhoc a 
SELECT * FROM campaign_data.carehire_crosssell_20200701 LIMIT 100; 
 
select decile,case when age<18 then 'under 18' 
            when age<25 then 'under 25' 
            when age<35 then 'under 35' 
            when age<45 then 'under 45' 
            when age<55 then 'under 55' 
            when age<65 then 'under 65' 
            when age<75 then 'under 75' 
            when age<85 then 'under 85' 
        else 'age_unavailable' end Age_bracket   
        ,count(distinct contact_id) 
 
from  
(select distinct m.decile,m.contact_id,(year(now()) - year(c.birth_dt)) as Age 
--,band,rand(0) as rng  
 
from gms.s_contact c                     --1951754 
 
inner join campaign_data.carehire_crosssell_20200701 m 
on m.contact_id=c.row_id 
 
inner JOIN gms.s_contact_x cx 
on cx.par_row_id=c.row_id 
 
inner JOIN gms.s_contact_fnx fn             --903490 
on fn.par_row_id=c.row_id 
 
-- inner join gms.s_org_ext as org 
-- on org.pr_con_id = c.row_id 
 
where 1=1 
and c.csn is not null 
 
and NVL(fn.deceased_flg,'N') = 'N' 
and NVL(c.x_nrma_title,'no title') != 'Estate Of The Late'      --899132 
 
and c.con_cd in ('Ordinary Member' , 'Affiliate Member')        --893688 
 
and (cx.attrib_36 ='Yes' or Upper(cx.attrib_36) ='NULL')                                                                
and case when trim(c.x_inv_email_1) = 'Y' or c.email_addr is null then 'N' else 'Y' end = 'Y'       --459346 
)a 
group by 1,2 
order by 1,2 
SELECT c.tertiary, case when to_date(pickupdate) between '2019-03-01' and '2019-06-30' then 'LY (Mar-Jun)' 
                        when to_date(pickupdate) between '2020-03-01' and '2020-06-30' then 'TY COVID' 
                        else 'other' end period 
        , count(DISTINCT c.driverrenterid) 
 
FROM era.postedtransaction bes 
 
inner JOIN campaign_data.aa_dmc2116_thriftyneeds_research_otheres_20200724_adhoc c 
on c.eventid=bes.eventid 
--where to_date(bes.checkindate) between '2018-07-01' and '2020-06-30' 
--and bes.transactionstatus not in ( 'Void', 'No Show', 'Cancelled') 
 
group by 1,2 
order by 1,2 
SELECT case when lower(bes.checkinlocation) like '%airport%' then 'Airport' else 'non-Airport' end checkin_location 
        ,c.tertiary 
        , count(DISTINCT c.driverrenterid) 
 
FROM era.postedtransaction bes 
 
inner JOIN campaign_data.aa_dmc2116_thriftyneeds_research_otheres_20200724_adhoc c 
on c.eventid=bes.eventid 
--where to_date(bes.checkindate) between '2018-07-01' and '2020-06-30' 
--and bes.transactionstatus not in ( 'Void', 'No Show', 'Cancelled') 
 
group by 1,2 
order by 1,2 