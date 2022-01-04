select year(pickupdate),month(pickupdate), count(bes.driverrenterid) from era.businesseventsnapshot bes 
 
where to_date(bes.pickupdate) between '2015-07-01' and '2020-06-30' 
and bes.originaltransactionstatus not in ('Void', 'No Show', 'Cancelled') 
 
group by 1,2 
order by 1,2 
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
 
group by 1,2,3 
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
select year(t.checkindate) 
        , month(t.checkindate) 
        , v.vehicletype 
        , count(distinct t.renterid) 
 
from era.postedtransaction t 
 
inner join vehicle v 
on v.vehicleid=t.vehicleid 
 
where to_date(t.checkindate) between '2019-07-01' and '2020-06-30' 
 
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
select distinct bodystyle from era.vehicle 
where vehicletype='Car' 