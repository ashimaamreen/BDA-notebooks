SELECT * FROM era.individual LIMIT 100; 
create table campaign_data.aa_dmc2071_ThriftyRDA_research_otheres_20200610_adhoc as 
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
with all_members as( 
SELECT DISTINCT i.eventid as RECORDID 
        , i.primarycountrycode as ISO 
        , i.primaryaddressline1 as ADDRESSLINE1 
        , case when i.primaryaddressline2 is null then i.primarycity else i.primaryaddressline2 end ADDRESSLINE2 
        , case when i.primaryaddressline2 is null and i.primaryaddressline3 is null then concat(i.primarystate,' ',i.primarypostcode) else i.primaryaddressline3 end ADDRESSLINE3 
        , i.primarycity as suburb 
        , i.primarystate as state 
        , i.primarypostcode as zipcode 
        , (year(now()) - year(i.dateofbirth)) as Age 
 
FROM era.postedtransaction a 
--count(DISTINCT a.eventid) 
INNER JOIN era.individual i 
on i.eventid=a.eventid 
 
where to_Date(a.checkindate) between  '2019-06-01' and '2020-05-31'  
and i.primarycountrycode is not NULL 
and i.primaryaddressline1 is not NULL 
--and i.primaryaddressline3 is null 
--799038 
)select count(distinct RECORDID) from all_members 
select * from campaign_data.aa_dmc2071_ThriftyRDA_research_otheres_20200610_adhoc limit 100; 
create table campaign_data.aa_dmc2071_ThriftyRDA_research_otheres_20200622_adhoc as 
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
 
where to_Date(a.checkindate) between  '2018-06-01' and '2019-05-31'  
and i.primarycountrycode is not NULL 
and i.primaryaddressline1 is not NULL 
select * from campaign_data.aa_dmc2071_ThriftyRDA_research_otheres_20200622_adhoc