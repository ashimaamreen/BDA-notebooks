Contents
select p.name, count(distinct c.row_id) 
        --, sum(case when c.x_nrma_join_dt between '2019-04-14' and '2019-06-30' then 1 else 0 end) during_offer  
        --, sum(case when c.x_nrma_join_dt between '2019-05-30' and '2019-06-30' then 1 else 0 end) after_offer  
        --, sum(case when c.x_nrma_join_dt < '2019-04-14' then 1 else 0 end) before  
--c.csn, X_NRMA_JOIN_DT, p.name, a.COMMENTS   
from gms.s_contact c 
 
inner join gms.s_asset a  
on a.owner_con_id = c.row_id 
--and a.status_cd='Active' 
 
inner join gms.s_prod_int p 
on a.prod_id = p.row_id 
and p.name in ('PC_IAG_Bundle_RSA_Offer_0319','CC_IAG_Bundle_RSA_Offer_0319') 
 
--where c.row_id='1-HPY-2149' 
--where c.x_nrma_join_dt between '2019-04-14' and now() 
group by p.name 
--, a.start_dt 
---ALL NEW MEMBERS WITH CC AND PC IN THE FY18 ONE YEAR----- 
 
select  weekofyear(from_utc_timestamp(X_NRMA_JOIN_DT,'AEST')) as week_number , count(distinct c.csn)    
--from_utc_timestamp(X_NRMA_JOIN_DT,'AEST'),month(from_utc_timestamp(X_NRMA_JOIN_DT,'AEST')), 
from gms.s_contact c 
     
inner join gms.s_asset a  
on a.owner_con_id = c.row_id 
 
inner join gms.s_prod_int p 
on a.prod_id = p.row_id 
 
where to_date(c.x_nrma_join_dt) between '2017-07-01' and '2018-06-30' 
    and c.cust_stat_cd = 'Active'  --Active Customer 
    and a.status_cd = 'Active' 
    and p.type = 'Membership' 
    and p.prod_cd = 'Product' 
    and c.csn is not null 
    --and p.name in ('PC_IAG_Bundle_RSA_Offer_0319','CC_IAG_Bundle_RSA_Offer_0319') 
    and p.name in ('Premium Care','Classic Care','MVB Premium Care','MVB Classic Care') 
    --and c.csn='990421270' 
group by weekofyear(from_utc_timestamp(X_NRMA_JOIN_DT,'AEST')) 
--from_utc_timestamp(X_NRMA_JOIN_DT,'AEST'),month(from_utc_timestamp(X_NRMA_JOIN_DT,'AEST')),  
order by 1 
limit 60 
---ALL NEW MEMBERS WITH CC AND PC IN THE LAST ONE YEAR----- 
 
 
select  weekofyear(from_utc_timestamp(X_NRMA_JOIN_DT,'AEST')) as week_number , count(distinct c.csn)    
--from_utc_timestamp(X_NRMA_JOIN_DT,'AEST'),month(from_utc_timestamp(X_NRMA_JOIN_DT,'AEST')), 
from gms.s_contact c 
     
inner join gms.s_asset a  
on a.owner_con_id = c.row_id 
 
inner join gms.s_prod_int p 
on a.prod_id = p.row_id 
 
where to_date(c.x_nrma_join_dt) between '2018-07-01' and '2019-06-30' 
    and c.cust_stat_cd = 'Active'  --Active Customer 
    and a.status_cd = 'Active' 
    and p.type = 'Membership' 
    and p.prod_cd = 'Product' 
    and c.csn is not null 
    --and p.name in ('PC_IAG_Bundle_RSA_Offer_0319','CC_IAG_Bundle_RSA_Offer_0319') 
    and p.name in ('Premium Care','Classic Care','MVB Premium Care','MVB Classic Care') 
    --and c.csn='990421270' 
group by weekofyear(from_utc_timestamp(X_NRMA_JOIN_DT,'AEST')) 
--from_utc_timestamp(X_NRMA_JOIN_DT,'AEST'),month(from_utc_timestamp(X_NRMA_JOIN_DT,'AEST')),  
order by 1 
limit 60 
---ALL NEW MEMBERS WITH JOIN OFFER IN THE LAST ONE YEAR----- 
 
select  weekofyear(from_utc_timestamp(c.X_NRMA_JOIN_DT,'AEST')) as week_number , count(distinct c.csn)    
--from_utc_timestamp(X_NRMA_JOIN_DT,'AEST'),month(from_utc_timestamp(X_NRMA_JOIN_DT,'AEST')), 
from gms.s_contact c 
     
inner join gms.s_asset a  
on a.owner_con_id = c.row_id 
 
inner join gms.s_prod_int p 
on a.prod_id = p.row_id 
 
where to_date(c.X_NRMA_JOIN_DT) between '2018-07-01' and '2019-06-30' 
    and c.cust_stat_cd = 'Active'  --Active Customer 
    and a.status_cd = 'Active' 
    and p.type = 'Membership' 
    and p.prod_cd = 'Promotion' 
    and c.csn is not null 
    --and p.name in ('PC_IAG_Bundle_RSA_Offer_0319','CC_IAG_Bundle_RSA_Offer_0319') 
    and (p.name like '%PC%' or p.name like '%CC%') 
    and p.X_NRMA_OFFER_FLG='Y' 
    --and c.csn='990421270' 
group by weekofyear(from_utc_timestamp(c.X_NRMA_JOIN_DT,'AEST')) 
 
order by 1 
limit 60 
---ALL NEW MEMBERS WITH IAG Offer CC AND PC IN THE LAST ONE YEAR----- 
 
select  to_date(c.X_NRMA_JOIN_DT), weekofyear(from_utc_timestamp(c.X_NRMA_JOIN_DT,'AEST')) as week_number , count(distinct c.csn)    
from gms.s_contact c 
     
inner join gms.s_asset a  
on a.owner_con_id = c.row_id 
 
inner join gms.s_prod_int p 
on a.prod_id = p.row_id 
 
where to_date(c.X_NRMA_JOIN_DT) between '2018-07-01' and '2019-06-30' 
    and c.cust_stat_cd = 'Active'  --Active Customer 
    and a.status_cd = 'Active' 
    and p.type = 'Membership' 
    and p.prod_cd = 'Promotion' 
    and c.csn is not null 
    and p.name in ('PC_IAG_Bundle_RSA_Offer_0319','CC_IAG_Bundle_RSA_Offer_0319') 
    --and weekofyear(from_utc_timestamp(c.X_NRMA_JOIN_DT,'AEST'))=27 
     
group by to_date(c.X_NRMA_JOIN_DT), weekofyear(from_utc_timestamp(c.X_NRMA_JOIN_DT,'AEST')) 
 
order by 1,2 
---ALL NEW MEMBERS WITH MVB PC and CC----- 
 
select  weekofyear(from_utc_timestamp(X_NRMA_JOIN_DT,'AEST')) as week_number , count(distinct c.csn)    
--from_utc_timestamp(X_NRMA_JOIN_DT,'AEST'),month(from_utc_timestamp(X_NRMA_JOIN_DT,'AEST')), 
from gms.s_contact c 
     
inner join gms.s_asset a  
on a.owner_con_id = c.row_id 
 
inner join gms.s_prod_int p 
on a.prod_id = p.row_id 
 
where to_date(c.x_nrma_join_dt) between '2018-07-01' and '2019-06-30' 
    and c.cust_stat_cd = 'Active'  --Active Customer 
    and a.status_cd = 'Active' 
    and p.type = 'Membership' 
    and p.prod_cd = 'Product' 
    and c.csn is not null 
    --and p.name in ('PC_IAG_Bundle_RSA_Offer_0319','CC_IAG_Bundle_RSA_Offer_0319') 
    and p.name in ('MVB Premium Care','MVB Classic Care') 
    --and c.csn='990421270' 
group by weekofyear(from_utc_timestamp(X_NRMA_JOIN_DT,'AEST')) 
--from_utc_timestamp(X_NRMA_JOIN_DT,'AEST'),month(from_utc_timestamp(X_NRMA_JOIN_DT,'AEST')),  
order by 1 
limit 60 
select  weekofyear(from_utc_timestamp(a.START_DT,'AEST')) as week_number , count(distinct c.csn)    
from gms.s_contact c 
     
inner join gms.s_asset a  
on a.owner_con_id = c.row_id 
 
inner join gms.s_prod_int p 
on a.prod_id = p.row_id 
 
where to_date(a.START_DT) between '2018-07-01' and '2019-06-30' 
    and c.cust_stat_cd = 'Active'  --Active Customer 
    and a.status_cd = 'Active' 
    and p.type = 'Membership' 
    and p.prod_cd = 'Promotion' 
    and c.csn is not null 
    and p.name in ('PC_IAG_Bundle_RSA_Offer_0319','CC_IAG_Bundle_RSA_Offer_0319') 
    and p.X_NRMA_OFFER_FLG='Y' 
    --and c.csn='990421270' 
group by weekofyear(from_utc_timestamp(a.START_DT,'AEST')) 
 
order by 1 
limit 60 
select month(a.START_DT), count(distinct c.csn) 
--p.name, , X_NRMA_JOIN_DT,p.prod_cd   
from gms.s_contact c 
inner join gms.s_asset a  
on a.owner_con_id = c.row_id 
 
inner join gms.s_prod_int p 
on a.prod_id = p.row_id 
--and p.name in ('PC_IAG_Bundle_RSA_Offer_0319','CC_IAG_Bundle_RSA_Offer_0319') 
 
where 1=1 
--p.X_NRMA_OFFER_FLG='Y' 
and p.prod_cd = 'Promotion' 
and a.START_DT between '2018-07-01' and '2019-06-30' 
and p.name in ('PC_IAG_Bundle_RSA_Offer_0319','CC_IAG_Bundle_RSA_Offer_0319') 
group by month(a.START_DT) 
order by 1 
select distinct name from gms.s_prod_int p 
where (p.name like '%PC%' or p.name like '%CC%') 
and p.prod_cd = 'Promotion' and p.X_NRMA_OFFER_FLG='Y' 
select count(distinct c.csn)   
--a.START_DT ,  c.csn, p.type,  p.prod_cd,p.name, p.X_NRMA_OFFER_FLG 
from gms.s_contact c 
     
inner join gms.s_asset a  
on a.owner_con_id = c.row_id 
 
inner join gms.s_prod_int p 
on a.prod_id = p.row_id 
 
where to_date(a.START_DT) between '2018-07-01' and '2019-06-30' 
    and c.cust_stat_cd = 'Active'  --Active Customer 
    and a.status_cd = 'Active' 
    and p.type = 'Membership' 
    and p.prod_cd = 'Promotion' 
    and c.csn is not null 
    and p.name in ('PC_IAG_Bundle_RSA_Offer_0319','CC_IAG_Bundle_RSA_Offer_0319') 
    and p.X_NRMA_OFFER_FLG='Y' 
    --and c.csn='990421270' 
--order by 1 
--limit 60 
select cx.attrib_55  AS Colour_Plus, count(distinct c.row_id) 
        --, sum(case when c.x_nrma_join_dt between '2019-04-14' and now() then 1 else 0 end) during_offer  
        --, sum(case when c.x_nrma_join_dt < '2019-04-14' then 1 else 0 end) before  
--c.csn, X_NRMA_JOIN_DT, p.name, a.COMMENTS   
from gms.s_contact c 
 
inner join gms.s_contact_x cx 
on cx.par_row_id = c.row_id 
 
 
inner join gms.s_asset a  
on a.owner_con_id = c.row_id 
 
inner join gms.s_prod_int p 
on a.prod_id = p.row_id 
and p.name in ('PC_IAG_Bundle_RSA_Offer_0319','CC_IAG_Bundle_RSA_Offer_0319') 
 
--where c.x_nrma_join_dt between '2019-04-14' and now() 
group by cx.attrib_55 
order by 1 
 
select cx.attrib_55  AS Colour_Plus, count(distinct c.row_id) 
        --, sum(case when c.x_nrma_join_dt between '2019-04-14' and now() then 1 else 0 end) during_offer  
        --, sum(case when c.x_nrma_join_dt < '2019-04-14' then 1 else 0 end) before  
--c.csn, X_NRMA_JOIN_DT, p.name, a.COMMENTS   
from gms.s_contact c 
 
inner join gms.s_contact_x cx 
on cx.par_row_id = c.row_id 
 
 
inner join gms.s_asset a  
on a.owner_con_id = c.row_id 
 
inner join gms.s_prod_int p 
on a.prod_id = p.row_id 
and p.name in ('MVB Premium Care','MVB Classic Care') 
 
--where c.x_nrma_join_dt between '2019-04-14' and now() 
group by cx.attrib_55 
order by 1 
drop table sandpit.aa_20190716_o_dmc1639_exisiting_a 
--and a.owner_con_id='1-DNP-1325' 
--drop table sandpit.aa_20190716_o_dmc1639_exisiting --and a.owner_con_id='1-DNP-1325' 
select a.owner_con_id as row_id 
        , to_date(max(a.end_dt)) as last_end_dt 
         
from gms.s_asset a  
 
inner join gms.s_Asset v 
on v.row_id=a.service_point_id 
 
inner join gms.s_prod_int p 
on a.prod_id = p.row_id 
and p.prod_cd ='Promotion' 
and p.name not in ('PC_IAG_Bundle_RSA_Offer_0319','CC_IAG_Bundle_RSA_Offer_0319','Membership') 
 
where v.lcns_num!='UNKNOWN' 
and a.start_dt<'2019-07-01' 
--and a.owner_con_id='1-DNP-1325' 
group by a.owner_con_id 
--create table sandpit.aa_20190716_o_dmc1639_exisiting as  
select v1.contact_id 
        , to_date(v1.NRMA_join_dt) as NRMA_join_dt 
        , v.lcns_num as previous_rego 
        , v1.rego_IAG 
        , p.name as previous_prod 
        , v1.iag_prod 
        , to_date(v1.iag_st_dt) as iag_st_dt 
        , to_date(max(a.end_dt)) as last_end_dt  
        , to_date(max(a.start_dt)) as last_st_dt 
        , a.status_cd 
        , sum (case when a.status_cd='Active' then 1 else 0 end) as count_Assets  
         
from gms.s_contact c 
 
inner join gms.s_asset a  
on a.owner_con_id = c.row_id 
 
inner join gms.s_Asset v 
on v.row_id=a.service_point_id 
 
inner join gms.s_prod_int p 
on a.prod_id = p.row_id 
and p.prod_cd ='Promotion' 
and p.name not in ('PC_IAG_Bundle_RSA_Offer_0319','CC_IAG_Bundle_RSA_Offer_0319','Membership') 
 
inner join ( 
select v.lcns_num as rego_IAG 
        , c.x_nrma_join_dt as NRMA_join_dt 
        , p.name as iag_prod 
        , a.start_dt as iag_st_dt 
        , c.row_id as contact_id  
         
from gms.s_contact c 
 
inner join gms.s_asset a  
on a.owner_con_id = c.row_id 
and a.start_dt<'2019-07-01' 
 
inner join gms.s_Asset v 
on v.row_id=a.service_point_id 
 
inner join gms.s_prod_int p 
on a.prod_id = p.row_id 
and p.name in ('PC_IAG_Bundle_RSA_Offer_0319','CC_IAG_Bundle_RSA_Offer_0319') 
 
where c.x_nrma_join_dt < '2019-04-14') v1 
on v1.contact_id=c.row_id 
 
--where contact_id='1-DNP-1325' 
 
group by v.lcns_num 
        , p.name  
        , v1.rego_IAG 
        , v1.NRMA_join_dt 
        , v1.iag_prod 
        , v1.iag_st_dt 
        , v1.contact_id 
        , a.status_cd 
         
order by count_Assets desc 
create table sandpit.aa_20190716_o_dmc1639_existing as  
select distinct a.contact_id 
        , a.nrma_join_dt 
        , a.previous_prod 
        , a.iag_prod 
        , a.previous_rego 
        , a.rego_iag 
        , a.iag_st_dt 
        , a.last_end_dt 
        , datediff(a.iag_st_dt,a.last_end_dt) as days_since 
        , a.last_st_dt 
        , count_Assets 
         
from sandpit.aa_20190716_o_dmc1639_exisiting a  
    inner join ( 
    select b.contact_id, max(b.last_end_dt) as max_last_end_dt 
    from sandpit.aa_20190716_o_dmc1639_exisiting b 
    group by b.contact_id 
     
    ) as t1 
    on t1.max_last_end_dt = a.last_end_dt 
   -- where a.contact_id='1-DNP-1325' 
    --limit 10 
--drop table sandpit.aa_20190716_o_dmc1639_existing_type; 
create table sandpit.aa_20190716_o_dmc1639_existing_type as 
select * 
, case 
        when rego_iag = previous_rego and days_since <90 then  'cannibalisation' 
        when rego_iag = previous_rego then 'winback' 
        when count_assets>0 then 'upsell' 
        when rego_iag != previous_rego then 'winback2' else 'others'  
    end as Member_type 
 
from sandpit.aa_20190716_o_dmc1639_existing 
--where contact_id='1-DKS-1159' 
select previous_prod, count(distinct contact_id) from sandpit.aa_20190716_o_dmc1639_existing_type  
group by previous_prod 
select days_since, count(distinct contact_id) from sandpit.aa_20190716_o_dmc1639_existing_type  
group by days_since 
order by 1 
select iag_prod, member_type, count(distinct contact_id) from sandpit.aa_20190716_o_dmc1639_existing_type  
where member_type not in ('others') 
group by iag_prod ,member_type 
order by 1,2 
select  member_type,previous_prod, count(distinct contact_id) from sandpit.aa_20190716_o_dmc1639_existing_type  
where member_type not in ('others') 
group by member_type,previous_prod  
order by 1,2 
select count(distinct contact_id) from sandpit.aa_20190716_o_dmc1639_existing_type 
where member_type in ('winback', 'winback2') 
select c.row_id   
from gms.s_contact c 
inner join gms.s_asset a  
on a.owner_con_id = c.row_id 
 
inner join gms.s_prod_int p 
on a.prod_id = p.row_id 
and p.name in ('PC_IAG_Bundle_RSA_Offer_0319','CC_IAG_Bundle_RSA_Offer_0319') 
 
where c.x_nrma_join_dt < '2019-04-14' 
--group by p.name 
select count(distinct contact_id) from sandpit.aa_20190716_o_dmc1639_existing_type 
where contact_id in (select c.row_id   
from gms.s_contact c 
inner join gms.s_asset a  
on a.owner_con_id = c.row_id 
 
inner join gms.s_prod_int p 
on a.prod_id = p.row_id 
and p.name in ('PC_IAG_Bundle_RSA_Offer_0319','CC_IAG_Bundle_RSA_Offer_0319') 
 
where c.x_nrma_join_dt < '2019-04-14') 
select    c.row_id ContactID 
        , c.csn MemberNumber 
        , c.CON_CD as contact_type 
        , o.order_num OrderNumber 
        , oi.order_id as OrderLineItemID 
        , ot.name OrderType 
        , p.name Product 
        , p.sub_type_cd as Product_type 
        , o.status_cd OrderStatus 
        , o.X_RENEWAL_RELATED RenewalRelated_flag 
        , o.X_PAY_BY_TERM as MPP_flag 
        , o.X_PAYMENT_STATUS as PaymentStatus 
        , oi.ACTION_CD as Action --if =add then upgrade/dwngrade/new 
        , to_date(o.X_STAT_CMPLTD_DT) as OrderCompletionDate  
 
 
from gms.s_order o 
 
inner join gms.s_order_type ot 
on o.order_type_id = ot.row_id 
 
inner join gms.s_order_item oi 
on o.row_id = oi.order_id 
--and oi.ACTION_CD in ('Update','Add') --when they renew only update line but add is upgrade/downgrade and new 
 
inner join gms.s_prod_int p 
on oi.prod_id = p.row_id 
and p.sub_type_cd in ('Non-RSA','Add-on','RSA') 
and p.prod_cd = 'Promotion' 
 
inner join gms.s_contact c 
on o.contact_id = c.row_id 
 
 
where p.name in ('PC_IAG_Bundle_RSA_Offer_0319','CC_IAG_Bundle_RSA_Offer_0319') 
and c.CON_CD in('Ordinary Member' , 'Affiliate Member') 
--and o.status_cd ='Cancelled' 
--NVL(o.X_RENEWAL_RELATED, 'N') = 'Y' 
--or ot.name = 'Renew' 
--and o.status_cd != 'Revised' 
select    --count(distinct  
        count(c.row_id) ContactID 
       , ot.name 
        , count (distinct o.order_num) OrderNumber 
       -- , count 
        --,(oi.order_id) as OrderLineItemID 
        --, ot.name OrderType 
        --, p.name Product 
        --, oi.ACTION_CD as Action  
 
from gms.s_order o 
 
inner join gms.s_order_type ot 
on o.order_type_id = ot.row_id 
 
inner join gms.s_order_item oi 
on o.row_id = oi.order_id 
--and oi.ACTION_CD in ('Update','Add') --when they renew only update line but add is upgrade/downgrade and new 
 
inner join gms.s_prod_int p 
on oi.prod_id = p.row_id 
and p.sub_type_cd in ('Non-RSA','Add-on','RSA') 
and p.prod_cd = 'Promotion' 
 
inner join gms.s_contact c 
on o.contact_id = c.row_id 
 
 
where p.name in ('PC_IAG_Bundle_RSA_Offer_0319','CC_IAG_Bundle_RSA_Offer_0319') 
--and c.CON_CD in('Ordinary Member' , 'Affiliate Member') 
--and o.status_cd ='Cancelled' 
--NVL(o.X_RENEWAL_RELATED, 'N') = 'Y' 
--or ot.name = 'Renew' 
--and ot.name = 'New' 
group by ot.name 
--order by 2 desc 