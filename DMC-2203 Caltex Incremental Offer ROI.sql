select distinct (case when partner in ('IAG','NRMA Multi Policy Discount','NRMA MPD Insurance','NRMA Insurance') then 'IAG'  
 
when partner LIKE 'NRMA Holiday%' then 'NRMA Parks and Resorts'  
 
when partner LIKE 'HP%' then 'NRMA Parks and Resorts'  
 
when partner LIKE 'BIG4%' then 'NRMA Parks and Resorts'  
 
when partner LIKE 'NRMA Parks and Resorts%' then 'NRMA Parks and Resorts'  
 
when partner LIKE 'NRMA Bowen Beachfront Holiday Park' then 'NRMA Parks and Resorts'  
 
else partner end) as partner from m4m.return_feed_header 
where to_date(time_stamp) between '2018-05-01' and '2020-06-01' 
order by 1 
select count(distinct trx_header_id) from m4m.return_feed_header 
where to_date(time_stamp) between '2019-05-01' and '2019-06-04' 
and partner='Caltex' 
with first_red as ( 
 
select  member_number, min(to_date(time_stamp)) as first_redemption  
from m4m.return_feed_header  
where partner not in ('IAG','NRMA Multi Policy Discount','NRMA MPD Insurance','NRMA Insurance')  
group by 1 
) 
 
select count(distinct m.member_number), count(m.trx_header_id)  
 
from first_red r 
 
inner join m4m.return_feed_header m 
on m.member_number=r.member_number 
and m.partner='Caltex' 
 
where first_redemption between '2019-05-01' and '2019-06-04' 
--and to_date(m.time_stamp) between '2019-05-01' and '2019-06-04' 
--and m.partner='Caltex' 
with first_red as ( 
 
select  member_number, min(time_stamp) as first_redemption  
from m4m.return_feed_header  
where partner not in ('IAG','NRMA Multi Policy Discount','NRMA MPD Insurance','NRMA Insurance')  
group by 1 
) 
 
select  CUST_VALUE_CD , count(distinct csn) from gms.s_contact c 
 
inner join gms.s_contact_x cx 
on cx.par_row_id=c.row_id 
 
inner join m4m.return_feed_header m 
on m.member_number=c.csn 
 
inner join first_red r 
on r.member_number=c.csn 
 
-- inner join sandpit.renewal_base b 
-- on b.membernumber=c.csn 
 
where to_date(m.time_stamp) between '2019-05-01' and '2019-06-04' 
and m.partner='Caltex' 
--and c.con_cd='Ordinary Member' 
 
group by 1 
order by 1 
select CUST_VALUE_CD ,count(distinct 
         c.csn) as member_number 
  
 
    from gms.s_contact as c  
     
	inner join gms.s_contact_x cx 
    on cx.par_row_id=c.row_id 
	 
	inner join gms.s_asset as a  
	on a.owner_accnt_id = c.pr_dept_ou_id 
 
	inner join gms.s_prod_int as p 
       on a.prod_id = p.row_id 
        
    inner join gms.s_contact_fnx as fn  
       on fn.par_row_id = c.row_id  
     
    where a.status_cd = 'Active' 
    and p.type = 'Membership' 
    and p.prod_cd = 'Promotion' 
    and NVL(fn.deceased_flg,'N') = 'N' 
    and c.csn is not null 
     
    group by 1 
    order by 1 
 
with first_red as ( 
 
select  member_number, min(time_stamp) as first_redemption  
from m4m.return_feed_header  
where m.partner not in ('IAG','NRMA Multi Policy Discount','NRMA MPD Insurance','NRMA Insurance','NRMA Insurance ControlPro','NRMA Insurance Branch')  
group by 1 
), caltex as ( 
select member_number, trx_header_id, time_stamp,partner from  m4m.return_feed_header  
where to_date(time_stamp) between '2019-05-01' and '2019-06-04' 
and partner='Caltex' 
) 
 
select  case when m.partner in ('IAG','NRMA Multi Policy Discount','NRMA MPD Insurance','NRMA Insurance') then 'IAG' 
                when m.partner LIKE 'NRMA Holiday%' then 'NRMA Parks and Resorts'  
                when m.partner LIKE 'HP%' then 'NRMA Parks and Resorts'  
                when m.partner LIKE 'BIG4%' then 'NRMA Parks and Resorts'  
                when m.partner LIKE 'NRMA Parks and Resorts%' then 'NRMA Parks and Resorts'  
                when m.partner LIKE 'NRMA Bowen Beachfront Holiday Park' then 'NRMA Parks and Resorts'  
                else m.partner end as Blue_benefit  
        , count(distinct csn)  
from caltex cal 
 
inner join gms.s_contact c 
on c.csn=cal.member_number 
 
inner join m4m.return_feed_header m 
on m.member_number=c.csn 
 
where 1=1 
and m.partner not in ('IAG','NRMA Multi Policy Discount','NRMA MPD Insurance','NRMA Insurance','NRMA Insurance ControlPro','NRMA Insurance Branch')  
 
group by 1 
order by 2 desc 
 
with first_red as ( 
select  member_number, min(time_stamp) as first_redemption  
from m4m.return_feed_header  
where partner not in ('IAG','NRMA Multi Policy Discount','NRMA MPD Insurance','NRMA Insurance','NRMA Insurance ControlPro','NRMA Insurance Branch')  
group by 1 
 
), caltex as ( 
select member_number, trx_header_id, time_stamp,partner from  m4m.return_feed_header  
where to_date(time_stamp) between '2019-05-01' and '2019-06-04' 
and partner='Caltex' 
) 
 
select count(distinct m.trx_header_id) 
from caltex cal 
 
inner join gms.s_contact c 
on c.csn=cal.member_number 
 
left join m4m.return_feed_header m 
on m.member_number=c.csn 
and to_date(m.time_stamp) between '2019-05-01' and '2020-04-30' 
and m.partner not in ('IAG','NRMA Multi Policy Discount','NRMA MPD Insurance','NRMA Insurance','NRMA Insurance ControlPro','NRMA Insurance Branch')  
 
where 1=1 
--and m.partner='Caltex' 
 
 
-- group by 1 
-- order by 2 desc 
with first_red as ( 
select  member_number, to_date(min(time_stamp)) as first_redemption  
from m4m.return_feed_header  
where partner not in ('IAG','NRMA Multi Policy Discount','NRMA MPD Insurance','NRMA Insurance','NRMA Insurance ControlPro','NRMA Insurance Branch')  
group by 1 
 
), caltex as ( 
select member_number, trx_header_id, to_date(time_stamp) as cal_time_stamp,partner from  m4m.return_feed_header  
where to_date(time_stamp) between '2019-05-01' and '2019-06-04' 
and partner='Caltex' 
) 
 
select count(distinct b.contact_id) 
        , sum(b.item_base_price) 
from sandpit.renewal_base b 
 
inner join first_red r 
on r.member_number=b.membernumber 
and r.first_redemption between '2019-05-01' and '2019-06-04' 
 
inner join caltex cal 
on b.membernumber=cal.member_number 
 
-- left join m4m.return_feed_header m 
-- on m.member_number=c.csn 
-- and to_date(m.time_stamp) between '2018-05-01' and '2019-04-30' 
-- and m.partner not in ('IAG','NRMA Multi Policy Discount','NRMA MPD Insurance','NRMA Insurance','NRMA Insurance ControlPro','NRMA Insurance Branch')  
 
where 1=1 
and to_date(date_add(order_end_dt,2)) between '2019-05-01' and '2020-04-30' 
and b.contact_cd in ('Affiliate Member','Ordinary Member') 
and renewed_order_id is not null 
--and r.first_redemption between '2019-05-01' and '2019-06-04' 
--and m.partner='Caltex' 
 
 
-- group by 1,2 
-- order by 1,2 
with first_red as ( 
select  member_number, to_date(min(time_stamp)) as first_redemption  
from m4m.return_feed_header  
where partner not in ('IAG','NRMA Multi Policy Discount','NRMA MPD Insurance','NRMA Insurance','NRMA Insurance ControlPro','NRMA Insurance Branch')  
group by 1 
 
), caltex as ( 
select member_number, trx_header_id, to_date(time_stamp) as cal_time_stamp,partner from  m4m.return_feed_header  
where to_date(time_stamp) between '2019-05-01' and '2019-06-04' 
and partner='Caltex' 
) 
 
select count(distinct b.contact_id) 
        , sum(b.item_base_price) 
from sandpit.renewal_base b 
 
inner join first_red r 
on r.member_number=b.membernumber 
and r.first_redemption between '2019-05-01' and '2019-06-04' 
 
inner join caltex cal 
on b.membernumber=cal.member_number 
 
-- left join m4m.return_feed_header m 
-- on m.member_number=c.csn 
-- and to_date(m.time_stamp) between '2018-05-01' and '2019-04-30' 
-- and m.partner not in ('IAG','NRMA Multi Policy Discount','NRMA MPD Insurance','NRMA Insurance','NRMA Insurance ControlPro','NRMA Insurance Branch')  
 
where 1=1 
and to_date(date_add(order_end_dt,2)) between '2019-05-01' and '2020-04-30' 
and b.contact_cd in ('Affiliate Member','Ordinary Member') 
and renewed_order_id is not null 
--and r.first_redemption between '2019-05-01' and '2019-06-04' 
--and m.partner='Caltex' 
 
 
-- group by 1,2 
-- order by 1,2 
select avg(vol) 
from ( 
with caltex as ( 
select member_number, trx_header_id, to_date(time_stamp) as cal_time_stamp,partner from  m4m.return_feed_header  
where to_date(time_stamp) between '2019-05-01' and '2019-06-04' 
and partner='Caltex' 
) 
 
select b.contact_id, b.membernumber, count(*) as vol 
--, count(distinct b.asset_id) 
from sandpit.renewal_base b 
 
inner join caltex cal 
on b.membernumber=cal.member_number 
 
-- left join m4m.return_feed_header m 
-- on m.member_number=c.csn 
-- and to_date(m.time_stamp) between '2018-05-01' and '2019-04-30' 
-- and m.partner not in ('IAG','NRMA Multi Policy Discount','NRMA MPD Insurance','NRMA Insurance','NRMA Insurance ControlPro','NRMA Insurance Branch')  
 
where 1=1 
and to_date(date_add(order_end_dt,2)) between '2019-05-01' and '2020-04-30' 
and b.contact_cd in ('Affiliate Member','Ordinary Member') 
--and b.contact_id='1-HPF-136' 
--and r.first_redemption between '2019-05-01' and '2019-06-04' 
--and m.partner='Caltex' 
group by 1,2 
order by 3 desc 
)aa 
--group by 1 
with caltex as ( 
select member_number, trx_header_id, to_date(time_stamp) as cal_time_stamp,partner from  m4m.return_feed_header  
where to_date(time_stamp) between '2019-05-01' and '2019-06-04' 
and partner='Caltex' 
) 
 
select b.* 
--, count(distinct b.asset_id) 
from sandpit.renewal_base b 
 
inner join caltex cal 
on b.membernumber=cal.member_number 
 
-- left join m4m.return_feed_header m 
-- on m.member_number=c.csn 
-- and to_date(m.time_stamp) between '2018-05-01' and '2019-04-30' 
-- and m.partner not in ('IAG','NRMA Multi Policy Discount','NRMA MPD Insurance','NRMA Insurance','NRMA Insurance ControlPro','NRMA Insurance Branch')  
 
where 1=1 
and to_date(date_add(order_end_dt,2)) between '2019-05-01' and '2020-04-30' 
and b.contact_cd in ('Affiliate Member','Ordinary Member') 
--and b.contact_id='1-HPF-136' 
--and r.first_redemption between '2019-05-01' and '2019-06-04' 
--and m.partner='Caltex' 
-- group by 1 
-- order by 2 desc 
 
with caltex as ( 
select distinct member_number from  m4m.return_feed_header  
where to_date(time_stamp) between '2019-05-01' and '2019-06-04' 
and partner='Caltex' 
) 
 
select case when cal.member_number is null then 'N' else 'Y' end Caltex 
        , case when renewed_order_id is null then 'Non-renewed' else 'Renewed' end renewal  
        , count(distinct b.asset_row_id) 
        , count(*) 
        , sum(b.item_base_price) 
from sandpit.renewal_base b 
 
left join caltex cal 
on b.membernumber=cal.member_number 
 
-- left join m4m.return_feed_header m 
-- on m.member_number=c.csn 
-- and to_date(m.time_stamp) between '2018-05-01' and '2019-04-30' 
-- and m.partner not in ('IAG','NRMA Multi Policy Discount','NRMA MPD Insurance','NRMA Insurance','NRMA Insurance ControlPro','NRMA Insurance Branch')  
 
where 1=1 
and to_date(date_add(order_end_dt,2)) between '2019-05-01' and '2020-04-30' 
and b.contact_cd in ('Affiliate Member','Ordinary Member') 
--and r.first_redemption between '2019-05-01' and '2019-06-04' 
--and m.partner='Caltex' 
 
 
group by 1,2 
order by 1,2 
 
 
with previous as ( 
select member_number,max(time_stamp) as last_use  
 
from m4m.return_feed_header 
where 1=1 
and partner='Caltex' 
and to_date(time_stamp)<'2019-05-01' 
group by 1 
 
),caltex as ( 
select member_number, time_stamp, partner from  m4m.return_feed_header  
where to_date(time_stamp) between '2019-05-01' and '2019-06-04' 
and partner='Caltex' 
 
), freq as( 
select member_number, count(distinct trx_header_id) as frequency from m4m.return_feed_header 
where partner='Caltex' 
and to_date(time_stamp) between '2018-05-01' and '2019-05-01' 
group by 1 
) 
select case when frequency>1.81 then 'Y' 
        else 'N' end Frequent 
        ,count(distinct cal.member_number)  
from caltex cal 
 
inner join previous p 
on p.member_number=cal.member_number 
 
left join freq f 
on f.member_number=cal.member_number 
 
where 1=1 
--and m.partner='Caltex' 
 
--and to_date(m.time_stamp)<'2019-05-01' 
--and to_date(cal.time_stamp) 
 
 
group by 1 
order by 1 
with freq as( 
select member_number, count(distinct trx_header_id) as frequency from m4m.return_feed_header 
where partner='Caltex' 
and year(time_stamp)=2019 
--between '2018-06-01' and '2019-04-30' 
 
group by 1 
order by 1 
) 
select frequency, count(distinct member_number) from freq 
group by 1 
 
 
with first_red as ( 
select  member_number, to_date(min(time_stamp)) as first_redemption  
from m4m.return_feed_header  
where partner not in ('IAG','NRMA Multi Policy Discount','NRMA MPD Insurance','NRMA Insurance','NRMA Insurance ControlPro','NRMA Insurance Branch')  
group by 1 
 
), previous as ( 
select member_number,time_stamp from m4m.return_feed_header 
where 1=1 
and partner='Caltex' 
and to_date(time_stamp)<'2019-05-01' 
),caltex as ( 
select member_number, time_stamp, partner from  m4m.return_feed_header  
where to_date(time_stamp) between '2019-05-01' and '2019-06-04' 
and partner='Caltex' 
) 
 
select case when to_date(first_redemption) between '2019-05-01' and '2019-06-04' then 'Y' else 'N' end New_blue 
        , count(distinct m.member_number) 
from caltex cal 
 
inner join m4m.return_feed_header m 
on m.member_number=cal.member_number 
 
left anti join previous p 
on p.member_number=cal.member_number 
 
inner join first_red f 
on f.member_number=m.member_number 
 
where 1=1 
and m.partner='Caltex' 
--and to_date(m.time_stamp)<'2019-05-01' 
--and to_date(cal.time_stamp) 
 
 
group by 1 
order by 1 
with caltex as ( 
select member_number, trx_header_id, time_stamp,partner from  m4m.return_feed_header  
where to_date(time_stamp) between '2019-05-01' and '2019-06-04' 
and partner='Caltex' 
) 
 
SELECT case when upper(m.partner) like '%NRMA%' or lower(m.partner) like 'thrifty' or m.partner in ('Fantasea','Mobile Vehicle Inspections','My Fast Ferry') then 'GB' 
        else 'Other' end Blue_benefit 
        , count(distinct m.member_number) 
        , sum(m.total_amount) 
        --, count(distinct m.member_number) 
from m4m.return_feed_header m 
 
inner join caltex cal 
on cal.member_number=m.member_number 
 
where 1=1 
and m.partner not in ('IAG','NRMA Multi Policy Discount','NRMA MPD Insurance','NRMA Insurance','NRMA Insurance ControlPro','NRMA Insurance Branch') 
and to_date(m.time_stamp) between '2018-05-01' and '2019-04-30' 
 
group by case when upper(m.partner) like '%NRMA%' or lower(m.partner) like 'thrifty' or m.partner in ('Fantasea','Mobile Vehicle Inspections','My Fast Ferry') then 'GB' 
        else 'Other' end 
order by 1 
with caltex as ( 
select member_number, trx_header_id, time_stamp,partner from  m4m.return_feed_header  
where to_date(time_stamp) between '2019-05-01' and '2019-06-04' 
and partner='Caltex' 
) 
 
SELECT case when upper(m.partner) like '%NRMA%' or lower(m.partner) like 'thrifty' or m.partner in ('Fantasea','Mobile Vehicle Inspections','My Fast Ferry') then 'GB' 
        else 'Other' end Blue_benefit 
        , count(distinct m.member_number) 
        , sum(m.total_amount) 
from m4m.return_feed_header m 
 
inner join caltex cal 
on cal.member_number=m.member_number 
 
where 1=1 
and m.partner not in ('IAG','NRMA Multi Policy Discount','NRMA MPD Insurance','NRMA Insurance','NRMA Insurance ControlPro','NRMA Insurance Branch') 
and to_date(m.time_stamp) between '2019-05-01' and '2020-04-30' 
 
group by 1 
order by 1 
 
with caltex as ( 
select member_number, trx_header_id, time_stamp,partner from  m4m.return_feed_header  
where to_date(time_stamp) between '2019-05-01' and '2019-06-04' 
and partner='Caltex' 
) 
 
SELECT count(distinct m.partner) 
       -- , count(distinct m.member_number) 
       -- , sum(m.total_amount) 
from m4m.return_feed_header m 
 
inner join caltex cal 
on cal.member_number=m.member_number 
 
where 1=1 
and m.partner not in ('IAG','NRMA Multi Policy Discount','NRMA MPD Insurance','NRMA Insurance','NRMA Insurance ControlPro','NRMA Insurance Branch') 
and to_date(m.time_stamp) between '2019-05-01' and '2020-04-30' 
and (upper(m.partner) like '%NRMA%' or lower(m.partner) like 'thrifty' or m.partner in ('Fantasea','Mobile Vehicle Inspections','My Fast Ferry')) 
 
-- group by 1 
-- order by 1 
 
with caltex as ( 
select member_number, trx_header_id, time_stamp,partner from  m4m.return_feed_header  
where to_date(time_stamp) between '2019-05-01' and '2019-06-04' 
and partner='Caltex' 
) 
 
SELECT --count(m.trx_header_id) 
        count(distinct m.partner) 
       -- , sum(m.total_amount) 
from m4m.return_feed_header m 
 
inner join caltex cal 
on cal.member_number=m.member_number 
 
where 1=1 
and m.partner not in ('IAG','NRMA Multi Policy Discount','NRMA MPD Insurance','NRMA Insurance','NRMA Insurance ControlPro','NRMA Insurance Branch') 
and to_date(m.time_stamp) between '2018-05-01' and '2019-04-30' 
and (upper(m.partner) like '%NRMA%' or lower(m.partner) like 'thrifty' or m.partner in ('Fantasea','Mobile Vehicle Inspections','My Fast Ferry')) 
 
-- group by 1 
-- order by 1 
 
with caltex as ( 
select member_number, trx_header_id, time_stamp,partner from  m4m.return_feed_header  
where to_date(time_stamp) between '2019-05-01' and '2019-06-04' 
and partner='Caltex' 
) 
 
SELECT min(e.send_event_date) from omc.send_level_summary e 
inner join caltex c 
on c.member_number=e.member_number 
where 1=1 
and e.bounce_event_date is null 
and e.click_event_date is not null 
and e.send_event_date between '2018-05-01' and '2019-04-30' 
with caltex as ( 
select member_number, trx_header_id, time_stamp,partner from  m4m.return_feed_header  
where to_date(time_stamp) between '2019-05-01' and '2019-06-04' 
and partner='Caltex' 
) 
 
SELECT count(distinct c.member_number) from omc.send_level_summary e 
inner join caltex c 
on c.member_number=e.member_number 
where 1=1 
and e.bounce_event_date is null 
and e.click_event_date is not null 
and e.send_event_date between '2019-05-01' and '2020-04-30' 
SELECT year(time_stamp),month(time_stamp),count(DISTINCT trx_header_id) from m4m.return_feed_header 
where partner='Caltex' 
and to_date(time_stamp) between '2018-05-01' and '2020-04-30' 
GROUP BY 1,2 
ORDER BY 1,2 
SELECT year(time_stamp),month(time_stamp),count(DISTINCT member_number) from m4m.return_feed_header 
where partner='Caltex' 
and to_date(time_stamp) between '2018-05-01' and '2020-04-30' 
GROUP BY 1,2 
ORDER BY 1,2 
select count(distinct m.member_number) 
--count(distinct m.trx_header_id) 
         
from m4m.return_feed_header m 
 
where 1=1 
and to_date(m.time_stamp) between '2018-05-01' and '2019-04-30' 
and m.partner not in ('IAG','NRMA Multi Policy Discount','NRMA MPD Insurance','NRMA Insurance','NRMA Insurance ControlPro','NRMA Insurance Branch')  
and m.partner='Caltex' 
select count(distinct m.member_number) 
--count(distinct m.trx_header_id) 
         
from m4m.return_feed_header m 
 
where 1=1 
and to_date(m.time_stamp) between '2019-05-01' and '2020-04-30' 
and m.partner not in ('IAG','NRMA Multi Policy Discount','NRMA MPD Insurance','NRMA Insurance','NRMA Insurance ControlPro','NRMA Insurance Branch')  
and m.partner='Caltex' 
with first_red as ( 
select  member_number, to_date(min(time_stamp)) as first_redemption  
from m4m.return_feed_header  
where partner not in ('IAG','NRMA Multi Policy Discount','NRMA MPD Insurance','NRMA Insurance','NRMA Insurance ControlPro','NRMA Insurance Branch')  
group by 1 
), Caltex as ( 
select distinct member_number, to_date(time_stamp) as cal_time_stamp  
from m4m.return_feed_header  
where partner='Caltex' 
) 
 
SELECT  year(member_join_dt),month(member_join_dt), count(DISTINCT contact_id)  
FROM sandpit.renewal_base  r 
 
inner join first_red f 
on f.member_number=r.membernumber 
 
inner join Caltex c 
on c.cal_time_stamp=f.first_redemption 
 
where to_date(member_join_dt) between '2018-05-01' and '2020-04-30' 
and contact_cd in ('Affiliate Member','Ordinary Member') 
and order_type='New' 
and asset_status_cd ='Active' 
and membernumber is not null 
and datediff(first_redemption,r.member_join_dt) between 0 and 7 
GROUP BY 1,2 
ORDER BY 1,2 
with first_red as ( 
select  member_number, to_date(min(time_stamp)) as first_redemption  
from m4m.return_feed_header  
where partner not in ('IAG','NRMA Multi Policy Discount','NRMA MPD Insurance','NRMA Insurance','NRMA Insurance ControlPro','NRMA Insurance Branch')  
group by 1 
), Caltex as ( 
select distinct member_number, to_date(time_stamp) as cal_time_stamp  
from m4m.return_feed_header  
where partner='Caltex' 
) 
 
SELECT  year(cal_time_stamp),month(cal_time_stamp), count(DISTINCT c.member_number)  
 
FROM Caltex c 
 
inner join first_red f 
on c.cal_time_stamp=f.first_redemption 
 
 
where to_date(c.cal_time_stamp) between '2018-05-01' and '2020-04-30' 
-- and contact_cd in ('Affiliate Member','Ordinary Member') 
-- and order_type='New' 
-- and asset_status_cd ='Active' 
-- and membernumber is not null 
-- and datediff(first_redemption,r.member_join_dt) between 0 and 7 
GROUP BY 1,2 
ORDER BY 1,2 
with first_red as ( 
select  member_number, to_date(min(time_stamp)) as first_redemption  
from m4m.return_feed_header  
where partner not in ('IAG','NRMA Multi Policy Discount','NRMA MPD Insurance','NRMA Insurance','NRMA Insurance ControlPro','NRMA Insurance Branch')  
group by 1 
) 
 
SELECT  year(first_redemption),month(first_redemption), count(DISTINCT member_number)  
 
FROM first_red  
 
 
where to_date(first_redemption) between '2018-05-01' and '2020-04-30' 
 
GROUP BY 1,2 
ORDER BY 1,2 
with first_Time as( 
select member_number, min(time_stamp) as first_redemption  
from m4m.return_feed_header 
where partner not in ('IAG','NRMA Multi Policy Discount','NRMA MPD Insurance','NRMA Insurance','NRMA Insurance ControlPro','NRMA Insurance Branch') 
and partner='Caltex' 
group by 1 
) 
select year(first_redemption),month(first_redemption), count(DISTINCT member_number) 
from first_Time 
group by 1,2 
order by 1,2 
 
 
SELECT  year(time_stamp),month(time_stamp), count(DISTINCT member_number)  
 
FROM m4m.return_feed_header  
 
 
where to_date(time_stamp) between '2018-05-01' and '2020-04-30' 
and partner='Caltex' 
 
GROUP BY 1,2 
ORDER BY 1,2 
with Caltex as ( 
select distinct member_number, to_date(time_stamp) as cal_time_stamp  
from m4m.return_feed_header  
where partner='Caltex' 
) 
SELECT  year(time_stamp),month(time_stamp), count(DISTINCT member_number)  
 
FROM m4m.return_feed_header  m 
 
left anti join Caltex c 
on m.member_number=c.member_number 
 
 
where to_date(time_stamp) between '2018-05-01' and '2020-04-30' 
and partner not in ('IAG','NRMA Multi Policy Discount','NRMA MPD Insurance','NRMA Insurance','NRMA Insurance ControlPro','NRMA Insurance Branch') 
 
GROUP BY 1,2 
ORDER BY 1,2 