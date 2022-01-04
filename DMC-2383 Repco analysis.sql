spark.sql(''' 
SELECT case when h.time_stamp<'2017-06-30' then 'FY17' 
            when h.time_stamp<'2018-06-30' then 'FY18' 
            when h.time_stamp<'2019-06-30' then 'FY19' 
            when h.time_stamp<'2020-06-30' then 'FY20' 
            WHEN h.time_stamp<'2021-06-30' then 'FY21' 
            else 'other' end FY_Jul_Apr 
            , count(DISTINCT h.trx_header_id) Transactions 
            , count(distinct h.member_number) Unique_Members 
            , sum(h.total_amount) Total_Amount 
            , sum(discount) Total_Savings 
 
from m4m.return_feed_header h 
 
where h.partner='Repco' 
and h.time_stamp BETWEEN '2016-07-01' and '2021-06-30' 
and h.time_stamp<'2021-05-01' 
and month(h.time_stamp) not in (5,6) 
 
GROUP BY 1 
ORDER BY 1 
''').show(10000,False) 
SELECT * 
-- case when h.time_stamp<'2017-06-30' then 'FY17' 
--             when h.time_stamp<'2018-06-30' then 'FY18' 
--             when h.time_stamp<'2019-06-30' then 'FY19' 
--             when h.time_stamp<'2020-06-30' then 'FY20' 
--             WHEN h.time_stamp<'2021-06-30' then 'FY21' 
--             else 'other' end FY 
--             , count(DISTINCT h.trx_header_id) 
--             --, count(h.member_number) 
--             , sum(h.total_amount) 
 
from m4m.return_feed_header h 
 
INNER JOIN m4m.return_feed_detail d 
on h.trx_header_id=d.trx_header_id 
 
where h.partner='Repco' 
and h.time_stamp BETWEEN '2016-07-01' and '2021-06-30' 
and h.time_stamp<'2021-05-01' 
and h.member_number='518593101' 
 
limit 1000; 
 
-- GROUP BY 1 
-- ORDER BY 1 
spark.sql(''' 
SELECT case when h.time_stamp<'2017-06-30' then 'FY17' 
            when h.time_stamp<'2018-06-30' then 'FY18' 
            when h.time_stamp<'2019-06-30' then 'FY19' 
            when h.time_stamp<'2020-06-30' then 'FY20' 
            WHEN h.time_stamp<'2021-06-30' then 'FY21' 
            else 'other' end FY 
            , count(DISTINCT h.trx_header_id) 
            , count(distinct h.member_number) 
            , sum(h.total_amount) 
            , sum(item_price*quantity) 
            , sum(discount) 
 
from m4m.return_feed_header h 
 
INNER JOIN m4m.return_feed_detail d 
on h.trx_header_id=d.trx_header_id 
 
where h.partner='Repco' 
and h.time_stamp BETWEEN '2016-07-01' and '2021-06-30' 
and h.time_stamp<'2021-05-01' 
 
GROUP BY 1 
ORDER BY 1 
''').show(10000,False) 
spark.sql(''' 
SELECT case when h.time_stamp<'2017-06-30' then 'FY17' 
            when h.time_stamp<'2018-06-30' then 'FY18' 
            when h.time_stamp<'2019-06-30' then 'FY19' 
            when h.time_stamp<'2020-06-30' then 'FY20' 
            WHEN h.time_stamp<'2021-06-30' then 'FY21' 
            else 'other' end FY_Jul_Apr 
            , count(DISTINCT h.trx_header_id) Transactions 
            , count(distinct h.member_number) Unique_Members 
            , sum(h.total_amount) Total_Amount 
            , sum(discount) Total_Savings 
 
from m4m.return_feed_header h 
 
where h.partner='Repco' 
and h.time_stamp BETWEEN '2016-07-01' and '2021-06-30' 
and h.time_stamp<'2021-05-01' 
and month(h.time_stamp) not in (4,5,6) 
 
GROUP BY 1 
ORDER BY 1 
''').show(10000,False) 
spark.sql(''' 
SELECT case when h.time_stamp<'2017-06-30' then 'FY17' 
            when h.time_stamp<'2018-06-30' then 'FY18' 
            when h.time_stamp<'2019-06-30' then 'FY19' 
            when h.time_stamp<'2020-06-30' then 'FY20' 
            WHEN h.time_stamp<'2021-06-30' then 'FY21' 
            else 'other' end FY_Jul_Apr 
            , count(DISTINCT h.trx_header_id) Transactions 
            , count(distinct h.member_number) Unique_Members 
            , sum(h.total_amount) Total_Amount 
            , sum(item_price*quantity) Total_Price 
            , sum(discount) Total_Savings 
 
from m4m.return_feed_header h 
 
INNER JOIN m4m.return_feed_detail d 
on h.trx_header_id=d.trx_header_id 
 
where h.partner='Repco' 
and h.time_stamp BETWEEN '2016-07-01' and '2021-06-30' 
and h.time_stamp<'2021-05-01' 
and month(h.time_stamp) not in (5,6) 
 
GROUP BY 1 
ORDER BY 1 
''').show(10000,False) 
spark.sql(''' 
with first_Repco as ( 
SELECT h.member_number 
            , min(h.time_stamp) as first_trans 
 
from m4m.return_feed_header h 
 
where h.partner='Repco' 
 
GROUP BY 1 
) 
select case when first_trans<'2017-06-30' then 'FY17' 
            when first_trans<'2018-06-30' then 'FY18' 
            when first_trans<'2019-06-30' then 'FY19' 
            when first_trans<'2020-06-30' then 'FY20' 
            WHEN first_trans<'2021-06-30' then 'FY21' 
            else 'other' end FY_Jul_Apr 
        , count(distinct member_number) 
 
from first_Repco 
 
where first_trans BETWEEN '2016-07-01' and '2021-06-30' 
and first_trans<'2021-05-01' 
and month(first_trans) not in (5,6) 
 
group by 1 
order by 1 
''').show(10000,False) 
with first_Repco as ( 
SELECT distinct h.member_number, h.trx_header_id 
            , h.time_stamp as first_trans 
 
from m4m.return_feed_header h 
 
where h.partner='Repco' 
) 
select year(first_trans), month(first_trans) 
        , count(distinct trx_header_id) 
 
from first_Repco 
 
where 1=1 
and first_trans BETWEEN '2019-05-01' and '2021-06-30' 
and first_trans<'2021-05-01' 
--and month(first_trans) not in (5,6) 
 
group by 1,2 
order by 1,2 
with first_Repco as ( 
SELECT h.member_number 
            , min(h.time_stamp) as first_trans 
 
from m4m.return_feed_header h 
 
where h.partner='Repco' 
 
GROUP BY 1 
) 
select year(first_trans), month(first_trans) 
        , count(distinct member_number) 
 
from first_Repco 
 
where first_trans BETWEEN '2019-05-01' and '2021-06-30' 
and first_trans<'2021-05-01' 
--and month(first_trans) not in (5,6) 
 
group by 1,2 
order by 1,2 
with Repco as ( 
SELECT count(distinct h.member_number) 
 
from m4m.return_feed_header h 
 
where h.partner='Repco' 
and h.time_stamp BETWEEN '2020-05-01' and '2021-05-01' 
) 
SELECT  case when partner in ('IAG','NRMA Multi Policy Discount','NRMA MPD Insurance','NRMA Insurance') then 'IAG'  
            when partner LIKE 'NRMA Holiday%' then 'NRMA Parks and Resorts'  
            when partner LIKE 'HP%' then 'NRMA Parks and Resorts'  
            when partner LIKE 'BIG4%' then 'NRMA Parks and Resorts'  
            when partner LIKE 'NRMA Parks and Resorts%' then 'NRMA Parks and Resorts'  
            when partner LIKE 'NRMA Bowen Beachfront Holiday Park' then 'NRMA Parks and Resorts'  
            else partner end as partner 
        , count(DISTINCT h.member_number) 
 
 
from m4m.return_feed_header h 
 
inner join Repco r 
on r.member_number=h.member_number 
 
INNER JOIN gms.s_contact c 
on c.csn=h.member_number 
 
inner join gms.s_contact_x as cx  
on c.row_id = cx.par_row_id 
 
where 1=1 
--h.partner!='Repco' 
and h.time_stamp BETWEEN '2020-05-01' and '2021-05-01' 
 
GROUP BY 1 
ORDER BY 2 desc 
with Repco as ( 
SELECT distinct h.member_number 
 
from m4m.return_feed_header h 
 
where h.partner='Repco' 
and h.time_stamp BETWEEN '2020-05-01' and '2021-05-01' 
) 
SELECT  case when partner in ('IAG','NRMA Multi Policy Discount','NRMA MPD Insurance','NRMA Insurance') then 'IAG'  
            when partner LIKE 'NRMA Holiday%' then 'NRMA Parks and Resorts'  
            when partner LIKE 'HP%' then 'NRMA Parks and Resorts'  
            when partner LIKE 'BIG4%' then 'NRMA Parks and Resorts'  
            when partner LIKE 'NRMA Parks and Resorts%' then 'NRMA Parks and Resorts'  
            when partner LIKE 'NRMA Bowen Beachfront Holiday Park' then 'NRMA Parks and Resorts'  
            else partner end as partner 
        , count(DISTINCT h.member_number) 
 
 
from m4m.return_feed_header h 
 
inner join Repco r 
on r.member_number=h.member_number 
 
INNER JOIN gms.s_contact c 
on c.csn=h.member_number 
 
inner join gms.s_contact_x as cx  
on c.row_id = cx.par_row_id 
 
where 1=1 
--h.partner!='Repco' 
and h.time_stamp BETWEEN '2020-05-01' and '2021-05-01' 
 
GROUP BY 1 
ORDER BY 2 desc 
SELECT  cx.attrib_55  AS Colour_Plus 
        --CUST_VALUE_CD as loyaltycolor 
        , count(DISTINCT h.member_number) 
        --, 
 
 
from m4m.return_feed_header h 
 
INNER JOIN gms.s_contact c 
on c.csn=h.member_number 
 
inner join gms.s_contact_x as cx  
on c.row_id = cx.par_row_id 
 
where h.partner='Repco' 
and h.time_stamp BETWEEN '2016-07-01' and '2021-05-01' 
 
GROUP BY 1 
ORDER BY 1 
select cx.attrib_55  AS Colour_Plus 
--c.cust_value_cd 
, count(distinct csn) 
 
    from gms.s_contact as c  
     
    inner join gms.s_contact_x as cx  
    on c.row_id = cx.par_row_id 
	 
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
SELECT year(h.time_stamp), month(h.time_stamp) 
            , count(distinct h.trx_header_id) 
 
from m4m.return_feed_header h 
 
where h.partner='Repco' 
--and first_trans BETWEEN '2016-07-01' and '2021-06-30' 
and h.time_stamp>'2019-06-30' 
and h.total_amount<300 
--between 500 and 1000 
 
group by 1,2 
order by 1,2 
spark.sql(''' 
SELECT min(h.total_amount), max(h.total_amount) 
 
from m4m.return_feed_header h 
 
where h.partner='Repco' 
--and first_trans BETWEEN '2016-07-01' and '2021-06-30' 
and h.time_stamp>'2019-06-30' 
and h.total_amount between 500 and 1000 
''').show() 
 
-- group by 1,2 
-- order by 1,2 
SELECT count(DISTINCT h.member_number) from m4m.return_feed_header h 
 
inner join gms.s_contact c 
on c.csn=h.member_number 
     
INNER JOIN gms.s_contact_x cx 
on cx.par_row_id=c.row_id 
 
INNER JOIN gms.s_contact_fnx fn 
on fn.par_row_id=c.row_id 
 
inner join gms.s_addr_per as ad         --813620 
on c.pr_per_addr_id = ad.row_id 
 
     
-- inner join omc.send_level_summary s 
-- on c.row_id=s.customer_id 
-- and s.channel='Push' 
-- and s.send_event_date>'2021-02-01' 
 
 
where  c.cust_stat_cd = 'Active'        --813576 
and c.con_cd in ('Ordinary Member' , 'Affiliate Member')        --813562 
and NVL(c.x_nrma_title,'no title') != 'Estate Of The Late' 
and NVL(fn.deceased_flg,'N') = 'N'                              --811922 
and c.csn is not null   
and case when trim(ad.premise_flg) = 'Y' or ad.addr_name is null then 'N' else 'Y' end = 'Y' --no return address --811257 
and nvl(cx.attrib_35,'Yes') <> 'No' --post permission   
-- and nvl(trim(c.x_inv_email_1),'N') <> 'Y' 
-- and c.email_addr is not null  
-- and nvl(cx.attrib_36,'Y') <> 'No' 
and partner='Repco' 
and time_stamp>'2019-06-30'