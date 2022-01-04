spark.sql(''' 
SELECT 
    COUNT(DISTINCT omc.riid), 
    COUNT(DISTINCT omc.customer_id) 
FROM 
    omc.send_level_summary AS omc 
     
WHERE 
    omc.program_id in ('55401522', '55412202') 
''').show(250, False) 
SELECT count(DISTINCT s.customer_id) from omc.send_level_summary s 
 
where s.program_id in ('55401522','55412202') 
 
--GROUP BY 1 
SELECT count(DISTINCT iag.contact_id) from omc.send_level_summary s 
 
INNER JOIN campaign_data.aa_dmc2226_hack_map_20210310 h 
on h.riid=s.riid 
 
INNER JOIN campaign_data.campaign_contact_adhoc_staging iag 
on iag.contact_id=h.customer_id 
where table_id='97' 
and s.program_id in ('55401522','55412202') 
with target as ( 
SELECT DISTINCT riid from omc.send_level_summary 
where program_id in ('55401522','55412202') 
and control is false 
) 
select case when c.cust_value_cd rlike 'Gold' then 'Gold and etc' 
            else c.cust_value_cd end loyalty_colour 
        , cx.attrib_55 as colour_plus 
        ,count(DISTINCT s.riid) from omc.send_level_summary s 
 
left anti join target t 
on t.riid=s.riid 
 
INNER JOIN campaign_data.aa_dmc2226_hack_map_20210310 h 
on h.riid=s.riid 
 
INNER JOIN gms.s_contact c 
on c.row_id=h.customer_id 
 
inner join gms.s_contact_x cx 
on c.row_id=cx.par_row_id 
 
-- INNER JOIN m4m.return_feed_header m 
-- on m.member_number=c.csn 
 
-- inner join m4m.return_feed_detail d 
-- on d.trx_header_id=m.trx_header_id 
 
-- inner join ft f 
-- on f.member_number=c.csn 
 
 
where s.program_id in ('55401522','55412202') 
-- and m.time_stamp>s.send_event_date 
-- and to_date(m.time_stamp)<='2021-02-14' 
-- and partner in ('IAG','NRMA Multi Policy Discount','NRMA MPD Insurance','NRMA Insurance') 
-- --and m.total_amount>0 
-- and d.item_description rlike 'MOT' 
and control is true 
 
group by 1,2 
order by 1,2 
SELECT case when c.cust_value_cd rlike 'Gold' then 'Gold and etc' 
            else c.cust_value_cd end loyalty_colour 
        , cx.attrib_55 as colour_plus,count(DISTINCT h.customer_id) from omc.send_level_summary s 
 
INNER JOIN campaign_data.aa_dmc2226_hack_map_20210310 h 
on h.riid=s.riid 
 
INNER JOIN gms.s_contact c 
on c.row_id=h.customer_id 
 
inner join gms.s_contact_x cx 
on c.row_id=cx.par_row_id 
 
-- INNER JOIN m4m.return_feed_header m 
-- on m.member_number=c.csn 
 
-- inner join m4m.return_feed_detail d 
-- on d.trx_header_id=m.trx_header_id 
 
-- inner join ft f 
-- on f.member_number=c.csn 
 
 
where s.program_id in ('55401522','55412202') 
-- and m.time_stamp>s.send_event_date 
-- and to_date(m.time_stamp)<='2021-02-14' 
-- and partner in ('IAG','NRMA Multi Policy Discount','NRMA MPD Insurance','NRMA Insurance') 
--and s.bounce_event_date is NULL 
--and d.item_description rlike 'MOT' 
and s.control is FALSE 
 
group by 1,2 
order by 1,2 
with target as ( 
SELECT DISTINCT riid from omc.send_level_summary 
where program_id in ('55401522','55412202') 
and control is false 
) 
select count(DISTINCT h.customer_id) from omc.send_level_summary s 
 
left anti join target t 
on t.riid=s.riid 
 
INNER JOIN campaign_data.aa_dmc2226_hack_map_20210310 h 
on h.riid=s.riid 
 
INNER JOIN gms.s_contact c 
on c.row_id=h.customer_id 
 
inner join gms.s_contact_x cx 
on c.row_id=cx.par_row_id 
 
INNER JOIN m4m.return_feed_header m 
on m.member_number=c.csn 
 
inner join m4m.return_feed_detail d 
on d.trx_header_id=m.trx_header_id 
 
-- inner join ft f 
-- on f.member_number=c.csn 
 
 
where s.program_id in ('55401522','55412202') 
and m.time_stamp>s.send_event_date 
and to_date(from_utc_timestamp(m.time_stamp,'AEST'))<='2021-02-14' 
and partner in ('IAG','NRMA Multi Policy Discount','NRMA MPD Insurance','NRMA Insurance') 
-- --and m.total_amount>0 
and d.item_description rlike 'MOT' 
and control is true 
with target as ( 
SELECT distinct m.member_number 
--,count(DISTINCT h.customer_id)  
from omc.send_level_summary s 
 
INNER JOIN campaign_data.aa_dmc2226_hack_map_20210310 h 
on h.riid=s.riid 
 
INNER JOIN gms.s_contact c 
on c.row_id=h.customer_id 
 
inner join gms.s_contact_x cx 
on c.row_id=cx.par_row_id 
 
INNER JOIN m4m.return_feed_header m 
on m.member_number=c.csn 
 
inner join m4m.return_feed_detail d 
on d.trx_header_id=m.trx_header_id 
 
-- inner join ft f 
-- on f.member_number=c.csn 
 
 
where s.program_id in ('55401522','55412202') 
and m.time_stamp>s.send_event_date 
and to_date(from_utc_timestamp(m.time_stamp,'AEST'))<='2021-02-14' 
and partner in ('IAG','NRMA Multi Policy Discount','NRMA MPD Insurance','NRMA Insurance') 
--and s.bounce_event_date is NULL 
and d.item_description rlike 'MOT' 
and s.control is FALSE 
 
-- GROUP BY 1 
-- having min(total_amount>0) 
) 
select count(*) from target 
SELECT s.control,count(DISTINCT s.customer_id) from omc.send_level_summary s 
 
INNER JOIN campaign_data.aa_dmc2226_hack_map_20210310 h 
on h.riid=s.riid 
 
where s.program_id in ('55401522','55412202') 
--and to_date(s.send_event_date)!='2021-01-17' 
--and s.bounce_event_date is null 
-- and (s.open_event_date is not null or s.click_event_date is not null) 
-- and s.control is FALSE 
--and s.click_event_date is not null 
 
GROUP BY 1 
ORDER BY 1 
with ft as ( 
SELECT  member_number, min(time_stamp) as first_trans from m4m.return_feed_header 
where partner in ('IAG','NRMA Multi Policy Discount','NRMA MPD Insurance','NRMA Insurance') 
group by 1 
) 
 
SELECT s.control 
        ,to_date(from_utc_timestamp(s.send_event_date,'AEST')) as send 
        ,count(DISTINCT s.customer_id) from omc.send_level_summary s 
 
INNER JOIN campaign_data.aa_dmc2226_hack_map_20210310 h 
on h.riid=s.riid 
 
INNER JOIN gms.s_contact c 
on c.row_id=h.customer_id 
 
INNER JOIN m4m.return_feed_header m 
on m.member_number=c.csn 
 
inner join m4m.return_feed_detail d 
on d.trx_header_id=m.trx_header_id 
 
-- inner join ft f 
-- on f.member_number=c.csn 
 
 
where s.program_id in ('55401522','55412202') 
--and f.first_trans>s.send_event_date 
and m.time_stamp>s.send_event_date 
and to_date(m.time_stamp)<='2021-02-14' 
--and to_date(f.first_trans)<='2021-02-14' 
and partner in ('IAG','NRMA Multi Policy Discount','NRMA MPD Insurance','NRMA Insurance') 
and (s.open_event_date is not null or s.click_event_date is not null) 
--and m.total_amount>0 
--and d.item_description rlike 'MOT' 
 
GROUP BY 1,2 
ORDER BY 1,2 
with ft as ( 
SELECT  member_number, min(time_stamp) as first_trans from m4m.return_feed_header 
where partner in ('IAG','NRMA Multi Policy Discount','NRMA MPD Insurance','NRMA Insurance') 
group by 1 
) 
 
SELECT s.control,count(DISTINCT h.customer_id) from omc.send_level_summary s 
 
INNER JOIN campaign_data.aa_dmc2226_hack_map_20210310 h 
on h.riid=s.riid 
 
INNER JOIN gms.s_contact c 
on c.row_id=h.customer_id 
 
INNER JOIN m4m.return_feed_header m 
on m.member_number=c.csn 
 
-- inner join ft f 
-- on f.member_number=c.csn 
 
where s.program_id in ('55401522','55412202') 
--and f.first_trans>s.send_event_date 
and m.time_stamp>s.send_event_date 
and to_date(m.time_stamp)<='2021-02-14' 
-- --and to_date(f.first_trans)<='2021-02-14' 
and partner in ('IAG','NRMA Multi Policy Discount','NRMA MPD Insurance','NRMA Insurance') 
 
 
GROUP BY 1 
ORDER BY 1 
with ft as ( 
SELECT  member_number, min(time_stamp) as first_trans from m4m.return_feed_header 
where partner in ('IAG','NRMA Multi Policy Discount','NRMA MPD Insurance','NRMA Insurance') 
group by 1 
) 
 
SELECT distinct f.member_number 
                , to_date(m.created_date) 
                , to_date(m.time_stamp) 
                , to_date(f.first_trans) 
                --, m.partner 
                , d.item_description 
                , m.total_amount 
                --, s.send_event_date 
                --, m.* 
 
from omc.send_level_summary s 
 
INNER JOIN campaign_data.aa_dmc2226_hack_map_20210310 h 
on h.riid=s.riid 
 
INNER JOIN gms.s_contact c 
on c.row_id=h.customer_id 
 
INNER JOIN m4m.return_feed_header m 
on m.member_number=c.csn 
 
inner join ft f 
on f.member_number=c.csn 
 
inner join m4m.return_feed_detail d 
on d.trx_header_id=m.trx_header_id 
 
 
where s.program_id in ('55401522','55412202') 
and f.first_trans>s.send_event_date 
--and m.time_stamp>s.send_event_date 
--and to_date(m.time_stamp)<='2021-02-14' 
and to_date(f.first_trans)<='2021-02-14' 
and partner in ('IAG','NRMA Multi Policy Discount','NRMA MPD Insurance','NRMA Insurance') 
and s.control is false 
--and f.member_number='458503601' 
 
 
-- GROUP BY 1 
ORDER BY 1 
spark.sql(''' 
with ft as ( 
SELECT  member_number, min(time_stamp) as first_trans from m4m.return_feed_header 
where partner in ('IAG','NRMA Multi Policy Discount','NRMA MPD Insurance','NRMA Insurance') 
group by 1 
) 
 
select year(m.first_trans),month(m.first_trans),count(distinct c.csn) as member_number 
 
    from gms.s_contact as c  
     
    inner join gms.s_contact_x cx 
    on cx.par_row_id=c.row_id 
 
	inner join gms.s_asset as a  
	on a.owner_accnt_id = c.pr_dept_ou_id 
 
	inner join gms.s_prod_int as p 
       on a.prod_id = p.row_id 
        
    inner join gms.s_contact_fnx as fn  
       on fn.par_row_id = c.row_id  
     
    left join ft as m 
    on m.member_number=c.csn 
     
    where a.status_cd = 'Active' 
    and p.type = 'Membership' 
    and p.prod_cd = 'Promotion' 
    and NVL(fn.deceased_flg,'N') = 'N' 
    and c.csn is not null 
    and c.cust_stat_cd='Active' 
    and c.con_cd in ('Ordinary Member' , 'Affiliate Member') 
    and NVL(c.x_nrma_title,'no title') != 'Estate Of The Late' 
    and case when trim(c.x_inv_email_1) = 'Y' or c.email_addr is null then 'N' else 'Y' end = 'Y' 
    and NVL(cx.attrib_36,'Yes')!='No' 
     
    group by 1,2 
    order by 1,2 
    ''').show(10000,False) 
SELECT d.*, h.partner  from m4m.return_feed_header h 
 
INNER JOIN m4m.return_feed_detail d 
on h.trx_header_id=d.trx_header_id 
 
where h.partner in ('IAG','NRMA Multi Policy Discount','NRMA MPD Insurance','NRMA Insurance') 
and d.item_description rlike 'MOT' 
---change this to all 
with ft_mot as ( 
SELECT member_number, min(time_stamp) as first_trans from m4m.return_feed_header h 
 
INNER JOIN m4m.return_feed_detail d 
on h.trx_header_id=d.trx_header_id 
 
where partner in ('IAG','NRMA Multi Policy Discount','NRMA MPD Insurance','NRMA Insurance') 
and  d.item_description rlike 'MOT' 
group by 1 
) 
 
select year(f.first_trans), month(f.first_trans), count(distinct h.member_number) from m4m.return_feed_header h 
 
-- INNER JOIN m4m.return_feed_detail d 
-- on h.trx_header_id=d.trx_header_id 
 
inner join ft_mot f 
on f.member_number=h.member_number 
 
where  1=1 
--d.item_description rlike 'MOT' 
and f.first_trans between '2019-01-01' and '2021-03-31' 
and partner in ('IAG','NRMA Multi Policy Discount','NRMA MPD Insurance','NRMA Insurance') 
 
group by 1,2 
order by 1,2 
---change this to all 
-- with ft as ( 
-- SELECT member_number, min(time_stamp) as first_trans from m4m.return_feed_header 
-- where partner in ('IAG','NRMA Multi Policy Discount','NRMA MPD Insurance','NRMA Insurance') 
-- group by 1 
-- ) 
 
select year(time_stamp), month(time_stamp), count(distinct member_number) from m4m.return_feed_header h 
 
INNER JOIN m4m.return_feed_detail d 
on h.trx_header_id=d.trx_header_id 
 
where  d.item_description rlike 'MOT' 
and time_stamp between '2019-01-01' and '2021-03-31' 
and partner in ('IAG','NRMA Multi Policy Discount','NRMA MPD Insurance','NRMA Insurance') 
 
group by 1,2 
order by 1,2 
with ft as ( 
SELECT member_number, min(time_stamp) as first_trans from m4m.return_feed_header 
where partner in ('IAG','NRMA Multi Policy Discount','NRMA MPD Insurance','NRMA Insurance') 
group by 1 
) 
 
select case when time_stamp between '2019-11-19' and '2020-02-14' then 'same_time_LY' 
            when time_stamp between '2019-08-19' and '2019-11-18' then 'before_time_LY' 
            when time_stamp between '2020-11-19' and '2021-02-14' then 'same_time_TY' 
            when time_stamp between '2020-08-19' and '2020-11-18' then 'before_time_TY' 
            else 'other_time' end timing 
        , count(distinct member_number) from m4m.return_feed_header h 
INNER JOIN m4m.return_feed_detail d 
on h.trx_header_id=d.trx_header_id 
 
where  d.item_description rlike 'MOT' 
and time_stamp between '2019-01-01' and '2021-03-31' 
and h.partner in ('IAG','NRMA Multi Policy Discount','NRMA MPD Insurance','NRMA Insurance') 
 
group by 1 
order by 1 
SELECT case when s.clicked_elements rlike 'fill_in_your_details' then 'Lead_Gen' 
            when s.clicked_elements rlike 'Switch_and_sav' then 'Header' 
            else s.clicked_elements end Clicks 
        ,count(DISTINCT s.customer_id)  
 
from omc.send_level_summary s 
 
INNER JOIN campaign_data.aa_dmc2226_hack_map_20210310 h 
on h.riid=s.riid 
 
INNER JOIN gms.s_contact c 
on c.row_id=h.customer_id 
 
-- INNER JOIN m4m.return_feed_header m 
-- on m.member_number=c.csn 
 
 
where s.program_id in ('55401522','55412202') 
and s.bounce_event_date is null 
and s.control is false 
-- and m.time_stamp>s.send_event_date 
-- and to_date(m.time_stamp)<='2021-02-14' 
-- and partner in ('IAG','NRMA Multi Policy Discount','NRMA MPD Insurance','NRMA Insurance') 
 
 
GROUP BY 1 
ORDER BY 2 DESC 
SELECT * from campaign_data.campaign_metadata_adhoc 
 
where table_id in (97,100) 
SELECT * FROM campaign_data.aa_dmc2230_iaggobig_campaign_edm_20201211  
with target as ( 
SELECT DISTINCT riid from omc.send_level_summary 
where program_id in ('55401522','55412202') 
and control is false 
) 
select iag.col9,count(DISTINCT s.customer_id) from omc.send_level_summary s 
 
left anti join target t 
on t.riid=s.riid 
 
-- INNER JOIN campaign_data.aa_dmc2226_hack_map_20210310 h 
-- on h.riid=s.riid 
 
INNER JOIN gms.s_contact c 
on c.row_id=s.customer_id 
 
inner join gms.s_contact_x cx 
on c.row_id=cx.par_row_id 
 
-- INNER JOIN m4m.return_feed_header m 
-- on m.member_number=c.csn 
 
INNER JOIN campaign_data.campaign_contact_adhoc_staging iag 
on iag.contact_id=c.row_id 
and iag.table_id='97' 
 
-- inner join m4m.return_feed_detail d 
-- on d.trx_header_id=m.trx_header_id 
 
-- inner join ft f 
-- on f.member_number=c.csn 
 
 
where s.program_id in ('55401522','55412202') 
-- and m.time_stamp>s.send_event_date 
-- and to_date(from_utc_timestamp(m.time_stamp,'AEST'))<='2021-02-14' 
-- and partner in ('IAG','NRMA Multi Policy Discount','NRMA MPD Insurance','NRMA Insurance') 
and s.control is true 
 
GROUP BY 1 
ORDER BY 1 
with target as ( 
SELECT DISTINCT riid from omc.send_level_summary 
where program_id in ('55401522','55412202') 
and control is false 
) 
select iag.col11,count(DISTINCT h.customer_id) from omc.send_level_summary s 
 
left anti join target t 
on t.riid=s.riid 
 
INNER JOIN campaign_data.aa_dmc2226_hack_map_20210310 h 
on h.riid=s.riid 
 
INNER JOIN gms.s_contact c 
on c.row_id=h.customer_id 
 
inner join gms.s_contact_x cx 
on c.row_id=cx.par_row_id 
 
-- INNER JOIN m4m.return_feed_header m 
-- on m.member_number=c.csn 
 
INNER JOIN campaign_data.campaign_contact_adhoc_staging iag 
on iag.contact_id=c.row_id 
and iag.table_id='97' 
 
 
where s.program_id in ('55401522','55412202') 
-- and m.time_stamp>s.send_event_date 
-- and to_date(from_utc_timestamp(m.time_stamp,'AEST'))<='2021-02-14' 
-- and partner in ('IAG','NRMA Multi Policy Discount','NRMA MPD Insurance','NRMA Insurance') 
and s.control is true 
 
GROUP BY 1 
ORDER BY 1 
SELECT iag.col9,count(DISTINCT s.customer_id) from omc.send_level_summary s 
 
-- INNER JOIN campaign_data.aa_dmc2226_hack_map_20210310 h 
-- on h.riid=s.riid 
 
INNER JOIN gms.s_contact c 
on c.row_id=s.customer_id 
 
inner join gms.s_contact_x cx 
on c.row_id=cx.par_row_id 
 
INNER JOIN m4m.return_feed_header m 
on m.member_number=c.csn 
 
INNER JOIN campaign_data.campaign_contact_adhoc_staging iag 
on iag.contact_id=c.row_id 
and iag.table_id='97' 
 
-- inner join m4m.return_feed_detail d 
-- on d.trx_header_id=m.trx_header_id 
 
-- inner join ft f 
-- on f.member_number=c.csn 
 
 
where s.program_id in ('55401522','55412202') 
and m.time_stamp>s.send_event_date 
and to_date(from_utc_timestamp(m.time_stamp,'AEST'))<='2021-02-14' 
and partner in ('IAG','NRMA Multi Policy Discount','NRMA MPD Insurance','NRMA Insurance') 
and s.bounce_event_date is NULL 
and s.skip_reason is null 
--and d.item_description rlike 'MOT' 
and s.control is FALSE 
 
GROUP BY 1 
ORDER BY 1 
--create table campaign_data.aa_dmc1918_iagxsell2leadgen_campaign_analysis_20200709_adhoc as 
SELECT t.control,count(DISTINCT form.riid) 
-- , 
--         form.customer_id as Contact_id, 
--         form.member_number,  
--         cast(from_unixtime(unix_timestamp(form.event_captured_dt,"dd-MMM-yyyy HH:mm:ss")) as timestamp) form_fill_dt, 
--         t.open_event_date,  
--         t.send_event_date, 
--         form.event_captured_dt, 
--         case when lower(form.lead_modified_by_user)='digital' then 'Digital' Else 'Call Center' end as lead_channel, 
--         form.send_email as email_perm, 
--         form.form_id 
 
from omc.audit_form form 
 
inner join omc.send_level_summary t 
on t.riid=form.riid  
 
 
 
where form_id like 'IAG_FORM%' 
and t.program_id in ('55401522','55412202') 
and datediff(to_date(cast(from_unixtime(unix_timestamp(form.event_captured_dt,"dd-MMM-yyyy HH:mm:ss")) as timestamp)),to_Date(t.send_event_date)) between 0 and 69 
--and datediff(to_date(cast(from_unixtime(unix_timestamp(form.event_captured_dt,"dd-MMM-yyyy HH:mm:ss")) as timestamp)),to_Date(t.send_event_date))>0 
--and (open_event_date is not null OR click_event_date is not null) 
 
GROUP BY t.control 
 
SELECT cx.attrib_55, count(DISTINCT h.customer_id) from omc.send_level_summary s 
 
INNER JOIN campaign_data.aa_dmc2226_hack_map_20210310 h 
on h.riid=s.riid 
 
INNER JOIN gms.s_contact c 
on c.row_id=h.customer_id 
 
inner join gms.s_contact_x cx 
on cx.par_row_id=c.row_id 
 
INNER JOIN m4m.return_feed_header m 
on m.member_number=c.csn 
 
-- INNER JOIN campaign_data.campaign_contact_adhoc_staging iag 
-- on iag.contact_id=c.row_id 
-- and iag.table_id='97' 
 
 
where s.program_id in ('55401522','55412202') 
and s.control is FALSE 
and m.time_stamp>s.send_event_date 
and to_date(m.time_stamp)<='2021-02-14' 
and partner in ('IAG','NRMA Multi Policy Discount','NRMA MPD Insurance','NRMA Insurance') 
 
 
GROUP BY 1 
ORDER BY 1 
select distinct control from campaign_data.campaign_contact_adhoc_staging 
where table_id in ('97') 
---and control is not null 
SELECT * from campaign_data.campaign_metadata_adhoc 
where table_id=97 
SELECT DISTINCT o.status_cd from gms.s_order o 
 
INNER JOIN gms.s_order_item oi 
on o.row_id = oi.order_id 
 
inner join gms.s_order_type ot 
on o.order_type_id = ot.row_id 
 
INNER JOIN gms.s_asset a 
on a.integration_id=oi.asset_integ_id 
SELECT DISTINCT o.X_PAYMENT_STATUS from gms.s_order o 
 
INNER JOIN gms.s_order_item oi 
on o.row_id = oi.order_id 
 
inner join gms.s_order_type ot 
on o.order_type_id = ot.row_id 
 
INNER JOIN gms.s_asset a 
on a.integration_id=oi.asset_integ_id 
SELECT DISTINCT status_cd from gms.s_asset 
with converted as ( 
SELECT DISTINCT h.customer_id,c.csn, s.send_event_date, s.control from omc.send_level_summary s 
 
INNER JOIN campaign_data.aa_dmc2226_hack_map_20210310 h 
on h.riid=s.riid 
 
INNER JOIN gms.s_contact c 
on c.row_id=h.customer_id 
 
inner join gms.s_contact_x cx 
on cx.par_row_id=c.row_id 
 
INNER JOIN m4m.return_feed_header m 
on m.member_number=c.csn 
 
where s.program_id in ('55401522','55412202') 
--and s.control is true 
and m.time_stamp>s.send_event_date 
and to_date(m.time_stamp)<='2021-02-14' 
and partner in ('IAG','NRMA Multi Policy Discount','NRMA MPD Insurance','NRMA Insurance') 
) 
 
select c.control 
        -- case when partner in ('IAG','NRMA Multi Policy Discount','NRMA MPD Insurance','NRMA Insurance') then 'IAG'  
        --     when partner LIKE 'NRMA Holiday%' then 'NRMA Parks and Resorts'  
        --     when partner LIKE 'HP%' then 'NRMA Parks and Resorts'  
        --     when partner LIKE 'BIG4%' then 'NRMA Parks and Resorts'  
        --     when partner LIKE 'NRMA Parks and Resorts%' then 'NRMA Parks and Resorts'  
        --     when partner LIKE 'NRMA Bowen Beachfront Holiday Park' then 'NRMA Parks and Resorts'  
        --     else partner end as partner 
        , count(distinct member_number) from m4m.return_feed_header m 
 
inner join converted c  
on c.csn=m.member_number 
 
where time_stamp>c.send_event_date 
and partner not in ('IAG','NRMA Multi Policy Discount','NRMA MPD Insurance','NRMA Insurance') 
 
group by 1 
order by 1 
SELECT cx.attrib_55,s.control, count(DISTINCT h.customer_id) from omc.send_level_summary s 
--c.cust_value_cd 
 
INNER JOIN campaign_data.aa_dmc2226_hack_map_20210310 h 
on h.riid=s.riid 
 
INNER JOIN gms.s_contact c 
on c.row_id=h.customer_id 
 
inner join gms.s_contact_x cx 
on cx.par_row_id=c.row_id 
 
INNER JOIN m4m.return_feed_header m 
on m.member_number=c.csn 
 
where s.program_id in ('55401522','55412202') 
--and s.control is TRUE 
and m.time_stamp>s.send_event_date 
and to_date(m.time_stamp)<='2021-02-14' 
and partner in ('IAG','NRMA Multi Policy Discount','NRMA MPD Insurance','NRMA Insurance') 
 
 
GROUP BY 1,2 
ORDER BY 1,2 
with ft as ( 
SELECT member_number, min(time_stamp) as first_trans from m4m.return_feed_header 
where partner in ('IAG','NRMA Multi Policy Discount','NRMA MPD Insurance','NRMA Insurance') 
group by 1 
) 
 
select  
--count(distinct m.member_number) 
s.control,count(distinct h.customer_id)  
 
from m4m.return_feed_header m 
 
INNER JOIN m4m.return_feed_detail d 
on m.trx_header_id=d.trx_header_id 
 
inner join ft t 
on t.member_number=m.member_number 
 
inner JOIN gms.s_contact c 
on c.csn=m.member_number 
 
left join omc.send_level_summary s 
on s.customer_id=c.row_id 
and s.program_id in ('55401522','55412202') 
 
left JOIN campaign_data.aa_dmc2226_hack_map_20210310 h 
on h.riid=s.riid 
 
where to_date(t.first_trans) between '2020-12-07' and '2021-03-14' 
and s.control is false 
-- and m.time_stamp>s.send_event_date 
-- and to_date(m.time_stamp)<='2021-02-14' 
and partner in ('IAG','NRMA Multi Policy Discount','NRMA MPD Insurance','NRMA Insurance') 
--and d.item_description rlike 'MOT' 
 
group by 1 
 
 
where h.partner in ('IAG','NRMA Multi Policy Discount','NRMA MPD Insurance','NRMA Insurance') 
and d.item_description rlike 'MOT' 
with converted as ( 
SELECT DISTINCT s.customer_id,c.csn, s.send_event_date, s.control from omc.send_level_summary s 
 
INNER JOIN campaign_data.aa_dmc2226_hack_map_20210310 h 
on h.riid=s.riid 
 
INNER JOIN gms.s_contact c 
on c.row_id=h.customer_id 
 
inner join gms.s_contact_x cx 
on cx.par_row_id=c.row_id 
 
INNER JOIN m4m.return_feed_header m 
on m.member_number=c.csn 
 
where s.program_id in ('55401522','55412202') 
--and s.control is false 
and m.time_stamp>s.send_event_date 
and to_date(m.time_stamp)<='2021-02-14' 
and partner in ('IAG','NRMA Multi Policy Discount','NRMA MPD Insurance','NRMA Insurance') 
) 
 
select c.control, count(distinct app.membernumber) 
        -- , sum(app.engaged_accom) 
        -- , sum(app.engaged_park) 
        -- , sum(app.engaged_fuel) 
        -- , sum(app.engaged_travel) 
        -- , sum(app.engaged_dining) 
        -- , sum(app.engaged_cad) 
        --, sum(app.engaged_events) 
 
from sandpit.ga_appsession app 
 
inner join converted c  
on c.csn=app.membernumber 
 
where to_date(app.event_dt)>to_date(c.send_event_date) 
--and partner not in ('IAG','NRMA Multi Policy Discount','NRMA MPD Insurance','NRMA Insurance') 
 
group by 1 
order by 1 
SELECT * from m4m.return_feed_header h 
where h.partner in ('IAG','NRMA Multi Policy Discount','NRMA MPD Insurance','NRMA Insurance') 
 
select count(distinct member_number) from m4m.return_feed_header h 
 
INNER JOIN m4m.return_feed_detail d 
on h.trx_header_id=d.trx_header_id 
 
where 1=1 
and d.item_description rlike 'MOT' 
and time_stamp between '2020-11-19' and '2021-02-14' 
and h.partner in ('IAG','NRMA Multi Policy Discount','NRMA MPD Insurance','NRMA Insurance') 
and h.total_amount>0 
 
 
-- group by 1 
-- order by 1 
with target as ( 
SELECT DISTINCT riid from omc.send_level_summary 
where program_id in ('55401522','55412202') 
and control is false 
), iag as ( 
SELECT s.customer_id from  
 
omc.send_level_summary s 
 
left anti join target t 
on t.riid=s.riid 
 
INNER JOIN gms.s_contact c 
on c.row_id=s.customer_id 
 
INNER JOIN m4m.return_feed_header m 
on m.member_number=c.csn 
 
inner join m4m.return_feed_detail d 
on d.trx_header_id=m.trx_header_id 
 
where s.program_id in ('55401522','55412202') 
and m.time_stamp>s.send_event_date 
and to_date(m.time_stamp)<='2021-02-14' 
and partner in ('IAG','NRMA Multi Policy Discount','NRMA MPD Insurance','NRMA Insurance') 
and s.control IS true 
and d.item_description rlike 'MOT' 
--and m.total_amount>0 
 
GROUP BY s.customer_id 
--HAVING min(m.total_amount>0) 
) 
select count(*) from iag 
DROP TABLE campaign_data.aa_dmc2226_hack_map_20210310; 
spark.sql(''' 
create table campaign_data.aa_dmc2226_hack_map_20210310 as  
SELECT DISTINCT 
    riid, 
    customer_id 
     
FROM 
    ( 
        SELECT riid, customer_id FROM omc.email_sent 
        UNION ALL 
        SELECT riid, customer_id FROM omc.email_skipped 
        UNION ALL 
        SELECT riid, customer_id FROM omc.sms_sent 
        UNION ALL 
        SELECT riid, customer_id FROM omc.sms_skipped 
        UNION ALL 
        SELECT riid, customer_id FROM omc.push_sent 
        UNION ALL 
         
        SELECT riid, customer_id FROM omc.push_skipped 
        UNION ALL 
        SELECT 
            SUBSTRING(contact_riid, 2, LENGTH(contact_riid) - 2) AS riid, 
            SUBSTRING(customer_id_, 2, LENGTH(customer_id_) - 2) AS customer_id 
        FROM omc.riid_mapping 
         
    ) AS aleph 
WHERE 
    customer_id != '' 
''').count() 
with iag as ( 
SELECT s.customer_id from  
 
omc.send_level_summary s 
 
-- INNER JOIN campaign_data.aa_dmc2226_hack_map_20210310 h 
-- on h.riid=s.riid 
 
INNER JOIN gms.s_contact c 
on c.row_id=s.customer_id 
 
INNER JOIN m4m.return_feed_header m 
on m.member_number=c.csn 
 
inner join m4m.return_feed_detail d 
on d.trx_header_id=m.trx_header_id 
 
where s.program_id in ('55401522','55412202') 
and m.time_stamp>s.send_event_date 
and to_date(m.time_stamp)<='2021-02-14' 
and partner in ('IAG','NRMA Multi Policy Discount','NRMA MPD Insurance','NRMA Insurance') 
and s.control IS FALSE 
--and m.total_amount>0 
 
GROUP BY s.customer_id 
HAVING min(m.total_amount>0) 
) 
select count(*) from iag 
with ft as ( 
SELECT  member_number, min(time_stamp) as first_trans from m4m.return_feed_header 
where partner in ('IAG','NRMA Multi Policy Discount','NRMA MPD Insurance','NRMA Insurance') 
group by 1 
 
), iag as ( 
 
select f.member_number 
        , (d.item_description rlike 'MOT') as motor 
        , min(m.total_amount>0) non_cancelled 
 
from ft f 
 
left JOIN gms.s_contact c 
on c.csn=f.member_number 
 
left JOIN m4m.return_feed_header m 
on m.member_number=c.csn 
 
left join m4m.return_feed_detail d 
on d.trx_header_id=m.trx_header_id 
 
-- left JOIN campaign_data.aa_dmc2226_hack_map_20210310 h 
-- on h.customer_id=c.row_id 
 
-- left join omc.send_level_summary s 
-- on s.riid=h.riid 
-- and s.program_id in ('55401522','55412202') 
 
left anti join omc.send_level_summary t 
on t.customer_id=c.row_id 
and t.program_id in ('55401522','55412202') 
and t.control is false 
 
where first_trans between '2020-11-11' and '2021-02-14' 
-- and s.control is true 
-- and m.time_stamp>s.send_event_date 
-- and to_date(m.time_stamp)<='2021-02-14' 
--and to_date(f.first_trans)<='2021-02-14' 
-- and partner in ('IAG','NRMA Multi Policy Discount','NRMA MPD Insurance','NRMA Insurance') 
-- and (s.open_event_date is not null or s.click_event_date is not null) 
-- --and m.total_amount>0 
--and d.item_description rlike 'MOT' 
 
GROUP 1,2 
--HAVING min(m.total_amount>0 and d.item_description rlike 'MOT') --looking for motoring transaction none of them can be less than 0 
--and max(m.total_amount<=0 and d.item_description not rlike 'MOT')       --but they must have atleast one non-motoring trans less than 0 
) 
select count(*) from iag 
spark.sql(''' 
SELECT DISTINCT 
    riid, 
    customer_id 
     
FROM 
    ( 
        SELECT riid, customer_id FROM omc.email_sent 
        UNION ALL 
        SELECT riid, customer_id FROM omc.email_skipped 
        UNION ALL 
        SELECT riid, customer_id FROM omc.sms_sent 
        UNION ALL 
        SELECT riid, customer_id FROM omc.sms_skipped 
        UNION ALL 
        SELECT riid, customer_id FROM omc.push_sent 
        UNION ALL 
        SELECT riid, customer_id FROM omc.push_skipped 
        UNION ALL 
        SELECT 
            SUBSTRING(contact_riid, 2, LENGTH(contact_riid) - 2) AS riid, 
            SUBSTRING(customer_id_, 2, LENGTH(customer_id_) - 2) AS customer_id 
        FROM omc.riid_mapping 
         
    ) AS aleph 
WHERE 
    customer_id != '' 
''').createOrReplaceTempView('hack_map') 
spark.sql(''' 
SELECT 
    riid, 
    CASE 
        WHEN COUNT(DISTINCT control) > 1 THEN 'Mixed' 
        WHEN MAX(control) THEN 'Control' 
        ELSE 'Target' 
    END AS send 
FROM 
    omc.send_level_summary 
     
WHERE 
    program_id in ('55401522', '55412202') 
    AND DATE(send_event_date) = DATE('2020-12-06') 
     
GROUP BY 
    1 
''').createOrReplaceTempView('test_send') 
spark.sql(''' 
SELECT 
    riid, 
    CASE 
        WHEN COUNT(DISTINCT control) > 1 THEN 'Mixed' 
        WHEN MAX(control) THEN 'Control' 
        ELSE 'Target' 
    END AS send 
FROM 
    omc.send_level_summary 
     
WHERE 
    program_id in ('55401522', '55412202') 
    AND DATE(send_event_date) = DATE('2020-12-15') 
     
GROUP BY 
    1 
''').createOrReplaceTempView('full_send') 
spark.sql(''' 
SELECT 
    riid, 
    CASE 
        WHEN COUNT(DISTINCT control) > 1 THEN 'Mixed' 
        WHEN MAX(control) THEN 'Control' 
        ELSE 'Target' 
    END AS send 
FROM 
    omc.send_level_summary 
     
WHERE 
    program_id in ('55401522', '55412202') 
    AND DATE(send_event_date) = DATE('2021-01-17') 
     
GROUP BY 
    1 
''').createOrReplaceTempView('resend') 
spark.sql(''' 
SELECT 
    test_send.send AS test_send, 
    full_send.send AS full_send, 
    resend.send AS resend, 
    COUNT(DISTINCT hack_map.riid) 
     
FROM 
    hack_map 
     
LEFT JOIN 
    test_send 
    ON test_send.riid = hack_map.riid 
     
FULL JOIN 
    full_send 
    ON full_send.riid = hack_map.riid 
     
FULL JOIN 
    resend 
    ON resend.riid = hack_map.riid 
GROUP BY 
    1, 
    2, 
    3 
     
ORDER BY 
    1, 
    2, 
    3 
''').show(750, False) 
control - 19344 who became part of target 
 
IAG_send = spark.read.load("/user/aamreen/IAG_GO_BIG_Extend_Send.csv",format="csv", sep=",", inferSchema="true", header="true") 
IAG_send.createOrReplaceTempView("IAG_send") 
spark.sql(''' 
SELECT control,count(DISTINCT h.customer_id) from omc.send_level_summary s 
 
--inner join control ctr 
--on s.riid=ctr.riid 
 
INNER JOIN campaign_data.aa_dmc2226_hack_map_20210310 h 
on h.riid=s.riid 
 
INNER JOIN gms.s_contact c 
on c.row_id=h.customer_id 
 
inner join IAG_send i 
on i.member_number=c.csn 
 
where s.program_id in ('55401522','55412202') 
--and control is false 
 
group by 1 
''').show(1000,False) 
spark.sql(''' 
SELECT control,count(DISTINCT h.customer_id) from omc.send_level_summary s 
 
--inner join control ctr 
--on s.riid=ctr.riid 
 
INNER JOIN campaign_data.aa_dmc2226_hack_map_20210310 h 
on h.riid=s.riid 
 
INNER JOIN gms.s_contact c 
on c.row_id=h.customer_id 
 
inner join IAG_send i 
on i.member_number=c.csn 
 
INNER JOIN m4m.return_feed_header m 
on m.member_number=c.csn 
 
where s.program_id in ('55401522','55412202') 
and m.time_stamp>s.send_event_date 
and to_date(m.time_stamp)<='2021-02-14' 
and partner in ('IAG','NRMA Multi Policy Discount','NRMA MPD Insurance','NRMA Insurance') 
--and control is false 
 
group by 1 
''').show(1000,False) 
with ft as ( 
SELECT  member_number, min(time_stamp) as first_trans from m4m.return_feed_header 
where partner in ('IAG','NRMA Multi Policy Discount','NRMA MPD Insurance','NRMA Insurance') 
group by 1 
 
), iag as ( 
 
select f.member_number 
        , (d.item_description rlike 'MOT') as motor 
        , min(m.total_amount>0) non_cancelled 
 
from ft f 
 
left JOIN gms.s_contact c 
on c.csn=f.member_number 
 
left JOIN m4m.return_feed_header m 
on m.member_number=c.csn 
 
left join m4m.return_feed_detail d 
on d.trx_header_id=m.trx_header_id 
 
-- left join omc.send_level_summary s 
-- on s.customer_id=c.row_id 
-- and s.program_id in ('55401522','55412202') 
 
left anti join omc.send_level_summary t 
on t.customer_id=c.row_id 
and t.program_id in ('55401522','55412202') 
and t.control is false 
 
where first_trans between '2020-11-11' and '2021-02-14' 
--and s.control is false 
-- and m.time_stamp>s.send_event_date 
-- and to_date(m.time_stamp)<='2021-02-14' 
-- --and to_date(f.first_trans)<='2021-02-14' 
and partner in ('IAG','NRMA Multi Policy Discount','NRMA MPD Insurance','NRMA Insurance') 
-- --and m.total_amount>0 
--and d.item_description rlike 'MOT' 
 
GROUP by 1,2 
) 
select count(*) from iag 
where non_cancelled is true 
--and motor is true 
--create table campaign_data.aa_dmc2333_gobig_campaign_analysis_list2iag_20210415 as  
with ft as ( 
SELECT  member_number, to_date(min(time_stamp)) as first_trans from m4m.return_feed_header 
where partner in ('IAG','NRMA Multi Policy Discount','NRMA MPD Insurance','NRMA Insurance') 
group by 1 
), iag as ( 
 
select f.member_number, f.first_trans, d.sub_category  
        , (d.item_description rlike 'MOT') as motor 
        , min(m.total_amount>0) non_cancelled 
 
from ft f 
 
left JOIN gms.s_contact c 
on c.csn=f.member_number 
 
left JOIN m4m.return_feed_header m 
on m.member_number=c.csn 
 
left join m4m.return_feed_detail d 
on d.trx_header_id=m.trx_header_id 
 
inner join omc.send_level_summary s 
on s.customer_id=c.row_id 
and s.program_id in ('55401522','55412202') 
 
-- left anti join omc.send_level_summary t 
-- on t.customer_id=c.row_id 
-- and t.program_id in ('55401522','55412202') 
-- and t.control is false 
 
where 1=1 
and s.control is false 
and m.time_stamp>s.send_event_date 
and to_date(m.time_stamp)<='2021-02-14' 
--and to_date(f.first_trans)<='2021-02-14' 
and partner in ('IAG','NRMA Multi Policy Discount','NRMA MPD Insurance','NRMA Insurance') 
GROUP by 1,2,3,4 
) 
select distinct member_number from iag 
where 1=1 
and motor is true 
--and first_trans between '2020-11-11' and '2021-02-14' 
and non_cancelled is true 
and sub_category='CRCP' 
 
group by 1 
SELECT count(*) from campaign_data.aa_dmc2333_gobig_campaign_analysis_list2iag_20210415 
spark.sql(''' 
with ft as ( 
SELECT  member_number, to_date(min(time_stamp)) as first_trans from m4m.return_feed_header 
where partner in ('IAG','NRMA Multi Policy Discount','NRMA MPD Insurance','NRMA Insurance') 
group by 1 
), iag as ( 
 
select f.member_number, f.first_trans, d.sub_category  
        , (d.item_description rlike 'MOT') as motor 
        , min(m.total_amount>0) non_cancelled 
 
from ft f 
 
left JOIN gms.s_contact c 
on c.csn=f.member_number 
 
left JOIN m4m.return_feed_header m 
on m.member_number=c.csn 
 
left join m4m.return_feed_detail d 
on d.trx_header_id=m.trx_header_id 
 
inner join omc.send_level_summary s 
on s.customer_id=c.row_id 
and s.program_id in ('55401522','55412202') 
 
-- left anti join omc.send_level_summary t 
-- on t.customer_id=c.row_id 
-- and t.program_id in ('55401522','55412202') 
-- and t.control is false 
 
where 1=1 
and s.control=false 
and m.time_stamp>s.send_event_date 
and to_date(m.time_stamp)<='2021-02-14' 
--and to_date(f.first_trans)<='2021-02-14' 
and partner in ('IAG','NRMA Multi Policy Discount','NRMA MPD Insurance','NRMA Insurance') 
GROUP by 1,2,3,4 
) 
select distinct member_number as Only_CRCP from iag 
where 1=1 
and motor=true 
--and first_trans between '2020-11-11' and '2021-02-14' 
--and non_cancelled=true 
and sub_category='CRCP' 
''').show(1000,False) 
with policy_count_LY as ( 
SELECT h.member_number, count(DISTINCT h.trx_header_id) as ly_policies from m4m.return_feed_header h 
 
inner join m4m.return_feed_detail d 
on d.trx_header_id=h.trx_header_id 
 
where to_date(time_stamp) between '2019-11-10' and '2020-11-10' 
and partner in ('IAG','NRMA Multi Policy Discount','NRMA MPD Insurance','NRMA Insurance') 
and d.item_description rlike 'MOT' 
GROUP BY 1 
having min(total_amount>0) 
 
), policy_count_TY as ( 
SELECT h.member_number, count(DISTINCT h.trx_header_id) as ty_policies from m4m.return_feed_header h 
 
inner join m4m.return_feed_detail d 
on d.trx_header_id=h.trx_header_id 
 
where to_date(time_stamp) between '2020-11-11' and '2021-02-14' 
and partner in ('IAG','NRMA Multi Policy Discount','NRMA MPD Insurance','NRMA Insurance') 
and d.item_description rlike 'MOT' 
GROUP BY 1 
having min(total_amount>0) 
 
),ft as ( 
SELECT  member_number, to_date(min(time_stamp)) as first_trans from m4m.return_feed_header 
where partner in ('IAG','NRMA Multi Policy Discount','NRMA MPD Insurance','NRMA Insurance') 
group by 1 
) 
 
select count(distinct ty.member_number) from policy_count_TY ty 
 
inner join policy_count_LY ly 
on ly.member_number=ty.member_number 
 
left anti join ft f 
on f.member_number=ty.member_number 
and first_trans between '2020-11-11' and '2021-02-14' 
 
where 1=1  
--and ty.ty_policies>ly.ly_policies 
 
with policy_count_LY as ( 
SELECT h.member_number, count(DISTINCT h.trx_header_id) as ly_policies from m4m.return_feed_header h 
 
inner join m4m.return_feed_detail d 
on d.trx_header_id=h.trx_header_id 
 
where to_date(time_stamp) between '2019-11-10' and '2020-11-10' 
and partner in ('IAG','NRMA Multi Policy Discount','NRMA MPD Insurance','NRMA Insurance') 
and d.item_description rlike 'MOT' 
GROUP BY 1 
having min(total_amount>0) 
 
), policy_count_TY as ( 
SELECT h.member_number, count(DISTINCT h.trx_header_id) as ty_policies from m4m.return_feed_header h 
 
inner join m4m.return_feed_detail d 
on d.trx_header_id=h.trx_header_id 
 
where to_date(time_stamp) between '2020-11-11' and '2021-02-14' 
and partner in ('IAG','NRMA Multi Policy Discount','NRMA MPD Insurance','NRMA Insurance') 
and d.item_description rlike 'MOT' 
GROUP BY 1 
having min(total_amount>0) 
 
),ft as ( 
SELECT  member_number, to_date(min(time_stamp)) as first_trans from m4m.return_feed_header 
where partner in ('IAG','NRMA Multi Policy Discount','NRMA MPD Insurance','NRMA Insurance') 
group by 1 
) 
 
select count(distinct ty.member_number) from policy_count_TY ty 
 
inner join policy_count_LY ly 
on ly.member_number=ty.member_number 
 
left anti join ft f 
on f.member_number=ty.member_number 
and first_trans between '2020-11-11' and '2021-02-14' 
 
where 1=1  
--and ty.ty_policies>ly.ly_policies 
 
spark.sql(''' 
select * from campaign_data.aa_dmc2333_gobig_campaign_analysis_list2iag_20210415 
''').show(1000,False) 
select distinct a.member_number, product_id, item_description from campaign_data.aa_dmc2333_gobig_campaign_analysis_list2iag_20210415 a 
 
inner join m4m.return_feed_header h 
on a.member_number=h.member_number 
 
inner join m4m.return_feed_detail d 
on d.trx_header_id=h.trx_header_id 
 
where partner in ('IAG','NRMA Multi Policy Discount','NRMA MPD Insurance','NRMA Insurance') 
and d.item_description rlike 'MOT' 
create table campaign_data.aa_dmc2333_gobig_campaign_analysis_list3iag_20210424 as  
with ft as ( 
SELECT  member_number, to_date(min(time_stamp)) as first_trans from m4m.return_feed_header 
where partner in ('IAG','NRMA Multi Policy Discount','NRMA MPD Insurance','NRMA Insurance') 
group by 1 
), iag as ( 
 
select f.member_number, f.first_trans, d.sub_category , product_id 
        , (d.item_description rlike 'MOT') as motor 
        , min(m.total_amount>0) non_cancelled 
 
from ft f 
 
left JOIN gms.s_contact c 
on c.csn=f.member_number 
 
left JOIN m4m.return_feed_header m 
on m.member_number=c.csn 
 
left join m4m.return_feed_detail d 
on d.trx_header_id=m.trx_header_id 
 
inner join omc.send_level_summary s 
on s.customer_id=c.row_id 
and s.program_id in ('55401522','55412202') 
 
where 1=1 
and s.control is false 
and m.time_stamp>s.send_event_date 
and to_date(m.time_stamp)<='2021-02-14' 
--and to_date(f.first_trans)<='2021-02-14' 
and partner in ('IAG','NRMA Multi Policy Discount','NRMA MPD Insurance','NRMA Insurance') 
GROUP by 1,2,3,4,5 
) 
select distinct member_number, product_id  
 
from iag 
where 1=1 
and motor is true 
and non_cancelled is true 
and sub_category='CRCP' 
SELECT * from campaign_data.aa_dmc2333_gobig_campaign_analysis_list3iag_20210424