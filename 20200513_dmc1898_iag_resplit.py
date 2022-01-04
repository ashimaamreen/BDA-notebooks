from pyspark.sql.functions import * 
 
 
 
# spark.sql("select * from campaign_data.campaign_metadata_adhoc").show(1000,False) 
 
# getting the original target from campaign table id 68 
tgt = spark.sql(""" 
    select contact_id 
        , member_id 
        , segment 
        , control 
        , col1 as asset_end_dt 
        , col2 as item_promo_name 
        , col3 as num 
        , col4 as updated_dt 
        from campaign_data.campaign_contact_adhoc  
        where  
            table_id = '68'  
        order by contact_id 
    """) 
     
tgt.createOrReplaceTempView("tgt") 
spark.sql("select segment, count(asset_end_dt) as vol from tgt group by segment").show(100,False) 
# getting base table asset end date >= 28th June 2020 
base = spark.sql(""" 
 
with cc as ( 
 
    -- only having classic care signed up through promo code 
 
    select distinct  
        tbl.asset_end_dt 
        , tbl.prod_name 
        , tbl.item_promo_name 
        , tbl.asset_row_id 
        , tbl.membernumber 
        , s.row_id as contact_id 
    from sandpit.renewal_base as tbl 
        inner join gms.s_contact as s 
        on s.csn = tbl.membernumber 
         
    where 
        1=1 
        and lower(tbl.item_promo_name) = "cc_iag_bundle_rsa_offer_0319" 
        and tbl.contact_cd in ("Affiliate Member", "Ordinary Member") 
        -- active customer and asset 
        and tbl.asset_status_cd = "Active" 
        and s.cust_stat_cd = "Active" 
        -- product selection 
        and tbl.prod_name = "Classic Care" 
 
 
), pc as ( 
 
    -- only having premium care 
 
    select distinct  
        tbl.asset_end_dt 
        , tbl.prod_name 
        , tbl.item_promo_name 
        , tbl.asset_row_id 
        , tbl.membernumber 
        , s.row_id as contact_id 
    from sandpit.renewal_base as tbl 
        inner join gms.s_contact as s 
        on s.csn = tbl.membernumber 
    where 
        1=1 
        and lower(tbl.item_promo_name) = "pc_iag_bundle_rsa_offer_0319" 
        and tbl.contact_cd in ("Affiliate Member", "Ordinary Member") 
        -- active customer and asset 
        and tbl.asset_status_cd = "Active" 
        and s.cust_stat_cd = "Active" 
        -- product selection 
        and tbl.prod_name = "Premium Care" 
), exclusion as ( 
 
    -- finding out who has both cc and pc promo on the same due date 
 
    select distinct cc.contact_id, cc.asset_end_dt 
    from pc inner join cc 
        on pc.asset_end_dt = cc.asset_end_dt 
        and pc.contact_id = cc.contact_id 
 
), result as ( 
 
    select distinct tbl.contact_id 
        , tbl.segment 
        , tbl.control 
        , cc.asset_row_id 
        , cc.prod_name 
        , tbl.col1 as asset_end_dt 
        , tbl.col2 as item_promo_name 
        , tbl.col3 as num_subs 
    from campaign_data.campaign_contact_adhoc as tbl 
         
        inner join cc  
        on cc.contact_id = tbl.contact_id 
        and tbl.col1 = to_date(cc.asset_end_dt) 
 
    where 
        1=1 
        -- selecting only those with treatments 
        and cast(tbl.table_id as string) = '68' 
        and to_date(tbl.col1) >= "2020-06-26" 
 
) 
 
 
-- the code is to get members who will receive treatment 
select r.* 
from result as r 
    left anti join exclusion as e 
    on r.contact_id = e.contact_id 
    and r.asset_end_dt = to_date(e.asset_end_dt)  
 
""") 
base.createOrReplaceTempView("base") 
base.printSchema() 
spark.sql(""" 
    select control, segment, prod_name, count(asset_row_id), count(distinct asset_row_id), max(asset_end_dt), min(asset_end_dt) 
    from base 
    group by control, segment, prod_name 
    order by control, segment, prod_name 
""").show() 
# reshuffle the base table with new split 
spark.sql("drop table campaign_data.yc_dmc1898_iagmvb_edm_20200513_adhoc") 
 
spark.sql(""" 
 
-- randomly assign a number to each row 
-- decile the random number by  
 
create table campaign_data.yc_dmc1898_iagmvb_edm_20200513_adhoc as  
 
with randomise as ( 
    select *, rand(100) as random_num 
    from base 
), bucket as ( 
    select *  
    , ntile(10) over (order by random_num) as index 
    from randomise 
)  
 
select contact_id 
    , case when index between 1 and 2 then "control"  
        when index between 3 and 6 then "offer" 
        when index between 7 and 10 then "no_offer" 
        else "na" end as segment 
    , case when index between 1 and 2 then "1" else "0" end as control 
    , asset_row_id 
    , prod_name 
    , asset_end_dt 
    , item_promo_name 
    , num_subs 
 
from bucket 
 
""") 
 
spark.sql("select * from campaign_data.yc_dmc1898_iagmvb_edm_20200513_adhoc").show(10,False) 
spark.sql(""" 
    select control, segment, prod_name, count(asset_row_id), count(distinct asset_row_id), max(asset_end_dt), min(asset_end_dt) 
    from campaign_data.yc_dmc1898_iagmvb_edm_20200513_adhoc 
    group by control, segment, prod_name 
    order by control, segment, prod_name 
""").show() 
spark.sql(""" 
 
select asset_row_id, "1-E646WUT" as offer_id 
from campaign_data.yc_dmc1898_iagmvb_edm_20200513_adhoc 
where 
    segment = "offer" 
 
""").show(1000,False) 
spark.sql(""" 
 
select * 
from campaign_data.yc_dmc1898_iagmvb_edm_20200513_adhoc 
where 
    segment = 'no_offer' 
 
""").show(1000,False) 
spark.sql(""" 
 
select r.item_promo_name, count(r.order_id), count(r.asset_row_id), max(r.order_end_dt), min(r.order_end_dt) 
from campaign_data.yc_dmc1898_iagmvb_edm_20200513_adhoc as tgt 
    inner join sandpit.renewal_base as r 
    on r.asset_row_id = tgt.asset_row_id 
where 
    tgt.segment = "offer" 
    --and year(r.order_end_dt) = 2020 
group by r.item_promo_name 
 
""").show(1000,False)