
from pyspark.sql.functions import * 
 
model_cs = spark.read.load("/user/harafah/CrossSellModels/03 Scores/04Mar19/Data/bt_member_CarServicing_score.csv",  
    format="csv", sep=",", inferSchema="true", header="true") 
model_cs.createOrReplaceTempView("model") 
spark.sql(""" 
     
    create table campaign_data.model_carservicing_crosssell_20190305 as  
     
    select * 
    from model 
 
""") 
m4m_header = spark.table("m4m.return_feed_header") 
m4m_header.createOrReplaceTempView("m4m_header") 
 
spark.sql(""" 
 
-- Preprocessing model 
 
/* 
Preprocessing model 
Purpose: 
1) this is to exclude members who have been to carservicing in 2019 
2) this is to add percentile and ranking for the model 
*/ 
 
create table campaign_data.model_carservicing_crosssell_20190305_nocsin2019 as 
 
select m.*,  
    ntile(10) over(order by m.transactionprobability desc) as bucket,  
    rank() over(order by m.transactionprobability desc) as ranking 
from model as m 
     
    left anti join ( 
         
    select distinct h.member_number 
    from  
        m4m_header h 
    where 
        h.partner in ("NRMA car servicing", "NRMA MotorServe") and  
        h.time_stamp < current_timestamp() and  
        h.time_stamp >= cast("2019-01-01" as timestamp) 
     
    ) as t1 
    on m.membernumber = cast(t1.member_number as int) 
     
order by m.transactionprobability desc 
 
 
""") 
s_asset = spark.table("gms.s_asset") 
s_asset.createOrReplaceTempView("s_asset") 
 
s_prod_int = spark.table("gms.s_prod_int") 
s_prod_int.createOrReplaceTempView("s_prod_int") 
 
s_contact_fnx = spark.table("gms.s_contact_fnx") 
s_contact_fnx.createOrReplaceTempView("s_contact_fnx") 
 
s_contact_x = spark.table("gms.s_contact_x") 
s_contact_x.createOrReplaceTempView("s_contact_x") 
 
s_addr_per = spark.table("gms.s_addr_per") 
s_addr_per.createOrReplaceTempView("s_addr_per") 
 
 
blue_edm = spark.sql(""" 
 
select distinct  
 
    c.csn, c.row_id, p.state, p.country, p.zipcode 
 
from gms.s_contact as c 
     
    inner join s_asset as a 
    on a.owner_con_id = c.row_id 
     
    inner join s_prod_int as i 
    on a.prod_id = i.row_id 
     
    inner join s_contact_fnx as f 
    on c.row_id = f.par_row_id 
     
    inner join s_contact_x as x 
    on c.row_id = x.par_row_id 
     
    inner join s_addr_per p 
    on c.pr_per_addr_id = p.row_id 
     
     
where  
    -- blue eligible 
    a.status_cd = "Active" 
    and i.type = "Membership"  
    and i.prod_cd = "Promotion" 
    and c.con_cd in ("Ordinary Member", "Affiliate Member") 
     
    -- marekting consent 
    and (c.x_nrma_title <> "Estate Of The Late" or c.x_nrma_title is null) 
    and (f.deceased_flg = "N" or f.deceased_flg is null) -- Excluded deceased members 
    and (lower(x.attrib_36) in ("yes", "null") or x.attrib_36 is null) -- Email Consent Yes 
    and (c.x_inv_email_1 = "N" or c.x_inv_email_1 is null) -- Valid email 
    and c.email_addr is not null -- valid email 
    and c.cust_stat_cd = "Active"  -- Active record 
    and (f.brloc_attrib13 is null or f.brloc_attrib13 = 'N') -- global opt out 
 
 
""") 
 
blue_edm.createOrReplaceTempView("blue_edm") 
print blue_edm.select('csn').count() 
print blue_edm.count() 
spark.sql(""" 
 
    create table campaign_data.yc_20190305_c_dmc1469_blueeligible_edm as 
         
        select *  
        from blue_edm 
 
""") 
 
spark.catalog.dropTempView("s_asset") 
spark.catalog.dropTempView("s_prod_int") 
spark.catalog.dropTempView("s_contact_fnx") 
spark.catalog.dropTempView("s_contact_x") 
spark.catalog.dropTempView("s_addr_per") 
cx_cam_response = spark.table("gms.cx_cam_response") 
cx_cam_response.createOrReplaceTempView("cx_cam_response") 
 
s_order = spark.table("gms.s_order") 
s_order.createOrReplaceTempView("s_order") 
 
s_order_x = spark.table("gms.s_order_x") 
s_order_x.createOrReplaceTempView("s_order_x") 
 
s_asset = spark.table(" gms.s_asset") 
s_asset.createOrReplaceTempView("s_asset") 
 
s_prod_int = spark.table("gms.s_prod_int") 
s_prod_int.createOrReplaceTempView("s_prod_int") 
 
s_contact = spark.table("gms.s_contact") 
s_contact.createOrReplaceTempView("s_contact") 
 
model_x = spark.table("campaign_data.model_carservicing_crosssell_20190305_nocsin2019") 
model_x.createOrReplaceTempView("model_x") 
 
member_distance = spark.table("campaign_data.member_distance_17122018") 
member_distance.createOrReplaceTempView("member_distance") 
 
outcome_edm_1 = spark.sql(""" 
 
select  
    distinct be.csn, be.country, be.zipcode,  
    model.transactionprobability as probability,  
    model.bucket, 
    model.ranking, 
    t4.name, t4.frombreak, t4.tobreak, t4.code 
from blue_edm as be 
     
    -- remove lapsed car servicing DMC-1101 
    left anti join 
    ( 
        select distinct contact_id 
        from cx_cam_response 
        where    
                campaign_id rlike "24784782" 
            and lower(status_cd) = "sent" 
    ) as t1 
    on t1.contact_id = be.row_id 
     
    -- remove dishonoured people 
    left anti join 
    ( 
        select distinct o.contact_id 
        from s_order as o 
            inner join s_order_x as ox 
            on o.row_id = ox.par_row_id 
        where 
            ox.attrib_05 = "Dishonoured Payment" 
    ) as t2 
    on t2.contact_id = be.row_id 
     
    -- remove members who have CMO or Go bundle products 
    -- remove day 30 and less 
    left anti join 
    ( 
        select distinct s_contact.row_id 
        from  
            s_asset  
            inner join s_prod_int 
            on s_asset.prod_id = s_prod_int.row_id 
             
            inner join s_contact  
            on s_asset.owner_con_id = s_contact.row_id 
         
        where 
            lower(s_prod_int.name) rlike "autoclub" 
            or lower(s_prod_int.name) rlike "bundle" 
            or  
            ( 
                lower(s_prod_int.name) rlike "^membership" 
                and datediff(current_timestamp(), s_asset.start_dt) <= 30 
            ) 
    ) as t3 
    on t3.row_id = be.row_id 
     
    -- join with model 
    inner join model_x as model 
    on cast(model.membernumber as string) = be.csn 
     
     
    -- join with member distance 
    inner join ( 
        select distinct c.row_id, md.name, md.frombreak, md.tobreak, md.code 
        from s_contact c inner join member_distance md 
        on c.pr_per_addr_id = md.addr_id 
    ) as t4 
    on t4.row_id = be.row_id 
 
 
""") 
 
outcome_edm_1.createOrReplaceTempView("outcome_edm_1") 
print outcome_edm_1.select("csn").count() 
print outcome_edm_1.count() 
spark.sql(""" 
 
    create table campaign_data.yc_20190305_c_dmc1469_outcome_edm_1 as 
         
        select *  
        from outcome_edm_1 
 
""") 
 
spark.catalog.dropTempView("cx_cam_response") 
spark.catalog.dropTempView("s_order") 
spark.catalog.dropTempView("s_order_x") 
spark.catalog.dropTempView("s_asset") 
spark.catalog.dropTempView("s_prod_int") 
spark.catalog.dropTempView("s_contact") 
spark.catalog.dropTempView("model_x") 
spark.catalog.dropTempView("member_distance") 
outcome_1 = spark.table("campaign_data.yc_20190305_c_dmc1469_outcome_edm_1") 
outcome_1.createOrReplaceTempView("outcome_1") 
outcome_1_filtered = outcome_1.filter(""" 
 
    lower(nvl(code,"N")) in ( 
        'at', "bv", 'cb', 'ct', "gv", 'gs', 'kt','lp', 'mp', "mk",'np', 
        'ps', "pr",'rd', 'rh', "sh",'sb', 'ss', 'tg', 'wg', 'ww',"hb" 
    ) or zipcode in ("2084", "2097", "2099", "2100", "2101", "2102", 
    "2103", "2104", "2105", "2106", "2107", "2108") 
 
""") 
outcome_1_total = outcome_1_filtered.count() 
outcome_1_filtered.createOrReplaceTempView("outcome_1_filtered") 
 
outcome_1_filtered.groupBy("bucket").count().show(100,False) 
outcome_2 = spark.sql(""" 
     
    select p1.*, 
        case  
            when  
             (p1.zipcode >= '2000' and p1.zipcode <= '2082' or  
              p1.zipcode >= '2000' and p1.zipcode <= '2082' or  
              p1.zipcode >= '2084' and p1.zipcode <= '2234' or  
              p1.zipcode >= '2555' and p1.zipcode <= '2574' or  
              p1.zipcode >= '2745' and p1.zipcode <= '2770' or  
              p1.zipcode >= '2775' and p1.zipcode <= '2775') 
              and (upper(p1.country) = 'AUSTRALIA' or upper(p1.country) = 'AU') 
              then 'METROPOLITAN' 
            when  
             (p1.zipcode >= '2083' and p1.zipcode <= '2083' or  
              p1.zipcode >= '2250' and p1.zipcode <= '2338' or  
              p1.zipcode >= '2415' and p1.zipcode <= '2423' or  
              p1.zipcode >= '2425' and p1.zipcode <= '2425' or  
              p1.zipcode >= '2428' and p1.zipcode <= '2428' or  
              p1.zipcode >= '2500' and p1.zipcode <= '2535' or  
              p1.zipcode >= '2538' and p1.zipcode <= '2541' or  
              p1.zipcode >= '2575' and p1.zipcode <= '2578' or  
              p1.zipcode >= '2600' and p1.zipcode <= '2617' or  
              p1.zipcode >= '2773' and p1.zipcode <= '2774' or  
              p1.zipcode >= '2776' and p1.zipcode <= '2786' or  
              p1.zipcode >= '2900' and p1.zipcode <= '2914') 
              and (upper(p1.country) = 'AUSTRALIA' or upper(p1.country) = 'AU')  
              then  'REGIONAL' 
            when  
              (p1.zipcode >= '2339' and p1.zipcode <= '2411' or  
              p1.zipcode >= '2424' and p1.zipcode <= '2424' or  
              p1.zipcode >= '2426' and p1.zipcode <= '2427' or  
              p1.zipcode >= '2429' and p1.zipcode <= '2490' or  
              p1.zipcode >= '2536' and p1.zipcode <= '2537' or  
              p1.zipcode >= '2545' and p1.zipcode <= '2551' or  
              p1.zipcode >= '2579' and p1.zipcode <= '2594' or  
              p1.zipcode >= '2618' and p1.zipcode <= '2739' or  
              p1.zipcode >= '2787' and p1.zipcode <= '2898' or  
              p1.zipcode >= '6798' and p1.zipcode <= '6799') 
              and (upper(p1.country) = 'AUSTRALIA' or upper(p1.country) = 'AU')  
              then  'RURAL' 
            when  
              (p1.zipcode >= '0800' and p1.zipcode <= '0886' or  
              p1.zipcode >= '3000' and p1.zipcode <= '6770' or  
              p1.zipcode >= '6907' and p1.zipcode <= '7470' or  
              p1.zipcode >= '7471' ) 
              and (upper(p1.country) = 'AUSTRALIA' or upper(p1.country) = 'AU')  
              then  'INTERSTATE' 
            else 'UNKNOWN' 
            end as area 
    from outcome_1_filtered as p1   
         
""") 
 
outcome_2.createOrReplaceTempView("outcome_2") 
 
spark.sql(""" 
    drop table campaign_data.yc_20190305_c_dmc1469_outcome_edm_2  
""") 
spark.sql(""" 
    create table campaign_data.yc_20190305_c_dmc1469_outcome_edm_2 as  
        select *  
        from outcome_2 
""") 
outcome_2 = spark.table("campaign_data.yc_20190305_c_dmc1469_outcome_edm_2") 
outcome_2.createOrReplaceTempView("outcome_2") 
 
outcome_2_filtered = spark.sql(""" 
 
        select * 
        from outcome_2  
        where 
            bucket >= 1 and bucket <= 6 
            -- and ( 
            --    (upper(area) = "METROPOLITAN" and cast(tobreak as int) <= 10000) or  
            --    (upper(area) <> "METROPOLITAN" and cast(tobreak as int) <= 20000) 
            --) 
             
 
""") 
outcome_2_total = outcome_2_filtered.count() 
print outcome_2_filtered.select('csn').distinct().count() 
print outcome_2_filtered.count() 
outcome_2_filtered.createOrReplaceTempView("outcome_2_filtered") 
 
outcome_ctl_2 = outcome_2_filtered.select("csn","bucket","code").sample(False, 0.05, 10000) 
outcome_ctl_2.createOrReplaceTempView("outcome_ctl_2") 
 
print outcome_ctl_2.select('csn').distinct().count() 
print outcome_ctl_2.count() 
 
outcome_ga = spark.sql(""" 
     
    select * 
    from outcome_2_filtered as o 
        left anti join outcome_ctl_2 as ctl 
        on o.csn = ctl.csn 
 
""") 
outcome_ga.createOrReplaceTempView("outcome_ga") 
print outcome_ga.select('csn').distinct().count() 
print outcome_ga.count() 
 
 
spark.sql(""" 
    drop table campaign_data.yc_20190305_c_dmc1469_edm_ctl  
""") 
 
spark.sql(""" 
    create table campaign_data.yc_20190305_c_dmc1469_edm_ctl as  
        select csn, bucket, code, "group_a" as segment 
        from outcome_ctl_2 
""") 
test = spark.table("campaign_data.yc_20190305_c_dmc1469_edm_ctl") 
test.groupBy("segment").count().show() 
print test.select('csn').distinct().count() 
print test.count() 
spark.sql(""" 
    drop table campaign_data.yc_20190305_c_dmc1469_edm_send 
""") 
 
spark.sql(""" 
    create table campaign_data.yc_20190305_c_dmc1469_edm_send as  
        select csn, bucket, code, "group_b" as segment 
        from outcome_ga 
""") 
 
 
 
test = spark.table("campaign_data.yc_20190305_c_dmc1469_edm_send") 
test.groupBy("segment").count().show() 
print test.select('csn').distinct().count() 
print test.count() 
edm_send = spark.table("campaign_data.yc_20190305_c_dmc1469_edm_send") 
edm_send.createOrReplaceTempView("edm_send") 
 
s_contact = spark.table("gms.s_contact") 
s_contact.createOrReplaceTempView("s_contact") 
 
s_contact_fnx = spark.table("gms.s_contact_fnx") 
s_contact_fnx.createOrReplaceTempView("s_contact_fnx") 
 
s_contact_x = spark.table("gms.s_contact_x") 
s_contact_x.createOrReplaceTempView("s_contact_x") 
 
s_addr_per = spark.table("gms.s_addr_per") 
s_addr_per.createOrReplaceTempView("s_addr_per") 
 
final_outcome = spark.sql(""" 
 
 
    select  
        distinct 
        c.csn as member_id, 
        c.row_id as contact_id, 
        case  
            when pr.zipcode in ("2084", "2097", "2099", "2100", "2101", "2102", "2103", "2104", "2105", "2106", "2107", "2108") then "NB" 
            else upper(dm.code) 
            end as store_code, 
        case  
            when lower(dm.code) = "at" then "Artarmon" 
            when lower(dm.code) = "ct" then "Campbelltown" 
            when lower(dm.code) = "cb" then "Caringbah" 
            when lower(dm.code) = "gs" then "Gosford" 
            when lower(dm.code) = "kt" then "Kotara" 
            when lower(dm.code) = "lp" then "Liverpool" 
            when lower(dm.code) in ("mp", "tg") then "Majura Park and Tuggeranong" 
            when lower(dm.code) = "np" then "North Parramatta" 
            when lower(dm.code) = "ps" then "Padstow" 
            when lower(dm.code) = "rd" then "Rockdale" 
            when lower(dm.code) = "rh" then "Rouse Hill" 
            when lower(dm.code) = "sb" then "Shellharbour" 
            when lower(dm.code) = "ss" then "South Strathfield" 
            when lower(dm.code) = "wg" then "Wagga Wagga" 
            when lower(dm.code) = "ww" then "Wollongong" 
            when pr.zipcode in ("2084", "2097", "2099", "2100", "2101", "2102", "2103", "2104", "2105", "2106", "2107", "2108") then "Narrabeen" 
            when lower(dm.code) = "bv" then "Brookvale" 
            when lower(dm.code) = "hb" then "Hornsby" 
            when lower(dm.code) = "mk" then "Marrickville" 
            when lower(dm.code) = "pr" then "Penrith" 
            when lower(dm.code) = "sh" then "Seven Hills" 
            when lower(dm.code) = "gv" then "Gladesville" 
        else "na" end as store_name 
         
    from s_contact as c 
     
        inner join edm_send as dm 
        on dm.csn = c.csn 
         
        inner join s_addr_per as pr 
        on pr.row_id = c.pr_per_addr_id 
         
        inner join s_contact_fnx as f 
        on c.row_id = f.par_row_id 
         
        inner join s_contact_x as x 
        on c.row_id = x.par_row_id 
     
    where  
        (c.x_nrma_title <> "Estate Of The Late" or c.x_nrma_title is null) 
        and (f.deceased_flg = "N" or f.deceased_flg is null) -- Excluded deceased members 
        and (lower(x.attrib_36) in ("yes", "null") or x.attrib_36 is null) -- Email Consent Yes 
        and (c.x_inv_email_1 = "N" or c.x_inv_email_1 is null) -- Valid email 
        and c.email_addr is not null -- valid email 
 
""") 
 
 
print final_outcome.count() 
print final_outcome.select("member_id").distinct().count() 
 
final_outcome.createOrReplaceTempView("final_outcome") 
 
spark.sql(""" 
    drop table campaign_data.yc_20190305_c_dmc1469_edm 
""") 
 
spark.sql(""" 
    create table campaign_data.yc_20190305_c_dmc1469_edm as  
    select * 
    from final_outcome 
""") 
final_outcome_2 = spark.table("campaign_data.yc_20190305_c_dmc1469_edm") 
final_outcome_2.groupBy("store_name").count().show(100,False) 
select count(member_id) 
from campaign_data.yc_20190305_c_dmc1469_edm 
where 
    store_code in ("WW", "WG") 
 
select count(csn) 
from campaign_data.yc_20190305_c_dmc1469_dm_send