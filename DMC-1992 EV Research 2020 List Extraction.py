spark.sql(""" 
SELECT 
    distinct  
    con.fst_name AS first_name, 
    con.last_name AS last_name, 
    con.email_addr AS email_address, 
    address.city AS suburb,  
    CASE 
        WHEN 
            ( 
                address.zipcode >= '2000' AND address.zipcode <= '2082' OR 
                address.zipcode >= '2084' AND address.zipcode <= '2234' OR 
                address.zipcode >= '2555' AND address.zipcode <= '2574' OR 
                address.zipcode >= '2745' AND address.zipcode <= '2770' OR 
                address.zipcode >= '2775' AND address.zipcode <= '2775' 
            ) 
            AND 
            ( 
                UPPER(address.country) = 'AUSTRALIA' OR 
                UPPER(address.country) = 'AU' 
            ) 
        THEN 
            'METROPOLITAN' 
        WHEN 
            ( 
                address.zipcode >= '2083' AND address.zipcode <= '2083' OR 
                address.zipcode >= '2250' AND address.zipcode <= '2338' OR 
                address.zipcode >= '2415' AND address.zipcode <= '2423' OR 
                address.zipcode >= '2425' AND address.zipcode <= '2425' OR 
                address.zipcode >= '2428' AND address.zipcode <= '2428' OR 
                address.zipcode >= '2500' AND address.zipcode <= '2535' OR 
                address.zipcode >= '2538' AND address.zipcode <= '2541' OR 
                address.zipcode >= '2575' AND address.zipcode <= '2578' OR 
                address.zipcode >= '2600' AND address.zipcode <= '2617' OR 
                address.zipcode >= '2773' AND address.zipcode <= '2774' OR 
                address.zipcode >= '2776' AND address.zipcode <= '2786' OR 
                address.zipcode >= '2900' AND address.zipcode <= '2914' 
            ) 
            AND 
            ( 
                UPPER(address.country) = 'AUSTRALIA' OR 
                UPPER(address.country) = 'AU' 
            ) 
        THEN 
            'REGIONAL' 
        WHEN 
            ( 
                address.zipcode >= '2339' AND address.zipcode <= '2411' OR 
                address.zipcode >= '2424' AND address.zipcode <= '2424' OR 
                address.zipcode >= '2426' AND address.zipcode <= '2427' OR 
                address.zipcode >= '2429' AND address.zipcode <= '2490' OR 
                address.zipcode >= '2536' AND address.zipcode <= '2537' OR 
                address.zipcode >= '2545' AND address.zipcode <= '2551' OR 
                address.zipcode >= '2579' AND address.zipcode <= '2594' OR 
                address.zipcode >= '2618' AND address.zipcode <= '2739' OR 
                address.zipcode >= '2787' AND address.zipcode <= '2898' OR 
                address.zipcode >= '6798' AND address.zipcode <= '6799' 
            ) 
            AND 
            ( 
                UPPER(address.country) = 'AUSTRALIA' OR 
                UPPER(address.country) = 'AU' 
            ) 
        THEN 
            'RURAL' 
        WHEN 
            ( 
                address.zipcode >= '0800' AND address.zipcode <= '0886' OR 
                address.zipcode >= '3000' AND address.zipcode <= '6770' OR 
                address.zipcode >= '6907' AND address.zipcode <= '7470' OR 
                address.zipcode >= '7471'  
            ) 
            AND 
            ( 
                UPPER(address.country) = 'AUSTRALIA' OR 
                UPPER(address.country) = 'AU' 
            ) 
        THEN 
            'INTERSTATE' 
        ELSE 
            'UNKNOWN' 
    END AS area, 
    YEAR(NOW()) - YEAR(con.birth_dt) AS age, 
    con.sex_mf AS gender, 
    conx.attrib_55 AS colour_plus, 
    conx.attrib_17 AS membership_tenure, 
    con.csn AS member_id 
     
 
FROM 
    gms.s_contact AS con 
 
INNER JOIN 
    gms.s_contact_x AS conx 
    ON conx.par_row_id = con.row_id 
 
INNER JOIN 
    gms.s_contact_fnx AS confnx 
    ON confnx.par_row_id = con.row_id 
 
INNER JOIN 
    gms.s_asset AS asset 
    ON asset.owner_accnt_id = con.pr_dept_ou_id 
 
INNER JOIN 
    gms.s_prod_int AS prod 
    ON prod.row_id = asset.prod_id 
     
INNER JOIN 
    gms.s_addr_per AS address 
    ON address.row_id = con.pr_per_addr_id 
 
WHERE 
    asset.status_cd = 'Active' 
    AND prod.type = 'Membership' 
    AND prod.prod_cd = 'Promotion' 
    AND con.cust_stat_cd = 'Active' 
    AND con.csn IS NOT NULL 
    AND con.con_cd IN ('Ordinary Member') 
    AND COALESCE(confnx.deceased_flg,'N') = 'N' 
    AND COALESCE(con.x_nrma_title, '') != 'Estate Of The Late' 
    AND COALESCE(con.x_inv_email_1, 'N') = 'N' 
    AND con.email_addr IS NOT NULL 
    AND address.state in ('NSW','ACT') 
""").createOrReplaceTempView("eligible") 
eligible = spark.sql(""" 
SELECT * FROM eligible 
WHERE AREA in ('METROPOLITAN','REGIONAL','RURAL') 
""") 
 
eligible.sample(False, 20100.0/eligible.count(), 0).repartition(1).write.saveAsTable("campaign_data.ek_dmc1992_ev_research_others_20200508_adhoc")  
--select count(*) from campaign_data.ek_dmc1992_ev_research_others_2020325_adhoc; 
select count(distinct member_id) from campaign_data.ek_dmc1992_ev_research_others_20200508_adhoc; 
--select age, count(*) from campaign_data.ek_dmc1992_ev_research_others_20200508_adhoc group by age order by age 
--select area, count(*) from campaign_data.ek_dmc1992_ev_research_others_20200508_adhoc group by area order by 1 
select gender, count(*) from campaign_data.ek_dmc1992_ev_research_others_20200508_adhoc group by gender order by 1 
eligible.groupby('gender').count().show(5,False) 
#eligible.groupby('area').count().show(5,False) 
#eligible.groupby('age').count().show(150,False) 
select first_name, last_name, email_address, suburb, area, age, gender, colour_plus, membership_tenure,  member_id 
from campaign_data.ek_dmc1992_ev_research_others_20200508_adhoc; 
select distinct table_id, table_name from campaign_data.campaign_contact_adhoc where table_name like '%ek_dmc1992%' 
--select  col5, count(*) from campaign_data.campaign_contact_adhoc where table_id = '70' group by col5; 
 
         
 select  member_id , count(*) 
from campaign_data.campaign_contact_adhoc where table_id = '71'  
group by member_id  
order by 2 desc 
select count(distinct member_id)  
from campaign_data.campaign_contact_adhoc where table_id = '71' 
 
--Output script for OB 
select  distinct member_id, 
        col1 as first_name,  
        col2 as last_name,  
        col3 as email_address, 
        col4 as suburb, 
        col5 as area,  
        col6 as age, 
        col7 as gender, 
        col8 as colourplus, 
        col9 as membertenure 
from campaign_data.campaign_contact_adhoc where table_id = '71' 
drop table campaign_data.ek_dmc1992_ev_research_others_2020324_adhocv1 purge; 
select  distinct member_id,         col1 as first_name,          col2 as last_name,          col3 as email_address,         col4 as suburb,         col5 as area,          col6 as age,         col7 as gender,         col8 as colourplus,         col9 as membertenure from campaign_data.campaign_contact_adhoc where table_id = '71' 
select  count(*)  
from campaign_data.campaign_contact_adhoc where table_id = '71' 