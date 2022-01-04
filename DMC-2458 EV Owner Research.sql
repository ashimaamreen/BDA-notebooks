DROP TABLE campaign_data.aa_dmc2458_evmember_research_others_20210712_adhoc; 
create table campaign_data.aa_dmc2458_EVmember_research_others_20210712_adhoc as  
with ev_car as ( 
 
    select "BMW" as vehicle_make, "I3" as vehicle_model union all 
    select "BMW" as vehicle_make, "I8" as vehicle_model union all 
    select "HYUNDAI" as vehicle_make, "IONIQ" as vehicle_model union all 
    select "NISSAN" as vehicle_make, "LEAF" as vehicle_model union all 
    select "RENAULT" as vehicle_make, "ZOE" as vehicle_model union all 
    select "TESLA" as vehicle_make, "MODEL 3" as vehicle_model union all 
    select "TESLA" as vehicle_make, "MODEL S" as vehicle_model union all 
    select "TESLA" as vehicle_make, "MODEL X" as vehicle_model union all 
    select "TESLA" as vehicle_make, "UNKNOWN" as vehicle_model union all 
    select "JAGUAR" as vehicle_make, "I-PACE" as vehicle_model union all 
    select "MERCEDES-BENZ" as vehicle_make, "EQC" as vehicle_model union all 
    select "MERCEDES BENZ" as vehicle_make, "EQC" as vehicle_model 
     
), base as (   
 
select distinct 
        b.membernumber 
        --, case when ev.vehicle_make is null then "no" else "yes" end as has_ev 
        , b.vehicle_make 
        , b.vehicle_model 
        , b.vehicle_rego 
        , b.member_colour 
 
    from sandpit.renewal_base b 
 
    inner join ev_car as ev 
        on upper(b.vehicle_make) = ev.vehicle_make 
        and upper(b.vehicle_model) = ev.vehicle_model 
     
    inner join gms.s_contact c  
        on b.owner_con_id = c.row_id 
 
    where    
            1=1 
        and date_add(b.order_end_dt,2) between "2019-07-01 00:00:00" and now() 
        and COALESCE(b.member_staff, 0)  = 0  
        and membernumber is not null 
        and b.prod_sub_type = 'RSA' 
), main as ( 
SELECT  
    con.row_id AS con_id, 
    b.membernumber, 
    upper(con.fst_name) AS first_name, 
    upper(con.last_name) AS last_name, 
    con.email_addr AS email_address, 
    upper(address.city) as suburb, 
    address.zipcode as postcode, 
    address.state as state, 
    conx.attrib_55 AS colour_plus, 
    conx.attrib_17 AS membership_tenure, 
    YEAR(NOW()) - YEAR(con.birth_dt) AS age, 
    con.sex_mf AS gender, 
    con.cust_value_cd as loyalty_colour, 
    CASE 
        WHEN 
            ( 
                address.zipcode >= '1215' AND address.zipcode <= '2082' OR 
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
    END AS region, 
    rand() as rnd, 
    group_concat(concat_ws(" ",b.vehicle_make, b.vehicle_model)," | ") as EV_Make_model 
 
FROM 
    gms.s_contact AS con 
 
INNER JOIN 
    gms.s_contact_x AS conx 
    ON conx.par_row_id = con.row_id 
 
INNER JOIN 
    gms.s_contact_fnx AS confnx 
    ON confnx.par_row_id = con.row_id 
 
INNER JOIN 
    gms.s_addr_per as address 
    ON address.row_id = con.pr_per_addr_id 
 
inner join base b 
on b.membernumber=con.csn 
     
WHERE 
    con.cust_stat_cd = 'Active' 
    AND con.csn IS NOT NULL 
    AND COALESCE(confnx.deceased_flg,'N') = 'N' 
    AND COALESCE(con.x_nrma_title, '') != 'Estate Of The Late'          --excluding deceased 
    AND COALESCE(con.x_inv_email_1, 'N') = 'N'                          --email validity check 
    AND con.email_addr IS NOT NULL 
    --and membernumber in ('732369101','950574101','339162601') 
group by 1,2,3,4,5,6,7,8,9,10,11,12,13,14 
)  
select distinct con_id as contact_id 
                , membernumber 
                , first_name 
                , last_name 
                , email_address 
                , suburb 
                , postcode 
                , state 
                , colour_plus 
                , EV_Make_model 
                , membership_tenure 
                , age 
                , gender 
                , region 
                 
from main 
where rnd<0.45 
SELECT * from campaign_data.aa_dmc2458_evmember_research_others_20210712_adhoc 
select membernumber,count(*) from campaign_data.aa_dmc2458_evmember_research_others_20210712_adhoc 
group by 1 
order by 2 desc 
spark.sql(""" 
SELECT DISTINCT 
    con.row_id AS con_id, 
    b.membernumber 
    upper(con.fst_name) AS first_name, 
    upper(con.last_name) AS last_name, 
    con.email_addr AS email_address, 
    upper(address.city) as suburb, 
    address.zipcode as postcode, 
    address.state as state, 
    conx.attrib_55 AS colour_plus, 
    b.vehicle_make, 
    b.vehicle_model, 
    conx.attrib_17 AS membership_tenure, 
    YEAR(NOW()) - YEAR(con.birth_dt) AS age, 
    con.sex_mf AS gender, 
    con.con_cd as loyalty_colour, 
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
    END AS region 
 
FROM 
    gms.s_contact AS con 
 
INNER JOIN 
    gms.s_contact_x AS conx 
    ON conx.par_row_id = con.row_id 
 
INNER JOIN 
    gms.s_contact_fnx AS confnx 
    ON confnx.par_row_id = con.row_id 
 
INNER JOIN 
    gms.s_addr_per as address 
    ON address.row_id = con.pr_per_addr_id 
     
WHERE 
    con.cust_stat_cd = 'Active' 
    AND con.csn IS NOT NULL 
    AND COALESCE(confnx.deceased_flg,'N') = 'N' 
    AND COALESCE(con.x_nrma_title, '') != 'Estate Of The Late'          --excluding deceased 
    AND COALESCE(con.x_inv_email_1, 'N') = 'N'                          --email validity check 
    AND con.email_addr IS NOT NULL 
""").createOrReplaceTempView("eligible") 
SELECT *, rand() as rnd 
FROM campaign_data.aa_dmc2458_evmember_research_others_20210712_adhoc 
where membernumber='741618901' 
with main as ( 
SELECT *, rand() as rnd 
FROM campaign_data.aa_dmc2458_evmember_research_others_20210712_adhoc 
) , final as ( 
select distinct con_id 
                , membernumber 
                , first_name 
                , last_name 
                , email_address 
                , suburb 
                , postcode 
                , state 
                , colour_plus 
                , vehicle_make 
                , vehicle_model 
                , membership_tenure 
                , age 
                , gender 
                , region 
from main 
where rnd<0.44 
) select count(*), count(distinct membernumber) from final