with join_offer as ( 
select  distinct  a.owner_con_id ContactID 
        , a.row_id product_asset_row_id 
        , x_nrma_promo_code as join_offer 
 
 
from gms.s_order o 
 
inner join gms.s_order_type ot 
on o.order_type_id = ot.row_id 
 
inner join gms.s_order_item oi 
on o.row_id = oi.order_id 
and oi.ACTION_CD in ('Update','Add') --when they renew only update line but add is upgrade/downgrade and new 
 
inner join gms.s_prod_int p 
on oi.prod_id = p.row_id 
and p.sub_type_cd in ('Non-RSA','RSA') 
and p.prod_cd = 'Product' 
 
INNER JOIN gms.s_asset a 
on oi.asset_integ_id=a.integration_id 
and a.status_cd='Active' 
 
where ot.name = 'New' 
and o.status_cd != 'Revised' 
and x_nrma_promo_code is not null 
) 
select count(*), count(distinct product_asset_row_id) from join_offer 
# Standardizes the inconsistent vehicle years and subtracts them  
# from the current year to calculate their age 
# Ones that cannot be reconciled have their age set to 0 
 
spark.sql(''' 
SELECT 
    row_id,  
    YEAR(NOW()) - INT( 
        CASE  
            WHEN x_year RLIKE '[0-9]{2}-[A-Z]{3}-[0-9]{2}' 
            THEN year(to_timestamp(x_year, 'dd-MMM-yy')) 
     
            WHEN x_year RLIKE '[0-9]{4}' 
                THEN x_year 
     
            ELSE  
                YEAR(NOW()) 
        END 
    ) AS vehicle_age 
     
FROM  
    gms.s_asset 
''').createOrReplaceTempView('vehicle') 
# Retrieves the following features for each upcoming renewal: 
#     - Associated Account 
#     - Due Date 
#     - Asset Integration ID (to uniquely identify them) 
#     - Billing Account Region 
#     - Billing Account State 
#     - Owner Car Servicing History 
#     - Product 
#     - Owner Loyalty Color 
#     - Payment Frequency 
#     - Direct Debit Status 
#     - Owner Membership Tenure 
#     - Vehicle Age 
#     - Owner Value Score 
#     - Asset Churn Score 
#     - Owner “Cancelled due to no MPP payment” Flag 
#     - Owner “Unable to collect a debt” Flag 
#     - Next Renewable Product Pay By Term Eligibility 
#     - Asset Length 
#     - Next Renewable Product 
#     - Current Loyalty 
 
spark.sql(''' 
 
with join_offer as ( 
select  distinct  o.contact_id ContactID 
        , a.row_id product_asset_row_id 
        , x_nrma_promo_code as join_offer 
 
 
from gms.s_order o 
 
inner join gms.s_order_type ot 
on o.order_type_id = ot.row_id 
 
inner join gms.s_order_item oi 
on o.row_id = oi.order_id 
and oi.ACTION_CD in ('Update','Add') --when they renew only update line but add is upgrade/downgrade and new 
 
inner join gms.s_prod_int p 
on oi.prod_id = p.row_id 
and p.sub_type_cd in ('Non-RSA','RSA') 
and p.prod_cd = 'Product' 
 
INNER JOIN gms.s_asset a 
on oi.asset_integ_id=a.integration_id 
and a.status_cd='Active' 
 
where ot.name = 'New' 
and o.status_cd != 'Revised' 
and x_nrma_promo_code is not null 
) 
 
SELECT DISTINCT  
    accnt.row_id AS `Account`, 
    to_date(date_add(asset.end_dt, 1)) AS `Due_Date`, 
    asset.integration_id, 
    CASE 
        WHEN 
            ( 
                addr.zipcode >= "2000" AND addr.zipcode <= "2082" OR 
                addr.zipcode >= "2084" AND addr.zipcode <= "2234" OR 
                addr.zipcode >= "2555" AND addr.zipcode <= "2574" OR 
                addr.zipcode >= "2745" AND addr.zipcode <= "2770" OR 
                addr.zipcode >= "2775" AND addr.zipcode <= "2775" 
            ) 
            AND 
            ( 
                UPPER(addr.country) = "AUSTRALIA" OR 
                UPPER(addr.country) = "AU" 
            ) 
        THEN 
            "METROPOLITAN" 
 
        WHEN 
            ( 
                addr.zipcode >= "2083" AND addr.zipcode <= "2083" OR 
                addr.zipcode >= "2250" AND addr.zipcode <= "2338" OR 
                addr.zipcode >= "2415" AND addr.zipcode <= "2423" OR 
                addr.zipcode >= "2425" AND addr.zipcode <= "2425" OR 
                addr.zipcode >= "2428" AND addr.zipcode <= "2428" OR 
                addr.zipcode >= "2500" AND addr.zipcode <= "2535" OR 
                addr.zipcode >= "2538" AND addr.zipcode <= "2541" OR 
                addr.zipcode >= "2575" AND addr.zipcode <= "2578" OR 
                addr.zipcode >= "2600" AND addr.zipcode <= "2617" OR 
                addr.zipcode >= "2773" AND addr.zipcode <= "2774" OR 
                addr.zipcode >= "2776" AND addr.zipcode <= "2786" OR 
                addr.zipcode >= "2900" AND addr.zipcode <= "2914" 
            ) 
            AND 
            ( 
                UPPER(addr.country) = "AUSTRALIA" OR 
                UPPER(addr.country) = "AU" 
            ) 
        THEN 
            "REGIONAL" 
         
        WHEN 
            ( 
                addr.zipcode >= "2339" AND addr.zipcode <= "2411" OR 
                addr.zipcode >= "2424" AND addr.zipcode <= "2424" OR 
                addr.zipcode >= "2426" AND addr.zipcode <= "2427" OR 
                addr.zipcode >= "2429" AND addr.zipcode <= "2490" OR 
                addr.zipcode >= "2536" AND addr.zipcode <= "2537" OR 
                addr.zipcode >= "2545" AND addr.zipcode <= "2551" OR 
                addr.zipcode >= "2579" AND addr.zipcode <= "2594" OR 
                addr.zipcode >= "2618" AND addr.zipcode <= "2739" OR 
                addr.zipcode >= "2787" AND addr.zipcode <= "2898" OR 
                addr.zipcode >= "6798" AND addr.zipcode <= "6799" 
            ) 
            AND 
            ( 
                UPPER(addr.country) = "AUSTRALIA" OR 
                UPPER(addr.country) = "AU" 
            ) 
        THEN 
            "RURAL" 
         
        WHEN 
            ( 
                addr.zipcode >= "0800" AND addr.zipcode <= "0886" OR 
                addr.zipcode >= "3000" AND addr.zipcode <= "6770" OR 
                addr.zipcode >= "6907" AND addr.zipcode <= "7470" OR 
                addr.zipcode >= "7471"  
            ) 
            AND 
            ( 
                UPPER(addr.country) = "AUSTRALIA" OR 
                UPPER(addr.country) = "AU" 
            ) 
        THEN 
            "INTERSTATE" 
 
        ELSE 
            "UNKNOWN" 
    END AS `Region`, 
    addr.state AS `State`, 
    j.join_offer AS `Car_Servicing_Customer`, 
    prod.name AS `Product`, 
    CASE  
        WHEN con.cust_value_cd = 'Gold Life' THEN 'Gold Life' 
        WHEN conx.attrib_17 >= 49 THEN 'Gold 50+' 
        WHEN conx.attrib_17 >= 24 THEN 'Gold' 
        WHEN conx.attrib_17 >= 9 THEN 'Silver' 
        ELSE 'Member' 
    END AS `Loyalty`, 
    CASE 
        WHEN asset.x_nrma_pay_by_term = 'Y' THEN 'Pay By Term' 
        ELSE 'Annual' 
    END AS `Asset_Payment_Frequency`, 
    CASE 
        WHEN (asset.bill_profile_id IS NOT NULL AND asset.x_nrma_pay_by_term != 'Y') THEN 'Y' 
        ELSE 'N' 
    END AS `Asset_Direct_Debit`, 
    CASE WHEN conx.attrib_19 < 1 THEN 0 ELSE 1 END AS `Membership_Tenure`,  
    veh.vehicle_age AS `Vehicle_Age`, 
    value.cvm_value_score AS `Value`, 
    churn.churn_score AS `Churn`, 
    COALESCE(con.x_nrma_bad_debt, 'N') AS x_nrma_bad_debt, 
    COALESCE(con.x_nrma_dishonor_flg, 'N') AS x_nrma_dishonor_flg, 
    nrp.x_paybyterm, 
    YEAR(date_add(asset.end_dt, 1)) - YEAR(asset.start_dt) AS length, 
    COALESCE(nrp.name, "N/A") AS `Next_Renewable_Product`, 
    con.cust_value_cd AS `Current_Loyalty` 
     
FROM 
    gms.s_asset AS asset 
     
INNER JOIN 
    gms.s_prod_int AS prod 
    ON prod.row_id = asset.prod_id 
 
LEFT JOIN join_offer j 
    on product_asset_row_id=asset.row_id 
 
LEFT OUTER JOIN 
    gms.s_prod_int AS nrp 
    ON nrp.row_id = asset.x_next_renewal_prod_id 
 
INNER JOIN 
    gms.s_contact AS con 
    ON con.row_id = asset.owner_con_id 
 
INNER JOIN 
    gms.s_contact_x AS conx 
    ON conx.par_row_id = con.row_id 
 
INNER JOIN 
    gms.s_contact_fnx AS confnx 
    ON confnx.par_row_id = con.row_id 
 
INNER JOIN 
    gms.s_org_ext AS accnt 
    ON accnt.row_id = asset.owner_accnt_id 
 
INNER JOIN 
    gms.s_org_ext AS billing 
    ON billing.par_ou_id = accnt.row_id 
     
INNER JOIN 
    gms.s_addr_per AS addr 
    ON addr.row_id = billing.pr_addr_id 
 
LEFT JOIN 
    gms.cx_cvm_value AS value 
    ON value.contact_row_id = con.row_id 
 
LEFT JOIN 
    gms.cx_cvm_churn AS churn 
    ON churn.asset_row_id = asset.row_id 
 
LEFT OUTER JOIN  
    vehicle AS veh 
    ON veh.row_id = asset.service_point_id 
 
WHERE  
    asset.status_cd = "Active" 
    AND prod.prod_cd != "Promotion" 
    AND (asset.asset_num IS NOT NULL OR prod.name = "Join on Road") 
    AND prod.sub_type_cd IN ('RSA', 'Non-RSA') 
    AND asset.x_next_renewal_prod_id IS NOT NULL 
    AND con.csn IS NOT NULL 
    AND COALESCE(confnx.deceased_flg, 'N') = 'N' 
    AND COALESCE(con.x_nrma_title, '') != 'Estate Of The Late' 
    AND COALESCE(conx.attrib_44, '') NOT RLIKE 'Honorary' 
    AND (date_add(asset.end_dt, 1) BETWEEN date_add(NOW(), 1) AND date_add(NOW(), 91)) 
''').createOrReplaceTempView('subs') 
# Calculates Order Pay By Term Eligibility using 
#     - Owner “Cancelled due to no MPP payment” Flag 
#     - Owner “Unable to collect a debt” Flag 
#     - Owner Loyalty Color 
#     - Next Renewable Product Pay By Term Eligibility 
#     - Direct Debit Status 
#     - Asset Length 
#     - Payment Frequency 
 
spark.sql(''' 
SELECT 
    `Account`, 
    `Due_Date`, 
    `Asset_Payment_Frequency`, 
    CASE 
        WHEN 
        ( 
            MIN(x_nrma_bad_debt != 'Y') AND  
            MIN(x_nrma_dishonor_flg != 'Y') AND  
            MIN(`Loyalty` NOT IN ('Gold Life', 'Gold 50+')) AND  
            COALESCE(MIN(x_paybyterm = 'Y'), False) AND  
            MIN(`Asset_Direct_Debit` = 'N') AND  
            MIN(length <= 1) AND  
            MIN(`Asset_Payment_Frequency` = 'Annual') 
        ) 
        THEN  
            "Y" 
             
        ELSE 
            "N" 
    END AS `Order_Eligible_for_Pay_By_Term` 
 
FROM 
    subs 
 
GROUP BY  
    `Account`, 
    `Due_Date`, 
    `Asset_Payment_Frequency` 
''').createOrReplaceTempView('orders') 
# Recombines the Order-Level Pay By Term Eligibility data with  
# the Asset-Level data and adds the current date so that the OAC dashboard can show  
# when it was last refreshed. Feel free to discard the `Churn And Value Scores Correct As Of` 
# field if there's a more elegant way of getting OAC to show when its data was last refreshed 
 
# The full output of the query below should be used to refresh the OAC dashboard located in: 
# /Shared Folders/NRMA/CVM/CVM1b Eligibility Pipeline 
 
 
df = spark.sql(''' 
SELECT 
 `Account`, 
    `Due_Date`, 
    `Region`, 
    `State`, 
    `Car_Servicing_Customer`, 
    `Product`, 
    `Next_Renewable_Product`, 
    `Loyalty`, 
    `Asset_Payment_Frequency`, 
    `Asset_Direct_Debit`, 
    `Order_Eligible_for_Pay_By_Term`, 
    `Membership_Tenure`, 
    `Vehicle_Age`, 
    `Value`, 
    `Churn`, 
    date(NOW()) AS `Churn And Value Scores Correct As Of` 
     
FROM 
    subs 
     
INNER JOIN 
    orders 
    USING (`Account`, `Due_Date`, `Asset_Payment_Frequency`) 
''').collect() 
columns = ['Account', 
    'Due_Date', 
    'Region', 
    'State', 
    'Car_Servicing_Customer', 
    'Product', 
    'Next_Renewable_Product', 
    'Loyalty', 
    'Asset_Payment_Frequency', 
    'Asset_Direct_Debit', 
    'Order_Eligible_for_Pay_By_Term', 
    'Membership_Tenure', 
    'Vehicle_Age', 
    'Value', 
    'Churn', 
'Churn_And_Value_Scores_Correct_As_Of'] 
 
 
print(len(df)) 
 
seg = 0 
mult = 100000 
###for i in range(mult*seg,len(df) for last 
print(seg) 
print('') 
 
print(','.join(columns)) 
 
for i in range(mult*seg,len(df)): 
    print(','.join([str(df[i][c]) for c in columns])) 
     
seg += 1 
spark.sql(''' 
create table aggregator.aggregator.cvm_1b_eligiblity_matrix as 
SELECT 
 `Account`, 
    `Due_Date`, 
    `Region`, 
    `State`, 
    `Car_Servicing_Customer`, 
    `Product`, 
    `Next_Renewable_Product`, 
    `Loyalty`, 
    `Asset_Payment_Frequency`, 
    `Asset_Direct_Debit`, 
    `Order_Eligible_for_Pay_By_Term`, 
    `Membership_Tenure`, 
    `Vehicle_Age`, 
    `Value`, 
    `Churn`, 
    date(NOW()) AS `Churn_And_Value_Scores_Correct_As_Of` 
     
FROM 
    subs 
     
INNER JOIN 
    orders 
    USING (`Account`, `Due_Date`, `Asset_Payment_Frequency`) 
''').repartition(1) 
DROP table aggregator.cvm_1b_eligiblity_matrix 
SELECT * FROM aggregator.20201028_cvm_1b_eligiblity_matrix LIMIT 100; 
SELECT * FROM aggregator.cvm_1b_eligiblity_matrix LIMIT 100; 