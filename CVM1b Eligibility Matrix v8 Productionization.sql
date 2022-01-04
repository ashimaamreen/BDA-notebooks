with vehicle_age as (
    SELECT
    row_id, 
    EXTRACT(YEAR FROM current_timestamp()) - (
        CASE 
            WHEN REGEXP_CONTAINS(x_year,'[0-9]{2}-[A-Z]{3}-[0-9]{2}')
            THEN EXTRACT(YEAR FROM parse_datetime( '%d-%b-%y',x_year))
    
            WHEN REGEXP_CONTAINS(x_year,'[0-9]{4}')
                THEN cast(x_year as INT)
    
            ELSE 
                EXTRACT(YEAR FROM current_timestamp())
        END
    ) AS vehicle_age 
    
FROM 
    gms.s_asset
) , renewing_asset as (
    SELECT DISTINCT 
    accnt.row_id AS `Account`,
    date((date_add(asset.end_dt, interval 1 day))) AS Due_Date,
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
    con.x_motosmart_cust_flg AS `Car_Servicing_Customer`,
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
    EXTRACT(YEAR FROM (date_add(asset.end_dt, interval 1 day))) - EXTRACT(YEAR FROM(asset.start_dt)) AS length,
    COALESCE(nrp.name, "N/A") AS `Next_Renewable_Product`,
    con.cust_value_cd AS `Current_Loyalty`
    
FROM
    gms.s_asset AS asset
    
INNER JOIN
    gms.s_prod_int AS prod
    ON prod.row_id = asset.prod_id

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
    vehicle_age  AS veh
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
    AND NOT regexp_contains(COALESCE(conx.attrib_44, ''), 'Honorary')
    AND (date_add(asset.end_dt, interval 1 day) BETWEEN date_add(current_date('Australia/Melbourne'), interval 1 day) AND date_add(current_date('Australia/Melbourne'), interval 91 day))

) , orders as (
    SELECT
    Account,
    Due_Date,
    Asset_Payment_Frequency,
    CASE
        WHEN
        (
            MIN(x_nrma_bad_debt != 'Y') AND 
            MIN(x_nrma_dishonor_flg != 'Y') AND 
            MIN(Loyalty NOT IN ('Gold Life', 'Gold 50+')) AND 
            COALESCE(MIN(x_paybyterm = 'Y'), False) AND 
            MIN(Asset_Direct_Debit = 'N') AND 
            MIN(length <= 1) AND 
            MIN(Asset_Payment_Frequency = 'Annual')
        )
        THEN 
            "Y"
            
        ELSE
            "N"
    END AS Order_Eligible_for_Pay_By_Term

FROM
    renewing_asset 

GROUP BY 
    1,2,3
)
--CREATE table campaign_data.aa_cvm_final as
SELECT
    s.Account,
    s.Due_Date,
    s.Region,
    State,
    Car_Servicing_Customer,
    Product,
    Next_Renewable_Product,
    Loyalty,
    s.Asset_Payment_Frequency,
    s.Asset_Direct_Debit,
    Order_Eligible_for_Pay_By_Term,
    Membership_Tenure,
    Vehicle_Age,
    Value,
    Churn,
    current_date('Australia/Melbourne') AS Churn_And_Value_Scores_Correct_As_Of
    
FROM
    renewing_asset  s
    
INNER JOIN
    orders o
    USING (Account, Due_Date, Asset_Payment_Frequency)