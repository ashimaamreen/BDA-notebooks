# Proof of concept script that creates a minimally viable contact history view  
#     for the Late Payers OTM Campaign using the Avaya Tables 
#  
# Load Date is the date in which a record was loaded into Avaya 
#     it is analogous to file_created_date 
# Customized Field 2 is the maximum churn score for an order 
# Customized Field 3 is the primary contact's value score for an order 
# Customized Fields 2 and 3 are NULL for HUG as it does not use CVM scores 
#  
# More fields can be found in avayaoutbound.pim_attribute 
#     using them is as simple as adding another LEFT JOIN using the field's code 
# Campaign codes can be looked up in avayaoutbound.pim_contact_store 
#     Store Code 35 is HRHV 
#     Store Code 5  is BAU 
#     Store Code 13 is HUG 
#      
# Please note that by joining with the two tables mentioned above,  
#     campaign and field names can be looked up in english, though it is less  
#     efficient especially in the latter case as it would add an additional join  
#     for every single added field 
 
spark.sql(''' 
SELECT 
    pimcon.first_name, 
    pimcon.last_name, 
    pimcon.user_contact_id, 
    pimcon.pim_contact_store_id, 
    TO_DATE(MAX(created_dt.attribute_value), 'dd-MM-yyyy') AS load_date, 
    MAX(churn.attribute_value) AS churn, 
    MAX(value.attribute_value) AS value, 
    MAX(ord.attribute_value) AS ord, 
    MAX(con.attribute_value) AS con 
 
FROM 
    avayaoutbound.pim_contact AS pimcon 
     
LEFT OUTER JOIN 
    avayaoutbound.pim_contact_attribute AS created_dt 
    ON  created_dt.contact_id = pimcon.contact_id 
    AND created_dt.attribute_id = 161 
     
LEFT OUTER JOIN 
    avayaoutbound.pim_contact_attribute AS churn 
    ON  churn.contact_id = pimcon.contact_id 
    AND churn.attribute_id = 131 
     
LEFT OUTER JOIN 
    avayaoutbound.pim_contact_attribute AS value 
    ON  value.contact_id = pimcon.contact_id 
    AND value.attribute_id = 132 
     
LEFT OUTER JOIN 
    avayaoutbound.pim_contact_attribute AS ord 
    ON  ord.contact_id = pimcon.contact_id 
    AND ord.attribute_id = 127 
 
LEFT OUTER JOIN 
    avayaoutbound.pim_contact_attribute AS con 
    ON  con.contact_id = pimcon.contact_id 
    AND con.attribute_id = 101 
 
 
WHERE 
    pimcon.pim_contact_store_id IN (35) 
     
GROUP BY 
    pimcon.first_name, 
    pimcon.last_name, 
    pimcon.user_contact_id, 
    pimcon.pim_contact_store_id 
 
HAVING 
    TO_DATE(MAX(created_dt.attribute_value), 'dd-MM-yyyy') = DATE_SUB(NOW(), 1) 
''').show(2500, False) 
# Run the BDA script for whatever you're doing, saving the results (could just be the order number) to a table 
# the earlier in the day you run it, the better (because the longer between the scheduled data warehouse run and  
# your manual run, the more time there is for people's status to change, leading to more natural discrepancies 
# between the two. 
 
# Wait at least one day (there's a 1 day latency on Avaya data coming into BDA) and run the above script with  
# your campaign's pimcon store code (refer to comments in the script) and the date you're examining. 
# You can now use various joins (FULL OUTER, LEFT, ANTI, etc.) to identify -who- is in one output and not the other 
# and then investigate using BDA and Siebel. 