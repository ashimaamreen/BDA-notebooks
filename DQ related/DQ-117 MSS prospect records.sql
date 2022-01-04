SELECT count(1), 
con_cd 
FROM gms.s_contact 
WHERE 1=1 
AND (gms.s_contact.fst_name LIKE 'MSS First Name' 
    OR last_name iLIKE 'MSS Last Name') 
AND cust_stat_cd = 'Active' 
GROUP BY con_cd 