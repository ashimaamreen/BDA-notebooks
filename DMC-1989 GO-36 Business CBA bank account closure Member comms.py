CBA_Accounts = spark.read.load("/user/aamreen/CBA_Business_Members_2020_02_29.csv", format="csv",sep=",",inferSchema="true", header="true") 
CBA_Accounts.createOrReplaceTempView("CBA_Accounts") 
spark.sql(""" select * from CBA_Accounts """).show() 
spark.sql(""" select * from CBA_Accounts """).count() 
spark.sql(""" 
create table campaign_data.aa_dmc1989_CBAaccounts_20200326 as 
select * from CBA_Accounts  
""").count() 
spark.sql(""" 
Select distinct ou_num as membernumber, x_legal_entity_name from gms.s_org_ext 
where x_legal_entity_name in (select Acc_name from CBA_Accounts where acc_type='Non-ADM') 
""").count() 
spark.sql(""" 
Select distinct ou_num as membernumber, x_legal_entity_name as business_name,b.*  
from gms.s_org_ext a 
 
inner join CBA_Accounts b 
on b.acc_name=a.x_legal_entity_name 
and acc_type='Non-ADM' 
""").count() 
SELECT b.* from campaign_data.aa_dmc1989_CBAaccounts_20200326 b 
 
LEFT ANTI JOIN gms.s_org_ext o 
on o.x_legal_entity_name = b.acc_name 
WITH cba_account as ( 
 
select distinct org.ou_num as membernumber, org.x_legal_entity_name as business_name,b.*  
 
from gms.s_org_ext org 
 
inner join campaign_data.aa_dmc1989_CBAaccounts_20200326 b          --587 
on b.acc_name=org.x_legal_entity_name 
and acc_type='Non-ADM'   
 
inner join gms.s_contact con                                        --584 
on con.row_id = org.pr_con_id 
 
-- inner join gms.s_contact_fnx as confnx                              --267 
-- on confnx.par_row_id = con.row_id  
 
-- inner join gms.s_contact_x as conx                                  --256 
-- on conx.par_row_id = con.row_id 
 
where 1=1 
--and (org.X_INVLD_EMAIL_ADDR is null or org.X_INVLD_EMAIL_ADDR='N') 
-- AND COALESCE(conx.attrib_36, "") != "No" -- Opt-out 
-- AND COALESCE(confnx.brloc_attrib13, "") != "Y" --THEN "Yes" else "No" end ="No" -- Global opt out  
-- AND (con.x_inv_email_1 = "N" OR con.x_inv_email_1 IS NULL)  -- Valid email  --238 
-- AND con.email_addr IS NOT NULL                                              --199 
-- AND COALESCE(confnx.deceased_flg, "N") = "N" -- Deceased                    --199 
-- AND COALESCE(con.x_nrma_title, "") != "Estate Of The Late"  
) 
SELECT count(*) FROM cba_account 
WITH cba_account as ( 
 
select distinct org.ou_num as membernumber, org.x_legal_entity_name as business_name,b.*  
 
from gms.s_org_ext org 
 
inner join campaign_data.aa_dmc1989_CBAaccounts_20200326 b          --587 
on b.acc_name=org.x_legal_entity_name 
and acc_type='Non-ADM'   
 
inner join gms.s_contact con                                        --584 
on con.row_id = org.pr_con_id 
 
-- inner join gms.s_contact_fnx as confnx                              --267 
-- on confnx.par_row_id = con.row_id  
 
-- inner join gms.s_contact_x as conx                                  --256 
-- on conx.par_row_id = con.row_id 
 
where 1=1 
--and (org.X_INVLD_EMAIL_ADDR is null or org.X_INVLD_EMAIL_ADDR='N') 
-- AND COALESCE(conx.attrib_36, "") != "No" -- Opt-out 
-- AND COALESCE(confnx.brloc_attrib13, "") != "Y" --THEN "Yes" else "No" end ="No" -- Global opt out  
-- AND (con.x_inv_email_1 = "N" OR con.x_inv_email_1 IS NULL)  -- Valid email  --238 
-- AND con.email_addr IS NOT NULL                                              --199 
-- AND COALESCE(confnx.deceased_flg, "N") = "N" -- Deceased                    --199 
-- AND COALESCE(con.x_nrma_title, "") != "Estate Of The Late"  
) 
SELECT count(*) FROM cba_account 
select distinct X_INVLD_EMAIL_ADDR from gms.s_org_ext org 
where (org.X_INVLD_EMAIL_ADDR is null or org.X_INVLD_EMAIL_ADDR='N') 
spark.sql(""" 
 
Select distinct org.ou_num as membernumber, org.x_legal_entity_name as business_name,b.*  
from gms.s_contact con 
 
 
inner join gms.s_org_ext org 
on con.row_id = org.pr_con_id 
 
inner join CBA_Accounts b 
on b.acc_name=org.x_legal_entity_name 
and acc_type='Non-ADM' 
 
inner join gms.s_contact_fnx as confnx  
on confnx.par_row_id = con.row_id  
 
inner join gms.s_contact_x as conx 
on conx.par_row_id = con.row_id 
 
where 1=1 
-- AND COALESCE(conx.attrib_36, "") != "No" -- Opt-out 
-- AND COALESCE(confnx.brloc_attrib13, "") != "Y" --THEN "Yes" else "No" end ="No" -- Global opt out  
AND (con.x_inv_email_1 = "N" OR con.x_inv_email_1 IS NULL)  -- Valid email 
AND con.email_addr IS NOT NULL  
AND COALESCE(confnx.deceased_flg, "N") = "N" -- Deceased 
AND COALESCE(con.x_nrma_title, "") != "Estate Of The Late"  
 
 
""").count() 
select count(*), count(distinct acc_name) from campaign_data.aa_dmc1989_cbaaccounts_20200326 
where acc_type='Non-ADM'   
WITH cba_account as ( 
 
select DISTINCT b.acc_name,org.pr_con_id,org.ou_num as membernumber, org.x_legal_entity_name as business_name , con.row_id 
 
from campaign_data.aa_dmc1989_CBAaccounts_20200326 b                    --589    
 
INNER join gms.s_org_ext org                                            --571 
on b.acc_name=org.x_legal_entity_name 
 
INNER join gms.s_contact con                                            --571 
on con.row_id = org.pr_con_id 
 
LEFT ANTI join gms.s_contact_fnx as confnx                                  --263 
on confnx.par_row_id = con.row_id  
 
 
where 1=1 
and acc_type='Non-ADM'   
--and (org.X_INVLD_EMAIL_ADDR is null or org.X_INVLD_EMAIL_ADDR='N') 
-- AND COALESCE(conx.attrib_36, "") != "No" -- Opt-out 
-- AND COALESCE(confnx.brloc_attrib13, "") != "Y" --THEN "Yes" else "No" end ="No" -- Global opt out  
-- AND (con.x_inv_email_1 = "N" OR con.x_inv_email_1 IS NULL)  -- Valid email  --238 
-- AND con.email_addr IS NOT NULL                                              --199 
-- AND COALESCE(confnx.deceased_flg, "N") = "N" -- Deceased                    --199 
AND COALESCE(con.x_nrma_title, "") != "Estate Of The Late"  
) 
SELECT * FROM cba_account --order by acc_name 
--where membernumber='8132' 
SELECT DISTINCT par_row_id from gms.s_contact_fnx 
where par_row_id='1-18S-415 
 
' 
select distinct c.row_id, o.pr_con_id, o.x_legal_entity_name, c.csn, o.ou_num from gms.s_contact c 
inner join gms.s_org_ext o 
on o.pr_con_id=c.row_id 
 
where o.x_legal_entity_name ='TEMORA SHIRE COUNCIL' 
WITH cba_account as ( 
 
select DISTINCT b.acc_name 
               -- , org.row_id as account_id  
                , org.pr_con_id primary_contact_id 
                , org.ou_num as account_membernumber 
                , org.x_legal_entity_name as Account_Leagl_entity_name  
 
from campaign_data.aa_dmc1989_CBAaccounts_20200326 b                    --589    
 
INNER join gms.s_org_ext org                                            --571 
on b.acc_name=org.x_legal_entity_name 
 
-- INNER JOIN gms.s_contact con                                            --571 
-- on con.row_id = org.pr_con_id 
 
 
where 1=1 
and acc_type='Non-ADM'   
--and (org.X_INVLD_EMAIL_ADDR is null or org.X_INVLD_EMAIL_ADDR='N') 
-- AND COALESCE(conx.attrib_36, "") != "No" -- Opt-out 
-- AND COALESCE(confnx.brloc_attrib13, "") != "Y" --THEN "Yes" else "No" end ="No" -- Global opt out  
-- AND (con.x_inv_email_1 = "N" OR con.x_inv_email_1 IS NULL)  -- Valid email  --238 
-- AND con.email_addr IS NOT NULL                                              --199 
-- AND COALESCE(confnx.deceased_flg, "N") = "N" -- Deceased                    --199 
--AND COALESCE(con.x_nrma_title, "") != "Estate Of The Late"  
) 
SELECT  * FROM cba_account  
where account_membernumber='10028'	 
1-4GNIB9W 
 
What can be added to our template - what is missing 
identify cluster analysis -  
high usage clusters 
low usage clusters 
important partners - 
opportunities in the market - what are we missing 
using our member data with consumer info roymorgan/abs/mosiac 
Product positioning overlay of benefits 
 
 
GOAL:  
-Product opportunities mapping for consumer memberbase categories 
-Overlay consumers 
-new partner benefits coming on board or revamp 
 
 
Web behaviour analysis 
- member types what is there behaviour and whatthey do 
member non-member mix 
 
Volume for stakeholders 
ROI for internal 
 
What space can i do fill in that the team cannot think about it 
 
 
CBA_Accounts_missing = spark.read.load("/user/aamreen/Bwise Missing Entity Name Accounts_20200401.csv", format="csv",sep=",",inferSchema="true", header="true") 
CBA_Accounts_missing.createOrReplaceTempView("CBA_Accounts_missing") 
spark.sql(""" select * from CBA_Accounts_missing """).show() 
spark.sql(""" select * from CBA_Accounts_missing """).count() 
spark.sql(""" 
 
Select distinct org.ou_num as membernumber, org.x_legal_entity_name as business_name,b.*  
from gms.s_org_ext org 
 
inner join CBA_Accounts_missing b 
on b.Account_name=org.x_legal_entity_name 
 
inner 
 
""").count() 
SELECT row_id from gms.s_contact 
where row_id in ('1-18S-415','1-19U-806','1-19M-741')