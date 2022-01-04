select * from omc.send_level_summary 
where campaign_id='41223942' 
select distinct contact_id, 'Y' flag from omc.member_campaign_summary_table 
where campaign_id='41223942' 
and program_id in ('43228922','41225742') 
and holdoutgroup='Y' 
select count(distinct contact_id) from omc.member_campaign_summary_table 
where campaign_id='41223942' 
and program_id in ('43228922','41225742') 
and holdoutgroup='Y' 
and contact_id in  
(select distinct contact_id from omc.member_campaign_summary_table 
where campaign_id='41223942' 
and holdoutgroup='N') 
select count(distinct contact_id) from omc.member_campaign_summary_table 
 where campaign_id='41223942' 
 and holdoutgroup='Y' 
CampaignId = 41223942 
Campaign Name = Consumer_Acquisition_winback_Wave1_Nov19 
 
Below are two programs which contains above winback campaign with control group 
 
This program was created with updated changes due to COVID-19 for existing campaign  as per business requested by applying control group and is currently running 
program Id=43228922 
program name: Consumer_Winback_D100_Daily_Trigger_Wave1 
 
This program was unpublished in month of march as per business requested which also containes the same campaign with control group.so we want to fetch control group members from these two programs 
Program Id =41225742 
Program name: Consumer_Winback_Daily_Trigger_Nov_2019