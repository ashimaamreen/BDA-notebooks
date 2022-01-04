#Drive-Time
#Contents
DM_bad_Address_List = spark.read.load("/user/hive/warehouse/campaign_data.db/external_files/drive_time/drive_time.csv",format="csv", sep=",", inferSchema="true", header="true") 
DM_bad_Address_List.createOrReplaceTempView("DM_bad_Address_List") 
dfQry = spark.sql('select * from DM_bad_Address_List') 
dfQry.show() 