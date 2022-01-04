from pyspark.sql.functions import * 
 
model_cs = spark.read.load("/user/harafah/CrossSellModels/03 Scores/04Jan19/Data/bt_member_CarServicing_score.csv",  
    format="csv", sep=",", inferSchema="true", header="true") 
     
model_cs.createOrReplaceTempView("model_cs") 
 
model_bt = spark.read.load("/user/harafah/CrossSellModels/03 Scores/04Feb19/Data/bt_member_CarBatteries_score.csv",  
    format="csv", sep=",", inferSchema="true", header="true") 
     
model_bt.createOrReplaceTempView("model_bt") 
model_bt.describe().show() 
data = model_cs.join(model_bt, model_bt.membernumber == model_cs.membernumber, "inner").select( 
    model_cs.membernumber, model_cs.transactionProbability.alias("cs"), model_bt.transactionProbability.alias("battery")) 
     
data.summary().show() 
data.createOrReplaceTempView("data") 
 
data_2 = spark.sql(""" 
     
    select *, 
        case  
            when cs < 0.1 then "cs_1"  
            when cs >= 0.1 and cs < 0.2 then "cs_2"  
            when cs >= 0.2 and cs < 0.3 then "cs_3"   
            when cs >= 0.3 and cs < 0.4 then "cs_4"  
            when cs >= 0.4 and cs < 0.5 then "cs_5"  
            when cs >= 0.5 and cs < 0.6 then "cs_6"  
            when cs >= 0.6 and cs < 0.7 then "cs_7"  
            when cs >= 0.7 and cs < 0.8 then "cs_8"  
            when cs >= 0.8 and cs < 0.9 then "cs_9"  
        else "cs_10" end as cs_band, 
        case  
            when battery < 0.1 then "bt_1"  
            when battery >= 0.1 and battery < 0.2 then "bt_2"  
            when battery >= 0.2 and battery < 0.3 then "bt_3"   
            when battery >= 0.3 and battery < 0.4 then "bt_4"  
            when battery >= 0.4 and battery < 0.5 then "bt_5"  
            when battery >= 0.5 and battery < 0.6 then "bt_6"  
            when battery >= 0.6 and battery < 0.7 then "bt_7"  
            when battery >= 0.7 and battery < 0.8 then "bt_8"  
            when battery >= 0.8 and battery < 0.9 then "bt_9" 
        else "bt_10" end as bt_band 
    from data 
         
""") 
 
data_2.show(1,False) 
 
data_2.stat.crosstab("cs_band", "bt_band").show() 
SELECT  
    bucket, round(max(model.transactionprobability),100),  
    round(min(model.transactionprobability),100), 
    count(*) 
FROM campaign_data.model_carservicing_crosssell_20190207_nocsin2019 as model 
GROUP BY bucket 
ORDER BY bucket ASC; 