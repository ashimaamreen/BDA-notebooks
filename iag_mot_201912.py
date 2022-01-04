import pyspark.sql.functions as psf# 
import numpy as np# 
from datetime import datetime#  
# 
s_time = datetime.now()# 
base_df = spark.sql("select * from sandpit.IAG_MOT_BASE where order_end_dt >= '2019-07-01 00:00:00'")# 
base_df = base_df.repartition(30)# 
# 
col_df = {# 
    "membernumber":{"type":None}# 
,   "integration_id":{"type":None}# 
,   "order_id":{"type":None}# 
,   "renewal_yyyymm":{"type":None}# 
,   "renewal_yyyy":{"type":None}# 
,   "order_start_dt":{"type":None}# 
,   "order_end_dt":{"type":None}# 
,   "order_completed_dt":{"type":None}# 
,   "x_renewal_count":{"type":None}# 
,   "order_channel":{"type":None}# 
,   "order_pay_type":{"type":None}# 
,   "iag_time_stamp":{"type":None}# 
,   "iag_mot_amt":{"type":None}# 
,   "iag_mot_desc":{"type":None}# 
,   "motor_policy":{"type":None,"fmt":"response"}# 
,   "iag_crn":{"type":None,"fmt":"train"}# 
,   "policy_action":{"type":None,"fmt":"response"}# 
,   "member_colour":{"type":None,"fmt":"categorical"}# 
,   "member_loyalty_colour":{"type":None,"fmt":"categorical"}# 
,   "member_gender":{"type":None,"fmt":"categorical"}# 
,   "prod_name":{"type":None}# 
,   "prod_grp":{"type":None,"fmt":"categorical"}# 
,   "tenure_member":{"type":None,"fmt":"numeric"}# 
,   "x_iag_loc_desc":{"type":None}# 
,   "item_net_price":{"type":None}# 
,   "item_base_price":{"type":None}# 
,   "vehicle_rego":{"type":None}# 
,   "vehicle_make":{"type":None}# 
,   "vehicle_model":{"type":None}# 
,   "vehicle_age":{"type":None,"fmt":"numeric"}# 
,   "vehicle_ancap":{"type":None,"fmt":"numeric"}# 
,   "vehicle_body":{"type":None}#,"fmt":"categorical" .. need to clean up first 
,   "vehicle_bore":{"type":None,"fmt":"numeric"}# 
,   "vehicle_stroke":{"type":None,"fmt":"numeric"}# 
,   "vehicle_co2":{"type":None,"fmt":"numeric"}# 
,   "vehicle_country":{"type":None}#,"fmt":"categorical" .. need to clean up first 
,   "vehicle_cylinders":{"type":None,"fmt":"numeric"}# 
,   "vehicle_drive":{"type":None}#,"fmt":"categorical".. kill cos I stuffed up case when 
,   "vehicle_engine":{"type":None,"fmt":"numeric"}# 
,   "vehicle_ratio":{"type":None,"fmt":"numeric"}# 
,   "vehicle_efficiency":{"type":None,"fmt":"numeric"}# 
,   "vehicle_tank":{"type":None,"fmt":"numeric"}# 
,   "vehicle_fuel":{"type":None}# ,"fmt":"categorical" .. need to clean up first 
,   "vehicle_clearance":{"type":None,"fmt":"numeric"}# 
,   "vehicle_height":{"type":None,"fmt":"numeric"}# 
,   "vehicle_weight":{"type":None,"fmt":"numeric"}# 
,   "vehicle_power":{"type":None,"fmt":"numeric"}# 
,   "vehicle_rpm":{"type":None,"fmt":"numeric"}# 
,   "vehicle_torque":{"type":None,"fmt":"numeric"}# 
,   "vehicle_power_r":{"type":None,"fmt":"numeric"}# 
,   "vehicle_tow":{"type":None,"fmt":"numeric"}# 
,   "vehicle_turn":{"type":None,"fmt":"numeric"}# 
,   "vehicle_width":{"type":None,"fmt":"numeric"}# 
,   "iag_avg_amt":{"type":None,"fmt":"numeric"}# 
,   "iag_volume":{"type":None,"fmt":"numeric"}# 
}# 
 
table_cols = list(base_df.columns)# 
req_cols = list(col_df.keys())# 
error1 = np.setdiff1d(table_cols,req_cols)# 
assert len(error1) == 0,"ERROR 1: missing col specs: {a}".format(a=','.join(error1))# 
### 
# ERROR 2-4: check 
for i in base_df.columns: # 
    col_info = col_df.get(i)# 
    # only diagnose model  
    if col_info.get("fmt"):# 
        # ERROR 2: NULLS in base table  
        if col_info.get("fmt") in ["numeric","categorical","ordinal"]:# 
            null_n = base_df.where(psf.col(i).isNull()).count()# 
            if null_n > 0 :# 
                print("ERROR 2: {c}:{d}".format(c=i,d=null_n))# 
        # ERROR 3:  
        if col_info.get("fmt") == "categorical":# 
            nuniq = base_df.select(i).distinct().count()# 
            if nuniq > 6: # 
                print("ERROR 3: '{c}': {d} values".format(c=i,d=nuniq))# 
 
import os, re# 
import numpy as np# 
from pyspark.sql import *# 
from pyspark.sql.types import *# 
from pyspark.sql.functions import *# 
from pyspark.mllib.tree import GradientBoostedTrees, GradientBoostedTreesModel# 
from pyspark.ml.regression import GBTRegressor# 
from pyspark.sql import HiveContext# 
from pyspark import SparkContext# 
from pyspark.ml import Pipeline, PipelineModel# 
from pyspark.ml.classification import RandomForestClassifier# 
from pyspark.ml.feature import IndexToString, StringIndexer, VectorIndexer, VectorAssembler, VectorSlicer# 
from pyspark.ml.evaluation import BinaryClassificationEvaluator, MulticlassClassificationEvaluator, RegressionEvaluator# 
from pyspark.ml.feature import OneHotEncoderEstimator , StringIndexer# 
from pyspark.ml.feature import CountVectorizer# 
from pyspark.ml.classification import GBTClassifier# 
from pyspark.mllib.tree import DecisionTree, DecisionTreeModel# 
 
# predictors_cat = ["prod_name", "member_colour", "member_loyalty_colour", "member_gender","make_model"]# 
predictors_cat = [x for x in col_df if col_df.get(x).get("fmt") == "categorical"]# 
predictors_num = [x for x in col_df if col_df.get(x).get("fmt") == "numeric"]## 
# 
for cat in predictors_cat: # 
    base_df = StringIndexer(inputCol=cat , outputCol="index_"+cat).fit(base_df).transform(base_df)# 
# 
predictors = predictors_num+["index_"+x for x in predictors_cat]# 
assembler = VectorAssembler(inputCols=predictors_num+["index_"+x for x in predictors_cat],outputCol="features")# 
data_model = assembler.transform(base_df)# 
# 
 
response_cols = {# 
    "motor_uptake":{# 
        "response":"motor_policy"# 
    ,   "model_set":{"col":"iag_crn","value":1}# 
    ,   "algo":[# 
            {   "model":'GBTClassifier'# 
            ,   "params":{# 
                    "maxIter":100# 
                ,   "seed":32# 
                ,   "maxDepth":3# 
                ,   "setThresholds":0.8# 
                }# 
            }# 
        ]# 
    }# 
,   "motor_action":{# 
        "response":"policy_action"# 
    ,   "model_set":{"col":"motor_policy","value":1}# 
    ,   "algo":[# 
            {   "model":"GBTRegressor"# 
            ,   "params":{# 
                    "maxIter":10# 
                }# 
            }# 
        ]# 
    }# 
}# 
response_i = "motor_uptake"# 
print("Response Name: {a}".format(a=response_i))# 
resp_df = response_cols.get(response_i)# 
req_fields = [resp_df.get("response"),'features']# 
assert len(np.setdiff1d(req_fields,data_model.columns)) == 0 , 'ERROR: missing field' + ','.join(np.setdiff1d(req_fields,data_model.columns))# 
# test/train 
(data_train,data_test) = data_model.filter("{c} == {f}".format(c=resp_df.get("model_set").get("col")# 
    ,   f=resp_df.get("model_set").get("value"))).select(req_fields).randomSplit([0.7, 0.3])# 
## Algo 
algo =  resp_df.get('algo')[0]# 
gbt = GBTClassifier(labelCol=resp_df.get("response"), featuresCol="features", maxIter=100,seed=32,maxDepth=4)# 
gbt.setThresholds=0.8# 
model = gbt.fit(data_train) #time take 00:05:00 
# DIAGNOSTICS: VAriable Importance 
cSchema = StructType([StructField("predictor", StringType()),StructField("importance", FloatType())])# 
imp_df=spark.createDataFrame([[predictors[idx],float(i)] for idx,i in enumerate(model.featureImportances)],cSchema)# 
imp_df.createOrReplaceTempView("imp_df")# 
spark.sql("DROP TABLE IF EXISTS sandpit.iag_mot_"+response_i+"_imp")# 
spark.sql("CREATE TABLE sandpit.iag_mot_"+response_i+"_imp AS select * from imp_df")# 
## DIAGNOSTICS Accuracy 
gbt_evaluator = BinaryClassificationEvaluator(labelCol=resp_df.get("response"),rawPredictionCol="rawPrediction",metricName="areaUnderROC")# 
pred_train = model.transform(data_train)# 
pred_test = model.transform(data_test)# 
# # pred_train.show(1)# 
train_auc = gbt_evaluator.evaluate(pred_train)# 
test_auc = gbt_evaluator.evaluate(pred_test)# 
print({"train":train_auc,"test":test_auc})# 
# Save output 
full_output = model.transform(data_model)# 
full_output2 = full_output.rdd.map(lambda x:# 
    (float(x.probability[1]),x.integration_id,x.membernumber,x.order_id) # 
    ).toDF((response_i+'_prob','integration_id','membernumber','order_id'))# 
full_output2.createOrReplaceTempView("full_output2")# 
spark.sql("DROP TABLE IF EXISTS sandpit.iag_mot_"+response_i)# 
spark.sql("CREATE TABLE sandpit.iag_mot_"+response_i+" AS select * from full_output2")# 
 
 
response_i = "motor_action"# 
print("Response Name: {a}".format(a=response_i))# 
resp_df = response_cols.get(response_i)# 
req_fields = [resp_df.get("response"),'features']# 
assert len(np.setdiff1d(req_fields,data_model.columns)) == 0 , 'ERROR: missing field' + ','.join(np.setdiff1d(req_fields,data_model.columns))# 
# test/train 
(data_train,data_test) = data_model.filter("{c} == {f}".format(c=resp_df.get("model_set").get("col")# 
    ,   f=resp_df.get("model_set").get("value"))).select(req_fields).randomSplit([0.7, 0.3])# 
## Algo 
algo =  resp_df.get('algo')[0]# 
gbt = GBTRegressor(labelCol=resp_df.get("response"), featuresCol="features", maxIter=100)# 
model = gbt.fit(data_train)#time take 13:00 
# DIAGNOSTICS: VAriable Importance 
cSchema = StructType([StructField("predictor", StringType()),StructField("importance", FloatType())])# 
imp_df=spark.createDataFrame([[predictors[idx],float(i)] for idx,i in enumerate(model.featureImportances)],cSchema)# 
imp_df.createOrReplaceTempView("imp_df")# 
spark.sql("DROP TABLE IF EXISTS sandpit.iag_mot_"+response_i+"_imp")# 
spark.sql("CREATE TABLE sandpit.iag_mot_"+response_i+"_imp AS select * from imp_df")# 
## PVO  
pred_train = model.transform(data_train)# 
pred_test = model.transform(data_test)# 
# Diagnostics 
for metric in ["rmse","r2"]:# 
    gbt_evaluator = RegressionEvaluator(labelCol=resp_df.get("response"),predictionCol="prediction",metricName=metric)# 
    train_rmse = gbt_evaluator.evaluate(pred_train)# 
    test_rmse = gbt_evaluator.evaluate(pred_test)# 
    print({"metric":metric,"train":train_rmse,"test":test_rmse})# 
# Save output 
full_output = model.transform(data_model)# 
full_output2 = full_output.rdd.map(lambda x:# 
    (float(x.prediction),x.integration_id,x.membernumber,x.order_id) # 
    ).toDF((response_i+'_prob','integration_id','membernumber','order_id'))# 
full_output2.createOrReplaceTempView("full_output2")# 
spark.sql("DROP TABLE IF EXISTS sandpit.iag_mot_"+response_i)# 
spark.sql("CREATE TABLE sandpit.iag_mot_"+response_i+" AS select * from full_output2")# 
 
full_output2.createOrReplaceTempView("full_output2")# 
 
summ_df = spark.sql(""" select * from sandpit.iag_summ""")# 
# 
for row in summ_df.toJSON().collect():# 
    print(row+',')# 