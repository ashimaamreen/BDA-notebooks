""" 
Author: Yu Chen 
Date: 23.08.2019d 
Description: the script here is to find the common columns for all the tables in campaign data. The idea is to consolidate all the tables into one. 
Goal: 
    - not change the current way of creating tables; 
    - consolidate all the ad-hoc campaigns into one big table; 
    - repartition 
Reference: https://docs.databricks.com/spark/latest/spark-sql/language-manual/show-table-properties.html 
""" 
from pyspark.sql.functions import * 
import re 
from datetime import datetime, date, timedelta 
from pyspark.sql.types import StringType, LongType 
from pyspark.sql.types import * 
from pytz import timezone, utc 
import re 
import sys 
def get_datetime(tbl_name, debug=True): 
    """ 
    Desc: the script is to convert unix epoch time to local Sydney time 
    To-do: 
    1. Solve issue for switching betwee AEST and AST 
    2. Solve issue where datetime format is not localised  
    """ 
     
    if debug==True: 
        tbl_schema = "sandpit" 
    else: 
        tbl_schema = "campaign_data" 
     
    au_tz = timezone('Australia/Sydney') 
    #fmt = '%Y-%m-%d %H:%M:%S %Z%z' 
     
    fmt = "%Y-%m-%d" 
    tbl_ppt = spark.sql("""show tblproperties %s.%s""" % (tbl_schema,tbl_name)) 
    for item in tbl_ppt.toLocalIterator(): 
        if item.key == "transient_lastDdlTime": 
            tbl_ppt_dt = datetime.utcfromtimestamp(int(item.value)) 
            tbl_ppt_dt_au = au_tz.localize(tbl_ppt_dt) 
            break 
    return tbl_ppt_dt_au.strftime(fmt) 
# the section here is to create audit file for the data transfer 
 
def get_audit_list(tbl_schema, tbl_list_targeted, mode, index): 
     
    tbl_category = list() 
    error_list = list() 
 
    for tbl in tbl_list_targeted: 
        try: 
            temp_list = tbl.split("_") 
            if len(temp_list) == 7 and temp_list[6] == mode: 
                 
                index += 1 
                 
                temp_tbl = spark.table("%s.%s" % (tbl_schema, tbl)) 
                temp_row_count = float(temp_tbl.count())  # getting the number of rows 
                temp_columns = temp_tbl.columns 
                temp_columns_key_excl = [col for col in temp_columns if col not in ["contact_id", "member_id", "segment", "control"]] 
                 
                temp_dict = { 
                    "table_id":index, 
                    "table_name": tbl, 
                    "created_by": temp_list[0], 
                    "dmc_id":temp_list[1][3:], 
                    "campaign_name": temp_list[2], 
                    "purpose": temp_list[3], 
                    "channel":temp_list[4], 
                    "created_date": temp_list[5], 
                    "frequency":temp_list[6], 
                    "row_count": temp_row_count, 
                    "contact_id": "contact_id" if "contact_id" in temp_columns else "", 
                    "member_id": "member_id" if "member_id" in temp_columns else "", 
                    "segment": "segment" if "segment" in temp_columns else "", 
                    "control": "control" if "control" in temp_columns else "" 
                    } 
                 
                i = 0 
                for col in temp_columns_key_excl: 
                    i+=1 
                    temp_dict["col%s" % str(i)] = col 
                if i<20: 
                    for j in range(i+1,21,1): # fixing issue on column selection 
                        temp_dict["col%s" % str(j)] = ""  
                elif i > 20: 
                    error_list.append(tbl) 
                    continue 
                tbl_category.append(temp_dict) 
            else: 
                error_list.append(tbl) 
        except: 
            error_list.append(tbl) 
             
    return tbl_category 
def create_audit(scan_dt, mode, tbl_col, debug): 
     
    """ 
    mode: otm, adhoc, triggered, model, history 
    """ 
     
    # debug or not 
    if debug==True: 
        tbl_schema = "sandpit" 
    else: 
        tbl_schema = "campaign_data" 
     
    tbl_list_df = spark.sql("show tables in %s" % tbl_schema).filter("isTemporary='false'") 
     
    # in tbl_list we create a tuple in the format (tbl_name, created_date) 
    tbl_list = [(tbl.tableName, get_datetime(tbl.tableName, debug)) for tbl in tbl_list_df.select("tableName").collect()] 
    tbl_list_targeted = [tbl[0] for tbl in tbl_list if tbl[1] == scan_dt] 
     
    # works for adhoc and triggered 
    if mode in ["adhoc", "triggered", "otm", "history"]: 
        temp_tbl_id_row = spark.sql("select max(table_id) as table_id from %s.campaign_metadata_%s" % (tbl_schema, mode)).collect() 
        index = int(temp_tbl_id_row[0].table_id) # adding the start of index 
        audit_list = get_audit_list(tbl_schema, tbl_list_targeted, mode, index) 
        if len(audit_list) > 0:  
            audit = spark.createDataFrame(audit_list) 
            audit_re = audit.select(tbl_col) 
        else: 
            audit_re = None 
    else: 
        # should work out on otm 
        pass 
     
    return audit_re 
def initialise_table(dest_tbl_name, standard_tbl_col, debug=True): 
    """ 
    add stand_tbl_col 
    """ 
     
    if debug==True: 
        tbl_schema = "sandpit" 
    else: 
        tbl_schema = "campaign_data" 
     
    # searching for campaign_base_table 
    tbl_list_df = spark.sql("show tables in %s" % tbl_schema).filter("isTemporary='false'") 
    tbl_list = [tbl.tableName for tbl in tbl_list_df.select("tableName").collect()] 
    # dest_tbl_name = "campaign_base_table" 
     
    if dest_tbl_name in tbl_list: 
        print "%s exists in %s" % (dest_tbl_name, tbl_schema)  
        return False 
    else: 
        # creating the base table with dummy 
        dest_tbl_row = dict() 
         
        # set the dummy table as id 0 
        for col in standard_tbl_col: 
            dest_tbl_row[col] = "" 
        dest_tbl_row["table_name"] = dest_tbl_name # overwrite table name 
        dest_tbl_row["table_id"] = 0    # overwrite table index 
        dest_tbl = spark.createDataFrame([dest_tbl_row]) 
        dest_tbl = dest_tbl.select(standard_tbl_col) 
        dest_tbl.repartition(1).write.saveAsTable("%s.%s" % (tbl_schema, dest_tbl_name)) 
        print "%s is created in %s" % (dest_tbl_name, tbl_schema) 
        return True 
def transform_table(audit, tbl_name, tbl_id, tbl_col,debug=True): 
    """ 
    audit: dataframe, includes table details, otm, model and campaign should all have different audit file 
    """ 
     
    # load audit info details 
    audit_details = audit.filter("table_name='%s'" % tbl_name).collect() 
    audit_row_dict = audit_details[0].asDict() 
    print "\ttransform 1 - load audit details" 
     
    # register source table 
    if debug==True: 
        tbl_schema = "sandpit" 
    else: 
        tbl_schema = "campaign_data" 
    source_tbl = spark.table("%s.%s" % (tbl_schema, tbl_name)) 
    source_tbl.createOrReplaceTempView("%s" % tbl_name) 
    print "\ttransform 2 - register source table" 
     
    # construct select statement 
    header_list = tbl_col[3:] #  
    query_parts = list() 
    header_none_list = list() 
     
    for key,value in audit_row_dict.iteritems(): 
        if value is not None and key in header_list: 
            if value.strip() != "": 
                query_parts.append(" as ".join([value,key])) 
         
            else: 
                header_none_list.append(key) 
        else: 
            header_none_list.append(key) 
    query = "select " + ",".join(query_parts) + " from %s" % tbl_name 
    # print query 
    dest_tbl = spark.sql(query) 
    print "\ttransform 3 - generate destination table" 
     
    # converting non string columns to string columns 
    for col in dest_tbl.dtypes: 
        if col[1] != "string":  
            dest_tbl = dest_tbl.withColumn(col[0], dest_tbl[col[0]].cast(StringType())) 
        else: 
            pass 
    print "\ttransform 4 - convert nonstring column" 
     
    for col in [item for item in header_none_list if item in header_list]: 
        dest_tbl = dest_tbl.withColumn(col, lit("")) # adding empty rows to the new column 
    dest_tbl = dest_tbl.withColumn("table_name",lit(tbl_name)) 
    dest_tbl = dest_tbl.withColumn("table_id",lit(tbl_id)) 
    dest_tbl = dest_tbl.withColumn("inserted_date", lit(datetime.strftime(datetime.now(),"%Y%m%d"))) 
    print "\ttransform 5 - add additional columns" 
     
    dest_tbl_re = dest_tbl.select(tbl_col) 
    #dets_tbl_re.show(1,False) 
    #dest_tbl_re.groupby("table_name").count().show(100,False) 
    print "\ttransform 6 - re-create the table with standard column" 
     
    return dest_tbl_re 
def union_table(audit, base_tbl_name, tbl_col, debug=True): 
     
    # find out the schema 
    if debug==True: 
        tbl_schema = "sandpit" 
    else: 
        tbl_schema = "campaign_data" 
     
    # load the base table  
    # currently it is loading all the table in base table 
    base_tbl = spark.table("%s.%s" % (tbl_schema, base_tbl_name)) 
    print "complete 1 - load base table" 
     
    # getting list of tables from the audit file 
    # excluding temporary tables 
    tbl_list_df = spark.sql("show tables in %s" % tbl_schema).filter("isTemporary='false'") 
    # tbl_list contains all the tables in the campaign data schema 
    tbl_list = [tbl.tableName for tbl in tbl_list_df.select("tableName").collect()] 
    # find the table where exists in audit file but also in campaign data schema 
    source_tbl_list = [tbl for tbl in tbl_list if tbl in [item.table_name for item in audit.select("table_name").collect()]] 
    print "complete 2 - decide initial dump or ongoing cleaning" 
     
    # dump the data 
    counter = 2 
    for tbl in source_tbl_list: 
        try: 
            # adding campaign table and table id 
            temp_row = audit.filter("table_name = '%s'" % tbl).collect() 
            tbl_id = str(temp_row[0].table_id) 
            temp_tbl = transform_table(audit, tbl, tbl_id, tbl_col, debug) 
            base_tbl = base_tbl.union(temp_tbl) 
            counter += 1  
            print "complete %d - dump %s to base table" %(counter, tbl) 
        except: 
            print "issue - table %s doesn't get pushed" % tbl 
            continue 
     
    return base_tbl 
def write_table(base_tbl, base_tbl_name, debug=True): 
     
    # find out the schema 
    if debug==True: 
        tbl_schema = "sandpit" 
    else: 
        tbl_schema = "campaign_data" 
     
    # writing results to staging table 
    #base_tbl_name = "campaign_base_table" 
    base_tbl.write.saveAsTable("%s.%s_staging" % (tbl_schema, base_tbl_name)) 
     
    # loading results from staging table 
    base_tbl = spark.table("%s.%s_staging" % (tbl_schema, base_tbl_name)) 
     
    # droping existing base table 
    spark.sql("drop table %s.%s" % (tbl_schema, base_tbl_name)) 
     
    # repartitioning and saving campaign table 
    base_tbl.repartition(1).write.mode("append").saveAsTable("%s.%s" % (tbl_schema, base_tbl_name)) 
     
    # droping staging table 
    spark.sql("drop table %s.%s_staging" % (tbl_schema, base_tbl_name)) 
def drop_table(audit, debug): 
     
    if debug==True: 
        tbl_schema = "sandpit" 
    else: 
        tbl_schema = "campaign_data" 
         
    for tbl in [row.table_name for row in audit.collect()]: 
        try: 
            spark.sql("""drop table if exists %s.%s""" % (tbl_schema, tbl)) 
            print "%s is dropped" % tbl 
        except: 
            print "%s can't be dropped" % tbl 
 
     
     
     
     
def main_program(debug, run_date): 
     
    """ 
    The following script is to create two tables 
        -> should be four  
        -> given the time being, i will create just two. 
    """ 
     
    if debug==True: 
        tbl_schema = "sandpit" 
    else: 
        tbl_schema = "campaign_data" 
     
    base_otm_tbl_col = [] 
    #base_model_tbl_col = [] 
     
    # loading contact history data 
    base_adhoc_tbl_col = ["table_id", "table_name", "inserted_date", "contact_id", "member_id", 
    "segment", "control", "col1","col2","col3","col4", 
    "col5","col6","col7","col8","col9","col10","col11","col12", "col13","col14", "col15", 
    "col16", "col17", "col18", "col19", "col20"] 
     
    adhoc_tbl_name = "campaign_contact_adhoc" 
    initialise_table(adhoc_tbl_name, base_adhoc_tbl_col, debug) 
     
    triggered_tbl_name = "campaign_contact_triggered" 
    initialise_table(triggered_tbl_name, base_adhoc_tbl_col, debug) 
     
     
    # loading meta data 
    meta_adhoc_tbl_col = ["table_id", "table_name", "created_by", "dmc_id", "campaign_name", "purpose", "channel", 
    "created_date", "frequency", "row_count", "contact_id", "member_id", "segment", "control",  
    "col1","col2","col3","col4","col5","col6","col7","col8","col9","col10","col11","col12", "col13", "col14", "col15", 
    "col16", "col17", "col18", "col19", "col20"] 
     
    meta_triggered_tbl_name = "campaign_metadata_triggered" 
    initialise_table(meta_triggered_tbl_name, meta_adhoc_tbl_col, debug) 
     
    meta_adhoc_tbl_name = "campaign_metadata_adhoc" 
    initialise_table(meta_adhoc_tbl_name, meta_adhoc_tbl_col, debug) 
     
    mode = "adhoc" 
     
    if mode == "adhoc": 
        audit = create_audit(run_date, mode, meta_adhoc_tbl_col, debug) 
        if audit is None: 
            print "no dataset created" 
        else: 
            audit.repartition(1).write.mode("append").saveAsTable("%s.campaign_metadata_%s" % (tbl_schema, mode)) 
            base_tbl = union_table(audit, "campaign_contact_adhoc", base_adhoc_tbl_col, debug) 
            write_table(base_tbl, "campaign_contact_adhoc",  debug) 
     
     
#main_program(False, datetime.strftime(datetime.now(),"%Y-%m-%d")) 
main_program(False, "2019-08-29") 
spark.sql("select table_id,table_name,count(*) as number from campaign_data.campaign_contact_adhoc group by table_id, table_name").show(10,False) 
spark.sql("select * from campaign_data.campaign_metadata_adhoc").show(10,False) 
spark.sql(""" drop table if exists campaign_data.campaign_contact_adhoc """) 
drop_table(audit, False) 
spark.sql(""" 
     
    select * 
    from campaign_data.campaign_metadata_adhoc 
    order by table_id 
     
""").show(100,False) 
table_list = [ 
    "bn_dmc1702_septemberenewscaltex_campaign_edm_20190822_adhoc", 
    "bn_dmc1718_enewsinterview1_campaign_edm_20190829_adhoc", 
    "bn_dmc1718_enewsinterview2_campaign_edm_20190829_adhoc", 
    "bn_dmc1718_enewsinterview3_campaign_edm_20190829_adhoc", 
    "lc_dmc1689_marketplaceappuserssegment1_survey_edm_20190820_adhoc", 
    "lc_dmc1689_marketplaceappuserssegment2_survey_edm_20190820_adhoc", 
    "lc_dmc1694_mppnpsseg1insufficientfunds_research_edm_20190829_adhoc", 
    "lc_dmc1694_mppnpsseg2otherreasons_research_edm_20190829_adhoc",  
    "lc_dmc1694_mppnpsseg3nomissedpayments_research_edm_20190829_adhoc"] 
     
for tbl in table_list: 
    spark.sql("select * from campaign_data.%s limit 1" % tbl).show(1,False) 
spark.sql("drop table if exists campaign_data.campaign_metadata_adhoc") 
spark.sql("drop table if exists campaign_data.campaign_contact_adhoc") 
spark.sql("select * from campaign_data.campaign_contact_history").show(10,False) 
# the section here is to create audit file for the data transfer 
 
def get_audit_list(tbl_schema, tbl_list_targeted, mode, index): 
     
    tbl_category = list() 
    error_list = list() 
    tbl_group = ["triggered", "adhoc", "otm"] 
 
    for tbl in tbl_list_targeted: 
        try: 
            temp_list = tbl.split("_") 
            if (len(temp_list) == 7 and temp_list[6] == mode) or (mode=="history"): 
                 
                index += 1 
                 
                temp_tbl = spark.table("%s.%s" % (tbl_schema, tbl)) 
                temp_row_count = float(temp_tbl.count())  # getting the number of rows 
                temp_columns = temp_tbl.columns 
                temp_columns_key_excl = [col for col in temp_columns if col not in ["contact_id", "member_id", "segment", "control"]] 
                 
                temp_dict = { 
                    "table_id":index, 
                    "table_name": tbl, 
                    "created_by": temp_list[0] if mode in tbl_group else "", 
                    "dmc_id":temp_list[1][3:] if mode in tbl_group else "", 
                    "campaign_name": temp_list[2] if mode in tbl_group else "", 
                    "purpose": temp_list[3] if mode in tbl_group else "", 
                    "channel":temp_list[4] if mode in tbl_group else "", 
                    "created_date": temp_list[5] if mode in tbl_group else "", 
                    "frequency":temp_list[6] if mode in tbl_group else "", 
                    "row_count": temp_row_count, 
                    "contact_id": "contact_id" if "contact_id" in temp_columns else "", 
                    "member_id": "member_id" if "member_id" in temp_columns else "", 
                    "segment": "segment" if "segment" in temp_columns else "", 
                    "control": "control" if "control" in temp_columns else "" 
                    } 
                 
                i = 0 
                for col in temp_columns_key_excl: 
                    i+=1 
                    temp_dict["col%s" % str(i)] = col 
                if i<20 and mode in tbl_group: 
                    for j in range(i+1,21,1): # fixing issue on column selection 
                        temp_dict["col%s" % str(j)] = "" 
                elif i < 50 and mode == "history": 
                    for j in range(i+1,51,1): 
                        temp_dict["col%s" % str(j)] = "" 
                else: 
                    error_list.append(tbl) 
                    continue 
                tbl_category.append(temp_dict) 
            else: 
                error_list.append(tbl) 
        except: 
            error_list.append(tbl) 
             
    return tbl_category 
tbl_schema = "campaign_data" 
scan_dt = "2019-06-21" 
debug = False 
mode = "history" 
 
tbl_list_df = spark.sql("show tables in %s" % tbl_schema).filter("isTemporary='false'") 
tbl_list = [(tbl.tableName, get_datetime(tbl.tableName, debug)) for tbl in tbl_list_df.select("tableName").collect()] 
tbl_list_targeted = [tbl[0] for tbl in tbl_list if tbl[1] == scan_dt] 
 
temp = get_audit_list(tbl_schema,tbl_list_targeted, mode, index = 1) 
print temp[0:10] 
import numpy as np 
 
#table_list = [item["table_name"] for item in temp] 
left = [item for item in all_tbl_list if item not in list(map(lambda x: x["table_name"], temp))] 
 
print left 
 
len(left) 
all_tbl_list = [row.tableName for row in tbl_list_df.collect()] 
tbl_schema="campaign_data" 
debug=False 
 
# loading contact history data 
base_history_tbl_col = ["table_id", "table_name", "inserted_date", "contact_id", "member_id", 
"segment", "control", 
"col1","col2","col3","col4","col5","col6","col7","col8","col9", 
"col10", "col11", "col12", "col13", "col14", "col15", "col16", "col17", "col18", "col19",  
"col20", "col21", "col22", "col23", "col24", "col25", "col26", "col27", "col28", "col29", 
"col30", "col31", "col32", "col33", "col34", "col35", "col36", "col37", "col38", "col39", 
"col40", "col41", "col42", "col43", "col44", "col45", "col46", "col47", "col48", "col49", "col50"] 
 
history_tbl_name = "campaign_contact_history" 
initialise_table(history_tbl_name, base_history_tbl_col, debug) 
 
 
# loading meta data 
meta_history_tbl_col = ["table_id", "table_name", "created_by", "dmc_id", "campaign_name", "purpose", "channel", 
"created_date", "frequency", "row_count", "contact_id", "member_id", "segment", "control",  
"col1","col2","col3","col4","col5","col6","col7","col8","col9", 
"col10", "col11", "col12", "col13", "col14", "col15", "col16", "col17", "col18", "col19",  
"col20", "col21", "col22", "col23", "col24", "col25", "col26", "col27", "col28", "col29", 
"col30", "col31", "col32", "col33", "col34", "col35", "col36", "col37", "col38", "col39", 
"col40", "col41", "col42", "col43", "col44", "col45", "col46", "col47", "col48", "col49", "col50"] 
 
 
meta_history_tbl_name = "campaign_metadata_history" 
initialise_table(meta_history_tbl_name, meta_history_tbl_col, debug) 
 
 
 
 
mode = "history" 
debug=False 
run_date = "2019-06-21" 
 
if mode == "history": 
    audit = create_audit(run_date, mode, meta_history_tbl_col, debug) 
     
print audit 
mode = "history" 
debug=False 
 
audit.repartition(1).write.mode("append").saveAsTable("%s.campaign_metadata_%s" % (tbl_schema, mode)) 
spark.sql("select table_id, table_name, row_count from campaign_data.campaign_metadata_history order by table_id").show(300,False) 
debug = False 
mode = "history" 
 
audit = spark.sql("select * from campaign_data.campaign_metadata_history") 
base_tbl = union_table(audit,"campaign_contact_%s" % mode, base_history_tbl_col, debug) 
#write_table(base_tbl, "campaign_contact_%s" % mode, debug) 
spark.sql(""" 
select cast(table_id as integer) as table_id, table_name,count(*)  
from campaign_data.campaign_contact_history 
group by cast(table_id as integer), table_name  
order by cast(table_id as integer), table_name""").show(200,False) 
spark.sql(""" 
    select *  
    from campaign_data.campaign_metadata_history 
    where 
        table_id = "3" 
""").show(100,False) 
audit = spark.sql("select * from campaign_data.campaign_metadata_history") 
result.repartition("table_id").write.saveAsTable("campaign_data.campaign_contact_history_staging") 
spark.sql("drop table if exists campaign_data.campaign_contact_history") 