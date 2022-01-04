Contents
# Data Collection 
# Step 1 - load flurry data 
 
offer_redeem = spark.read.load("/user/ychen/redeem_clean.csv", format="csv", sep=",", inferSchema="true", header="true") 
# Data Collection 
# Step 2 - create Flurry dataframe and display 
 
offer_redeem.createOrReplaceTempView("redeem") 
 
spark.sql(""" 
    select * 
    from redeem 
    limit 10  
""").show(5) 
# Data Collection 
# Loading GMS data with attributes like tenure, segment and postcode 
 
s_contact = spark.table("gms.s_contact") 
s_contact.createOrReplaceTempView("s_contact") 
s_contact_x = spark.table("gms.s_contact_x") 
s_contact_x.createOrReplaceTempView("s_contact_x") 
 
gms = spark.sql(""" 
     
    select * 
    from  
     
    ( 
     
        select par_row_id, attrib_17 as tenure, attrib_55 as segment, attrib_35 as postcode 
        from s_contact_x 
    ) a inner join  
    ( 
        select 
            row_id, csn as memberid,  
            con_cd as memberstatus,  
            cust_stat_cd as active,  
            cust_value_cd as membertype, 
            age, 
            split(split(birth_dt," ")[0],"\\-")[0] as bd_year 
    from s_contact 
     
    ) b 
    on a.par_row_id = b.row_id 
    where  
        memberid <> "null" 
        and not memberid rlike "387327401|89511001|244252201|543004501|526044602"  
         
 
 
""") 
gms.show(5) 
gms.createOrReplaceTempView("gms") 
# Data Collection 
# Merging Flurry data with GMS 
# Categorising data into different bucket 
 
data = spark.sql(""" 
     
    select *, 
    case  
        when lower(trim(partner)) rlike "travelodge" then "Hotel" 
        when lower(trim(partner)) rlike "kurrajong" then "Hotel" 
        when lower(trim(partner)) rlike "nrma parks and resorts" then "Resort" 
        when lower(trim(partner)) rlike "nrma holiday parks" then "Resort" 
        when lower(trim(partner)) rlike "nrma travel" then "Travel" 
    else "Others" end as destination,  
    case 
        when offerid rlike "632" then "Tassie" 
        when offerid rlike "61646|654|647|655|652" then "Domestic Holiday" 
        when offerid rlike "616|634|681" then "Asia Escape" 
        when offerid rlike "606" then "Europe" 
        when offerid rlike "633" then "Alaska" 
        when offerid rlike "591" then "Travel Insurance" 
        when offerid rlike "572|649|617" then "Hotel" 
        when offerid rlike "594|634|650" then "Cruising" 
    else "Others" end as interest 
    from 
    ( 
        select 
            r.year, r.month, r.date, r.os, r.event, r.type, r.memberid, r.partner, r.offer, r.offerid, 
            r.lat, r.long, r.suburb, g.tenure, g.segment, g.memberstatus, g.active, g.membertype, 
            g.age, g.bd_year, g.postcode 
        from redeem as r 
        left join gms as g 
        on r.memberid = g.memberid 
    ) 
     
 
 
""") 
data.createOrReplaceTempView("data") 
# Data Collection 
# This is to calculate how many members in the data set 
 
spark.sql(""" 
 
    select count(distinct memberid) 
    from data 
 
""").show(1) 
# Data Collection 
# segment related insights 
 
spark.sql(""" 
 
select segment, destination, interest,count(event) as number 
from data 
where 
    lower(trim(segment)) <> "null" 
    and destination <> "Others" 
group by segment, destination,interest 
order by segment, destination,interest 
""").show(100) 
# Data Collection 
# what's the most spend for all the members who are in the "data" dataset 
 
spark.sql(""" 
 
select concat(year(split(time_stamp," ")[0]), "-", month(split(time_stamp," ")[0])) as date, partner, cast(sum(number_of_items) as int) as number 
from m4m.return_feed_header 
where 
    member_number in ( 
     
        select distinct memberid 
        from data) 
    and split(time_stamp," ")[0] >= "2017-08-01" and split(time_stamp," ")[0] <= "2018-05-31" 
group by concat(year(split(time_stamp," ")[0]), "-", month(split(time_stamp," ")[0])), partner 
order by concat(year(split(time_stamp," ")[0]), "-", month(split(time_stamp," ")[0])), partner 
 
 
""").show(10) 
# Data Collection 
# what's the most spend for all the members who are in the "data" dataset 
 
spark.sql(""" 
 
select partner_group, sum(number) as number 
from( 
select *,  
case  
    when lower(trim(partner)) rlike "nrma.holiday" then "nrma holiday" 
    when lower(trim(partner)) rlike "nrma.parks" then "nrma parks" 
    when lower(trim(partner)) rlike "racv" then "racv resort" 
    else lower(trim(partner)) end as partner_group 
from( 
 
select partner, sum(number) as number 
from 
 
( 
 
select concat(year(split(time_stamp," ")[0]), "-", month(split(time_stamp," ")[0])) as date, partner, cast(sum(number_of_items) as int) as number 
from m4m.return_feed_header 
where 
    member_number in ( 
     
        select distinct memberid 
        from data) 
    and split(time_stamp," ")[0] >= "2017-08-01" and split(time_stamp," ")[0] <= "2018-05-31" 
group by concat(year(split(time_stamp," ")[0]), "-", month(split(time_stamp," ")[0])), partner 
order by concat(year(split(time_stamp," ")[0]), "-", month(split(time_stamp," ")[0])), partner 
)  
where lower(partner) rlike "experiences|nrma.holiday|nrma.parks|racv.resort|travelodge|nrma.travel|nrma.attraction|nrma.online|ticketek|ticketmaster|nrma.?tix|ticketmates" 
group by partner 
order by number desc 
) 
)  
group by partner_group 
 
""").show(10) 
# Desktop Analysis 
# This is to research on benefit consumption from desktop 
# First step is to read desktop data 
 
benefit_desktop = spark.read.load("/user/ychen/benefit_desktop_v2.csv", format="csv", sep=",", inferSchema="true", header="true") 
benefit_desktop.createOrReplaceTempView("benefit") 
# Desktop Analysis 
# Second step is to merge with gms data 
 
data2 = spark.sql(""" 
 
select g.memberid, b.pagepath, b.numbers, g.par_row_id, g.tenure, g.segment, g.postcode, g.row_id, g.memberstatus, g.active, g.membertype, g.age, g.bd_year 
from benefit b 
left join gms g 
on cast(b.memberid as string) = g.memberid 
where 
    g.memberid <> "null" 
 
""") 
 
data2.createOrReplaceTempView("data2") 
# Desktop Analysis 
# Third Step is to categorise desktop data based on different interests 
 
data3 = spark.sql(""" 
 
select *,  
    case  
        when pagepath rlike "travelodge|tfe|kurrajong" then "hotel" 
        when pagepath rlike "gold\-coast|nrma-holiday-parks|racv\-resorts" then "domestic" 
        when pagepath rlike "nrma\-travel|attractions" then "oversea" 
        else "others" end as destination 
from data2 
where  
    split(pagepath, "/")[3] <> "" 
    and split(pagepath, "/")[3] <> "null" 
    and not split(pagepath, "/")[3] rlike "www.eventcinema" 
 
 
 
""") 
 
data3.createOrReplaceTempView("data3") 
# Desktop Analysis 
# Fourth Step is to find the stats based on different segments 
 
spark.sql(""" 
 
select destination, segment, sum(numbers) as number 
from data3 
group by destination, segment 
order by destination, segment 
""").show(100) 
# Data Collection 
# Creating AMS dataframe  
# with metrics like most spend, money saved etc 
 
m4m = spark.sql(""" 
     
    select  
        a.ams_memberid, a.first_redeem_date, a.last_redeem_date, 
        a.ams_item, a.ams_spend, a.ams_savings, a.daily_spend, b.iag_flag 
    from 
    ( 
        select *, ams_spend/datediff(to_date(last_redeem_date), to_date(first_redeem_date)) as daily_spend 
        from  
        ( 
         
            select  
                member_number as ams_memberid, min(split(time_stamp," ")[0]) as first_redeem_date, 
                max(split(time_stamp," ")[0]) as last_redeem_date, sum(number_of_items) as ams_item,  
                sum(total_amount) as ams_spend, sum(discount) as ams_savings 
            from m4m.return_feed_header 
            where  
                total_amount >= 0  
                and discount >= 0 
            group by member_number 
        )  
    ) a left join  
    ( 
      
        select member_number, if(number >= 1, "Y", "N") as iag_flag 
        from ( 
            select member_number, count(partner) as number 
            from m4m.return_feed_header 
            where 
                trim(lower(partner)) rlike "insurance|iag" 
            group by member_number 
        ) 
    ) b 
    on a.ams_memberid = b.member_number 
 
""") 
m4m.createOrReplaceTempView("m4m") 
 
# Data Collection 
# Merging data (GMS + Flurry) with AMS 
 
data_final = spark.sql(""" 
 
    select *,  
        2018-cast(d.bd_year as int) as age_v2,  
        datediff(to_date("2018-07-31"), to_date(m.last_redeem_date)) as days_from_last_redeem, 
        date_format(m.last_redeem_date, 'EEEE') as weekday, 
        m.ams_spend/m.ams_savings as savings_per_dollar_spend 
    from data d left join m4m m 
    on d.memberid = m.ams_memberid 
 
""") 
data_final.createOrReplaceTempView("data_final") 
# Data Collection 
# To print out the schema for the data final 
 
data_final.printSchema() 
# Data Collection 
# This is to define for all the columns, the categorical and numeric variables that we will use 
 
cols_n = [ 
       "tenure", "age_v2", 
        "days_from_last_redeem", "daily_spend", 
        "ams_item", "savings_per_dollar_spend" 
    ] 
cols_c = [ 
        "destination", "interest", "segment", 
         "iag_flag", "weekday", "membertype" 
    ] 
# Modelling 
# To find out all the nas in different categorical columns 
 
from pyspark.sql.functions import isnan, when, count, col 
 
 
for item in cols_c: 
 
    data_final.select(item).groupBy(item).agg(count(item)).show(10, False) 
# Modelling 
# Data Cleaning - To Clean all Null values from data final 
 
#data_final = data_final.filter(col("destination") <> "Others") 
#data_final.select("destination").distinct().show(10) 
data_final = data_final.dropna(subset = cols_c) 
data_final = data_final.fillna(0, subset = cols_n) 
# cols_c = [ 
#         "destination", "interest", "segment", 
#          "iag_flag", "weekday", "membertype" 
#     ] 
 
# To generate Index for Interest 
 
from pyspark.ml.feature import OneHotEncoder, StringIndexer 
 
stringIndexer = StringIndexer(inputCol="interest", outputCol="interestIndex") 
model = stringIndexer.fit(data_final) 
indexed = model.transform(data_final) 
encoder = OneHotEncoder(inputCol="interestIndex", outputCol="interestVec") 
encoded = encoder.transform(indexed) 
encoded.show() 
 
# # for destination 
# from pyspark.ml.feature import OneHotEncoder, StringIndexer 
 
# stringIndexer = StringIndexer(inputCol="destination", outputCol="destinationIndex") 
# model = stringIndexer.fit(data_final) 
# indexed = model.transform(data_final) 
# encoder = OneHotEncoder(inputCol="destinationIndex", outputCol="destinationVec") 
# encoded = encoder.transform(indexed) 
# encoded.show() 
# Modelling 
# Vectorise the features 
# VectorAssembler is to create a vector columns withe all the features provided 
 
from pyspark.ml.feature import VectorAssembler 
 
cols_n = [ 
      "tenure", "age_v2", 
        "days_from_last_redeem", "daily_spend", 
        "ams_item", "savings_per_dollar_spend",  
        "interestVec" 
    ] 
 
 
# Step 1 is to initialise the assembler 
# Step 2 is to transform the columns to the additional column called "features" 
assembler = VectorAssembler(inputCols = cols_n, outputCol="features") 
assembled_data = assembler.transform(encoded) 
 
 
# # For destination 
# from pyspark.ml.feature import VectorAssembler 
 
# cols_n = [ 
#       "tenure", "age_v2", 
#         "days_from_last_redeem", "daily_spend", 
#         "ams_item", "savings_per_dollar_spend",  
#         "destinationVec" 
#     ] 
 
 
# # Step 1 is to initialise the assembler 
# # Step 2 is to transform the columns to the additional column called "features" 
# assembler = VectorAssembler(inputCols = cols_n, outputCol="features") 
# assembled_data = assembler.transform(encoded) 
 
# Modelling 
# Starndarise the model 
# Standarise or normalisation is to make sure feature column is at the same scale  
 
from pyspark.ml.feature import StandardScaler 
 
scaler = StandardScaler(inputCol="features", outputCol="scaledFeatures") 
scaler_model = scaler.fit(assembled_data) 
scaled_data = scaler_model.transform(assembled_data) 
scaled_data.show(5, False) 
 
# Modelling 
# Applying Kmeans with three clusters 
 
from pyspark.ml.clustering import KMeans 
from pyspark.ml.linalg import Vectors 
 
k_means_3 = KMeans(featuresCol='scaledFeatures', k=3) 
model_k3 = k_means_3.fit(scaled_data) 
model_k3_data = model_k3.transform(scaled_data) 
model_k3_data.groupBy('prediction').count().show() 
model_k3_data.createOrReplaceTempView("modelk3data") 
model_k3_data.show(5) 
# Modelling 
# Applying Kmeans with two clusters 
 
from pyspark.ml.clustering import KMeans 
from pyspark.ml.linalg import Vectors 
 
k_means_2 = KMeans(featuresCol='scaledFeatures', k=2) 
model_k2 = k_means_2.fit(scaled_data) 
model_k2_data = model_k2.transform(scaled_data) 
model_k2_data.groupBy('prediction').count().show() 
model_k2_data.createOrReplaceTempView("modelk2data") 
# this is to find the stats 
 
model_k3_data.groupBy("prediction").agg(mean(col("daily_spend"))).show() 
model_k3_data.groupBy("prediction").agg(mean(col("savings_per_dollar_spend"))).show() 
model_k3_data.groupBy("prediction").agg(mean(col("ams_item"))).show() 
model_k3_data.groupBy("prediction").agg(mean(col("days_from_last_redeem"))).show() 
model_k3_data.groupBy("prediction").agg(mean(col("ams_savings"))).show() 
 
# This is to explore the stats of the interesting columns 
 
cols_des = ["daily_spend", "savings_per_dollar_spend",  
"ams_item", "days_from_last_redeem", "ams_savings", "ams_spend"] 
 
 
for item in cols_des: 
    print "stats for %s" % item 
     
    print "cluster 0" 
    model_k3_data.filter(col("prediction")==0).describe(item).show() 
     
    print "cluster 1" 
    model_k3_data.filter(col("prediction")==1).describe(item).show() 
     
    print "cluster 2" 
    model_k3_data.filter(col("prediction")==2).describe(item).show() 
 
 
 
cols_c = [ 
        "destination", "interest", "segment", "weekday", "membertype" 
    ] 
 
for item in cols_c: 
    model_k3_data.groupBy("prediction", item).agg(count(col("prediction")).alias("numbers")).orderBy("prediction",item).show(1000) 
data.printSchema() 
data.groupBy("segment").agg(countDistinct("memberid")) 