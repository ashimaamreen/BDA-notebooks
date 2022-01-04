#Contents
QBR_List = spark.read.load("/user/hive/warehouse/campaign_data.db/external_files/QBR/ANRMAL01H.044.csv",format="csv", sep=",", inferSchema="true", header="true") 
QBR_List.createOrReplaceTempView("QBR_List") 
dfQry = spark.sql('create table sandpit.QBR_List AS select * from QBR_List') 
dfQry.show(5) 
QBR_Header = spark.read.load("/user/aamreen/QBR_Header.csv",format="csv", sep=",", inferSchema="true", header="true") 
#ABN	CAR_SEQ_NUM	BUSINESS_NAME	SALES_ORDER_NUMBER	MEMBERSHIP_NUMBER	DESCRIPTION	BRAND	LOCATION	TRANSACTION_START_DATE	TRANSACTION_END_DATE	POS_OR_NEG_SIGN	GROSS_AMOUNT	POS_OR_NEG_SIGN	BASE_DISCOUNT	POS_OR_NEG_SIGN	BONUS_DISCOUNT	POS_OR_NEG_SIGN	BASE_REBATE	POS_OR_NEG_SIGN	BONUS_REBATE	POS_OR_NEG_SIGN	NET_AMOUNT	POS_OR_NEG_SIGN	BASE_POINTS	POS_OR_NEG_SIGN	BONUS_POINTS	BONUS_POINTS_BASIS	POS_OR_NEG_SIGN	TOTAL_POINTS	RECORD_NUMBER	ACCEPTED_OR_REJECT	REASON_FOR_REJECTION 
#H	ANRMAL01	20180807	   12:41:34	      _c4	                 _c5	 _c6	 _c7	     _c8	     _c9	_c10	 _c11	_c12	_c13	_c14	_c15	_c16	_c17	_c18	_c19	_c20	 _c21	_c22	_c23	_c24	_c25	_c26	_c27	_c28	_c29	_c30	       _c31 
 
spark.sql(""" Select * from sandpit.QBR_List""").show(5) 
spark.sql("""drop table sandpit.QBR_PIR_20180817""") 
spark.sql("""create table  
             sandpit.QBR_PIR_20180817 AS 
             Select H as ABN, 
             ANRMAL01 as CAR_SEQ_NUM, 
             _c4 as MEMBERSHIP_NUMBER, 
            _c5 as DESCRIPTION, 
            _c6 as BRAND, 
            _c7 as LOCATION, 
            _c8 as TRANSACTION_START_DATE, 
            _c9 as TRANSACTION_END_DATE, 
            _c10 as POS_OR_NEG_SIGN1, 
            _c11 as GROSS_AMOUNT, 
            _c12 as POS_OR_NEG_SIGN2, 
            _c13 as BASE_DISCOUNT, 
            _c14 as POS_OR_NEG_SIGN3, 
            _c15 as BONUS_DISCOUNT, 
            _c16 as POS_OR_NEG_SIGN4, 
            _c17 as BASE_REBATE, 
            _c18 as POS_OR_NEG_SIGN5, 
            _c19 as BONUS_REBATE, 
            _c20 as POS_OR_NEG_SIGN6, 
            _c21 as NET_AMOUNT, 
            _c22 as POS_OR_NEG_SIGN7, 
            _c23 as BASE_POINTS, 
            _c24 as POS_OR_NEG_SIGN8, 
            _c25 as BONUS_POINTS, 
            _c26 as BONUS_POINTS_BASIS, 
            _c27 as POS_OR_NEG_SIGN9, 
            _c28 as TOTAL_POINTS, 
            _c29 as RECORD_NUMBER, 
            _c30 as ACCEPTED_OR_REJECT, 
            _c31 as REASON_FOR_REJECTION 
         FROM sandpit.QBR_List 
          """) 
 
 
spark.sql("""select * from sandpit.QBR_PIR_20180817""").show()