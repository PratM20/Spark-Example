package com.crif.highmark.CrifSemantic.DataLoad
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._


object MfiDataIngestion extends App{
  val spark = SparkSession.builder().appName("MfiDataIngestion")
.config("hive.metastore.uris", "thrift://chmcisprbdmn01.chm.intra:9083")
.config("hive.exec.dynamic.partition", "true")
.config("hive.exec.dynamic.partition.mode", "nonstrict")
.config("hive.enforce.bucketing", "true")
.config("hive.exec.max.dynamic.partitions", "20000")
.enableHiveSupport().getOrCreate()

val prodMFIQuery = "(SELECT INSERT_DT ,ACCOUNT_KEY ,ACCOUNT_NBR ,REPORTED_DT ,CENTRE_ID ,LOAN_CATEGORY ,MFI_GROUP_ID ,LOAN_CYCLE_ID ,LOAN_PURPOSE ,ACCOUNT_STATUS ,DISBURSED_DT ,CLOSED_DT ,DISBURSED_AMOUNT ,NUM_INSTALLMENT ,INSTAL_FREQ ,CURRENT_BALANCE ,AMOUNT_OVERDUE_TOTAL ,DAYS_PAST_DUE ,CHARGEOFF_AMT ,NUM_ABSENTEES ,INSURANCE_IND ,INSURANCE_TYP ,BRANCH_CODE ,MAX_DEL ,MAX_DEL_DT ,DEL_BUCKET_30 ,DEL_BUCKET_60 ,DEL_BUCKET_90 ,SIX_M_DEL_BUCKET_30 ,SIX_M_DEL_BUCKET_60 ,SIX_M_DEL_BUCKET_90 ,HIGH_CREDIT ,LAST_PAYMENT_DT ,RECENT_DELIQUENCY_DT ,MFI_ID ,WORST_AMOUNT_OVERDUE ,FIRST_DELIQUENCY_DT ,UI_FLAG ,CONSUMER_KEY ,MEM_ID ,SUPPRESS_INDICATOR ,DAS ,CONTRIBUTOR_ID ,ACT_REPORTED_DT ,MIN_AMOUNT_DUE ,CHARGEOFF_DT ,SANCTIONED_AMOUNT ,SANCTIONED_DT ,ACCOUNT_TYPE ,CREDIT_LIMIT ,MOD(ACCOUNT_KEY,10) as part FROM HMRACPROD.HM_MFI_ACCOUNT)A"
val trendMFIQuery = "(SELECT ACCOUNT_KEY ,MFI_ID ,ACCOUNT_STATUS ,REPORTED_DT ,ACT_REPORTED_DT ,CLOSED_DT ,MIN_AMOUNT_DUE ,CURRENT_BALANCE ,AMOUNT_OVERDUE_TOTAL ,DAYS_PAST_DUE ,CHARGEOFF_AMT ,CHARGEOFF_DT ,DAS ,LAST_PAYMENT_DT ,MOD(ACCOUNT_KEY,10) as part FROM CHMMFITREND.HM_MFI_ACCOUNT)B"
val updMFIQuery = "(SELECT ACCOUNT_KEY ,ACCOUNT_NBR ,MFI_ID ,REPORTED_DT ,ACT_REPORTED_DT ,DISBURSED_DT ,SANCTIONED_AMOUNT ,DISBURSED_AMOUNT ,NUM_INSTALLMENT ,INSTAL_FREQ ,LOAN_CATEGORY ,LOAN_CYCLE_ID ,LOAN_PURPOSE ,ACTIVE ,SUPPRESS_INDICATOR ,MOD(ACCOUNT_KEY,10) as part FROM CHMMFITREND.HM_MFI_ACCOUNT_UPD)C"
val prodGlMFIQuery = "(SELECT INSERT_DT ,ACCOUNT_KEY ,ACCOUNT_NBR ,REPORTED_DT ,CENTRE_ID ,LOAN_CATEGORY ,MFI_GROUP_ID ,LOAN_CYCLE_ID ,LOAN_PURPOSE ,ACCOUNT_STATUS ,DISBURSED_DT ,CLOSED_DT ,DISBURSED_AMOUNT ,NUM_INSTALLMENT ,INSTAL_FREQ ,CURRENT_BALANCE ,AMOUNT_OVERDUE_TOTAL ,DAYS_PAST_DUE ,CHARGEOFF_AMT ,NUM_ABSENTEES ,INSURANCE_IND ,INSURANCE_TYP ,BRANCH_CODE ,MAX_DEL ,MAX_DEL_DT ,DEL_BUCKET_30 ,DEL_BUCKET_60 ,DEL_BUCKET_90 ,SIX_M_DEL_BUCKET_30 ,SIX_M_DEL_BUCKET_60 ,SIX_M_DEL_BUCKET_90 ,HIGH_CREDIT ,LAST_PAYMENT_DT ,RECENT_DELIQUENCY_DT ,MFI_ID ,WORST_AMOUNT_OVERDUE ,FIRST_DELIQUENCY_DT ,UI_FLAG ,CONSUMER_KEY ,MEM_ID ,SUPPRESS_INDICATOR ,DAS ,CONTRIBUTOR_ID ,ACT_REPORTED_DT ,MIN_AMOUNT_DUE ,CHARGEOFF_DT ,SANCTIONED_AMOUNT ,SANCTIONED_DT ,ACCOUNT_TYPE ,CREDIT_LIMIT ,MOD(ACCOUNT_KEY,10) as part FROM HMRACPROD.HM_MFI_ACCOUNT_GL)D"
val trendGlMFIQuery = "(SELECT ACCOUNT_KEY ,MFI_ID ,ACCOUNT_STATUS ,REPORTED_DT ,ACT_REPORTED_DT ,CLOSED_DT ,MIN_AMOUNT_DUE ,CURRENT_BALANCE ,AMOUNT_OVERDUE_TOTAL ,DAYS_PAST_DUE ,CHARGEOFF_AMT ,CHARGEOFF_DT ,DAS ,LAST_PAYMENT_DT ,MOD(ACCOUNT_KEY,10) as part FROM CHMMFITREND.HM_MFI_ACCOUNT_GL)E"
val updGlMFIQuery = "(SELECT ACCOUNT_KEY ,ACCOUNT_NBR ,MFI_ID ,REPORTED_DT ,ACT_REPORTED_DT ,DISBURSED_DT ,SANCTIONED_AMOUNT ,DISBURSED_AMOUNT ,NUM_INSTALLMENT ,INSTAL_FREQ ,LOAN_CATEGORY ,LOAN_CYCLE_ID ,LOAN_PURPOSE ,ACTIVE ,SUPPRESS_INDICATOR ,MOD(ACCOUNT_KEY,10) as part FROM CHMMFITREND.HM_MFI_ACCOUNT_UPD_GL)F"
val cnsMFIQuery = "(select  INSERT_DT,PIN_CODE, STD_STATE_CODE, STD_LOCALITY1_VALUE, STD_LOCALITY1A_VALUE, STD_CITY_1, STD_CITY_2, STD_DERIVED_CITY, DISTRICT, CONSUMER_KEY,BRANCH_ID, MONTHLY_INCOME,MEM_ID,KENDRA_ID,GROUP_ID,AGE,AGE_ASON_DT, BIRTH_DT, GENDER,MFI_ID,OCCUPATION ,MOD(CONSUMER_KEY,10) as part from HMRACPROD.HM_MFI_ANALYTICS_NEW@x7_to_x4)G"

val mon_rn = spark.sparkContext.getConf.get("spark.driver.args")


val prodMfiDF = spark.read.format("jdbc") 
.option("url", "jdbc:oracle:thin:@172.16.5.2:1521:CHM01")
.option("dbtable",prodMFIQuery)
.option("user", "HMANALYTICS") .option("password", "HM#2019") 
.option("driver", "oracle.jdbc.driver.OracleDriver")
.option("numPartitions", 9)
.option("partitionColumn", "PART")
.option("lowerBound", 0).option("upperBound", 9)
.option("fetchSize","10000").load()

val trendMfiDF = spark.read.format("jdbc") 
.option("url", "jdbc:oracle:thin:@172.16.5.2:1521:CHM01")
.option("dbtable",trendMFIQuery)
.option("user", "HMANALYTICS") .option("password", "HM#2019") 
.option("driver", "oracle.jdbc.driver.OracleDriver")
.option("numPartitions", 9)
.option("partitionColumn", "PART")
.option("lowerBound", 0).option("upperBound", 9)
.option("fetchSize","10000").load()

val updMfiDF = spark.read.format("jdbc") 
.option("url", "jdbc:oracle:thin:@172.16.5.2:1521:CHM01")
.option("dbtable",updMFIQuery)
.option("user", "HMANALYTICS") .option("password", "HM#2019") 
.option("driver", "oracle.jdbc.driver.OracleDriver")
.option("numPartitions", 9)
.option("partitionColumn", "PART")
.option("lowerBound", 0).option("upperBound", 9)
.option("fetchSize","10000").load()

val prodGlMfiDF = spark.read.format("jdbc") 
.option("url", "jdbc:oracle:thin:@172.16.5.2:1521:CHM01")
.option("dbtable",prodGlMFIQuery)
.option("user", "HMANALYTICS") .option("password", "HM#2019") 
.option("driver", "oracle.jdbc.driver.OracleDriver")
.option("numPartitions", 9)
.option("partitionColumn", "PART")
.option("lowerBound", 0).option("upperBound", 9)
.option("fetchSize","10000").load()

val trendGlMfiDF = spark.read.format("jdbc") 
.option("url", "jdbc:oracle:thin:@172.16.5.2:1521:CHM01")
.option("dbtable",trendGlMFIQuery)
.option("user", "HMANALYTICS") .option("password", "HM#2019") 
.option("driver", "oracle.jdbc.driver.OracleDriver")
.option("numPartitions", 9)
.option("partitionColumn", "PART")
.option("lowerBound", 0).option("upperBound", 9)
.option("fetchSize","10000").load()

val updGlMfiDF = spark.read.format("jdbc") 
.option("url", "jdbc:oracle:thin:@172.16.5.2:1521:CHM01")
.option("dbtable",updGlMFIQuery)
.option("user", "HMANALYTICS") .option("password", "HM#2019") 
.option("driver", "oracle.jdbc.driver.OracleDriver")
.option("numPartitions", 9)
.option("partitionColumn", "PART")
.option("lowerBound", 0).option("upperBound", 9)
.option("fetchSize","10000").load()

val consMfiDF = spark.read.format("jdbc")
.option("url", "jdbc:oracle:thin:@172.16.5.2:1521:CHM01")
.option("dbtable", cnsMFIQuery)
.option("user", "HMANALYTICS").option("password", "HM#2019") 
.option("driver", "oracle.jdbc.driver.OracleDriver")
.option("numPartitions", 9)
.option("partitionColumn", "PART") 
.option("lowerBound", 0)
.option("upperBound", 9)
.option("fetchSize","10000").load()

val mfiClustDF = spark.read.format("jdbc") .option("url", "jdbc:oracle:thin:@172.16.5.2:1521:CHM01")
.option("dbtable", "(SELECT  CLST_ID,CANDIDATE_ID,MOD(CANDIDATE_ID,10) as part FROM HMCORE.HM_INQ_MATCH_INVOL_P1_R01) a")
.option("user", "HMANALYTICS") .option("password", "HM#2019")
.option("driver", "oracle.jdbc.driver.OracleDriver").option("numPartitions", 10)
.option("partitionColumn", "part").option("lowerBound", 0).option("upperBound", 9).option("fetchSize","10000").load()

val maxmfiCLstDF = mfiClustDF.distinct.groupBy(col("CANDIDATE_ID")).agg(max(col("CLST_ID")).as("MAX_CLST_ID"))
maxmfiCLstDF.drop(col("PART")).repartition(500).write.mode("overwrite").saveAsTable("hmanalytics.hm_mfi_clst_"+mon_rn+"")

prodMfiDF.drop(col("PART")).repartition(500).write.mode("overwrite").insertInto("hmanalytics.hm_mfi_account_racprd")
trendMfiDF.drop(col("PART")).repartition(500).write.mode("overwrite").insertInto("hmanalytics.hm_mfi_account_mfitrend")
updMfiDF.drop(col("PART")).repartition(500).write.mode("overwrite").insertInto("hmanalytics.hm_mfi_account_upd_mfitrend")
prodGlMfiDF.drop(col("PART")).repartition(500).write.mode("overwrite").insertInto("hmanalytics.hm_mfi_account_gl_racprd")
trendGlMfiDF.drop(col("PART")).repartition(500).write.mode("overwrite").insertInto("hmanalytics.hm_mfi_account_gl_mfitrend")
updGlMfiDF.drop(col("PART")).repartition(500).write.mode("overwrite").insertInto("hmanalytics.hm_mfi_account_upd_gl_mfitrend")
consMfiDF.drop(col("PART")).repartition(500).write.mode("overwrite").insertInto("hmanalytics.hm_mfi_analytics")


/*Dimension table Ingestion */

var dimDF = spark.read.format("jdbc") .option("url", "jdbc:oracle:thin:@10.10.10.117:1521:CHMDB2") .option("dbtable", "(select *  from HM_LOAN_ACCOUNT_TYPE_DIM ) a") .option("user", "HMANALYTICS") .option("password", "hmanalytics") .option("driver", "oracle.jdbc.driver.OracleDriver") .option("fetchSize","10000").load()
dimDF.write.mode("overwrite").saveAsTable("hmanalytics.HM_LOAN_ACCOUNT_TYPE_DIM")
dimDF = spark.read.format("jdbc") .option("url", "jdbc:oracle:thin:@10.10.10.117:1521:CHMDB2") .option("dbtable", "(select *  from HM_LOAN_ACCOUNT_STATUS_DIM ) a") .option("user", "HMANALYTICS") .option("password", "hmanalytics") .option("driver", "oracle.jdbc.driver.OracleDriver") .option("fetchSize","10000").load()
dimDF.write.mode("overwrite").saveAsTable("hmanalytics.HM_LOAN_ACCOUNT_STATUS_DIM")
dimDF = spark.read.format("jdbc") .option("url", "jdbc:oracle:thin:@10.10.10.117:1521:CHMDB2") .option("dbtable", "(select *  from HM_DAS_DIM ) a").option("user", "HMANALYTICS") .option("password", "hmanalytics") .option("driver", "oracle.jdbc.driver.OracleDriver") .option("fetchSize","10000").load()
dimDF.write.mode("overwrite").saveAsTable("hmanalytics.HM_DAS_DIM")
dimDF = spark.read.format("jdbc") .option("url", "jdbc:oracle:thin:@10.10.10.117:1521:CHMDB2") .option("dbtable", "(select *  from HM_LOAN_DPD_BUCKET_DIM_NEW ) a") .option("user", "HMANALYTICS") .option("password", "hmanalytics") .option("driver", "oracle.jdbc.driver.OracleDriver") .option("fetchSize","10000").load()
dimDF.write.mode("overwrite").saveAsTable("hmanalytics.HM_LOAN_DPD_BUCKET_DIM_NEW")
dimDF = spark.read.format("jdbc") .option("url", "jdbc:oracle:thin:@10.10.10.117:1521:CHMDB2") .option("dbtable", "(select *  from HM_LOAN_TICKET_SIZE_DIM_NEW2 ) a") .option("user", "HMANALYTICS") .option("password", "hmanalytics") .option("driver", "oracle.jdbc.driver.OracleDriver") .option("fetchSize","10000").load()
dimDF.write.mode("overwrite").saveAsTable("hmanalytics.HM_LOAN_TICKET_SIZE_DIM_NEW2")
dimDF = spark.read.format("jdbc") .option("url", "jdbc:oracle:thin:@10.10.10.117:1521:CHMDB2") .option("dbtable", "(select *  from HM_LOAN_CYCLE_DIM ) a") .option("user", "HMANALYTICS") .option("password", "hmanalytics") .option("driver", "oracle.jdbc.driver.OracleDriver") .option("fetchSize","10000").load()
dimDF.write.mode("overwrite").saveAsTable("hmanalytics.HM_LOAN_CYCLE_DIM")
dimDF = spark.read.format("jdbc") .option("url", "jdbc:oracle:thin:@10.10.10.117:1521:CHMDB2") .option("dbtable", "(select *  from HM_LOAN_CYCLE_DIM_V1 ) a") .option("user", "HMANALYTICS") .option("password", "hmanalytics") .option("driver", "oracle.jdbc.driver.OracleDriver") .option("fetchSize","10000").load()
dimDF.write.mode("overwrite").saveAsTable("hmanalytics.HM_LOAN_CYCLE_DIM_V1")
dimDF = spark.read.format("jdbc") .option("url", "jdbc:oracle:thin:@10.10.10.117:1521:CHMDB2") .option("dbtable", "(select *  from HM_LOAN_NUM_INSTAL_BUCKET_DIM ) a") .option("user", "HMANALYTICS") .option("password", "hmanalytics") .option("driver", "oracle.jdbc.driver.OracleDriver") .option("fetchSize","10000").load()
dimDF.write.mode("overwrite").saveAsTable("hmanalytics.HM_LOAN_NUM_INSTAL_BUCKET_DIM")
dimDF = spark.read.format("jdbc") .option("url", "jdbc:oracle:thin:@10.10.10.117:1521:CHMDB2") .option("dbtable", "(select *  from HM_LOAN_INSTALL_FREQ_DIM ) a") .option("user", "HMANALYTICS") .option("password", "hmanalytics") .option("driver", "oracle.jdbc.driver.OracleDriver") .option("fetchSize","10000").load()
dimDF.write.mode("overwrite").saveAsTable("hmanalytics.HM_LOAN_INSTALL_FREQ_DIM")
dimDF = spark.read.format("jdbc") .option("url", "jdbc:oracle:thin:@10.10.10.117:1521:CHMDB2") .option("dbtable", "(select *  from HM_BORROWER_INCOME_LEVEL_DIM ) a") .option("user", "HMANALYTICS") .option("password", "hmanalytics") .option("driver", "oracle.jdbc.driver.OracleDriver") .option("fetchSize","10000").load()
dimDF.write.mode("overwrite").saveAsTable("hmanalytics.HM_BORROWER_INCOME_LEVEL_DIM")
dimDF = spark.read.format("jdbc") .option("url", "jdbc:oracle:thin:@10.10.10.117:1521:CHMDB2") .option("dbtable", "(select *  from HM_BORROWER_AGE_BUCKET_DIM_N ) a") .option("user", "HMANALYTICS") .option("password", "hmanalytics") .option("driver", "oracle.jdbc.driver.OracleDriver") .option("fetchSize","10000").load()
dimDF.write.mode("overwrite").saveAsTable("hmanalytics.HM_BORROWER_AGE_BUCKET_DIM_N")
dimDF = spark.read.format("jdbc") .option("url", "jdbc:oracle:thin:@10.10.10.117:1521:CHMDB2") .option("dbtable", "(select *  from HM_BORROWER_AGE_BUCKET_DIM ) a") .option("user", "HMANALYTICS") .option("password", "hmanalytics") .option("driver", "oracle.jdbc.driver.OracleDriver") .option("fetchSize","10000").load()
dimDF.write.mode("overwrite").saveAsTable("hmanalytics.HM_BORROWER_AGE_BUCKET_DIM")
dimDF = spark.read.format("jdbc") .option("url", "jdbc:oracle:thin:@10.10.10.117:1521:CHMDB2") .option("dbtable", "(select *  from HM_BORROWER_AGE_BUCKET_DIM_V1 ) a") .option("user", "HMANALYTICS") .option("password", "hmanalytics") .option("driver", "oracle.jdbc.driver.OracleDriver") .option("fetchSize","10000").load()
dimDF.write.mode("overwrite").saveAsTable("hmanalytics.HM_BORROWER_AGE_BUCKET_DIM_V1")
dimDF = spark.read.format("jdbc") .option("url", "jdbc:oracle:thin:@10.10.10.117:1521:CHMDB2") .option("dbtable", "(select *  from HM_BORROWER_OCCUPATION_DIM ) a") .option("user", "HMANALYTICS") .option("password", "hmanalytics") .option("driver", "oracle.jdbc.driver.OracleDriver") .option("fetchSize","10000").load()
dimDF.write.mode("overwrite").saveAsTable("hmanalytics.HM_BORROWER_OCCUPATION_DIM")
dimDF = spark.read.format("jdbc") .option("url", "jdbc:oracle:thin:@10.10.10.117:1521:CHMDB2") .option("dbtable", "(select *  from HM_BORROWER_LOAN_PURPOSE_DIM ) a") .option("user", "HMANALYTICS") .option("password", "hmanalytics") .option("driver", "oracle.jdbc.driver.OracleDriver") .option("fetchSize","10000").load()
dimDF.write.mode("overwrite").saveAsTable("hmanalytics.HM_BORROWER_LOAN_PURPOSE_DIM")
dimDF = spark.read.format("jdbc") .option("url", "jdbc:oracle:thin:@10.10.10.117:1521:CHMDB2") .option("dbtable", "(select *  from HM_SUPPRESS_IND_DIM ) a") .option("user", "HMANALYTICS") .option("password", "hmanalytics") .option("driver", "oracle.jdbc.driver.OracleDriver") .option("fetchSize","10000").load()
dimDF.write.mode("overwrite").saveAsTable("hmanalytics.HM_SUPPRESS_IND_DIM")
dimDF = spark.read.format("jdbc") .option("url", "jdbc:oracle:thin:@10.10.10.117:1521:CHMDB2") .option("dbtable", "(select *  from HM_BORROWER_MINAMT_DUE_DIM ) a") .option("user", "HMANALYTICS") .option("password", "hmanalytics") .option("driver", "oracle.jdbc.driver.OracleDriver") .option("fetchSize","10000").load()
dimDF.write.mode("overwrite").saveAsTable("hmanalytics.HM_BORROWER_MINAMT_DUE_DIM")
dimDF = spark.read.format("jdbc") .option("url", "jdbc:oracle:thin:@10.10.10.117:1521:CHMDB2") .option("dbtable", "(select *  from HM_DATE_DIM2 ) a") .option("user", "HMANALYTICS") .option("password", "hmanalytics") .option("driver", "oracle.jdbc.driver.OracleDriver") .option("fetchSize","10000").load()
dimDF.write.mode("overwrite").saveAsTable("hmanalytics.HM_DATE_DIM2")
dimDF = spark.read.format("jdbc") .option("url", "jdbc:oracle:thin:@10.10.10.117:1521:CHMDB2") .option("dbtable", "(select *  from HM_SDP_DIM2_V1 ) a") .option("user", "HMANALYTICS") .option("password", "hmanalytics") .option("driver", "oracle.jdbc.driver.OracleDriver") .option("fetchSize","10000").load()
dimDF.write.mode("overwrite").saveAsTable("hmanalytics.HM_SDP_DIM2_V1")
dimDF = spark.read.format("jdbc") .option("url", "jdbc:oracle:thin:@10.10.10.117:1521:CHMDB2") .option("dbtable", "(select *  from HM_SDP_DIM2 ) a") .option("user", "HMANALYTICS") .option("password", "hmanalytics") .option("driver", "oracle.jdbc.driver.OracleDriver") .option("fetchSize","10000").load()
dimDF.write.mode("overwrite").saveAsTable("hmanalytics.HM_SDP_DIM2")
dimDF = spark.read.format("jdbc") .option("url", "jdbc:oracle:thin:@10.10.10.117:1521:CHMDB2") .option("dbtable", "(select *  from HM_CONTRIBUTOR_DIM ) a") .option("user", "HMANALYTICS") .option("password", "hmanalytics") .option("driver", "oracle.jdbc.driver.OracleDriver") .option("fetchSize","10000").load()
dimDF.write.mode("overwrite").saveAsTable("hmanalytics.HM_CONTRIBUTOR_DIM")



}