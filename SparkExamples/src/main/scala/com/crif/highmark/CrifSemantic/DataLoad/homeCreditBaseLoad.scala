package com.crif.highmark.CrifSemantic.DataLoad
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.types._
import org.apache.spark.sql.SQLContext
import com.crif.highmark.CrifSemantic.Service.homeCreditDataBlend



object homeCreditBaseLoad  extends App{
	
	 val spark = SparkSession.builder().appName("homeCreditLoad")
											.config("hive.metastore.uris", "thrift://chmcisprbdmn01.chm.intra:9083")
											.config("hive.exec.dynamic.partition", "true")
                      .config("hive.exec.dynamic.partition.mode", "nonstrict")
                      .config("hive.enforce.bucketing", "true")
                      .config("hive.exec.max.dynamic.partitions", "20000")
											.enableHiveSupport().getOrCreate()
											
val argument = spark.sparkContext.getConf.get("spark.driver.args").split("\\s+")
val mon_rn=argument(0)
val end_dt=argument(1)
val rpt_dt=argument(2)										
  
import spark.implicits._

val a13ActDF = spark.sql(s"""
select 
  CONSUMER_KEY 
from 
  hmanalytics.hm_cns_macro_analysis_"""+mon_rn+"""_parquet 

		""")
		
		
val a13CloDF = spark.sql(s"""
select 
  CONSUMER_KEY 
from 
  hmanalytics.hm_cns_closed_macro_analysis 
where 
  (
    closed_dt IS NULL 
    or closed_dt <= from_unixtime(
      unix_timestamp('"""+end_dt+"""', 'dd-MMM-yy'), 
      'yyyy-MM-dd'
    )
  ) 
  and REPORTED_DT <= from_unixtime(
    unix_timestamp('"""+rpt_dt+"""', 'ddMMyyyy'), 
    'yyyy-MM-dd'
  ) 
  and ACT_INSERT_DT <= from_unixtime(
    unix_timestamp('"""+rpt_dt+"""', 'ddMMyyyy'), 
    'yyyy-MM-dd'
  ) 
 
		

""")

val unionA13 = a13ActDF.union(a13CloDF).distinct()

val clusterDF = spark.sql(s"select CANDIDATE_ID,MAX_CLST_ID from hmanalytics.hm_cns_clst_nov_19").distinct()

val joinDF = unionA13.join(clusterDF , unionA13("CONSUMER_KEY") === clusterDF("CANDIDATE_ID")).select(clusterDF("MAX_CLST_ID").as("CLUSTER_ID"))

val joinClustDF = clusterDF.join(joinDF, joinDF("CLUSTER_ID") === clusterDF("MAX_CLST_ID")).drop(joinDF("CLUSTER_ID"))

val allClstActDF = spark.sql(s"""
SELECT 
  STD_STATE_CODE, 
  DISTRICT, 
  PIN_CODE, 
  CONSUMER_KEY, 
  ACCOUNT_KEY, 
  MFI_ID, 
  ACCOUNT_TYPE, 
  DISBURSED_DT, 
  REPORTED_DT, 
  abs(HIGH_CREDIT) HIGH_CREDIT, 
  abs(DISBURSED_AMOUNT) DISBURSED_AMOUNT, 
  abs(CREDIT_LIMIT) CREDIT_LIMIT, 
  abs(AMOUNT_OVERDUE_TOTAL) AMOUNT_OVERDUE_TOTAL, 
  abs(CURRENT_BALANCE) CURRENT_BALANCE, 
  abs(CHARGEOFF_AMT) CHARGEOFF_AMT, 
  CLOSED_DT, 
  nvl(
    cast(
      regexp_extract(DAYS_PAST_DUE, '[0-9]+', 0) as int
    ), 
    0
  ) DAYS_PAST_DUE, 
  CASE WHEN INSTAL_FREQ IN ('1', '01', 'FO1') THEN 'F01' WHEN INSTAL_FREQ IN ('2', '02', 'FO2') THEN 'F02' WHEN INSTAL_FREQ IN ('3', '03', 'FO3') THEN 'F03' WHEN INSTAL_FREQ IN ('4', '04', 'FO4') THEN 'F04' WHEN INSTAL_FREQ IN ('5', '05', 'FO5') THEN 'F05' WHEN INSTAL_FREQ IN ('6', '06', 'FO6') THEN 'F06' WHEN INSTAL_FREQ IN ('7', '07', 'FO7') THEN 'F07' WHEN INSTAL_FREQ IN ('8', '08', 'FO8') THEN 'F08' WHEN (
    INSTAL_FREQ = '9' 
    OR INSTAL_FREQ = 'F09'
  ) THEN 'F10' WHEN INSTAL_FREQ IN ('10') THEN 'F10' WHEN INSTAL_FREQ IS NULL THEN 'BLANK' WHEN INSTAL_FREQ NOT IN (
    'F01', 'F02', 'F03', 'F04', 'F05', 'F06', 
    'F07', 'F08', 'F10'
  ) THEN 'NA' ELSE UPPER(INSTAL_FREQ) END INSTAL_FREQ, 
  DAS_FLAG, 
  IS_COMMERCIAL, 
  DAS, 
  DAC, 
  BIRTH_DT, 
  cast(
    (
      MONTHS_BETWEEN(
        from_unixtime(
          unix_timestamp('"""+end_dt+"""', 'dd-MMM-yy'), 
          'yyyy-MM-dd'
        ), 
        BIRTH_DT
      )/ 12
    ) as int
  ) AGE, 
  GENDER, 
  cast(
    CASE WHEN regexp_extract(DAYS_PAST_DUE, '[0-9]+', 0) IS NOT NULL 
    AND TRIM(DAC) IS NULL THEN nvl(
      cast(
        regexp_extract(DAYS_PAST_DUE, '[0-9]+', 0) as int
      ), 
      0
    ) WHEN regexp_extract(DAYS_PAST_DUE, '[0-9]+', 0) IS NULL 
    AND TRIM(DAC) IS NOT NULL THEN CASE WHEN TRIM(DAC) = 'L01' THEN 0 WHEN TRIM(DAC) IN ('L02') THEN 91 WHEN TRIM(DAC) IN ('L03', 'L04') THEN 361 WHEN TRIM(DAC) IN ('L05') THEN 1 END WHEN regexp_extract(DAYS_PAST_DUE, '[0-9]+', 0) IS NOT NULL 
    AND TRIM(DAC) IS NOT NULL THEN CASE WHEN NVL(
      cast(
        CASE WHEN TRIM(DAC) = 'L01' THEN 0 WHEN TRIM(DAC) IN ('L02') THEN 91 WHEN TRIM(DAC) IN ('L03', 'L04') THEN 361 WHEN TRIM(DAC) IN ('L05') THEN 1 END as int
      ), 
      0
    ) <= nvl(
      cast(
        regexp_extract(DAYS_PAST_DUE, '[0-9]+', 0) as int
      ), 
      0
    ) THEN nvl(
      cast(
        regexp_extract(DAYS_PAST_DUE, '[0-9]+', 0) as int
      ), 
      0
    ) ELSE NVL(
      cast(
        CASE WHEN TRIM(DAC) = 'L01' THEN 0 WHEN TRIM(DAC) IN ('L02') THEN 91 WHEN TRIM(DAC) IN ('L03', 'L04') THEN 361 WHEN TRIM(DAC) IN ('L05') THEN 1 END as int
      ), 
      0
    ) END WHEN regexp_extract(DAYS_PAST_DUE, '[0-9]+', 0) IS NULL 
    AND TRIM(DAC) IS NULL THEN CASE WHEN DAS = 'S04' THEN 0 WHEN DAS = 'S05' THEN 1 ELSE 0 END END as int
  ) DPD, 
  regexp_extract(DAYS_PAST_DUE, '[0-9]+', 0) ACTUAL_DPD, 
  CASE WHEN STD_STATE_CODE IS NOT NULL 
  AND DISTRICT IS NOT NULL 
  AND PIN_CODE IS NOT NULL THEN 1 WHEN STD_STATE_CODE IS NOT NULL 
  AND DISTRICT IS NOT NULL 
  AND PIN_CODE IS NULL THEN 2 WHEN STD_STATE_CODE IS NOT NULL 
  AND DISTRICT IS NULL 
  AND PIN_CODE IS NOT NULL THEN 3 WHEN STD_STATE_CODE IS NULL 
  AND DISTRICT IS NOT NULL 
  AND PIN_CODE IS NOT NULL THEN 4 ELSE 5 END SDP, 
  OWNERSHIP_IND, 
  NVL(
    SUBSTR(OWNERSHIP_IND, 3, 1), 
    9
  ) OWNERSHIP_IND_FLAG 
FROM 

  hmanalytics.hm_cns_macro_analysis_"""+mon_rn+"""_parquet

""")

val allClstClosDF = spark.sql(s"""
SELECT 
  STD_STATE_CODE, 
  DISTRICT, 
  PIN_CODE, 
  CONSUMER_KEY, 
  ACCOUNT_KEY, 
  MFI_ID, 
  ACCOUNT_TYPE, 
  DISBURSED_DT, 
  REPORTED_DT, 
  abs(HIGH_CREDIT) HIGH_CREDIT, 
  abs(DISBURSED_AMOUNT) DISBURSED_AMOUNT, 
  abs(CREDIT_LIMIT) CREDIT_LIMIT, 
  abs(AMOUNT_OVERDUE_TOTAL) AMOUNT_OVERDUE_TOTAL, 
  abs(CURRENT_BALANCE) CURRENT_BALANCE, 
  abs(CHARGEOFF_AMT) CHARGEOFF_AMT, 
  CLOSED_DT, 
  nvl(
    cast(
      regexp_extract(DAYS_PAST_DUE, '[0-9]+', 0) as int
    ), 
    0
  ) DAYS_PAST_DUE, 
  CASE WHEN INSTAL_FREQ IN ('1', '01', 'FO1') THEN 'F01' WHEN INSTAL_FREQ IN ('2', '02', 'FO2') THEN 'F02' WHEN INSTAL_FREQ IN ('3', '03', 'FO3') THEN 'F03' WHEN INSTAL_FREQ IN ('4', '04', 'FO4') THEN 'F04' WHEN INSTAL_FREQ IN ('5', '05', 'FO5') THEN 'F05' WHEN INSTAL_FREQ IN ('6', '06', 'FO6') THEN 'F06' WHEN INSTAL_FREQ IN ('7', '07', 'FO7') THEN 'F07' WHEN INSTAL_FREQ IN ('8', '08', 'FO8') THEN 'F08' WHEN (
    INSTAL_FREQ = '9' 
    OR INSTAL_FREQ = 'F09'
  ) THEN 'F10' WHEN INSTAL_FREQ IN ('10') THEN 'F10' WHEN INSTAL_FREQ IS NULL THEN 'BLANK' WHEN INSTAL_FREQ NOT IN (
    'F01', 'F02', 'F03', 'F04', 'F05', 'F06', 
    'F07', 'F08', 'F10'
  ) THEN 'NA' ELSE UPPER(INSTAL_FREQ) END INSTAL_FREQ, 
  0 as DAS_FLAG, 
  IS_COMMERCIAL, 
  DAS, 
  DAC, 
  BIRTH_DT, 
  cast(
    (
      MONTHS_BETWEEN(
        from_unixtime(
          unix_timestamp('"""+end_dt+"""', 'dd-MMM-yy'), 
          'yyyy-MM-dd'
        ), 
        BIRTH_DT
      )/ 12
    ) as int
  ) AGE, 
  GENDER, 
  cast(
    CASE WHEN regexp_extract(DAYS_PAST_DUE, '[0-9]+', 0) IS NOT NULL 
    AND TRIM(DAC) IS NULL THEN nvl(
      cast(
        regexp_extract(DAYS_PAST_DUE, '[0-9]+', 0) as int
      ), 
      0
    ) WHEN regexp_extract(DAYS_PAST_DUE, '[0-9]+', 0) IS NULL 
    AND TRIM(DAC) IS NOT NULL THEN CASE WHEN TRIM(DAC) = 'L01' THEN 0 WHEN TRIM(DAC) IN ('L02') THEN 91 WHEN TRIM(DAC) IN ('L03', 'L04') THEN 361 WHEN TRIM(DAC) IN ('L05') THEN 1 END WHEN regexp_extract(DAYS_PAST_DUE, '[0-9]+', 0) IS NOT NULL 
    AND TRIM(DAC) IS NOT NULL THEN CASE WHEN NVL(
      cast(
        CASE WHEN TRIM(DAC) = 'L01' THEN 0 WHEN TRIM(DAC) IN ('L02') THEN 91 WHEN TRIM(DAC) IN ('L03', 'L04') THEN 361 WHEN TRIM(DAC) IN ('L05') THEN 1 END as int
      ), 
      0
    ) <= nvl(
      cast(
        regexp_extract(DAYS_PAST_DUE, '[0-9]+', 0) as int
      ), 
      0
    ) THEN nvl(
      cast(
        regexp_extract(DAYS_PAST_DUE, '[0-9]+', 0) as int
      ), 
      0
    ) ELSE NVL(
      cast(
        CASE WHEN TRIM(DAC) = 'L01' THEN 0 WHEN TRIM(DAC) IN ('L02') THEN 91 WHEN TRIM(DAC) IN ('L03', 'L04') THEN 361 WHEN TRIM(DAC) IN ('L05') THEN 1 END as int
      ), 
      0
    ) END WHEN regexp_extract(DAYS_PAST_DUE, '[0-9]+', 0) IS NULL 
    AND TRIM(DAC) IS NULL THEN CASE WHEN DAS = 'S04' THEN 0 WHEN DAS = 'S05' THEN 1 ELSE 0 END END as int
  ) DPD, 
  regexp_extract(DAYS_PAST_DUE, '[0-9]+', 0) ACTUAL_DPD, 
  CASE WHEN STD_STATE_CODE IS NOT NULL 
  AND DISTRICT IS NOT NULL 
  AND PIN_CODE IS NOT NULL THEN 1 WHEN STD_STATE_CODE IS NOT NULL 
  AND DISTRICT IS NOT NULL 
  AND PIN_CODE IS NULL THEN 2 WHEN STD_STATE_CODE IS NOT NULL 
  AND DISTRICT IS NULL 
  AND PIN_CODE IS NOT NULL THEN 3 WHEN STD_STATE_CODE IS NULL 
  AND DISTRICT IS NOT NULL 
  AND PIN_CODE IS NOT NULL THEN 4 ELSE 5 END SDP, 
  OWNERSHIP_IND, 
  NVL(
    SUBSTR(OWNERSHIP_IND, 3, 1), 
    9
  ) OWNERSHIP_IND_FLAG 
FROM 
  hmanalytics.hm_cns_closed_macro_analysis 
where 
  (
    closed_dt IS NULL 
    or closed_dt <= from_unixtime(
      unix_timestamp('"""+end_dt+"""', 'dd-MMM-yy'), 
      'yyyy-MM-dd'
    )
  ) 
  and REPORTED_DT <= from_unixtime(
    unix_timestamp('"""+rpt_dt+"""', 'ddMMyyyy'), 
    'yyyy-MM-dd'
  ) 
  and ACT_INSERT_DT <= from_unixtime(
    unix_timestamp('"""+rpt_dt+"""', 'ddMMyyyy'), 
    'yyyy-MM-dd'
  )

""")

val unionAllClstDF = allClstActDF.union(allClstClosDF)



val joinAllClstDF = unionAllClstDF.join(joinClustDF , unionAllClstDF("CONSUMER_KEY") === joinClustDF("CANDIDATE_ID"))

val extraProdDF = spark.sql(s"""
select 
  ACCOUNT_KEY, 
  CONSUMER_KEY, 
  SUPPRESS_INDICATOR, 
  MIN_AMOUNT_DUE, 
  ANNUAL_INCOME, 
  MONTHLY_INCOME 
from 
  hmanalytics.hm_hc_cluster_tbl_extra_col_new

""").distinct()

val extraJoinDF1 = joinAllClstDF.join(extraProdDF , joinAllClstDF("CONSUMER_KEY") === extraProdDF("CONSUMER_KEY") && joinAllClstDF("ACCOUNT_KEY") === extraProdDF("ACCOUNT_KEY"),"left")
															 .drop(extraProdDF("CONSUMER_KEY")).drop(extraProdDF("ACCOUNT_KEY")).distinct().persist()
		extraJoinDF1.repartition(2000).write.mode("overwrite").saveAsTable(s"""hmanalytics.hm_hc_work_1""")
															 

val extraJoinDF = spark.sql("select * from hmanalytics.hm_hc_work_1")

homeCreditDataBlend.DataBlend(spark , extraJoinDF ,argument)


}