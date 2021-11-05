package com.crif.highmark.CrifSemantic.DataLoad

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

object accountTrendLoad extends App {
  val spark = SparkSession.builder().appName("accountTrendLoad")
.config("hive.metastore.uris", "thrift://chmcisprbdmn01.chm.intra:9083")
.config("hive.exec.dynamic.partition", "true")
.config("hive.exec.dynamic.partition.mode", "nonstrict")
.config("hive.enforce.bucketing", "true")
.config("hive.exec.max.dynamic.partitions", "20000")
.enableHiveSupport().getOrCreate()

val mon_rn = spark.sparkContext.getConf.get("spark.driver.args")

val accDF = spark.read.format("jdbc") 
.option("url", "jdbc:oracle:thin:@172.16.5.2:1521:CHM01")
.option("dbtable", "(select cast(ACCOUNT_KEY as number(38,0)) ACCOUNT_KEY ,MFI_ID ,REPORTED_DT ,ACT_REPORTED_DT ,MIN_AMOUNT_DUE ,CURRENT_BALANCE ,AMOUNT_OVERDUE_TOTAL ,DAYS_PAST_DUE ,CHARGEOFF_AMT ,ASSET_CLASS ,SUIT_FILED ,WRITTEN_OFF_STATUS ,SETLLEMENT_AMT ,SPECIAL_REMARKS_1 ,MANUAL_UPDATE_DT ,MANUAL_UPDATE_IND ,DAS ,UPDATE_DT ,CREDIT_FACILITY_STATUS ,ACCOUNT_TYPE ,CHARGEOFF_DT ,LAST_PAYMENT_DT ,to_char(INTEREST_RATE) INTEREST_RATE  ,CHARGEOFF_AMT_PRINCIPAL ,to_char(ACTUAL_PAYMT_AMT) ACTUAL_PAYMT_AMT ,HIGH_CREDIT,CAST(EXTRACT(month FROM ACT_REPORTED_DT) AS INTEGER) MONTH from CHMCNSTREND.HM_MFI_ACCOUNT where EXTRACT(year FROM ACT_REPORTED_DT) = "+mon_rn+" ) a")
.option("user", "E082") .option("password", "E082##123") 
.option("driver", "oracle.jdbc.driver.OracleDriver")
.option("numPartitions", 11)
.option("partitionColumn", "MONTH")
.option("lowerBound", 1).option("upperBound", 12)
.option("fetchSize","10000").load()

accDF.drop(col("MONTH")).repartition(2000).write.mode("append").insertInto("hmanalytics.hm_mfi_account_trend")


/*val df = spark.sql("""
select ACCOUNT_KEY ,MFI_ID ,ACT_REPORTED_DT ,MIN_AMOUNT_DUE ,CURRENT_BALANCE ,
AMOUNT_OVERDUE_TOTAL ,DAYS_PAST_DUE ,CHARGEOFF_AMT ,ASSET_CLASS ,SUIT_FILED ,
WRITTEN_OFF_STATUS ,SETLLEMENT_AMT ,SPECIAL_REMARKS_1 ,MANUAL_UPDATE_DT ,MANUAL_UPDATE_IND ,
DAS ,UPDATE_DT ,CREDIT_FACILITY_STATUS ,ACCOUNT_TYPE ,CHARGEOFF_DT ,LAST_PAYMENT_DT ,
INTEREST_RATE  ,CHARGEOFF_AMT_PRINCIPAL ,ACTUAL_PAYMT_AMT ,HIGH_CREDIT,
CAST(REPORTED_DT as date) as REPORTED_DT  from hmanalytics.hm_mfi_account_trend_latest_mar19
""")

df.repartition(2000).write.mode("append").insertInto("hmanalytics.hm_cns_account_trend")*/

/*val df = spark.sql("""
insert into table hmanalytics.hm_cns_account_trend
partition(REPORTED_DT)
select ACCOUNT_KEY ,MFI_ID ,ACT_REPORTED_DT ,MIN_AMOUNT_DUE ,CURRENT_BALANCE ,
AMOUNT_OVERDUE_TOTAL ,DAYS_PAST_DUE ,CHARGEOFF_AMT ,ASSET_CLASS ,SUIT_FILED ,
WRITTEN_OFF_STATUS ,SETLLEMENT_AMT ,SPECIAL_REMARKS_1 ,MANUAL_UPDATE_DT ,MANUAL_UPDATE_IND ,
DAS ,UPDATE_DT ,CREDIT_FACILITY_STATUS ,ACCOUNT_TYPE ,CHARGEOFF_DT ,LAST_PAYMENT_DT ,
INTEREST_RATE  ,CHARGEOFF_AMT_PRINCIPAL ,ACTUAL_PAYMT_AMT ,HIGH_CREDIT,
CAST(REPORTED_DT as date) as REPORTED_DT  from hmanalytics.hm_mfi_account_trend_latest_mar19
""")*/

/*val accDF = spark.read.format("jdbc") 
.option("url", "jdbc:oracle:thin:@172.16.5.2:1521:CHM01")
.option("dbtable", "(select cast(ACCOUNT_KEY as number(38,0)) ACCOUNT_KEY ,MFI_ID , ACT_REPORTED_DT ,MIN_AMOUNT_DUE ,CURRENT_BALANCE ,AMOUNT_OVERDUE_TOTAL ,DAYS_PAST_DUE , CHARGEOFF_AMT ,ASSET_CLASS ,SUIT_FILED ,WRITTEN_OFF_STATUS ,SETLLEMENT_AMT ,SPECIAL_REMARKS_1 , MANUAL_UPDATE_DT ,MANUAL_UPDATE_IND ,DAS ,UPDATE_DT ,CREDIT_FACILITY_STATUS ,ACCOUNT_TYPE , CHARGEOFF_DT ,LAST_PAYMENT_DT ,to_char(INTEREST_RATE) INTEREST_RATE  ,CHARGEOFF_AMT_PRINCIPAL , to_char(ACTUAL_PAYMT_AMT) ACTUAL_PAYMT_AMT ,HIGH_CREDIT,CAST(REPORTED_DT as DATE) REPORTED_DT from CHMCNSTREND.HM_MFI_ACCOUNT where  manual_update_dt >= to_date('13-05-2019','dd-mm-yyyy') and manual_update_ind=1 ) a")
.option("user", "E082") .option("password", "E082##123") 
.option("driver", "oracle.jdbc.driver.OracleDriver")
.option("fetchSize","10000").load()
accDF.write.mode("overwrite").insertInto("hmanalytics.hm_cns_trend_olm_delta")*/


/*val df = spark.sql("""
insert overwrite table hmanalytics.hm_cns_account_trend
partition(reported_dt)
select 
A.*
from hmanalytics.hm_cns_account_trend A
JOIN (select distinct reported_dt from hmanalytics.hm_cns_trend_olm_delta) B
 ON(A.reported_dt = B.reported_dt)
Left Outer Join (select distinct reported_dt,account_key from hmanalytics.hm_cns_trend_olm_delta) C
 ON(A.reported_dt = C.reported_dt
 AND A.account_key = C.account_key)
where C.account_key is null
UNION ALL 
select * from hmanalytics.hm_cns_trend_olm_delta
""")*/

}