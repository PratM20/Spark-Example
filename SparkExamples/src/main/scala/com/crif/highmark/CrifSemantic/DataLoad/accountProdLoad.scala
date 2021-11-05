package com.crif.highmark.CrifSemantic.DataLoad
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

object accountProdLoad extends App {
	
val spark = SparkSession.builder().appName("accountProdLoad")
.config("hive.metastore.uris", "thrift://chmcisprbdmn01.chm.intra:9083")
.config("hive.exec.dynamic.partition", "true")
.config("hive.exec.dynamic.partition.mode", "nonstrict")
.config("hive.enforce.bucketing", "true")
.config("hive.exec.max.dynamic.partitions", "20000")
.enableHiveSupport().getOrCreate()

//val mon_rn = spark.sparkContext.getConf.get("spark.driver.args")

val accDF = spark.read.format("jdbc") 
.option("url", "jdbc:oracle:thin:@172.16.5.2:1521:CHM01")
.option("dbtable", "(SELECT cast(ACCOUNT_KEY as number(38,0)) ACCOUNT_KEY ,REPORTED_DT ,ACT_REPORTED_DT ,MIN_AMOUNT_DUE ,CURRENT_BALANCE ,AMOUNT_OVERDUE_TOTAL ,DAYS_PAST_DUE ,CHARGEOFF_AMT ,ASSET_CLASS ,SUIT_FILED ,WRITTEN_OFF_STATUS ,SETLLEMENT_AMT ,SPECIAL_REMARKS_1  ,DAS  ,CREDIT_FACILITY_STATUS ,ACCOUNT_TYPE ,CHARGEOFF_DT ,LAST_PAYMENT_DT ,to_char(INTEREST_RATE) INTEREST_RATE ,CHARGEOFF_AMT_PRINCIPAL ,to_char(ACTUAL_PAYMT_AMT) ACTUAL_PAYMT_AMT ,HIGH_CREDIT ,ACCOUNT_NBR ,DISBURSED_DT ,SANCTIONED_AMOUNT ,DISBURSED_AMOUNT ,NUM_INSTALLMENT ,INSTAL_FREQ ,CREDIT_LIMIT ,CASH_LIMIT ,MANUAL_UPDATE_DT ,MANUAL_UPDATE_IND ,SUPPRESS_INDICATOR ,CLOSED_DT ,UPDATE_DT,MFI_ID,ACTIVE,CAST(EXTRACT(year FROM ACT_REPORTED_DT) AS INTEGER) YEAR FROM HMCNSPROD.HM_MFI_ACCOUNT) a")
.option("user", "E082") .option("password", "E082##123") 
.option("driver", "oracle.jdbc.driver.OracleDriver")
.option("numPartitions", 11)
.option("partitionColumn", "YEAR")
.option("lowerBound", 2008).option("upperBound", 2019)
.option("fetchSize","10000").load()

accDF.drop(col("YEAR")).repartition(500).write.mode("append").insertInto("hmanalytics.hm_cns_account_prod")
  
}