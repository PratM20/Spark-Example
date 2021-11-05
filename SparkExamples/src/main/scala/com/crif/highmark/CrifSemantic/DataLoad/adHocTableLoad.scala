package com.crif.highmark.CrifSemantic.DataLoad
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._


object adHocTableLoad extends App {
  val spark = SparkSession.builder().appName("adHocTableLoad")
.config("hive.metastore.uris", "thrift://chmcisprbdmn01.chm.intra:9083")
.config("hive.exec.dynamic.partition", "true")
.config("hive.exec.dynamic.partition.mode", "nonstrict")
.config("hive.enforce.bucketing", "true")
.config("hive.exec.max.dynamic.partitions", "20000")
.enableHiveSupport().getOrCreate()

val argument = spark.sparkContext.getConf.get("spark.driver.args").split("\\s+")
val mon_rn=argument(0)

val out = spark.sql(s"""select * from hmanalytics.hm_cns_macro_analysis_"""+mon_rn+ """""")
out.write.mode("overwrite").insertInto(s"""hmanalytics.hm_cns_macro_analysis_"""+mon_rn+ """_parquet""")

/*val mon_rn = spark.sparkContext.getConf.get("spark.driver.args")

val accDF = spark.read.format("jdbc") 
.option("url", "jdbc:oracle:thin:@172.16.3.12:1521:HM1")
//.option("dbtable", "(select cast(ACCOUNT_KEY as number(38,2)) ACCOUNT_KEY ,MFI_ID ,REPORTED_DT ,ACT_REPORTED_DT ,cast(MIN_AMOUNT_DUE as number(38,2)) MIN_AMOUNT_DUE ,cast(CURRENT_BALANCE as number(38,2)) CURRENT_BALANCE ,cast(AMOUNT_OVERDUE_TOTAL as number(38,2)) AMOUNT_OVERDUE_TOTAL,DAYS_PAST_DUE ,cast(CHARGEOFF_AMT as number(38,2)) CHARGEOFF_AMT ,ASSET_CLASS ,SUIT_FILED ,WRITTEN_OFF_STATUS ,cast(SETLLEMENT_AMT as number(38,2))SETLLEMENT_AMT ,SPECIAL_REMARKS_1 ,MANUAL_UPDATE_DT ,MANUAL_UPDATE_IND ,DAS ,UPDATE_DT ,CREDIT_FACILITY_STATUS ,ACCOUNT_TYPE ,CHARGEOFF_DT ,LAST_PAYMENT_DT ,cast(INTEREST_RATE as number(38,2)) INTEREST_RATE  ,cast(CHARGEOFF_AMT_PRINCIPAL as number(38,2)) CHARGEOFF_AMT_PRINCIPAL ,cast(ACTUAL_PAYMT_AMT as number(38,2)) ACTUAL_PAYMT_AMT ,cast(HIGH_CREDIT as number(38,2)) HIGH_CREDIT from CHMCNSTREND.HM_MFI_ACCOUNT where EXTRACT(year FROM ACT_REPORTED_DT) = 2018 and EXTRACT(month FROM ACT_REPORTED_DT) = "+mon_rn+") a")
//.option("dbtable", "(select cast(ACCOUNT_KEY as number(38,0)) ACCOUNT_KEY ,MFI_ID ,REPORTED_DT ,ACT_REPORTED_DT ,MIN_AMOUNT_DUE ,CURRENT_BALANCE ,AMOUNT_OVERDUE_TOTAL ,DAYS_PAST_DUE ,CHARGEOFF_AMT ,ASSET_CLASS ,SUIT_FILED ,WRITTEN_OFF_STATUS ,SETLLEMENT_AMT ,SPECIAL_REMARKS_1 ,MANUAL_UPDATE_DT ,MANUAL_UPDATE_IND ,DAS ,UPDATE_DT ,CREDIT_FACILITY_STATUS ,ACCOUNT_TYPE ,CHARGEOFF_DT ,LAST_PAYMENT_DT ,to_char(INTEREST_RATE) INTEREST_RATE  ,CHARGEOFF_AMT_PRINCIPAL ,to_char(ACTUAL_PAYMT_AMT) ACTUAL_PAYMT_AMT ,HIGH_CREDIT from CHMCNSTREND.HM_MFI_ACCOUNT where EXTRACT(year FROM ACT_REPORTED_DT) = 2017 and EXTRACT(month FROM ACT_REPORTED_DT) = "+mon_rn+") a")
.option("dbtable", "(select cast(ACCOUNT_KEY as number(38,0)) ACCOUNT_KEY ,MFI_ID ,REPORTED_DT ,ACT_REPORTED_DT ,MIN_AMOUNT_DUE ,CURRENT_BALANCE ,AMOUNT_OVERDUE_TOTAL ,DAYS_PAST_DUE ,CHARGEOFF_AMT ,ASSET_CLASS ,SUIT_FILED ,WRITTEN_OFF_STATUS ,SETLLEMENT_AMT ,SPECIAL_REMARKS_1 ,MANUAL_UPDATE_DT ,MANUAL_UPDATE_IND ,DAS ,UPDATE_DT ,CREDIT_FACILITY_STATUS ,ACCOUNT_TYPE ,CHARGEOFF_DT ,LAST_PAYMENT_DT ,to_char(INTEREST_RATE) INTEREST_RATE  ,CHARGEOFF_AMT_PRINCIPAL ,to_char(ACTUAL_PAYMT_AMT) ACTUAL_PAYMT_AMT ,HIGH_CREDIT,CAST(EXTRACT(month FROM ACT_REPORTED_DT) AS INTEGER) MONTH from CHMCNSTREND.HM_MFI_ACCOUNT where EXTRACT(year FROM ACT_REPORTED_DT) = "+mon_rn+" ) a")
.option("user", "E312") .option("password", "E312##123") 
.option("driver", "oracle.jdbc.driver.OracleDriver")
.option("numPartitions", 11)
.option("partitionColumn", "MONTH")
.option("lowerBound", 1).option("upperBound", 12)
.option("fetchSize","10000").load()
//val acc1DF =accDF.withColumn("accnt_key", col("ACCOUNT_KEY").cast(LongType)).drop("ACCOUNT_KEY").withColumnRenamed("accnt_key", "ACCOUNT_KEY") 
accDF.drop(col("MONTH")).repartition(2000).write.mode("append").insertInto("hmanalytics.hm_mfi_account_trend1_2018")*/

}