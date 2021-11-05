package com.crif.highmark.CrifSemantic.DataLoad
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import scala.concurrent.{Await, Future}
import scala.concurrent.duration._
import scala.concurrent.ExecutionContext.Implicits.global

object ProdTrendCons extends App {
   
val spark = SparkSession.builder().appName("ProdTrendCons")
.config("hive.metastore.uris", "thrift://chmcisprbdmn01.chm.intra:9083")
.config("hive.exec.dynamic.partition", "true")
.config("hive.exec.dynamic.partition.mode", "nonstrict")
.config("hive.enforce.bucketing", "true")
.config("hive.exec.max.dynamic.partitions", "20000")
.enableHiveSupport().getOrCreate()


val mon_rn = spark.sparkContext.getConf.get("spark.driver.args")


val prodDF = spark.read.format("jdbc") 
.option("url", "jdbc:oracle:thin:@172.16.5.2:1521:CHM01")
.option("dbtable", "(SELECT cast(ACCOUNT_KEY as number(38,0)) ACCOUNT_KEY ,REPORTED_DT ,ACT_REPORTED_DT ,MIN_AMOUNT_DUE ,CURRENT_BALANCE ,AMOUNT_OVERDUE_TOTAL ,DAYS_PAST_DUE ,CHARGEOFF_AMT ,ASSET_CLASS ,SUIT_FILED ,WRITTEN_OFF_STATUS ,SETLLEMENT_AMT ,SPECIAL_REMARKS_1  ,DAS  ,CREDIT_FACILITY_STATUS ,ACCOUNT_TYPE ,CHARGEOFF_DT ,LAST_PAYMENT_DT ,to_char(INTEREST_RATE) INTEREST_RATE ,CHARGEOFF_AMT_PRINCIPAL ,to_char(ACTUAL_PAYMT_AMT) ACTUAL_PAYMT_AMT ,HIGH_CREDIT ,ACCOUNT_NBR ,DISBURSED_DT ,SANCTIONED_AMOUNT ,DISBURSED_AMOUNT ,NUM_INSTALLMENT ,INSTAL_FREQ ,CREDIT_LIMIT ,CASH_LIMIT ,MANUAL_UPDATE_DT ,MANUAL_UPDATE_IND ,SUPPRESS_INDICATOR ,CLOSED_DT ,UPDATE_DT,MFI_ID,ACTIVE,MOD(ACCOUNT_KEY,10) as part  FROM HMCNSPROD.HM_MFI_ACCOUNT) a")
.option("user", "HMANALYTICS") .option("password", "HM#2019") 
.option("driver", "oracle.jdbc.driver.OracleDriver")
.option("numPartitions", 10)
.option("partitionColumn", "part")
.option("lowerBound", 0).option("upperBound", 9)
.option("fetchSize","10000").load()



val consDF = spark.read.format("jdbc")
.option("url", "jdbc:oracle:thin:@172.16.3.12:1521:HM1")
.option("dbtable", "(select  INSERT_DT,PIN_CODE, STD_STATE_CODE, STD_LOCALITY1_VALUE, STD_LOCALITY1A_VALUE, STD_CITY_1, STD_CITY_2, STD_DERIVED_CITY, DISTRICT, CONSUMER_KEY, ACCOUNT_KEY, to_char(MONTHLY_INCOME), to_char(ANNUAL_INCOME), OWNERSHIP_IND, BIRTH_DT, GENDER, IS_COMMERCIAL,MFI_ID,OCCUPATION ,MOD(CONSUMER_KEY,10) as part from HMCNSPROD.HM_CNS_ANALYTICS_NEW) a") 
.option("user", "HMANALYTICS").option("password", "HM#2019") 
.option("driver", "oracle.jdbc.driver.OracleDriver")
.option("numPartitions", 10)
.option("partitionColumn", "part") 
.option("lowerBound", 0)
.option("upperBound", 9)
.option("fetchSize","10000").load()

val trendDF = spark.read.format("jdbc") 
.option("url", "jdbc:oracle:thin:@172.16.5.2:1521:CHM01")
.option("dbtable", "(select cast(ACCOUNT_KEY as number(38,0)) ACCOUNT_KEY ,MFI_ID ,REPORTED_DT ,ACT_REPORTED_DT ,MIN_AMOUNT_DUE ,CURRENT_BALANCE ,AMOUNT_OVERDUE_TOTAL ,DAYS_PAST_DUE ,CHARGEOFF_AMT ,ASSET_CLASS ,SUIT_FILED ,WRITTEN_OFF_STATUS ,SETLLEMENT_AMT ,SPECIAL_REMARKS_1 ,MANUAL_UPDATE_DT ,MANUAL_UPDATE_IND ,DAS ,UPDATE_DT ,CREDIT_FACILITY_STATUS ,ACCOUNT_TYPE ,CHARGEOFF_DT ,LAST_PAYMENT_DT ,to_char(INTEREST_RATE) INTEREST_RATE  ,CHARGEOFF_AMT_PRINCIPAL ,to_char(ACTUAL_PAYMT_AMT) ACTUAL_PAYMT_AMT ,HIGH_CREDIT,CLOSED_DT_NEW,CAST(EXTRACT(month FROM ACT_REPORTED_DT) AS INTEGER) MONTH from CHMCNSTREND.HM_MFI_ACCOUNT where EXTRACT(year FROM ACT_REPORTED_DT) = 2015 ) a")
.option("user", "HMANALYTICS") .option("password", "HM#2019") 
.option("driver", "oracle.jdbc.driver.OracleDriver")
.option("numPartitions", 11)
.option("partitionColumn", "MONTH")
.option("lowerBound", 1).option("upperBound", 12)
.option("fetchSize","10000").load()




val f1 = Future{
	println("start of prod load")
prodDF.drop(col("PART")).repartition(500).write.mode("overwrite").insertInto("hmanalytics.hm_cns_account_prod")
println("end of prod load")
	}
	
	val f2 = Future{
		println("start of consumer data load")
consDF.drop(col("PART")).repartition(500).write.mode("overwrite").insertInto("hmanalytics.hm_cns_analytics")
println("end of consumer data load")
	}
	

	
	Await.ready(f1, Duration.Inf)
	Await.ready(f2, Duration.Inf)
	
println("start of trend load 2015" )
trendDF.drop(col("MONTH")).repartition(2000).write.mode("overwrite").insertInto("hmanalytics.hm_mfi_account_trend")
println("end of trend load 2015" )	
	
	for(yr <- 2016 until mon_rn.toInt + 1 ){
		
 val trendDF = spark.read.format("jdbc") 
.option("url", "jdbc:oracle:thin:@172.16.5.2:1521:CHM01")
.option("dbtable", "(select cast(ACCOUNT_KEY as number(38,0)) ACCOUNT_KEY ,MFI_ID ,REPORTED_DT ,ACT_REPORTED_DT ,MIN_AMOUNT_DUE ,CURRENT_BALANCE ,AMOUNT_OVERDUE_TOTAL ,DAYS_PAST_DUE ,CHARGEOFF_AMT ,ASSET_CLASS ,SUIT_FILED ,WRITTEN_OFF_STATUS ,SETLLEMENT_AMT ,SPECIAL_REMARKS_1 ,MANUAL_UPDATE_DT ,MANUAL_UPDATE_IND ,DAS ,UPDATE_DT ,CREDIT_FACILITY_STATUS ,ACCOUNT_TYPE ,CHARGEOFF_DT ,LAST_PAYMENT_DT ,to_char(INTEREST_RATE) INTEREST_RATE  ,CHARGEOFF_AMT_PRINCIPAL ,to_char(ACTUAL_PAYMT_AMT) ACTUAL_PAYMT_AMT ,HIGH_CREDIT,CLOSED_DT_NEW,CAST(EXTRACT(month FROM ACT_REPORTED_DT) AS INTEGER) MONTH from CHMCNSTREND.HM_MFI_ACCOUNT where EXTRACT(year FROM ACT_REPORTED_DT) = "+yr+" ) a")
.option("user", "HMANALYTICS") .option("password", "HM#2019") 
.option("driver", "oracle.jdbc.driver.OracleDriver")
.option("numPartitions", 11)
.option("partitionColumn", "MONTH")
.option("lowerBound", 1).option("upperBound", 12)
.option("fetchSize","10000").load()

println("start of trend load " +yr)
trendDF.drop(col("MONTH")).repartition(2000).write.mode("append").insertInto("hmanalytics.hm_mfi_account_trend")
println("end of trend load " +yr)		
	}

}