package com.crif.highmark.CrifSemantic.DataLoad
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import scala.concurrent.{Await, Future}
import scala.concurrent.duration._
import scala.concurrent.ExecutionContext.Implicits.global


object adHocCommercialLoad  extends App{
	
val spark = SparkSession.builder().appName("adHocCommercialLoad")
.config("hive.metastore.uris", "thrift://chmcisprbdmn01.chm.intra:9083")
.config("hive.exec.dynamic.partition", "true")
.config("hive.exec.dynamic.partition.mode", "nonstrict")
.config("hive.enforce.bucketing", "true")
.config("hive.exec.max.dynamic.partitions", "20000")
.enableHiveSupport().getOrCreate()
  

val prodDF = spark.read.format("jdbc") 
//.option("url", "jdbc:oracle:thin:@172.16.3.26:1521:HMSTBY2")
.option("url", "jdbc:oracle:thin:@172.16.5.2:1521:CHM01")
.option("dbtable","(SELECT REPORTED_DT,CONTRIBUTOR_ID,BORROWER_KEY ,ACCOUNT_KEY ,CREDIT_TYPE ,SANCTIONED_DT ,DAS ,CLOSED_DT ,LOAN_EXPIRY_DT ,SANCTIONED_AMOUNT ,CURRENT_BALANCE ,ASSET_CLASS ,DPD ,DRAWING_POWER ,ACCOUNT_NUMBER ,CURRENCY_CODE ,AVG_MATURE_CONTRACT_PERIOD ,CLOSED_STATUS ,WILFUL_DEFAULT_STATUS ,WILFUL_DEFAULT_DATE ,SUIT_FILED_STATUS ,SUIT_AMOUNT ,SUIT_DATE ,CREDIT_FACILITY_STATUS ,WRITE_OFF_AMT    ,AMT_SETTLED ,LOAN_RENEWAL_DT ,DEL_BUCKET_30 ,DEL_BUCKET_60 ,DEL_BUCKET_90 ,AMOUNT_OVERDUE_TOTAL ,INSTALL_FREQ ,LAST_PAYMENT_AMT ,HIGH_CREDIT ,RESTRUCTURE_REASON ,CONTRACT_CLASS_NPA ,SECURITY_COVERAGE ,GUARANTEE_COVERAGE ,OUTSTAND_RESTUCTURE_CONTRACT ,INSTALLMENT_AMOUNT ,SUIT_REF_NUMBER	 ,CREDIT_FACILITY_DT ,ACCOUNT_STATUS ,SUPPRESS_INDICATOR,ACTIVE,MOD(BORROWER_KEY,10) as PART  FROM HMBURCOMPROD.HM_ACCOUNT ) a")
.option("user", "HMANALYTICS") .option("password", "HM#2019") 
.option("driver", "oracle.jdbc.driver.OracleDriver")
.option("numPartitions", 9)
.option("partitionColumn", "PART")
.option("lowerBound", 0).option("upperBound", 9)
.option("fetchSize","10000").load()



val trendDF = spark.read.format("jdbc") 
.option("url", "jdbc:oracle:thin:@172.16.5.2:1521:CHM01")
.option("dbtable","(select reported_dt,contributor_id,borrower_key,account_key,credit_type,sanctioned_dt,das,closed_dt,loan_expiry_dt,sanctioned_amount ,current_balance,asset_class,dpd,drawing_power,account_number,currency_code,avg_mature_contract_period ,closed_status,wilful_default_status,wilful_default_date,suit_filed_status,suit_amount,suit_date,credit_facility_status,write_off_amt,amt_settled,loan_renewal_dt,del_bucket_30,del_bucket_60,del_bucket_90,amount_overdue_total,install_freq,last_payment_amt,high_credit,restructure_reason,contract_class_npa,security_coverage,guarantee_coverage,outstand_restucture_contract,installment_amount,suit_ref_number,credit_facility_dt,account_status,suppress_indicator,MOD(BORROWER_KEY,10) as PART  FROM HMBURCOMTREND.HM_ACCOUNT) a")
.option("user", "HMANALYTICS") .option("password", "HM#2019") 
.option("driver", "oracle.jdbc.driver.OracleDriver")
.option("numPartitions", 9)
.option("partitionColumn", "PART")
.option("lowerBound", 0).option("upperBound", 9)
.option("fetchSize","10000").load()



val consDF = spark.read.format("jdbc")
.option("url", "jdbc:oracle:thin:@172.16.3.12:1521:HM1")
.option("dbtable", "(select  INSERT_DT ,BORROWER_NAME_1 ,CONTRIBUTOR_ID ,BORROWER_LEGAL_CONSTITUTION ,BORROWER_ACTIVITY_CLASS_1 ,BORROWER_ACTIVITY_CLASS_2 ,BORROWER_ACTIVITY_CLASS_3 ,COMPANY_CIN ,BOR_BUSINESS_TYPE ,BOR_BUSINESS_CATEGORY ,REPORTED_DT ,BORROWER_KEY ,IS_COMMERCIAL ,SHG_IND ,SECONDARY_QUALIFIER ,SUB_CATEGORY ,STD_DISTRICT ,STD_CITY ,STD_PIN_CODE ,STD_STATE_CODE ,STD_DERIVED_DISTRICT ,STD_DERIVED_CITY ,STD_DERIVED_PIN_CODE_1 ,STD_DERIVED_PIN_CODE_2 ,STD_LOCALITY1_VALUE ,STD_LOCALITY2_VALUE ,STD_LOCALITY3_VALUE ,STD_DERIVED_LOCALITY1_CITY ,STD_LOCALITY1A_VALUE ,STD_DISTRICT_1 ,STD_DISTRICT_2 ,STD_CITY_1 ,STD_CITY_2,MOD(BORROWER_KEY,10) as PART FROM HMBURCOMPROD.HM_COMM_ANALYTICS) a") 
.option("user", "HMANALYTICS").option("password", "HM#2019") 
.option("driver", "oracle.jdbc.driver.OracleDriver")
.option("numPartitions", 9)
.option("partitionColumn", "PART") 
.option("lowerBound", 0)
.option("upperBound", 9)
.option("fetchSize","10000").load()



val f1 = Future{
	println("start of cml prod load")
prodDF.drop(col("PART")).repartition(500).write.mode("overwrite").insertInto("hmanalytics.hm_cml_account_prd")
println("end of cml prod load")
	}
	
	val f2 = Future{
		println("start of cml consumer data load")
consDF.drop(col("PART")).repartition(500).write.mode("overwrite").insertInto("hmanalytics.hm_comm_analytics")
println("end of cml consumer data load")
	}
	
		val f3 = Future{
println("start of cml trend load " )
trendDF.drop(col("PART")).repartition(500).write.mode("overwrite").insertInto("hmanalytics.hm_cml_account_trend")
println("end of cml trend load " )	

	}
	Await.ready(f1, Duration.Inf)
	Await.ready(f2, Duration.Inf)
	Await.ready(f3, Duration.Inf)
	
	
}