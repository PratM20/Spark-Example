package com.crif.highmark.CrifSemantic.DataLoad
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import scala.concurrent.{Await, Future}
import scala.concurrent.duration._
import scala.concurrent.ExecutionContext.Implicits.global

object macroLoad extends App {
  val spark = SparkSession.builder().appName("macroLoad")
.config("hive.metastore.uris", "thrift://chmcisprbdmn01.chm.intra:9083")
.config("hive.exec.dynamic.partition", "true")
.config("hive.exec.dynamic.partition.mode", "nonstrict")
.config("hive.enforce.bucketing", "true")
.config("hive.exec.max.dynamic.partitions", "20000")
.enableHiveSupport().getOrCreate()

val argument = spark.sparkContext.getConf.get("spark.driver.args").split("\\s+")
val mon_rn=argument(0)


val cloDF = spark.read.format("jdbc") 
.option("url", "jdbc:oracle:thin:@10.10.10.117:1521:CHMDB2")
.option("dbtable", "(select std_state_code,district,pin_code,consumer_key,account_key,account_type,disbursed_dt,high_credit,disbursed_amount,credit_limit,amount_overdue_total,current_balance,chargeoff_amt,closed_dt,days_past_due,instal_freq,is_commercial,das,dac,birth_dt,gender,ownership_ind,insert_dt,act_insert_dt,reported_dt,mfi_id,CAST(EXTRACT(month FROM reported_dt) AS INTEGER) MONTH from hm_cns_closed_macro_anal_new ) a")
.option("user", "HMANALYTICS") .option("password", "hmanalytics") 
.option("driver", "oracle.jdbc.driver.OracleDriver")
.option("numPartitions", 11)
.option("partitionColumn", "MONTH")
.option("lowerBound", 1).option("upperBound", 12)
.option("fetchSize","10000").load()



val actDF = spark.read.format("jdbc") 
.option("url", "jdbc:oracle:thin:@10.10.10.117:1521:CHMDB2")
.option("dbtable", "(select std_state_code,district,pin_code,consumer_key,account_key,account_type,disbursed_dt,high_credit,disbursed_amount,credit_limit,amount_overdue_total,current_balance,chargeoff_amt,closed_dt,days_past_due,instal_freq,das_flag,is_commercial,das,dac,birth_dt,gender,ownership_ind,insert_dt,act_insert_dt,reported_dt,mfi_id,CAST(EXTRACT(month FROM reported_dt) AS INTEGER) MONTH from HM_CNS_MACRO_ANALYSIS_"+mon_rn+" ) a")
.option("user", "HMANALYTICS") .option("password", "hmanalytics") 
.option("driver", "oracle.jdbc.driver.OracleDriver")
.option("numPartitions", 11)
.option("partitionColumn", "MONTH")
.option("lowerBound", 1).option("upperBound", 12)
.option("fetchSize","10000").load()



val f1 = Future{
cloDF.drop(col("MONTH")).repartition(2000).write.mode("append").insertInto("hmanalytics.hm_cns_closed_macro_analysis")
	}
	
	val f2 = Future{
actDF.drop(col("MONTH")).repartition(2000).write.mode("append").insertInto(s"""hmanalytics.hm_cns_macro_analysis_"""+mon_rn+ """_parquet""")
	}
	
	Await.ready(f1, Duration.Inf)
	Await.ready(f2, Duration.Inf)


}