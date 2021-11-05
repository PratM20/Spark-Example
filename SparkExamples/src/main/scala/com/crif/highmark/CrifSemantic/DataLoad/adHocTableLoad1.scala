package com.crif.highmark.CrifSemantic.DataLoad
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._


object adHocTableLoad1  extends App{
    val spark = SparkSession.builder().appName("adHocTableLoad1")
.config("hive.metastore.uris", "thrift://chmcisprbdmn01.chm.intra:9083")
.config("hive.exec.dynamic.partition", "true")
.config("hive.exec.dynamic.partition.mode", "nonstrict")
.config("hive.enforce.bucketing", "true")
.config("hive.exec.max.dynamic.partitions", "20000")
.enableHiveSupport().getOrCreate()

/*val accDF = spark.read.format("jdbc") 
.option("url", "jdbc:oracle:thin:@172.16.3.12:1521:HM1")
.option("dbtable", "(select consumer_key,account_nbr,account_key,to_char(annual_income) annual_income,to_char(monthly_income) monthly_income,reported_dt,mfi_id,CAST(EXTRACT(year FROM REPORTED_DT) AS INTEGER) YEAR  from HMCNSPROD.HM_CONSUMER where active=1) a")
.option("user", "E312") .option("password", "E312##123") 
.option("driver", "oracle.jdbc.driver.OracleDriver")
.option("numPartitions", 11)
.option("partitionColumn", "YEAR")
.option("lowerBound", 2008).option("upperBound", 2019)
.option("fetchSize","10000").load()

accDF.drop(col("YEAR")).repartition(2000).write.mode("append").insertInto("hmanalytics.hm_hc_mfi_consumer_feb")*/

/*val accDF = spark.read.format("jdbc") 
.option("url", "jdbc:oracle:thin:@10.10.10.117:1521:CHMDB2")
.option("dbtable", "(select std_state_code,district,pin_code,consumer_key,account_key,account_type,disbursed_dt,high_credit,disbursed_amount,credit_limit,amount_overdue_total,current_balance,chargeoff_amt,closed_dt,days_past_due,instal_freq,is_commercial,das,dac,birth_dt,gender,ownership_ind,insert_dt,act_insert_dt,reported_dt,mfi_id,CAST(EXTRACT(year FROM REPORTED_DT) AS INTEGER) YEAR from hm_cns_closed_macro_anal_new ) a")
.option("user", "HMANALYTICS") .option("password", "hmanalytics") 
.option("driver", "oracle.jdbc.driver.OracleDriver")
.option("numPartitions", 11)
.option("partitionColumn", "YEAR")
.option("lowerBound", 2008).option("upperBound", 2019)
.option("fetchSize","10000").load()

accDF.drop(col("YEAR")).repartition(2000).write.mode("append").insertInto("hmanalytics.hm_cns_closed_macro_anal_parquet_new")
*/


/*val accDF = spark.read.format("jdbc") 
.option("url", "jdbc:oracle:thin:@172.16.3.12:1521:HM1")
.option("dbtable", "(select *  from E115.HM_CNS_CLST_19FEB19) a")
.option("user", "E312") .option("password", "E312##123") 
.option("driver", "oracle.jdbc.driver.OracleDriver")
.option("fetchSize","10000").load()

accDF.write.mode("append").insertInto("hmanalytics.hm_cns_clst_19feb19")
*/

val accDF = spark.sql("select  A.CONSUMER_KEY,A.ACCOUNT_NBR,A.ACCOUNT_KEY, A.MFI_ID ,A.ANNUAL_INCOME,A.MONTHLY_INCOME,A.REPORTED_DT,B.MAX_CLST_ID ,C.MIN_AMOUNT_DUE,C.ACCOUNT_TYPE,C.DISBURSED_DT,C.SUPPRESS_INDICATOR from  hmanalytics.hm_hc_mfi_consumer_feb  A JOIN  hmanalytics.hm_cns_clst_16may19 B ON (A.CONSUMER_KEY=B.CANDIDATE_ID ) JOIN  hmanalytics.hm_cns_account_prod C ON (A.ACCOUNT_KEY=C.ACCOUNT_KEY AND UPPER(TRIM(A.ACCOUNT_NBR))=UPPER(TRIM(C.ACCOUNT_NBR))) ")

accDF.write.mode("overwrite").saveAsTable("hmanalytics.hm_hc_cluster_tbl_extra_col_new")

}