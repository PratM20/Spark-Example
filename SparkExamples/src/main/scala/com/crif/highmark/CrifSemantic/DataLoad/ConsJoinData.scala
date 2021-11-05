package com.crif.highmark.CrifSemantic.DataLoad
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._


object ConsJoinData extends App{
	
val spark = SparkSession.builder().appName("ConsJoinData")
.config("hive.metastore.uris", "thrift://chmcisprbdmn01.chm.intra:9083")
.config("hive.exec.dynamic.partition", "true")
.config("hive.exec.dynamic.partition.mode", "nonstrict")
.config("hive.enforce.bucketing", "true")
.config("hive.exec.max.dynamic.partitions", "20000")
.enableHiveSupport().getOrCreate()

val mon_rn = spark.sparkContext.getConf.get("spark.driver.args")

val df = spark.sql("""
select 
B.ACCOUNT_NBR
,B.CASH_LIMIT
,CASE WHEN A.DAS IN('S06','S07') THEN B.CLOSED_DT ELSE '' END  CLOSED_DT
,B.CREDIT_LIMIT
,B.DISBURSED_AMOUNT
,B.DISBURSED_DT
,B.INSTAL_FREQ
,B.NUM_INSTALLMENT
,B.SANCTIONED_AMOUNT
,B.suppress_indicator
,A.*
,'T' AS CLUST_IND
from hmanalytics.hm_mfi_account_trend A
JOIN  (select distinct ACCOUNT_KEY,ACCOUNT_NBR ,CASH_LIMIT ,CLOSED_DT ,CREDIT_LIMIT ,DISBURSED_AMOUNT ,DISBURSED_DT ,INSTAL_FREQ ,NUM_INSTALLMENT ,SANCTIONED_AMOUNT ,suppress_indicator from hmanalytics.hm_cns_account_prod where ACTIVE=1 ) B
ON(
 A.ACCOUNT_KEY = B.ACCOUNT_KEY 
)
UNION ALL
select 
ACCOUNT_NBR
,CASH_LIMIT
,CLOSED_DT
,CREDIT_LIMIT
,DISBURSED_AMOUNT
,DISBURSED_DT
,INSTAL_FREQ
,NUM_INSTALLMENT
,SANCTIONED_AMOUNT
,suppress_indicator
,account_key             
,mfi_id                  
,reported_dt             
,act_reported_dt         
,min_amount_due          
,current_balance         
,amount_overdue_total    
,days_past_due           
,chargeoff_amt           
,asset_class             
,suit_filed              
,written_off_status      
,setllement_amt          
,special_remarks_1       
,manual_update_dt        
,manual_update_ind       
,das                     
,update_dt               
,credit_facility_status  
,account_type            
,chargeoff_dt            
,last_payment_dt         
,interest_rate           
,chargeoff_amt_principal 
,actual_paymt_amt        
,high_credit 
,'P' AS CLUST_IND

from hmanalytics.hm_cns_account_prod
where ACTIVE = 1 

""")


val consDF = spark.sql("""
select *
from
(select *,
ROW_NUMBER() OVER(PARTITION BY CONSUMER_KEY ORDER BY INSERT_DT DESC) ORD
from hmanalytics.hm_cns_analytics
)A
where A.ORD=1
""")

val joineDF = consDF.join(df,consDF("ACCOUNT_KEY") === df("ACCOUNT_KEY"),"right")
.drop(consDF("account_key"))
.drop(consDF("ORD"))
.drop(consDF("insert_dt"))

 joineDF.createOrReplaceTempView("tempTrend")
 
 val finalDF= spark.sql("""
	Select * from tempTrend where clust_ind='P'
	""")
	
println("start of prod load")	
finalDF.repartition(500).write.mode("overwrite").insertInto("hmanalytics.hm_cns_account_trend_prod")
println("end of prod load")

for(yr <- 2015 until mon_rn.toInt + 1 ){
	val finalDF= spark.sql("""
	Select * from tempTrend where clust_ind='T' AND  year(ACT_REPORTED_DT) = '"""+yr+ """'
	""")
	println("start of trend load " +yr)	
 	finalDF.repartition(2000).write.mode("append").insertInto("hmanalytics.hm_cns_account_trend_prod")
 	println("end of trend load " +yr)
}
}