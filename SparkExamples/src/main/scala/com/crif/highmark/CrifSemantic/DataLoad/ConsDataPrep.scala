package com.crif.highmark.CrifSemantic.DataLoad
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import scala.concurrent.{Await, Future}
import scala.concurrent.duration._
import scala.concurrent.ExecutionContext.Implicits.global

object ConsDataPrep extends App{
  
	
val spark = SparkSession.builder().appName("ConsDataPrep")
.config("hive.metastore.uris", "thrift://chmcisprbdmn01.chm.intra:9083")
.config("hive.exec.dynamic.partition", "true")
.config("hive.exec.dynamic.partition.mode", "nonstrict")
.config("hive.enforce.bucketing", "true")
.config("hive.exec.max.dynamic.partitions", "20000")
.enableHiveSupport().getOrCreate()

val mon_rn = spark.sparkContext.getConf.get("spark.driver.args")



/*Retail bureau unique standard address @ account level*/

val baseDF = spark.sql("""
SELECT
insert_dt             
,COALESCE(A.pin_code,B.pin_code) pin_code           
,COALESCE(A.std_state_code,B.std_state_code) std_state_code     
,std_locality1_value   
,std_locality1a_value  
,std_city_1            
,std_city_2            
,std_derived_city      
,COALESCE(A.district,B.district) district              
,A.consumer_key          
,A.account_key           
,monthly_income        
,annual_income         
,ownership_ind         
,birth_dt              
,gender                
,is_commercial         
,mfi_id                
,occupation            

from 
hmanalytics.hm_cns_analytics A
LEFT OUTER JOIN
hmanalytics.hm_cns_sdp_base B
ON(A.consumer_key = B.consumer_key)
""")

baseDF.createOrReplaceTempView("tmp")


val DF = spark.sql("""
select *,
CASE WHEN STD_STATE_CODE IS NOT NULL AND DISTRICT IS NOT NULL AND PIN_CODE IS NOT NULL THEN 1
     WHEN STD_STATE_CODE IS NOT NULL AND DISTRICT IS NOT NULL AND PIN_CODE IS NULL THEN 2
     WHEN STD_STATE_CODE IS NOT NULL AND DISTRICT IS NULL AND PIN_CODE IS NOT NULL THEN 3
     WHEN STD_STATE_CODE IS NULL AND DISTRICT IS NOT NULL AND PIN_CODE IS NOT NULL THEN 4
ELSE 5
END SDP,
NVL(SUBSTR(OWNERSHIP_IND,3,1),9) OWNERSHIP_IND_FLAG,
ROW_NUMBER () OVER (PARTITION BY MFI_ID,ACCOUNT_KEY ORDER BY CONSUMER_KEY) CONSUMER_KEY_RW

FROM
tmp
""")



DF.createOrReplaceTempView("temp")


val rowDF  = spark.sql("""
select *,
ROW_NUMBER () OVER (PARTITION BY ACCOUNT_KEY ORDER BY CONCAT(SDP,OWNERSHIP_IND_FLAG,CONSUMER_KEY_RW)) RW
FROM temp
""")

/*Retail & Commercial cross bureau product indicator.*/

val conComOvDF= spark.sql("""
select 
A.mfi_id,
A.account_nbr,
A.account_key,
B.borrower_key
from
hmanalytics.hm_cns_account_prod A
JOIN hmanalytics.hm_cml_account_prd B
ON(A.mfi_id = B.contributor_id
AND A.account_nbr = B.account_number)
""")

conComOvDF.createOrReplaceTempView("temp1")


val prodDF = spark.sql("""
select A.*,NVL(B.commercial_ind,0) commercial_ind from hmanalytics.hm_cns_account_prod A 
LEFT OUTER JOIN (select distinct account_key, 1 as commercial_ind from temp1) B
ON(A.account_key = B.account_key) """)



val f1 = Future{
println("start of prod load")
prodDF.write.mode("overwrite").saveAsTable("hmanalytics.hm_cns_account_prod_new")
println("end of prod load")
	}
	
	val f2 = Future{
println("start of standard consumer unique address data load")
rowDF.where(col("RW") === 1).drop(col("CONSUMER_KEY_RW")).drop(col("RW")).repartition(500).write.mode("overwrite").saveAsTable("hmanalytics.hm_cns_unique_row")
println("end of standard consumer unique address data load")
	}
	

	
	Await.ready(f1, Duration.Inf)
	Await.ready(f2, Duration.Inf)
	
	/*Cluster one view unique rows*/

	clustOneView.oneView(spark, mon_rn)


}