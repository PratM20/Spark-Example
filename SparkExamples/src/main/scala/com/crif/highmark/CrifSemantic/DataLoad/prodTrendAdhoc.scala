package com.crif.highmark.CrifSemantic.DataLoad
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.types._
import org.apache.spark.sql.SQLContext
import com.crif.highmark.CrifSemantic.Service.homeCreditDataBlend

object prodTrendAdhoc extends App{
  val spark = SparkSession.builder().appName("homeCreditLoad")
											.config("hive.metastore.uris", "thrift://chmcisprbdmn01.chm.intra:9083")
											.config("hive.exec.dynamic.partition", "true")
                      .config("hive.exec.dynamic.partition.mode", "nonstrict")
                      .config("hive.enforce.bucketing", "true")
                      .config("hive.exec.max.dynamic.partitions", "20000")
											.enableHiveSupport().getOrCreate()
											
val argument = spark.sparkContext.getConf.get("spark.driver.args").split("\\s+")
val mfi_id=argument(0)
val end_dt=argument(1)

val tempdf = spark.sql("""select  
account_key , 
max(act_reported_dt) max_rpt_dt  
from hmanalytics.hm_mfi_account_trend
where  MFI_ID IN ('"""+mfi_id+ """')
AND CAST(act_reported_dt AS DATE) <= '"""+end_dt+ """'
group by
account_key """).distinct()


val prodDF =spark.sql(""" select distinct account_key  from hmanalytics.hm_cns_account_prod where  
act_reported_dt <='"""+end_dt+ """'  
AND ACTIVE = 1 
AND mfi_id IN ('"""+mfi_id+ """') """)


val tempProdDf = tempdf.join(prodDF, tempdf("ACCOUNT_KEY") === prodDF("ACCOUNT_KEY") ,"left_anti").distinct()

tempProdDf.createOrReplaceTempView("tempTrend")



val df = spark.sql("""
select 
'"""+end_dt+ """' AS LOAD_DT
,'T' AS CLUST_IND
,B.ACCOUNT_NBR
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
from hmanalytics.hm_mfi_account_trend A
JOIN tempTrend C
ON(A.ACCOUNT_KEY = C.ACCOUNT_KEY
AND A.act_reported_dt = C.max_rpt_dt)
JOIN  (select distinct ACCOUNT_KEY,ACCOUNT_NBR ,CASH_LIMIT ,CLOSED_DT ,CREDIT_LIMIT ,DISBURSED_AMOUNT ,DISBURSED_DT ,INSTAL_FREQ ,NUM_INSTALLMENT ,SANCTIONED_AMOUNT ,suppress_indicator from hmanalytics.hm_cns_account_prod where ACTIVE=1 ) B
ON(
 A.ACCOUNT_KEY = B.ACCOUNT_KEY 
)
UNION ALL
select distinct
'"""+end_dt+ """' AS LOAD_DT
,'P' AS CLUST_IND
,ACCOUNT_NBR
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

from hmanalytics.hm_cns_account_prod
where  act_reported_dt <='"""+end_dt+ """'  AND ACTIVE = 1 
AND mfi_id IN ('"""+mfi_id+ """')
""")


df.write.mode("overwrite").saveAsTable("hmanalytics.hm_cns_prod_plus_trend_adhoc")
}