package com.crif.highmark.CrifSemantic.DataLoad
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.types._
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.SaveMode
import scala.concurrent.{Await, Future}
import scala.concurrent.duration._
import scala.concurrent.ExecutionContext.Implicits.global


object nocMfiReport extends App{

	
	val spark = SparkSession.builder().appName("nocReport")
.config("hive.metastore.uris", "thrift://chmcisprbdmn01.chm.intra:9083")
.config("hive.exec.dynamic.partition", "true")
.config("hive.exec.dynamic.partition.mode", "nonstrict")
.config("hive.enforce.bucketing", "true")
.config("hive.exec.max.dynamic.partitions", "20000")
.enableHiveSupport().getOrCreate()



val argument = spark.sparkContext.getConf.get("spark.driver.args").split("\\s+")
 	
val mon_rn=argument(0)
val end_dt=argument(1)
val load_dt=argument(2)


val nocDF= spark.sql("""
SELECT 
       B.CONTRIBUTOR_ID MFI_ID, 
       B.institution_name INSTITUTION, 
       From_unixtime(unix_timestamp('"""+load_dt+ """', 'yyyyMM'),'yyyy-MM-dd') REPORTED_DT, 
       C.ACCOUNT_TYPE,
       SUM(TOTAL_ACCOUNTS) TOTAL_LOANS,
        SUM(TOTAL_DISBURSED_AMOUNT) TOTAL_DISB_AMOUNT,
        SUM(CASE WHEN LOAN_ACCOUNT_STATUS_KEY=1 THEN TOTAL_ACCOUNTS ELSE 0 END) ACTIVE_LOANS,
        SUM(CASE WHEN LOAN_ACCOUNT_STATUS_KEY=1 THEN TOTAL_DISBURSED_AMOUNT ELSE 0 END) ACTIVE_DISB_AMOUNT,
        SUM(CASE WHEN LOAN_ACCOUNT_STATUS_KEY=1 THEN TOTAL_OUTSTANDING_AMOUNT ELSE 0 END) ACTIVE_OUTSTANDING,
        SUM(CASE WHEN LOAN_ACCOUNT_STATUS_KEY=1 AND LOAN_DPD_BUCKET_KEY IN(1,15) THEN TOTAL_ACCOUNTS ELSE 0 END) LAR_0,
	   SUM(CASE WHEN LOAN_ACCOUNT_STATUS_KEY=1 AND OVERDUE_IND=1 AND LOAN_DPD_BUCKET_KEY IN(2) THEN TOTAL_ACCOUNTS ELSE 0 END) LAR_1_30,
        SUM(CASE WHEN LOAN_ACCOUNT_STATUS_KEY=1 AND OVERDUE_IND=1 AND LOAN_DPD_BUCKET_KEY IN(3) THEN TOTAL_ACCOUNTS ELSE 0 END) LAR_31_60,
        SUM(CASE WHEN LOAN_ACCOUNT_STATUS_KEY=1 AND OVERDUE_IND=1 AND LOAN_DPD_BUCKET_KEY IN(4) THEN TOTAL_ACCOUNTS ELSE 0 END) LAR_61_90,
        SUM(CASE WHEN LOAN_ACCOUNT_STATUS_KEY=1 AND OVERDUE_IND=1 AND LOAN_DPD_BUCKET_KEY IN(5,6,7) THEN TOTAL_ACCOUNTS ELSE 0 END) LAR_91_180,
        SUM(CASE WHEN LOAN_ACCOUNT_STATUS_KEY=1 AND OVERDUE_IND=1 AND LOAN_DPD_BUCKET_KEY BETWEEN 8 AND 13 THEN TOTAL_ACCOUNTS ELSE 0 END) LAR_181_360,
        SUM(CASE WHEN LOAN_ACCOUNT_STATUS_KEY=1 AND OVERDUE_IND=1 AND LOAN_DPD_BUCKET_KEY IN(14)THEN TOTAL_ACCOUNTS ELSE 0 END) LAR_360_plus,
	   SUM(CASE WHEN LOAN_ACCOUNT_STATUS_KEY=1 AND LOAN_DPD_BUCKET_KEY IN(1,15) THEN TOTAL_OUTSTANDING_AMOUNT ELSE 0 END) PAR_0,
        SUM(CASE WHEN LOAN_ACCOUNT_STATUS_KEY=1 AND OVERDUE_IND=1 AND LOAN_DPD_BUCKET_KEY IN(2) THEN TOTAL_OUTSTANDING_AMOUNT ELSE 0 END) PAR_1_30,
        SUM(CASE WHEN LOAN_ACCOUNT_STATUS_KEY=1 AND OVERDUE_IND=1 AND LOAN_DPD_BUCKET_KEY IN(3) THEN TOTAL_OUTSTANDING_AMOUNT ELSE 0 END) PAR_31_60,
        SUM(CASE WHEN LOAN_ACCOUNT_STATUS_KEY=1 AND OVERDUE_IND=1 AND LOAN_DPD_BUCKET_KEY IN(4) THEN TOTAL_OUTSTANDING_AMOUNT ELSE 0 END) PAR_61_90,
        SUM(CASE WHEN LOAN_ACCOUNT_STATUS_KEY=1 AND OVERDUE_IND=1 AND LOAN_DPD_BUCKET_KEY IN(5,6,7) THEN TOTAL_OUTSTANDING_AMOUNT ELSE 0 END) PAR_91_180,
        SUM(CASE WHEN LOAN_ACCOUNT_STATUS_KEY=1 AND OVERDUE_IND=1 AND LOAN_DPD_BUCKET_KEY BETWEEN 8 AND 13 THEN TOTAL_OUTSTANDING_AMOUNT ELSE 0 END) PAR_181_360,
        SUM(CASE WHEN LOAN_ACCOUNT_STATUS_KEY=1 AND OVERDUE_IND=1 AND LOAN_DPD_BUCKET_KEY IN(14)THEN TOTAL_OUTSTANDING_AMOUNT ELSE 0 END) PAR_360_PLUS,
         SUM(CASE WHEN DAS_ID IN (6,10) THEN TOTAL_ACCOUNTS ELSE 0 END) TOTAL_WRITTEN_OFF_LOANS,
        SUM(CASE WHEN DAS_ID IN (6,10) THEN TOTAL_WRITE_OFF_AMOUNT ELSE 0 END) TOTAL_WRITTEN_OFF_AMOUNT

FROM HMANALYTICS.HM_MFI_ACT_FACT_TBL A
JOIN HMANALYTICS.HM_CONTRIBUTOR_DIM B
ON (B.ACTIVE=1 
AND A.CONTRI_KEY =B.CONTRI_KEY)
JOIN
HMANALYTICS.HM_LOAN_ACCOUNT_TYPE_DIM C
ON (A.LOAN_ACCOUNT_TYPE_KEY=C.LOAN_ACCOUNT_TYPE_KEY 
AND C.BUREAU_FLAG='MFI' AND C.ACTIVE=1) 	
		WHERE A.LOAD_YYYYMM= '"""+load_dt+ """' AND supress_ind_key <> 1
GROUP BY
       B.CONTRIBUTOR_ID , 
       B.institution_name , 
       C.ACCOUNT_TYPE		
""")		

nocDF.repartition(1).write.mode("append").saveAsTable("hmanalytics.hm_mfi_nocReport")


val rowDF = spark.sql("""select *, ROW_NUMBER () OVER (PARTITION BY mfi_id,reported_dt,account_type ORDER BY total_loans desc) rw  from hmanalytics.hm_mfi_nocReport""")
rowDF.createOrReplaceTempView("dedupe")



val fourMonthsData = spark.sql(""" select distinct * from dedupe where reported_dt <= From_unixtime(unix_timestamp('"""+load_dt+ """', 'yyyyMM'),'yyyy-MM-dd') and reported_dt >= add_months(From_unixtime(unix_timestamp('"""+load_dt+ """', 'yyyyMM'),'yyyy-MM-dd'),-3) and rw=1""")
fourMonthsData.createOrReplaceTempView("fourMonthsData")

val diffDF = spark.sql("""
select 
'"""+mon_rn+ """' AS month_run,
CASE WHEN B.mfi_id is null THEN A.mfi_id ELSE B.mfi_id  END as mfi_id,
CASE WHEN B.institution is null THEN A.institution ELSE B.institution  END as institution,
CASE WHEN B.account_type is null THEN A.account_type ELSE B.account_type  END as account_type,
A.reported_dt as reported_dt_from,
B.reported_dt as reported_dt_to,
NVL(B.total_loans,0) as total_loans_to,
(NVL(B.total_loans,0) - NVL(A.total_loans,0))  total_loans_diff,
CASE WHEN A.total_loans is null  OR  B.total_loans is null THEN 0 ELSE ROUND(((B.total_loans - A.total_loans)/A.total_loans)*100,2) END total_loans_perc_diff,
NVL(B.total_disb_amount,0) as total_disb_amount_to,
(NVL(B.total_disb_amount,0) - NVL(A.total_disb_amount,0))  total_disb_amount_diff,
CASE WHEN A.total_disb_amount is null  OR  B.total_disb_amount is null THEN 0 ELSE ROUND(((B.total_disb_amount - A.total_disb_amount)/A.total_disb_amount)*100,2) END total_disb_amount_perc_diff,
NVL(B.active_loans,0) as active_loans_to,
(NVL(B.active_loans,0) - NVL(A.active_loans,0))  active_loans_diff,
CASE WHEN A.active_loans is null  OR  B.active_loans is null THEN 0 ELSE ROUND(((B.active_loans - A.active_loans)/A.active_loans)*100,2) END active_loans_perc_diff,
NVL(B.active_disb_amount,0) as active_disb_amount_to,
(NVL(B.active_disb_amount,0) - NVL(A.active_disb_amount,0))  active_disb_amount_diff,
CASE WHEN A.active_disb_amount is null  OR  B.active_disb_amount is null THEN 0 ELSE ROUND(((B.active_disb_amount - A.active_disb_amount)/A.active_disb_amount)*100,2) END active_disb_amount_perc_diff,
NVL(B.active_outstanding,0) as active_outstanding_to,
(NVL(B.active_outstanding,0) - NVL(A.active_outstanding,0))  active_outstanding_diff,
CASE WHEN A.active_outstanding is null  OR  B.active_outstanding is null THEN 0 ELSE ROUND(((B.active_outstanding - A.active_outstanding)/A.active_outstanding)*100,2) END active_outstanding_perc_diff,
(NVL(B.lar_0,0) - NVL(A.lar_0,0))  lar_0_diff,
CASE WHEN A.lar_0 is null  OR  B.lar_0 is null THEN 0 ELSE ROUND(((B.lar_0 - A.lar_0)/A.lar_0)*100,2) END lar_0_perc_diff,
(NVL(B.lar_1_30,0) - NVL(A.lar_1_30,0))  lar_1_30_diff,
CASE WHEN A.lar_1_30 is null  OR  B.lar_1_30 is null THEN 0 ELSE ROUND(((B.lar_1_30 - A.lar_1_30)/A.lar_1_30)*100,2) END lar_1_30_perc_diff,
(NVL(B.lar_31_60,0) - NVL(A.lar_31_60,0))  lar_31_60_diff,
CASE WHEN A.lar_31_60 is null  OR  B.lar_31_60 is null THEN 0 ELSE ROUND(((B.lar_31_60 - A.lar_31_60)/A.lar_31_60)*100,2) END lar_31_60_perc_diff,
(NVL(B.lar_61_90,0) - NVL(A.lar_61_90,0))  lar_61_90_diff,
CASE WHEN A.lar_61_90 is null  OR  B.lar_61_90 is null THEN 0 ELSE ROUND(((B.lar_61_90 - A.lar_61_90)/A.lar_61_90)*100,2) END lar_61_90_perc_diff,
(NVL(B.lar_91_180,0) - NVL(A.lar_91_180,0))  lar_91_180_diff,
CASE WHEN A.lar_91_180 is null  OR  B.lar_91_180 is null THEN 0 ELSE ROUND(((B.lar_91_180 - A.lar_91_180)/A.lar_91_180)*100,2) END lar_91_180_perc_diff,
(NVL(B.lar_181_360,0) - NVL(A.lar_181_360,0))  lar_181_360_diff,
CASE WHEN A.lar_181_360 is null  OR  B.lar_181_360 is null THEN 0 ELSE ROUND(((B.lar_181_360 - A.lar_181_360)/A.lar_181_360)*100,2) END lar_181_360_perc_diff,
(NVL(B.lar_360_plus,0) - NVL(A.lar_360_plus,0))  lar_360_plus_diff,
CASE WHEN A.lar_360_plus is null  OR  B.lar_360_plus is null THEN 0 ELSE ROUND(((B.lar_360_plus - A.lar_360_plus)/A.lar_360_plus)*100,2) END lar_360_plus_perc_diff,
(NVL(B.par_0,0) - NVL(A.par_0,0))  par_0_diff,
CASE WHEN A.par_0 is null  OR  B.par_0 is null THEN 0 ELSE ROUND(((B.par_0 - A.par_0)/A.par_0)*100,2) END par_0_perc_diff,
(NVL(B.par_1_30,0) - NVL(A.par_1_30,0))  par_1_30_diff,
CASE WHEN A.par_1_30 is null  OR  B.par_1_30 is null THEN 0 ELSE ROUND(((B.par_1_30 - A.par_1_30)/A.par_1_30)*100,2) END par_1_30_perc_diff,
(NVL(B.par_31_60,0) - NVL(A.par_31_60,0))  par_31_60_diff,
CASE WHEN A.par_31_60 is null  OR  B.par_31_60 is null THEN 0 ELSE ROUND(((B.par_31_60 - A.par_31_60)/A.par_31_60)*100,2) END par_31_60_perc_diff,
(NVL(B.par_61_90,0) - NVL(A.par_61_90,0))  par_61_90_diff,
CASE WHEN A.par_61_90 is null  OR  B.par_61_90 is null THEN 0 ELSE ROUND(((B.par_61_90 - A.par_61_90)/A.par_61_90)*100,2) END par_61_90_perc_diff,
(NVL(B.par_91_180,0) - NVL(A.par_91_180,0))  par_91_180_diff,
CASE WHEN A.par_91_180 is null  OR  B.par_91_180 is null THEN 0 ELSE ROUND(((B.par_91_180 - A.par_91_180)/A.par_91_180)*100,2) END par_91_180_perc_diff,
(NVL(B.par_181_360,0) - NVL(A.par_181_360,0))  par_181_360_diff,
CASE WHEN A.par_181_360 is null  OR  B.par_181_360 is null THEN 0 ELSE ROUND(((B.par_181_360 - A.par_181_360)/A.par_181_360)*100,2) END par_181_360_perc_diff,
(NVL(B.par_360_plus,0) - NVL(A.par_360_plus,0))  par_360_plus_diff,
CASE WHEN A.par_360_plus is null  OR  B.par_360_plus is null THEN 0 ELSE ROUND(((B.par_360_plus - A.par_360_plus)/A.par_360_plus)*100,2) END par_360_plus_perc_diff,
NVL(B.total_written_off_loans,0) as total_written_off_loans_to,
(NVL(B.total_written_off_loans,0) - NVL(A.total_written_off_loans,0))  total_written_off_loans_diff,
CASE WHEN A.total_written_off_loans is null  OR  B.total_written_off_loans is null THEN 0 ELSE ROUND(((B.total_written_off_loans - A.total_written_off_loans)/A.total_written_off_loans)*100,2) END total_written_off_loans_perc_diff,
NVL(B.total_written_off_amount,0) as total_written_off_amount_to,
(NVL(B.total_written_off_amount,0) - NVL(A.total_written_off_amount,0))  total_written_off_amount_diff,
CASE WHEN A.total_written_off_amount is null  OR  B.total_written_off_amount is null THEN 0 ELSE ROUND(((B.total_written_off_amount - A.total_written_off_amount)/A.total_written_off_amount)*100,2) END total_written_off_amount_perc_diff

FROM
fourMonthsData A
FULL OUTER JOIN fourMonthsData B
ON(A.mfi_id = B.mfi_id
AND A.account_type = B.account_type
AND add_months(A.reported_dt,1) = B.reported_dt)
WHERE (B.reported_dt <> add_months(From_unixtime(unix_timestamp('"""+load_dt+ """', 'yyyyMM'),'yyyy-MM-dd'),-3)
OR A.reported_dt <> From_unixtime(unix_timestamp('"""+load_dt+ """', 'yyyyMM'),'yyyy-MM-dd'))
AND (A.ACCOUNT_TYPE is not null OR B.ACCOUNT_TYPE is not null)
""")

diffDF.repartition(1).write.mode("overwrite").saveAsTable("hmanalytics.hm_mfi_noc_month_diff")

val actOutDF = spark.sql("""
select
reported_dt,
mfi_id,
account_type,
active_outstanding,
sum(active_outstanding) over(partition by reported_dt,account_type) as active_outstanding_total

from fourMonthsData
""")

val actOutPerDF = actOutDF.withColumn("active_outstanding_tot_percent", (col("active_outstanding")/col("active_outstanding_total"))*100)
val grtrPt5DF = actOutPerDF.where(col("active_outstanding_tot_percent") > 0.5 ).select(col("reported_dt"),col("mfi_id"),col("account_type")).distinct

val joinDF = diffDF.join(grtrPt5DF , diffDF("mfi_id") === grtrPt5DF("mfi_id") && diffDF("account_type") === grtrPt5DF("account_type") && (diffDF("reported_dt_from") === grtrPt5DF("reported_dt") || diffDF("reported_dt_to") === grtrPt5DF("reported_dt"))) .where( abs(col("active_loans_perc_diff")) >= 10 || abs(col("active_outstanding_perc_diff")) >= 10  || abs(col("lar_1_30_perc_diff")) >= 20 || abs(col("lar_31_60_perc_diff")) >= 20 || abs(col("lar_61_90_perc_diff")) >= 20 || abs(col("lar_91_180_perc_diff")) >= 20 || abs(col("lar_181_360_perc_diff")) >= 20 || abs(col("lar_360_plus_perc_diff")) >= 20).drop("reported_dt").where(diffDF("reported_dt_to").isNotNull && diffDF("reported_dt_from").isNotNull).distinct
val outLieDF = joinDF.select(diffDF("mfi_id").alias("mfi"),diffDF("account_type").alias("act_type")).distinct

val finalDF = diffDF.join(outLieDF , diffDF("mfi_id") === outLieDF("mfi") && diffDF("account_type") === outLieDF("act_type")).drop("mfi","act_type")

finalDF.orderBy(col("mfi_id"), col("account_type"), col("reported_dt_from")).repartition(1).write.mode("overwrite").saveAsTable("hmanalytics.hm_mfi_noc_month_outlier")
finalDF.orderBy(col("mfi_id")).repartition(1).write.mode("overwrite").option("header", "true").csv("/tmp/prateek/NOCMfi")
   
}