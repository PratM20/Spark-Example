package com.crif.highmark.CrifSemantic.DataLoad
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import scala.concurrent.{Await, Future}
import scala.concurrent.duration._
import scala.concurrent.ExecutionContext.Implicits.global
import com.crif.highmark.CrifSemantic.Aggregation.mfiBfil

object mfiFact extends App{
	
val spark = SparkSession.builder().appName("mfiFact")
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



/* Trend Hanging Records Calculating Max_RPT_DT on the basis of ACCOUNT_KEY 
*/
val tempdf = spark.sql("""SELECT  
account_key , 
max(act_reported_dt) max_rpt_dt  
FROM 
hmanalytics.hm_mfi_account_mfitrend
WHERE   act_reported_dt <= '"""+end_dt+"""'
GROUP BY
account_key """).distinct()

val prodDF =spark.sql(""" SELECT 
distinct account_key  
FROM 
hmanalytics.hm_mfi_account_racprd 
WHERE  act_reported_dt <= '"""+end_dt+"""'  
 """)

val tempProdDf = tempdf.join(prodDF, tempdf("ACCOUNT_KEY") === prodDF("ACCOUNT_KEY") ,"left_anti").distinct()
tempProdDf.createOrReplaceTempView("tempTrend")


/* Selecting columns from UPD table with max reported date 
*/
val tempUpddf = spark.sql("""
SELECT A.* FROM
hmanalytics.hm_mfi_account_upd_mfitrend A
JOIN
(SELECT distinct
account_key , 
max(act_reported_dt) max_rpt_dt  
FROM hmanalytics.hm_mfi_account_upd_mfitrend
WHERE   act_reported_dt <= '"""+end_dt+"""'
GROUP BY
account_key)B 
ON(A.ACCOUNT_KEY = B.ACCOUNT_KEY
AND A.act_reported_dt = B.max_rpt_dt) """).distinct

tempUpddf.createOrReplaceTempView("tempUpdTrend")


/* Trend Hanging Records Calculating Max_RPT_DT on the basis of ACCOUNT_KEY for GL 
*/
val tempGLdf = spark.sql("""SELECT  
account_key , 
max(act_reported_dt) max_rpt_dt  
FROM hmanalytics.hm_mfi_account_gl_mfitrend
WHERE   act_reported_dt <= '"""+end_dt+"""'
GROUP BY
account_key """).distinct()


val prodGLDF =spark.sql(""" SELECT distinct account_key  FROM hmanalytics.hm_mfi_account_gl_racprd WHERE  
act_reported_dt <= '"""+end_dt+"""'  
 """)


val tempProdGLDf = tempGLdf.join(prodGLDF, tempGLdf("ACCOUNT_KEY") === prodGLDF("ACCOUNT_KEY") ,"left_anti").distinct()

tempProdGLDf.createOrReplaceTempView("tempTrendGL")

/* Selecting columns from UPD table with max reported date GL 
*/

val tempUpdGldf = spark.sql("""
SELECT A.* FROM
hmanalytics.hm_mfi_account_upd_gl_mfitrend A
JOIN
(SELECT distinct
account_key , 
max(act_reported_dt) max_rpt_dt  
FROM hmanalytics.hm_mfi_account_upd_gl_mfitrend
WHERE   act_reported_dt <= '"""+end_dt+"""'
GROUP BY
account_key)B 
ON(A.ACCOUNT_KEY = B.ACCOUNT_KEY
AND A.act_reported_dt = B.max_rpt_dt) """).distinct

tempUpdGldf.createOrReplaceTempView("tempUpdGlTrend")

/* Creating As Of data from trend , prod hanging and trend hanging for all MFIs and GL 
*/
val mfiDF= spark.sql("""
SELECT '"""+end_dt+"""' as_of, A.* FROM(
SELECT 
C.INSERT_DT,
A.act_reported_dt,
A.MFI_ID,
C.CONSUMER_KEY,
A.ACCOUNT_KEY,
B.ACCOUNT_NBR,
A.ACCOUNT_STATUS,
A.DAS,
A.AMOUNT_OVERDUE_TOTAL,
A.CURRENT_BALANCE,
B.DISBURSED_AMOUNT,
A.CHARGEOFF_AMT,
C.HIGH_CREDIT,
C.WORST_AMOUNT_OVERDUE,
B.DISBURSED_DT,
A.CLOSED_DT,
A.LAST_PAYMENT_DT,
A.DAYS_PAST_DUE,
B.INSTAL_FREQ,
B.LOAN_CATEGORY,
B.LOAN_CYCLE_ID,
B.LOAN_PURPOSE,
B.NUM_INSTALLMENT,
C.CENTRE_ID,
C.MFI_GROUP_ID,
C.UI_FLAG,
B.SUPPRESS_INDICATOR,
A.MIN_AMOUNT_DUE
FROM
hmanalytics.hm_mfi_account_mfitrend A
JOIN tempTrend D
ON(A.ACCOUNT_KEY = D.ACCOUNT_KEY
AND A.act_reported_dt = D.max_rpt_dt)
JOIN tempUpdTrend B
ON(A.ACCOUNT_KEY = B.ACCOUNT_KEY)
LEFT OUTER JOIN hmanalytics.hm_mfi_account_racprd C
ON(B.ACCOUNT_KEY = C.ACCOUNT_KEY)

UNION ALL

SELECT
INSERT_DT,
act_reported_dt,
MFI_ID,
CONSUMER_KEY,
ACCOUNT_KEY,
ACCOUNT_NBR,
ACCOUNT_STATUS,
DAS,
AMOUNT_OVERDUE_TOTAL,
CURRENT_BALANCE,
DISBURSED_AMOUNT,
CHARGEOFF_AMT,
HIGH_CREDIT,
WORST_AMOUNT_OVERDUE,
DISBURSED_DT,
CLOSED_DT,
LAST_PAYMENT_DT,
DAYS_PAST_DUE,
INSTAL_FREQ,
LOAN_CATEGORY,
LOAN_CYCLE_ID,
LOAN_PURPOSE,
NUM_INSTALLMENT,
CENTRE_ID,
MFI_GROUP_ID,
UI_FLAG,
SUPPRESS_INDICATOR,
MIN_AMOUNT_DUE
FROM
hmanalytics.hm_mfi_account_racprd
WHERE act_reported_dt <= '"""+end_dt+"""'

UNION ALL

SELECT 
C.INSERT_DT,
A.act_reported_dt,
A.MFI_ID,
C.CONSUMER_KEY,
A.ACCOUNT_KEY,
B.ACCOUNT_NBR,
A.ACCOUNT_STATUS,
A.DAS,
A.AMOUNT_OVERDUE_TOTAL,
A.CURRENT_BALANCE,
B.DISBURSED_AMOUNT,
A.CHARGEOFF_AMT,
C.HIGH_CREDIT,
C.WORST_AMOUNT_OVERDUE,
B.DISBURSED_DT,
A.CLOSED_DT,
A.LAST_PAYMENT_DT,
A.DAYS_PAST_DUE,
B.INSTAL_FREQ,
B.LOAN_CATEGORY,
B.LOAN_CYCLE_ID,
B.LOAN_PURPOSE,
B.NUM_INSTALLMENT,
C.CENTRE_ID,
C.MFI_GROUP_ID,
C.UI_FLAG,
B.SUPPRESS_INDICATOR,
A.MIN_AMOUNT_DUE
FROM
hmanalytics.hm_mfi_account_gl_mfitrend A
JOIN tempTrendGL D
ON(A.ACCOUNT_KEY = D.ACCOUNT_KEY
AND A.act_reported_dt = D.max_rpt_dt)
JOIN tempUpdGlTrend B
ON(A.ACCOUNT_KEY = B.ACCOUNT_KEY)
LEFT OUTER JOIN hmanalytics.hm_mfi_account_gl_racprd C
ON(B.ACCOUNT_KEY = C.ACCOUNT_KEY)

UNION ALL

SELECT
INSERT_DT,
act_reported_dt,
MFI_ID,
CONSUMER_KEY,
ACCOUNT_KEY,
ACCOUNT_NBR,
ACCOUNT_STATUS,
DAS,
AMOUNT_OVERDUE_TOTAL,
CURRENT_BALANCE,
DISBURSED_AMOUNT,
CHARGEOFF_AMT,
HIGH_CREDIT,
WORST_AMOUNT_OVERDUE,
DISBURSED_DT,
CLOSED_DT,
LAST_PAYMENT_DT,
DAYS_PAST_DUE,
INSTAL_FREQ,
LOAN_CATEGORY,
LOAN_CYCLE_ID,
LOAN_PURPOSE,
NUM_INSTALLMENT,
CENTRE_ID,
MFI_GROUP_ID,
UI_FLAG,
SUPPRESS_INDICATOR,
MIN_AMOUNT_DUE
FROM
hmanalytics.hm_mfi_account_gl_racprd
WHERE act_reported_dt <= '"""+end_dt+"""'
)A
""")

mfiDF.write.mode("overwrite").saveAsTable("hmanalytics.hm_mfi_account_as_of")

/* Cluster Level Minimum Disbursement Date
*/

val minDisbDF = spark.sql("""
SELECT 
distinct
A.max_clst_id,
B.consumer_key,
min(B.disbursed_dt) over (partition by A.max_clst_id) min_disb_dt
FROM
(SELECT distinct candidate_id,max_clst_id FROM hmanalytics.hm_mfi_clst_"""+mon_rn+""") A
JOIN
(SELECT  distinct consumer_key,disbursed_dt FROM hmanalytics.hm_mfi_account_as_of) B
ON(A.candidate_id = B.consumer_key)
""")

minDisbDF.write.mode("overwrite").saveAsTable("hmanalytics.hm_mfi_clst_mindisb_dt")

/* Consumer Data Calculation and min_disb_dt
*/

val cnsMfiDF= spark.sql("""
SELECT 
A.*,
MEM_ID,
 KENDRA_ID,
 GROUP_ID,
 GENDER,
 AGE,
 AGE_ASON_DT,
 BIRTH_DT,
 OCCUPATION,
 MONTHLY_INCOME,
 STD_STATE_CODE,
 DISTRICT,
 PIN_CODE
FROM
hmanalytics.hm_mfi_account_as_of A
JOIN
(SELECT
 consumer_key,
 MEM_ID,
 KENDRA_ID,
 GROUP_ID,
 GENDER,
 AGE,
 AGE_ASON_DT,
 BIRTH_DT,
 OCCUPATION,
 MONTHLY_INCOME,
 STD_STATE_CODE,
 DISTRICT,
 PIN_CODE
FROM( 
SELECT 
 consumer_key,
 MEM_ID,
 KENDRA_ID,
 GROUP_ID,
 GENDER,
 AGE,
 AGE_ASON_DT,
 BIRTH_DT,
 OCCUPATION,
 MONTHLY_INCOME,
 STD_STATE_CODE,
 DISTRICT,
 PIN_CODE,
 ROW_NUMBER () OVER (PARTITION BY consumer_key ORDER BY insert_dt desc) rw
FROM
hmanalytics.hm_mfi_analytics)C
WHERE rw=1) B
ON(A.consumer_key = B.consumer_key)
""")

cnsMfiDF.createOrReplaceTempView("tempCnsMfi")

val sepFnlDF = spark.sql("""
SELECT A.* ,
min_disb_dt ,
COALESCE(B.max_clst_id,A.consumer_key) clst_id
FROM
tempCnsMfi A
Left Outer Join
hmanalytics.hm_mfi_clst_mindisb_dt B
ON(A.consumer_key = B.consumer_key)
""")

sepFnlDF.write.mode("overwrite").saveAsTable("hmanalytics.hm_mfi_step1_fact")

/* GL data Aggregation
*/

val glAggDf = spark.sql("""SELECT as_of,A.MFI_ID,
        D.KEY SDP_KEY,
        C.CONTRI_KEY,
		clst_id,
		min_disb_dt,
        CASE WHEN (CASE WHEN NVL(CHARGEOFF_AMT,0) >0 THEN 'S06'
             WHEN NVL(CLOSED_DT,date_add(as_of,100)) <= as_of THEN 'S07'
             WHEN CAST(REGEXP_REPLACE(DAYS_PAST_DUE,'[^[:digit:]]','') AS INT)>0 THEN 'S05'
             WHEN NVL(AMOUNT_OVERDUE_TOTAL,0) > (0.01 * NVL(DISBURSED_AMOUNT,0)) THEN 'S05'
        ELSE
            ACCOUNT_STATUS
        END) IN ('S04', 'S05') AND NVL(CLOSED_DT,date_add(as_of,100)) > as_of THEN A.MFI_ID ELSE NULL END ACTIV_MFI,
        ACCOUNT_NBR ACT_NBR,
        ACCOUNT_KEY ACT_KEY,
        CASE WHEN NVL(CHARGEOFF_AMT,0) >0 THEN 'S06'
             WHEN NVL(CLOSED_DT,date_add(as_of,100)) <= as_of THEN 'S07'
             WHEN CAST(REGEXP_REPLACE(DAYS_PAST_DUE,'[^[:digit:]]','') AS INT)>0 THEN 'S05'
             WHEN NVL(AMOUNT_OVERDUE_TOTAL,0) > (0.01 * NVL(DISBURSED_AMOUNT,0)) THEN 'S05'
        ELSE
            ACCOUNT_STATUS
        END DAS,
        CASE WHEN LOAN_CATEGORY='T01' THEN NVL(ROUND((ABS(AMOUNT_OVERDUE_TOTAL))/CNT,2),0)
             ELSE ABS(AMOUNT_OVERDUE_TOTAL)
        END AMT_DUE,
        CASE WHEN LOAN_CATEGORY='T01' THEN NVL(ROUND((ABS(CHARGEOFF_AMT))/CNT,2),0)
             ELSE ABS(CHARGEOFF_AMT)
        END CHARG_AMT,
        CLOSED_DT,
        CASE WHEN LOAN_CATEGORY='T01' THEN NVL(ROUND((ABS(CURRENT_BALANCE))/CNT,2),0)
             ELSE ABS(CURRENT_BALANCE)
        END CUR_BAL,
        A.GROUP_ID,
        CASE WHEN H.OCCUPATION IS NULL THEN 'NULL' ELSE H.HM_CATEGORY END OCCUPATION,
        CAST(REGEXP_REPLACE(DAYS_PAST_DUE,'[^[:digit:]]','') AS INT) DPD,
        CASE WHEN LOAN_CATEGORY='T01' THEN NVL(ROUND((ABS(CAST(REGEXP_REPLACE(DISBURSED_AMOUNT,'[^[:digit:]]','') AS DECIMAL(38,2))))/CNT,2),0)
             ELSE ABS(CAST(REGEXP_REPLACE(DISBURSED_AMOUNT,'[^[:digit:]]','') AS DECIMAL(38,2)))
        END DISB_AMT,
        DISBURSED_DT DISB_DT,
        ABS(CAST(REGEXP_REPLACE(HIGH_CREDIT,'[^[:digit:]]','') AS DECIMAL(38,2))) HIGH_CREDIT,
        CASE WHEN UPPER(INSTAL_FREQ) NOT IN ('F01','F02','F03','F04','F05','F06','F07','F08','F10') THEN 'NA'
             ELSE UPPER(INSTAL_FREQ)
        END INSTAL_FREQ,
        LOAN_CATEGORY LN_CAT,
        CASE WHEN B.LOAN_PURPOSE IS NULL THEN 'NA' ELSE B.STATUS END LN_PURPOSE,
        ACT_REPORTED_DT RPT_DT,
        CONSUMER_KEY CNS_KEY,
        CASE WHEN BIRTH_DT IS NOT NULL THEN MONTHS_BETWEEN(as_of,BIRTH_DT)/12
             WHEN BIRTH_DT IS NULL AND AGE IS NOT NULL AND AGE_ASON_DT IS NOT NULL THEN MONTHS_BETWEEN(as_of,AGE_ASON_DT)/12 + AGE
             WHEN BIRTH_DT IS NULL AND AGE IS NOT NULL AND AGE_ASON_DT IS NULL THEN NULL
        ELSE NULL
        END AGE,
        (CASE WHEN INSTAL_FREQ='F01' then ROUND(NUM_INSTALLMENT/4)
              WHEN INSTAL_FREQ='F02' then ROUND(NUM_INSTALLMENT/2)
              WHEN INSTAL_FREQ='F03' then ROUND(NUM_INSTALLMENT/1)
              WHEN INSTAL_FREQ='F04' then ROUND(NUM_INSTALLMENT/0.5)
              WHEN INSTAL_FREQ='F05' then ROUND(NUM_INSTALLMENT/0.25)
              WHEN INSTAL_FREQ='F06' then ROUND(NUM_INSTALLMENT/0.15)
              WHEN INSTAL_FREQ='F07' then ROUND(NUM_INSTALLMENT/0.08)
         ELSE NULL END
        ) NUM_INSTAL,
        (CASE WHEN INSTAL_FREQ='F01' then FLOOR(NUM_INSTALLMENT/4)
              WHEN INSTAL_FREQ='F02' then FLOOR(NUM_INSTALLMENT/2)
              WHEN INSTAL_FREQ='F03' then FLOOR(NUM_INSTALLMENT/1)
              WHEN INSTAL_FREQ='F04' then FLOOR(NUM_INSTALLMENT/0.5)
              WHEN INSTAL_FREQ='F05' then FLOOR(NUM_INSTALLMENT/0.25)
              WHEN INSTAL_FREQ='F06' then FLOOR(NUM_INSTALLMENT/0.15)
              WHEN INSTAL_FREQ='F07' then FLOOR(NUM_INSTALLMENT/0.08)
         ELSE NULL END
        ) NEW_NUMINSTAL,NUM_INSTALLMENT,
        GENDER,
        ABS(MONTHLY_INCOME) MONTHLY_INCOME,
        TRIM(LOAN_CYCLE_ID) LOAN_CYCLE_ID,CENTRE_ID,
        SUPPRESS_INDICATOR,
        CASE WHEN LOAN_CATEGORY='T01' THEN NVL(ROUND((ABS(MIN_AMOUNT_DUE))/CNT,2),0)
             ELSE ABS(MIN_AMOUNT_DUE)
        END  MIN_AMOUNT_DUE,
        ROW_NUMBER () OVER (PARTITION BY ACCOUNT_KEY,as_of ORDER BY DAS) RW
   FROM hmanalytics.hm_mfi_step1_fact A
  LEFT OUTER JOIN
   (select * from hmanalytics.HM_CONTRIBUTOR_DIM where active=1) C
   ON (A.MFI_ID=C.CONTRIBUTOR_ID)
   LEFT OUTER JOIN hmanalytics.HM_LOAN_PURPOSE_MASTER B
     ON (A.MFI_ID = B.MFI_ID AND A.LOAN_PURPOSE = B.LOAN_PURPOSE)
   JOIN
       (SELECT DISTINCT KEY,STD_STATE_CODE,DISTRICT,STD_PIN_CODE FROM hmanalytics.HM_SDP_DIM2 WHERE ACTIVE=1) D
         ON (NVL(A.STD_STATE_CODE,'A')=NVL(D.STD_STATE_CODE,'A')
             AND NVL(A.DISTRICT,'A')=NVL(D.DISTRICT,'A')
             AND NVL(REGEXP_REPLACE(A.PIN_CODE,'[^[:digit:]]',''),123456)=NVL(D.STD_PIN_CODE,123456)
             )
   LEFT OUTER JOIN hmanalytics.HM_OCCUPATION_FINAL H
     ON (A.MFI_ID=H.MFI_ID AND A.OCCUPATION = H.OCCUPATION)
   LEFT OUTER JOIN (SELECT GROUP_ID,COUNT(1) CNT
    FROM hmanalytics.hm_mfi_step1_fact A
   WHERE LOAN_CATEGORY='T01' AND mfi_id='PRB0000003'
     AND A.ACCOUNT_STATUS NOT IN ('S01','S02','S03')
     AND NVL(LENGTH(ABS(DISBURSED_AMOUNT)),0)<15
   GROUP BY GROUP_ID) I
     ON (A.GROUP_ID=I.GROUP_ID)
  WHERE A.ACCOUNT_STATUS NOT IN ('S01','S02','S03')
     AND A.MFI_ID = 'PRB0000003'
    AND NVL(LENGTH(ABS(DISBURSED_AMOUNT)),0)<15
	""")
	
	
glAggDf.write.mode("overwrite").saveAsTable("hmanalytics.hm_mfi_step2_fact")

/* ALL data Aggregation
*/
	
	val allAggDf = spark.sql("""SELECT as_of,A.MFI_ID,
        D.KEY SDP_KEY,
        C.CONTRI_KEY,
		clst_id,
		min_disb_dt,		
        CASE WHEN (CASE WHEN NVL(CHARGEOFF_AMT,0) >0 THEN 'S06'
             WHEN NVL(CLOSED_DT,date_add(as_of,100)) <= as_of THEN 'S07'
             WHEN CAST(REGEXP_REPLACE(DAYS_PAST_DUE,'[^[:digit:]]','') AS INT)>0 THEN 'S05'
             WHEN NVL(AMOUNT_OVERDUE_TOTAL,0) > (0.01 * NVL(DISBURSED_AMOUNT,0)) THEN 'S05'
        ELSE
            ACCOUNT_STATUS
        END) IN ('S04', 'S05') AND NVL(CLOSED_DT,date_add(as_of,100)) > as_of THEN A.MFI_ID ELSE NULL END ACTIV_MFI,
        ACCOUNT_NBR ACT_NBR,
        ACCOUNT_KEY ACT_KEY,
        CASE WHEN NVL(CHARGEOFF_AMT,0) >0 THEN 'S06'
             WHEN NVL(CLOSED_DT,date_add(as_of,100)) <= as_of THEN 'S07'
             WHEN CAST(REGEXP_REPLACE(DAYS_PAST_DUE,'[^[:digit:]]','') AS INT)>0 THEN 'S05'
             WHEN NVL(AMOUNT_OVERDUE_TOTAL,0) > (0.01 * NVL(DISBURSED_AMOUNT,0)) THEN 'S05'
        ELSE
            ACCOUNT_STATUS
        END DAS,
       ABS(AMOUNT_OVERDUE_TOTAL) AMT_DUE,
       ABS(CHARGEOFF_AMT) CHARG_AMT,
       CLOSED_DT,
       ABS(CURRENT_BALANCE) CUR_BAL,
       GROUP_ID,
        CASE WHEN H.OCCUPATION IS NULL THEN 'NULL' ELSE H.HM_CATEGORY END OCCUPATION,
        CAST(REGEXP_REPLACE(DAYS_PAST_DUE,'[^[:digit:]]','') AS INT) DPD,
		ABS(CAST(REGEXP_REPLACE(DISBURSED_AMOUNT,'[^[:digit:]]','') AS DECIMAL(38,2))) DISB_AMT,
        DISBURSED_DT DISB_DT,
        ABS(CAST(REGEXP_REPLACE(HIGH_CREDIT,'[^[:digit:]]','') AS DECIMAL(38,2))) HIGH_CREDIT,
        CASE WHEN UPPER(INSTAL_FREQ) NOT IN ('F01','F02','F03','F04','F05','F06','F07','F08','F10') THEN 'NA'
             ELSE UPPER(INSTAL_FREQ)
        END INSTAL_FREQ,
        LOAN_CATEGORY LN_CAT,
        CASE WHEN B.LOAN_PURPOSE IS NULL THEN 'NA' ELSE B.STATUS END LN_PURPOSE,
        ACT_REPORTED_DT RPT_DT,
        CONSUMER_KEY CNS_KEY,
        CASE WHEN BIRTH_DT IS NOT NULL THEN MONTHS_BETWEEN(as_of,BIRTH_DT)/12
             WHEN BIRTH_DT IS NULL AND AGE IS NOT NULL AND AGE_ASON_DT IS NOT NULL THEN MONTHS_BETWEEN(as_of,AGE_ASON_DT)/12 + AGE
             WHEN BIRTH_DT IS NULL AND AGE IS NOT NULL AND AGE_ASON_DT IS NULL THEN NULL
        ELSE NULL
        END AGE,
        (CASE WHEN INSTAL_FREQ='F01' then ROUND(NUM_INSTALLMENT/4)
              WHEN INSTAL_FREQ='F02' then ROUND(NUM_INSTALLMENT/2)
              WHEN INSTAL_FREQ='F03' then ROUND(NUM_INSTALLMENT/1)
              WHEN INSTAL_FREQ='F04' then ROUND(NUM_INSTALLMENT/0.5)
              WHEN INSTAL_FREQ='F05' then ROUND(NUM_INSTALLMENT/0.25)
              WHEN INSTAL_FREQ='F06' then ROUND(NUM_INSTALLMENT/0.15)
              WHEN INSTAL_FREQ='F07' then ROUND(NUM_INSTALLMENT/0.08)
         ELSE NULL END
        ) NUM_INSTAL,
        (CASE WHEN INSTAL_FREQ='F01' then FLOOR(NUM_INSTALLMENT/4)
              WHEN INSTAL_FREQ='F02' then FLOOR(NUM_INSTALLMENT/2)
              WHEN INSTAL_FREQ='F03' then FLOOR(NUM_INSTALLMENT/1)
              WHEN INSTAL_FREQ='F04' then FLOOR(NUM_INSTALLMENT/0.5)
              WHEN INSTAL_FREQ='F05' then FLOOR(NUM_INSTALLMENT/0.25)
              WHEN INSTAL_FREQ='F06' then FLOOR(NUM_INSTALLMENT/0.15)
              WHEN INSTAL_FREQ='F07' then FLOOR(NUM_INSTALLMENT/0.08)
         ELSE NULL END
        ) NEW_NUMINSTAL,NUM_INSTALLMENT,
        GENDER,
        ABS(MONTHLY_INCOME) MONTHLY_INCOME,
        TRIM(LOAN_CYCLE_ID) LOAN_CYCLE_ID,CENTRE_ID,
        SUPPRESS_INDICATOR,
        ABS(MIN_AMOUNT_DUE) MIN_AMOUNT_DUE,
        ROW_NUMBER () OVER (PARTITION BY ACCOUNT_KEY,as_of ORDER BY DAS) RW
   FROM hmanalytics.hm_mfi_step1_fact A
  LEFT OUTER JOIN
   (select * from hmanalytics.HM_CONTRIBUTOR_DIM where active=1) C
   ON (A.MFI_ID=C.CONTRIBUTOR_ID)
   LEFT OUTER JOIN hmanalytics.HM_LOAN_PURPOSE_MASTER B
     ON (A.MFI_ID = B.MFI_ID AND A.LOAN_PURPOSE = B.LOAN_PURPOSE)
   JOIN
       (SELECT DISTINCT KEY,STD_STATE_CODE,DISTRICT,STD_PIN_CODE FROM hmanalytics.HM_SDP_DIM2 WHERE ACTIVE=1) D
         ON (NVL(A.STD_STATE_CODE,'A')=NVL(D.STD_STATE_CODE,'A')
             AND NVL(A.DISTRICT,'A')=NVL(D.DISTRICT,'A')
             AND NVL(REGEXP_REPLACE(A.PIN_CODE,'[^[:digit:]]',''),123456)=NVL(D.STD_PIN_CODE,123456)
             )
   LEFT OUTER JOIN hmanalytics.HM_OCCUPATION_FINAL H
     ON (A.MFI_ID=H.MFI_ID AND A.OCCUPATION = H.OCCUPATION)
  WHERE A.ACCOUNT_STATUS NOT IN ('S01','S02','S03')
    AND A.MFI_ID <> 'PRB0000003'
    AND NVL(LENGTH(ABS(DISBURSED_AMOUNT)),0)<15
	""")
	
allAggDf.write.mode("append").saveAsTable("hmanalytics.hm_mfi_step2_fact")

 /*Step 3 Aggregation */

 val stp3Df = spark.sql("""SELECT B.SDP_KEY,
          B.CONTRI_KEY,
          CAST(date_format(as_of,'yyyyMM') as STRING) as LOAD_YYYYMM,
          CAST(date_format(disb_dt,'yyyyMM') as STRING) as DISB_YYYYMM,
		  CAST(date_format(min_disb_dt,'yyyyMM') as STRING) as min_disb_YYYYMM,
          CASE WHEN C.ACCOUNT_TYPE IS NULL THEN 'NA'
               ELSE LN_CAT
          END LN_CAT,
          CASE WHEN CLOSED_DT IS NOT NULL AND CLOSED_DT BETWEEN add_months(as_of,-6) AND as_of THEN 'CLOSED WITHIN 6 MONTHS'
               WHEN CLOSED_DT IS NOT NULL AND CLOSED_DT < add_months(as_of,-6) THEN 'CLOSED BEFORE 6 MONTHS'
              WHEN ((CLOSED_DT IS NULL OR CLOSED_DT>as_of) AND DAS IN ('S04','S05')) THEN 'ACTIVE'
               WHEN DAS IN ('S06','S07') THEN 'CLOSED'
          ELSE 'NA'
          END LOAN_ACCOUNT_STATUS,
          CASE WHEN (DPD=0 OR DPD IS NULL) AND NVL(amt_due,0) > (0.01 * NVL(DISB_AMT,0)) THEN 'Not Stipulated(Bad)'
               WHEN DPD BETWEEN 1 AND 30     THEN '1_30'
               WHEN DPD BETWEEN 31 AND 60    THEN '31_60'
               WHEN DPD BETWEEN 61 AND 90    THEN '61_90'
               WHEN DPD BETWEEN 91 AND 120   THEN '91_120'
               WHEN DPD BETWEEN 121 AND 150  THEN '121_150'
               WHEN DPD BETWEEN 151 AND 180  THEN '151_180'
               WHEN DPD BETWEEN 181 AND 210  THEN '181_210'
               WHEN DPD BETWEEN 211 AND 240  THEN '211_240'
               WHEN DPD BETWEEN 241 AND 270  THEN '241_270'
               WHEN DPD BETWEEN 271 AND 300  THEN '271_300'
               WHEN DPD BETWEEN 301 AND 330  THEN '301_330'
               WHEN DPD BETWEEN 331 AND 360  THEN '331_360'
               WHEN DPD > 360 THEN '>360'
               WHEN (DPD=0 OR DPD IS NULL) AND NVL(amt_due,0) <= (0.01 * NVL(DISB_AMT,0)) THEN 'Not Deliquent(Good)'
               ELSE 'NA'
          END DPD,
          CASE WHEN ((DPD>0 AND DPD IS NOT NULL) OR NVL(amt_due,0) > (0.01 * NVL(DISB_AMT,0))) THEN 1
               WHEN (DPD=0 OR DPD IS NULL) AND NVL(amt_due,0) <= (0.01 * NVL(DISB_AMT,0)) THEN 0
               ELSE NULL
          END OVERDUE_IND,
          DAS,
          CASE WHEN ABS(DISB_AMT) BETWEEN 0 AND 5000 THEN '0_5K'
               WHEN ABS(DISB_AMT) BETWEEN 5001 AND 10000 THEN '5K_10K'
               WHEN ABS(DISB_AMT) BETWEEN 10001 AND 15000 THEN '10K_15K'
               WHEN ABS(DISB_AMT) BETWEEN 15001 AND 20000 THEN '15K_20K'
               WHEN ABS(DISB_AMT) BETWEEN 20001 AND 25000 THEN '20K_25K'
               WHEN ABS(DISB_AMT) BETWEEN 25001 AND 30000 THEN '25K_30K'
               WHEN ABS(DISB_AMT) BETWEEN 30001 AND 35000 THEN '30K_35K'
               WHEN ABS(DISB_AMT) BETWEEN 35001 AND 40000 THEN '35K_40K'
               WHEN ABS(DISB_AMT) BETWEEN 40001 AND 45000 THEN '40K_45K'
               WHEN ABS(DISB_AMT) BETWEEN 45001 AND 50000 THEN '45K_50K'
               WHEN ABS(DISB_AMT) BETWEEN 50001 AND 55000 THEN '50K_55K'
               WHEN ABS(DISB_AMT) BETWEEN 55001 AND 60000 THEN '55K_60K'
               WHEN ABS(DISB_AMT) BETWEEN 60001 AND 65000 THEN '60K_65K'
               WHEN ABS(DISB_AMT) BETWEEN 65001 AND 70000 THEN '65K_70K'
               WHEN ABS(DISB_AMT) BETWEEN 70001 AND 75000 THEN '70K_75K'
               WHEN ABS(DISB_AMT) BETWEEN 75001 AND 80000 THEN '75K_80K'
               WHEN ABS(DISB_AMT) BETWEEN 80001 AND 90000 THEN '80K_90K'
               WHEN ABS(DISB_AMT) BETWEEN 90001 AND 100000 THEN '90K_1LAC'
               WHEN ABS(DISB_AMT) BETWEEN 100001 AND 150000 THEN '1LAC_1.5LAC'
               WHEN ABS(DISB_AMT) BETWEEN 150001 AND 200000 THEN '1.5LAC_2LAC'
               WHEN ABS(DISB_AMT) BETWEEN 200001 AND 250000 THEN '2LAC_2.5LAC'
               WHEN ABS(DISB_AMT) BETWEEN 250001 AND 300000 THEN '2.5LAC_3LAC'
               WHEN ABS(DISB_AMT) BETWEEN 300001 AND 350000 THEN '3LAC_3.5LAC'
               WHEN ABS(DISB_AMT) BETWEEN 350001 AND 400000 THEN '3.5LAC_4LAC'
               WHEN ABS(DISB_AMT) BETWEEN 400001 AND 450000 THEN '4LAC_4.5LAC'
               WHEN ABS(DISB_AMT) BETWEEN 450001 AND 500000 THEN '4.5LAC_5LAC'
               WHEN ABS(DISB_AMT) BETWEEN 500001 AND 700000 THEN '5LAC_7LAC'
               WHEN ABS(DISB_AMT) BETWEEN 700001 AND 1000000 THEN '7LAC_10LAC'
               WHEN ABS(DISB_AMT) > 1000000 THEN 'GRTR_10LAC'
               WHEN ABS(DISB_AMT) IS NULL THEN 'BLANK'
          ELSE 'NA'
          END LOAN_TICKET_SIZE,
          CASE WHEN (LOAN_CYCLE_ID='0' OR LOAN_CYCLE_ID='00' OR LOAN_CYCLE_ID='1' OR LOAN_CYCLE_ID='01'  OR LOAN_CYCLE_ID='001' OR LOAN_CYCLE_ID IS NULL) THEN '01'
               WHEN (LOAN_CYCLE_ID='2' OR LOAN_CYCLE_ID='02' OR LOAN_CYCLE_ID='002') THEN '02'
               WHEN LOAN_CYCLE_ID NOT IN ('0','00','1','01','001','2','02','002')  THEN '>=03'
          END LOAN_CYCLE_ID,
          CASE WHEN (LOAN_CYCLE_ID='0' OR LOAN_CYCLE_ID='00' OR LOAN_CYCLE_ID='1' OR LOAN_CYCLE_ID='01'  OR LOAN_CYCLE_ID='001' OR LOAN_CYCLE_ID IS NULL) THEN '01'
               WHEN (LOAN_CYCLE_ID='2' OR LOAN_CYCLE_ID='02' OR LOAN_CYCLE_ID='002') THEN '02'
               WHEN (LOAN_CYCLE_ID='3' OR LOAN_CYCLE_ID='03' OR LOAN_CYCLE_ID='003') THEN '03'
               WHEN (LOAN_CYCLE_ID='4' OR LOAN_CYCLE_ID='04' OR LOAN_CYCLE_ID='004') THEN '04'
               WHEN (LOAN_CYCLE_ID='5' OR LOAN_CYCLE_ID='05' OR LOAN_CYCLE_ID='005') THEN '05'
               WHEN (LOAN_CYCLE_ID='6' OR LOAN_CYCLE_ID='06' OR LOAN_CYCLE_ID='006') THEN '06'
               WHEN (LOAN_CYCLE_ID='7' OR LOAN_CYCLE_ID='07' OR LOAN_CYCLE_ID='007') THEN '07'
               when (LOAN_CYCLE_ID='8' OR LOAN_CYCLE_ID='08' OR LOAN_CYCLE_ID='008' OR LOAN_CYCLE_ID='9' OR LOAN_CYCLE_ID='09' OR LOAN_CYCLE_ID='009' OR LOAN_CYCLE_ID='10' OR LOAN_CYCLE_ID='010' OR LOAN_CYCLE_ID='0010') THEN '8-10'
               else '>10'
          END LOAN_CYCLE_ID_NEW,
          CASE WHEN (NUM_INSTAL BETWEEN 0 AND 6) OR (NUM_INSTAL IS NULL) THEN '0_6'
               WHEN NUM_INSTAL BETWEEN 7 AND 12 THEN '7_12'
               WHEN NUM_INSTAL BETWEEN 13  AND 24 THEN '13_24'
               WHEN NUM_INSTAL>24 THEN 'GRTR24'
          ELSE 'NA'
          END NUM_INSTAL,
          CASE WHEN (NEW_NUMINSTAL BETWEEN 0 AND 6) OR (NEW_NUMINSTAL IS NULL) THEN '0_6'
               WHEN NEW_NUMINSTAL BETWEEN 7 AND 12 THEN '7_12'
               WHEN NEW_NUMINSTAL BETWEEN 13  AND 24 THEN '13_24'
               WHEN NEW_NUMINSTAL>24 THEN 'GRTR24'
          ELSE 'NA'
          END NEW_NUMINSTAL,
          INSTAL_FREQ,
          CASE WHEN MONTHLY_INCOME BETWEEN 0 AND 5000 THEN '0_5K'
               WHEN MONTHLY_INCOME BETWEEN 5001 AND 10000 THEN '5K_10K'
               WHEN MONTHLY_INCOME BETWEEN 10001 AND 15000 THEN '10K_15K'
               WHEN MONTHLY_INCOME BETWEEN 15001 AND 50000 THEN '15K_50K'
               WHEN MONTHLY_INCOME BETWEEN 50001 AND 100000 THEN '50K_1LACS'
               WHEN MONTHLY_INCOME>100000 THEN 'GRTR_1LACS'
               WHEN MONTHLY_INCOME IS NULL THEN 'BLANK'
               ELSE 'NA'
          END BORROWER_INCOME,

          GENDER,
          CASE WHEN AGE BETWEEN 1 AND 17 THEN '<18'
               WHEN AGE BETWEEN 18 AND 21  THEN '18-21'
               WHEN AGE BETWEEN 22 AND 25  THEN '22-25'
               WHEN AGE BETWEEN 26  AND 30 THEN '26-30'
               WHEN AGE BETWEEN 31  AND 35 THEN '31-35'
               WHEN AGE BETWEEN 36 AND 40 THEN '36-40'
               WHEN AGE BETWEEN 41 AND 45 THEN '41-45'
               WHEN AGE BETWEEN 46 AND 50 THEN '46-50'
               WHEN AGE BETWEEN 51 AND 55 THEN '51-55'
               WHEN AGE BETWEEN 56 AND 60 THEN '56-60'
               WHEN AGE BETWEEN 61 AND 65 THEN '61-65'
               WHEN AGE > 65 THEN  '>65'
          ELSE 'NA'
          END AGE,
          CASE WHEN UPPER(LN_PURPOSE) = 'OTHERS' THEN 'OTHERS'
               WHEN UPPER(LN_PURPOSE) = 'INCOME' THEN 'INCOME'
               WHEN UPPER(LN_PURPOSE) = 'EDUCATION' THEN 'EDU'
               WHEN UPPER(LN_PURPOSE) = 'HOUSING' THEN 'HOUSE'
               WHEN UPPER(LN_PURPOSE) = 'AUTOMOBILE' THEN 'AUTOMOBILE'
               WHEN UPPER(LN_PURPOSE) = 'REAL ESTATE' THEN 'REALESTATE'
               WHEN UPPER(LN_PURPOSE) = 'NA' THEN 'NA'
          ELSE 'INVALID'
          END LN_PURPOSE,
          CASE  WHEN UPPER(OCCUPATION) = 'OTHERS' THEN 'OTHERS'
                WHEN UPPER(OCCUPATION) = 'AGRICULTURE AND ALLIED' THEN 'AGRI'
                WHEN UPPER(OCCUPATION) = 'CONTRACT EMPLOYEE' THEN 'CONTRACT_EMP'
                WHEN UPPER(OCCUPATION) = 'SELF EMPLOYED' THEN 'SELF_EMP'
                WHEN UPPER(OCCUPATION) = 'EMPLOYED' THEN 'EMP'
                WHEN UPPER(OCCUPATION) = 'NULL' THEN 'BLANK'
          ELSE 'NA'
          END OCCUPATION,
          CASE WHEN UPPER(SUPPRESS_INDICATOR)='Y' THEN '1'
               WHEN UPPER(SUPPRESS_INDICATOR)='N' THEN '0'
               WHEN UPPER(SUPPRESS_INDICATOR) IS NULL THEN 'BLANK'
          ELSE 'NA'
          END SUPPRESS_INDICATOR ,
          CASE WHEN MIN_AMOUNT_DUE<=250 THEN '<=250'
               WHEN MIN_AMOUNT_DUE BETWEEN 251 AND 500 THEN '250_500'
               WHEN MIN_AMOUNT_DUE BETWEEN 501 AND 750 THEN '500_750'
               WHEN MIN_AMOUNT_DUE BETWEEN 751 AND 1000 THEN '750_1000'
               WHEN MIN_AMOUNT_DUE BETWEEN 1001 AND 1250 THEN '1000_1250'
               WHEN MIN_AMOUNT_DUE BETWEEN 1251 AND 1500 THEN '1250_1500'
               WHEN MIN_AMOUNT_DUE BETWEEN 1501 AND 2000 THEN '1500_2000'
               WHEN MIN_AMOUNT_DUE BETWEEN 2001 AND 2500 THEN '2001_2500'
               WHEN MIN_AMOUNT_DUE BETWEEN 2501 AND 3000 THEN '2500_3000'
               WHEN MIN_AMOUNT_DUE BETWEEN 3001 AND 4000 THEN '3000_4000'
               WHEN MIN_AMOUNT_DUE BETWEEN 4001 AND 5000 THEN '4001_5000'
               when MIN_AMOUNT_DUE>5000 THEN '>5000'
               when MIN_AMOUNT_DUE IS NULL THEN 'BLANK'
           else 'NA'
          END MIN_AMT_DUE,
          CASE WHEN MIN_AMT_DUE_CAL<=250 THEN '<=250'
               WHEN MIN_AMT_DUE_CAL BETWEEN 251 AND 500 THEN '250_500'
               WHEN MIN_AMT_DUE_CAL BETWEEN 501 AND 750 THEN '500_750'
               WHEN MIN_AMT_DUE_CAL BETWEEN 751 AND 1000 THEN '750_1000'
               WHEN MIN_AMT_DUE_CAL BETWEEN 1001 AND 1250 THEN '1000_1250'
               WHEN MIN_AMT_DUE_CAL BETWEEN 1251 AND 1500 THEN '1250_1500'
               WHEN MIN_AMT_DUE_CAL BETWEEN 1501 AND 2000 THEN '1500_2000'
               WHEN MIN_AMT_DUE_CAL BETWEEN 2001 AND 2500 THEN '2001_2500'
               WHEN MIN_AMT_DUE_CAL BETWEEN 2501 AND 3000 THEN '2500_3000'
               WHEN MIN_AMT_DUE_CAL BETWEEN 3001 AND 4000 THEN '3000_4000'
               WHEN MIN_AMT_DUE_CAL BETWEEN 4001 AND 5000 THEN '4001_5000'
               when MIN_AMT_DUE_CAL>5000 THEN '>5000'
               when MIN_AMT_DUE_CAL IS NULL THEN 'BLANK'
           else 'NA'
          END MIN_AMT_DUE_CALC,
          CASE WHEN MIN_AMOUNT_DUE_BOTH<=250 THEN '<=250'
               WHEN MIN_AMOUNT_DUE_BOTH BETWEEN 251 AND 500 THEN '250_500'
               WHEN MIN_AMOUNT_DUE_BOTH BETWEEN 501 AND 750 THEN '500_750'
               WHEN MIN_AMOUNT_DUE_BOTH BETWEEN 751 AND 1000 THEN '750_1000'
              WHEN MIN_AMOUNT_DUE_BOTH BETWEEN 1001 AND 1250 THEN '1000_1250'
               WHEN MIN_AMOUNT_DUE_BOTH BETWEEN 1251 AND 1500 THEN '1250_1500'
               WHEN MIN_AMOUNT_DUE_BOTH BETWEEN 1501 AND 2000 THEN '1500_2000'
               WHEN MIN_AMOUNT_DUE_BOTH BETWEEN 2001 AND 2500 THEN '2001_2500'
               WHEN MIN_AMOUNT_DUE_BOTH BETWEEN 2501 AND 3000 THEN '2500_3000'
               WHEN MIN_AMOUNT_DUE_BOTH BETWEEN 3001 AND 4000 THEN '3000_4000'
               WHEN MIN_AMOUNT_DUE_BOTH BETWEEN 4001 AND 5000 THEN '4001_5000'
               when MIN_AMOUNT_DUE_BOTH>5000 THEN '>5000'
               when MIN_AMOUNT_DUE_BOTH IS NULL THEN 'BLANK'
           else 'NA'
          END MIN_AMT_DUE_BOTH,
          NVL(COUNT(DISTINCT CENTRE_ID),0) TOTAL_CENTRES,
          NVL(COUNT(DISTINCT GROUP_ID),0) TOTAL_GROUPS,
          NVL(COUNT(DISTINCT ACT_KEY),0) TOTAL_ACCOUNTS,
          NVL(COUNT(DISTINCT CNS_KEY),0) TOTAL_CONSUMERS,
          NVL(SUM(CUR_BAL),0) TOTAL_OUTSTANDING_AMOUNT,
          NVL(SUM(DISB_AMT),0) TOTAL_DISBURSED_AMOUNT,
          NVL(SUM(AMT_DUE),0) TOTAL_OVERDUE_AMOUNT,
          NVL(SUM(CHARG_AMT),0) TOTAL_WRITE_OFF_AMOUNT,
          MIN(DISB_DT) OLDEST_LOAN
     FROM (SELECT *,
                  ROUND(ABS(CAST(REGEXP_REPLACE(DISB_AMT,'[^[:digit:]]','') AS DECIMAL(38,2)))/(CASE WHEN NUM_INSTALLMENT is null OR NUM_INSTALLMENT =0  THEN 1 ELSE NUM_INSTALLMENT END),2) MIN_AMT_DUE_CAL,
                  CASE WHEN MIN_AMOUNT_DUE IS NULL THEN (ROUND(ABS(CAST(REGEXP_REPLACE(DISB_AMT,'[^[:digit:]]','') AS DECIMAL(38,2)))/(CASE WHEN NUM_INSTALLMENT is null OR NUM_INSTALLMENT =0  THEN 1 ELSE NUM_INSTALLMENT END),2)) ELSE MIN_AMOUNT_DUE
                  END MIN_AMOUNT_DUE_BOTH
            FROM hmanalytics.hm_mfi_step2_fact
           ) B
     Left Outer Join ( SELECT DISTINCT ACCOUNT_TYPE FROM hmanalytics.HM_LOAN_ACCOUNT_TYPE_DIM WHERE BUREAU_FLAG='MFI') C
     ON(B.LN_CAT = C.ACCOUNT_TYPE)
      WHERE RW=1
                  GROUP BY  B.SDP_KEY,
                            B.CONTRI_KEY,
                            as_of,
                            DISB_DT,
                            min_disb_dt,
							CASE WHEN C.ACCOUNT_TYPE IS NULL THEN 'NA'
                                  ELSE LN_CAT                           
                            END,
                            CASE WHEN CLOSED_DT IS NOT NULL AND CLOSED_DT BETWEEN add_months(as_of,-6) AND as_of THEN 'CLOSED WITHIN 6 MONTHS'
                                 WHEN CLOSED_DT IS NOT NULL AND CLOSED_DT < add_months(as_of,-6) THEN 'CLOSED BEFORE 6 MONTHS'
                                 WHEN ((CLOSED_DT IS NULL OR CLOSED_DT>as_of) AND DAS IN ('S04','S05')) THEN 'ACTIVE'
                                 WHEN DAS IN ('S06','S07') THEN 'CLOSED'
                            ELSE 'NA'
                            END,
                            CASE WHEN (DPD=0 OR DPD IS NULL) AND NVL(amt_due,0) > (0.01 * NVL(DISB_AMT,0)) THEN 'Not Stipulated(Bad)'
                                 WHEN DPD BETWEEN 1 AND 30     THEN '1_30'
                                 WHEN DPD BETWEEN 31 AND 60    THEN '31_60'
                                 WHEN DPD BETWEEN 61 AND 90    THEN '61_90'
                                 WHEN DPD BETWEEN 91 AND 120   THEN '91_120'
                                 WHEN DPD BETWEEN 121 AND 150  THEN '121_150'
                                 WHEN DPD BETWEEN 151 AND 180  THEN '151_180'
                                 WHEN DPD BETWEEN 181 AND 210  THEN '181_210'
                                 WHEN DPD BETWEEN 211 AND 240  THEN '211_240'
                                 WHEN DPD BETWEEN 241 AND 270  THEN '241_270'
                                 WHEN DPD BETWEEN 271 AND 300  THEN '271_300'
                                 WHEN DPD BETWEEN 301 AND 330  THEN '301_330'
                                 WHEN DPD BETWEEN 331 AND 360  THEN '331_360'
                                 WHEN DPD > 360 THEN '>360'
                                 WHEN (DPD=0 OR DPD IS NULL) AND NVL(amt_due,0) <= (0.01 * NVL(DISB_AMT,0)) THEN 'Not Deliquent(Good)'
                                 ELSE 'NA'
                            END,
                            CASE WHEN ((DPD>0 AND DPD IS NOT NULL) OR NVL(amt_due,0) > (0.01 * NVL(DISB_AMT,0))) THEN 1
                                 WHEN (DPD=0 OR DPD IS NULL) AND NVL(amt_due,0) <= (0.01 * NVL(DISB_AMT,0)) THEN 0
                                 ELSE NULL
                            END,
                            DAS,
                            CASE WHEN ABS(DISB_AMT) BETWEEN 0 AND 5000 THEN '0_5K'
                                 WHEN ABS(DISB_AMT) BETWEEN 5001 AND 10000 THEN '5K_10K'
                                 WHEN ABS(DISB_AMT) BETWEEN 10001 AND 15000 THEN '10K_15K'
                                 WHEN ABS(DISB_AMT) BETWEEN 15001 AND 20000 THEN '15K_20K'
                                 WHEN ABS(DISB_AMT) BETWEEN 20001 AND 25000 THEN '20K_25K'
                                 WHEN ABS(DISB_AMT) BETWEEN 25001 AND 30000 THEN '25K_30K'
                                 WHEN ABS(DISB_AMT) BETWEEN 30001 AND 35000 THEN '30K_35K'
                                 WHEN ABS(DISB_AMT) BETWEEN 35001 AND 40000 THEN '35K_40K'
                                 WHEN ABS(DISB_AMT) BETWEEN 40001 AND 45000 THEN '40K_45K'
                                 WHEN ABS(DISB_AMT) BETWEEN 45001 AND 50000 THEN '45K_50K'
                                 WHEN ABS(DISB_AMT) BETWEEN 50001 AND 55000 THEN '50K_55K'
                                 WHEN ABS(DISB_AMT) BETWEEN 55001 AND 60000 THEN '55K_60K'
                                 WHEN ABS(DISB_AMT) BETWEEN 60001 AND 65000 THEN '60K_65K'
                                 WHEN ABS(DISB_AMT) BETWEEN 65001 AND 70000 THEN '65K_70K'
                                 WHEN ABS(DISB_AMT) BETWEEN 70001 AND 75000 THEN '70K_75K'
                                 WHEN ABS(DISB_AMT) BETWEEN 75001 AND 80000 THEN '75K_80K'
                                 WHEN ABS(DISB_AMT) BETWEEN 80001 AND 90000 THEN '80K_90K'
                                 WHEN ABS(DISB_AMT) BETWEEN 90001 AND 100000 THEN '90K_1LAC'
                                 WHEN ABS(DISB_AMT) BETWEEN 100001 AND 150000 THEN '1LAC_1.5LAC'
                                 WHEN ABS(DISB_AMT) BETWEEN 150001 AND 200000 THEN '1.5LAC_2LAC'
                                 WHEN ABS(DISB_AMT) BETWEEN 200001 AND 250000 THEN '2LAC_2.5LAC'
                                 WHEN ABS(DISB_AMT) BETWEEN 250001 AND 300000 THEN '2.5LAC_3LAC'
                                 WHEN ABS(DISB_AMT) BETWEEN 300001 AND 350000 THEN '3LAC_3.5LAC'
                                 WHEN ABS(DISB_AMT) BETWEEN 350001 AND 400000 THEN '3.5LAC_4LAC'
                                 WHEN ABS(DISB_AMT) BETWEEN 400001 AND 450000 THEN '4LAC_4.5LAC'
                                 WHEN ABS(DISB_AMT) BETWEEN 450001 AND 500000 THEN '4.5LAC_5LAC'
                                 WHEN ABS(DISB_AMT) BETWEEN 500001 AND 700000 THEN '5LAC_7LAC'
                                 WHEN ABS(DISB_AMT) BETWEEN 700001 AND 1000000 THEN '7LAC_10LAC'
                                 WHEN ABS(DISB_AMT) > 1000000 THEN 'GRTR_10LAC'
                                 WHEN ABS(DISB_AMT) IS NULL THEN 'BLANK'
                            ELSE 'NA'
                            END,
                            CASE WHEN (LOAN_CYCLE_ID='0' OR LOAN_CYCLE_ID='00' OR LOAN_CYCLE_ID='1' OR LOAN_CYCLE_ID='01'  OR LOAN_CYCLE_ID='001' OR LOAN_CYCLE_ID IS NULL) THEN '01'
                                 WHEN (LOAN_CYCLE_ID='2' OR LOAN_CYCLE_ID='02' OR LOAN_CYCLE_ID='002') THEN '02'
                                 WHEN LOAN_CYCLE_ID NOT IN ('0','00','1','01','001','2','02','002')  THEN '>=03'
                            END,
                            CASE WHEN (LOAN_CYCLE_ID='0' OR LOAN_CYCLE_ID='00' OR LOAN_CYCLE_ID='1' OR LOAN_CYCLE_ID='01'  OR LOAN_CYCLE_ID='001' OR LOAN_CYCLE_ID IS NULL) THEN '01'
                                 WHEN (LOAN_CYCLE_ID='2' OR LOAN_CYCLE_ID='02' OR LOAN_CYCLE_ID='002') THEN '02'
                                 WHEN (LOAN_CYCLE_ID='3' OR LOAN_CYCLE_ID='03' OR LOAN_CYCLE_ID='003') THEN '03'
                                 WHEN (LOAN_CYCLE_ID='4' OR LOAN_CYCLE_ID='04' OR LOAN_CYCLE_ID='004') THEN '04'
                                 WHEN (LOAN_CYCLE_ID='5' OR LOAN_CYCLE_ID='05' OR LOAN_CYCLE_ID='005') THEN '05'
                                 WHEN (LOAN_CYCLE_ID='6' OR LOAN_CYCLE_ID='06' OR LOAN_CYCLE_ID='006') THEN '06'
                                 WHEN (LOAN_CYCLE_ID='7' OR LOAN_CYCLE_ID='07' OR LOAN_CYCLE_ID='007') THEN '07'
                                 when (LOAN_CYCLE_ID='8' OR LOAN_CYCLE_ID='08' OR LOAN_CYCLE_ID='008' OR LOAN_CYCLE_ID='9' OR LOAN_CYCLE_ID='09' OR LOAN_CYCLE_ID='009' OR LOAN_CYCLE_ID='10' OR LOAN_CYCLE_ID='010' OR LOAN_CYCLE_ID='0010') THEN '8-10'
                                 else '>10'
                            END,
                            CASE WHEN (NUM_INSTAL BETWEEN 0 AND 6) OR (NUM_INSTAL IS NULL) THEN '0_6'
                                 WHEN NUM_INSTAL BETWEEN 7 AND 12 THEN '7_12'
                                 WHEN NUM_INSTAL BETWEEN 13  AND 24 THEN '13_24'
                                 WHEN NUM_INSTAL>24 THEN 'GRTR24'
                            ELSE 'NA'
                            END,
                            CASE WHEN (NEW_NUMINSTAL BETWEEN 0 AND 6) OR (NEW_NUMINSTAL IS NULL) THEN '0_6'
                                 WHEN NEW_NUMINSTAL BETWEEN 7 AND 12 THEN '7_12'
                                 WHEN NEW_NUMINSTAL BETWEEN 13  AND 24 THEN '13_24'
                                 WHEN NEW_NUMINSTAL>24 THEN 'GRTR24'
                            ELSE 'NA'
                            END,
                            INSTAL_FREQ,
                            CASE WHEN MONTHLY_INCOME BETWEEN 0 AND 5000 THEN '0_5K'
                                 WHEN MONTHLY_INCOME BETWEEN 5001 AND 10000 THEN '5K_10K'
                                 WHEN MONTHLY_INCOME BETWEEN 10001 AND 15000 THEN '10K_15K'
                                 WHEN MONTHLY_INCOME BETWEEN 15001 AND 50000 THEN '15K_50K'
                                 WHEN MONTHLY_INCOME BETWEEN 50001 AND 100000 THEN '50K_1LACS'
                                 WHEN MONTHLY_INCOME>100000 THEN 'GRTR_1LACS'
                                 WHEN MONTHLY_INCOME IS NULL THEN 'BLANK'
                                 ELSE 'NA'
                            END,

                            GENDER,
                            CASE WHEN AGE BETWEEN 1 AND 17 THEN '<18'
                                 WHEN AGE BETWEEN 18 AND 21  THEN '18-21'
                                 WHEN AGE BETWEEN 22 AND 25  THEN '22-25'
                                 WHEN AGE BETWEEN 26  AND 30 THEN '26-30'
                                 WHEN AGE BETWEEN 31  AND 35 THEN '31-35'
                                 WHEN AGE BETWEEN 36 AND 40 THEN '36-40'
                                 WHEN AGE BETWEEN 41 AND 45 THEN '41-45'
                                 WHEN AGE BETWEEN 46 AND 50 THEN '46-50'
                                 WHEN AGE BETWEEN 51 AND 55 THEN '51-55'
                                 WHEN AGE BETWEEN 56 AND 60 THEN '56-60'
                                 WHEN AGE BETWEEN 61 AND 65 THEN '61-65'
                                 WHEN AGE > 65 THEN  '>65'
                            ELSE 'NA'
                            END,
                            CASE WHEN UPPER(LN_PURPOSE) = 'OTHERS' THEN 'OTHERS'
                                 WHEN UPPER(LN_PURPOSE) = 'INCOME' THEN 'INCOME'
                                 WHEN UPPER(LN_PURPOSE) = 'EDUCATION' THEN 'EDU'
                                 WHEN UPPER(LN_PURPOSE) = 'HOUSING' THEN 'HOUSE'
                                 WHEN UPPER(LN_PURPOSE) = 'AUTOMOBILE' THEN 'AUTOMOBILE'
                                 WHEN UPPER(LN_PURPOSE) = 'REAL ESTATE' THEN 'REALESTATE'
                                 WHEN UPPER(LN_PURPOSE) = 'NA' THEN 'NA'
                            ELSE 'INVALID'
                            END,
                            CASE  WHEN UPPER(OCCUPATION) = 'OTHERS' THEN 'OTHERS'
                                  WHEN UPPER(OCCUPATION) = 'AGRICULTURE AND ALLIED' THEN 'AGRI'
                                  WHEN UPPER(OCCUPATION) = 'CONTRACT EMPLOYEE' THEN 'CONTRACT_EMP'
                                  WHEN UPPER(OCCUPATION) = 'SELF EMPLOYED' THEN 'SELF_EMP'
                                  WHEN UPPER(OCCUPATION) = 'EMPLOYED' THEN 'EMP'
                                  WHEN UPPER(OCCUPATION) = 'NULL' THEN 'BLANK'
                            ELSE 'NA'
                            END,
                            CASE WHEN UPPER(SUPPRESS_INDICATOR)='Y' THEN '1'
                                 WHEN UPPER(SUPPRESS_INDICATOR)='N' THEN '0'
                                 WHEN UPPER(SUPPRESS_INDICATOR) IS NULL THEN 'BLANK'
                            ELSE 'NA'
                            END,
                            CASE WHEN MIN_AMOUNT_DUE<=250 THEN '<=250'
                                 WHEN MIN_AMOUNT_DUE BETWEEN 251 AND 500 THEN '250_500'
                                 WHEN MIN_AMOUNT_DUE BETWEEN 501 AND 750 THEN '500_750'
                                 WHEN MIN_AMOUNT_DUE BETWEEN 751 AND 1000 THEN '750_1000'
                                 WHEN MIN_AMOUNT_DUE BETWEEN 1001 AND 1250 THEN '1000_1250'
                                 WHEN MIN_AMOUNT_DUE BETWEEN 1251 AND 1500 THEN '1250_1500'
                                 WHEN MIN_AMOUNT_DUE BETWEEN 1501 AND 2000 THEN '1500_2000'
                                 WHEN MIN_AMOUNT_DUE BETWEEN 2001 AND 2500 THEN '2001_2500'
                                 WHEN MIN_AMOUNT_DUE BETWEEN 2501 AND 3000 THEN '2500_3000'
                                 WHEN MIN_AMOUNT_DUE BETWEEN 3001 AND 4000 THEN '3000_4000'
                                 WHEN MIN_AMOUNT_DUE BETWEEN 4001 AND 5000 THEN '4001_5000'
                                 when MIN_AMOUNT_DUE>5000 THEN '>5000'
                                 when MIN_AMOUNT_DUE IS NULL THEN 'BLANK'
                             else 'NA'
                            END,
                        CASE WHEN MIN_AMT_DUE_CAL<=250 THEN '<=250'
                             WHEN MIN_AMT_DUE_CAL BETWEEN 251 AND 500 THEN '250_500'
                             WHEN MIN_AMT_DUE_CAL BETWEEN 501 AND 750 THEN '500_750'
                             WHEN MIN_AMT_DUE_CAL BETWEEN 751 AND 1000 THEN '750_1000'
                             WHEN MIN_AMT_DUE_CAL BETWEEN 1001 AND 1250 THEN '1000_1250'
                             WHEN MIN_AMT_DUE_CAL BETWEEN 1251 AND 1500 THEN '1250_1500'
                             WHEN MIN_AMT_DUE_CAL BETWEEN 1501 AND 2000 THEN '1500_2000'
                             WHEN MIN_AMT_DUE_CAL BETWEEN 2001 AND 2500 THEN '2001_2500'
                             WHEN MIN_AMT_DUE_CAL BETWEEN 2501 AND 3000 THEN '2500_3000'
                             WHEN MIN_AMT_DUE_CAL BETWEEN 3001 AND 4000 THEN '3000_4000'
                             WHEN MIN_AMT_DUE_CAL BETWEEN 4001 AND 5000 THEN '4001_5000'
                             when MIN_AMT_DUE_CAL>5000 THEN '>5000'
                             when MIN_AMT_DUE_CAL IS NULL THEN 'BLANK'
                         else 'NA'
                       END,
                        CASE WHEN MIN_AMOUNT_DUE_BOTH<=250 THEN '<=250'
                             WHEN MIN_AMOUNT_DUE_BOTH BETWEEN 251 AND 500 THEN '250_500'
                             WHEN MIN_AMOUNT_DUE_BOTH BETWEEN 501 AND 750 THEN '500_750'
                             WHEN MIN_AMOUNT_DUE_BOTH BETWEEN 751 AND 1000 THEN '750_1000'
                             WHEN MIN_AMOUNT_DUE_BOTH BETWEEN 1001 AND 1250 THEN '1000_1250'
                             WHEN MIN_AMOUNT_DUE_BOTH BETWEEN 1251 AND 1500 THEN '1250_1500'
                             WHEN MIN_AMOUNT_DUE_BOTH BETWEEN 1501 AND 2000 THEN '1500_2000'
                             WHEN MIN_AMOUNT_DUE_BOTH BETWEEN 2001 AND 2500 THEN '2001_2500'
                             WHEN MIN_AMOUNT_DUE_BOTH BETWEEN 2501 AND 3000 THEN '2500_3000'
                             WHEN MIN_AMOUNT_DUE_BOTH BETWEEN 3001 AND 4000 THEN '3000_4000'
                             WHEN MIN_AMOUNT_DUE_BOTH BETWEEN 4001 AND 5000 THEN '4001_5000'
                             when MIN_AMOUNT_DUE_BOTH>5000 THEN '>5000'
                             when MIN_AMOUNT_DUE_BOTH IS NULL THEN 'BLANK'
                         else 'NA'
                        END
						""")
stp3Df.write.mode("overwrite").saveAsTable("hmanalytics.hm_mfi_step3_fact")


 /*Step 4 Final Fact Creation*/


val fnlDF= spark.sql("""
SELECT SDP_KEY,
 CONTRI_KEY,
 A.LOAD_YYYYMM,
 DISB_YYYYMM,
 min_disb_YYYYMM,
 ACT.LOAN_ACCOUNT_TYPE_KEY,
 LOAN_ACCOUNT_STATUS_KEY,
 LOAN_DPD_BUCKET_KEY,
 OVERDUE_IND,
 DAS_ID,
 LOAN_TICKET_SIZE_KEY,
 LN_CYCLE.LOAN_CYCLE_KEY,
 LN_CYCLE_V1.LOAN_CYCLE_KEY LOAN_CYCLE_NEW_KEY,
 NUM_INSTAL.LOAN_NUM_INSTAL_BUCKET_KEY,
 NUM_INSTAL_NEW.LOAN_NUM_INSTAL_BUCKET_KEY LOAN_NUM_INSTAL_BUCKET_NEW_KEY,
 LOAN_INSTALL_FREQ_KEY,
 BOR_INCOME.BORROWER_INCOME_LEVEL_KEY BORROWER_INCOME,
 GENDER,
 BOR_AGE_V1.BORROWER_AGE_BUCKET_KEY AGE,
 BOR_OCCUPATION.BORROWER_OCCUPATION_KEY OCCUPATION,
 BORROWER_LOAN_PURPOSE_KEY,
 SUPRESS_IND_KEY ,
 MIN_AMT_DUE.borrower_minamtdue_key,
 MIN_AMT_DUE_CAL.borrower_minamtdue_key borrower_minamtdue_key_CAL,
 MIN_AMT_DUE_BOTH.borrower_minamtdue_key borrower_minamtdue_key_BOTH,
 TOTAL_CENTRES,
 TOTAL_GROUPS,
 TOTAL_ACCOUNTS,
 TOTAL_CONSUMERS,
 TOTAL_OUTSTANDING_AMOUNT,
 TOTAL_DISBURSED_AMOUNT,
 TOTAL_OVERDUE_AMOUNT,
 TOTAL_WRITE_OFF_AMOUNT,
 OLDEST_LOAN
            FROM hmanalytics.hm_mfi_step3_fact  A
 LEFT OUTER JOIN hmanalytics.HM_LOAN_ACCOUNT_TYPE_DIM ACT
              ON (A.LN_CAT =ACT.ACCOUNT_TYPE)
 LEFT OUTER JOIN hmanalytics.HM_LOAN_ACCOUNT_STATUS_DIM LN_ACT_STATUS
              ON (A.LOAN_ACCOUNT_STATUS=LN_ACT_STATUS.ACCOUNT_STATUS)
 LEFT OUTER JOIN hmanalytics.HM_LOAN_DPD_BUCKET_DIM_NEW LN_DPD
              ON (A.DPD=LN_DPD.DPD)
 LEFT OUTER JOIN hmanalytics.HM_DAS_DIM DAS
              ON (A.DAS=DAS.DAS)
 LEFT OUTER JOIN hmanalytics.HM_LOAN_TICKET_SIZE_DIM_NEW2  LN_TICKET
              ON (A.LOAN_TICKET_SIZE=LN_TICKET.TICKET_SIZE)
 LEFT OUTER JOIN hmanalytics.HM_LOAN_CYCLE_DIM LN_CYCLE
              ON (A.LOAN_CYCLE_ID=LN_CYCLE.LOAN_CYCLE)
 LEFT OUTER JOIN hmanalytics.HM_LOAN_CYCLE_DIM_V1 LN_CYCLE_V1
              ON (A.LOAN_CYCLE_ID_NEW=LN_CYCLE_V1.LOAN_CYCLE)
 LEFT OUTER JOIN hmanalytics.HM_LOAN_NUM_INSTAL_BUCKET_DIM NUM_INSTAL
              ON (A.NUM_INSTAL=NUM_INSTAL.NUM_INSTAL)
 LEFT OUTER JOIN hmanalytics.HM_LOAN_NUM_INSTAL_BUCKET_DIM NUM_INSTAL_NEW
              ON (A.NEW_NUMINSTAL=NUM_INSTAL_NEW.NUM_INSTAL)
 LEFT OUTER JOIN hmanalytics.HM_LOAN_INSTALL_FREQ_DIM INSTAL_FREQ
              ON (A.INSTAL_FREQ=INSTAL_FREQ.INSTAL_BUCKET)
 LEFT OUTER JOIN hmanalytics.HM_BORROWER_INCOME_LEVEL_DIM BOR_INCOME
              ON (A.BORROWER_INCOME=BOR_INCOME.INCOME)
LEFT OUTER JOIN hmanalytics.HM_BORROWER_AGE_BUCKET_DIM_V1 BOR_AGE_V1
              ON (A.AGE=BOR_AGE_V1.AGE)
 LEFT OUTER JOIN hmanalytics.HM_BORROWER_OCCUPATION_DIM BOR_OCCUPATION
              ON (A.OCCUPATION=BOR_OCCUPATION.OCCUPATION)
 LEFT OUTER JOIN hmanalytics.HM_BORROWER_LOAN_PURPOSE_DIM LN_PURPOSE
              ON (A.LN_PURPOSE=LN_PURPOSE.LOAN_PURPOSE)
 LEFT OUTER JOIN hmanalytics.HM_SUPPRESS_IND_DIM SUPPRESS_IND
              ON (A.SUPPRESS_INDICATOR=SUPPRESS_IND.SUPRESS_IND_DESC)
 LEFT OUTER JOIN hmanalytics.HM_BORROWER_MINAMT_DUE_DIM MIN_AMT_DUE
              ON (A.MIN_AMT_DUE=MIN_AMT_DUE.MINAMTDUE_BUCKET)
 LEFT OUTER JOIN hmanalytics.HM_BORROWER_MINAMT_DUE_DIM MIN_AMT_DUE_CAL
              ON (A.MIN_AMT_DUE_CALC=MIN_AMT_DUE_CAL.MINAMTDUE_BUCKET)
 LEFT OUTER JOIN hmanalytics.HM_BORROWER_MINAMT_DUE_DIM MIN_AMT_DUE_BOTH
              ON (A.MIN_AMT_DUE_BOTH=MIN_AMT_DUE_BOTH.MINAMTDUE_BUCKET)
           WHERE LN_TICKET.BUREAU_FLAG='MFI'
             AND INSTAL_FREQ.BUREAU_FLAG='CNS/MFI'
             AND BOR_INCOME.BUREAU='MFI'
             AND ACT.BUREAU_FLAG='MFI'
             AND LN_TICKET.ACTIVE=1
             AND LN_ACT_STATUS.ACTIVE=1
             AND INSTAL_FREQ.ACTIVE=1
             AND LN_DPD.ACTIVE=1
             AND LN_PURPOSE.ACTIVE=1
             AND NUM_INSTAL.ACTIVE=1
             AND LN_CYCLE.ACTIVE=1
             AND BOR_INCOME.ACTIVE=1
             AND BOR_OCCUPATION.ACTIVE=1
             AND SUPPRESS_IND.ACTIVE=1
""")


							 
fnlDF.write.mode("append").saveAsTable("hmanalytics.HM_MFI_ACT_FACT_TBL")

mfiBfil.DataAggregation(spark,argument)


  
}