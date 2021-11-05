package com.crif.highmark.CrifSemantic.Aggregation
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
import org.apache.hadoop.fs._

object mfiBfil {
  def DataAggregation(spark: SparkSession,argument : Array[String]): Unit = {
 val fs = FileSystem.get(spark.sparkContext.hadoopConfiguration)
  	
 val argument = spark.sparkContext.getConf.get("spark.driver.args").split("\\s+")
 	
val mon_rn=argument(0)
val end_dt=argument(1)
val load_dt=argument(2)

/* Customer Level Data File */


/* 1. All cluster portfolio*/

 val onedf = spark.sql(""" SELECT  A.*,C.KEY SDP_KEY,
          C.REV_STD_STATE_CODE,C.REVISED_DIST
     FROM hmanalytics.hm_mfi_step2_fact A
     JOIN (SELECT DISTINCT KEY,STD_STATE_CODE,DISTRICT,STD_PIN_CODE,REV_STD_STATE_CODE,REVISED_DIST  FROM hmanalytics.HM_SDP_DIM2 WHERE ACTIVE=1) C
      ON (A.sdp_key = C.key)
    WHERE (((CLOSED_DT IS NULL OR CLOSED_DT> as_of) AND DAS IN ('S04','S05')) OR DAS IN ('S06'))
      AND NVL(SUPPRESS_INDICATOR,'N')<>'Y'
   AND rw=1
 """)    
    
onedf.createOrReplaceTempView("HM_ACTIVE_DATA_BFIL")


 val twodf = spark.sql("""       
                      SELECT 
                             C.REV_STD_STATE_CODE STD_STATE_CODE,
                             C.REVISED_DIST DISTRICT,
                             CLST_ID,
                             COUNT(DISTINCT CASE WHEN ((CLOSED_DT IS NULL OR CLOSED_DT>as_of) AND DAS IN ('S04','S05','S11','S12','S16','S18','S20')) THEN ACT_KEY ELSE NULL END) ACTIVE_ACCOUNTS,
                             COUNT(DISTINCT MFI_ID) OVERALL_MFI_ASSOCIATION,
                             COUNT(DISTINCT CASE WHEN ((CLOSED_DT IS NULL OR CLOSED_DT>as_of) AND DAS IN ('S04','S05','S11','S12','S16','S18','S20')) THEN MFI_ID ELSE NULL END) ACTIVE_MFI_ASSOCIATION,
                             SUM(CUR_BAL) TOTAL_OUTSTANDING_AMOUNT,
                             SUM(CASE WHEN ((CLOSED_DT IS NULL OR CLOSED_DT>as_of) AND DAS IN ('S04','S05','S11','S12','S16','S18','S20')) THEN CUR_BAL ELSE 0 END) PORTFOLIO_OUTSTANDING,
                             MAX(DPD) MAX_DPD,
                             MAX(CASE WHEN ((CLOSED_DT IS NULL OR CLOSED_DT>as_of) AND DAS IN ('S04','S05','S11','S12','S16','S18','S20')) THEN DPD ELSE NULL END) MAX_DPD_ACTIV,
                             MIN(A.DISB_DT) MIN_DISB,
                             MIN(CASE WHEN ((CLOSED_DT IS NULL OR CLOSED_DT>as_of) AND DAS IN ('S04','S05','S11','S12','S16','S18','S20')) THEN A.DISB_DT ELSE NULL END) MIN_DISB_ACTIV
                        FROM hmanalytics.hm_mfi_step2_fact A
                        JOIN (SELECT DISTINCT KEY,STD_STATE_CODE,DISTRICT,STD_PIN_CODE,REV_STD_STATE_CODE,REVISED_DIST  FROM hmanalytics.HM_SDP_DIM2 WHERE ACTIVE=1) C
                          ON (A.sdp_key = C.key)
                      WHERE NVL(A.SUPPRESS_INDICATOR,'N')<>'Y'
                         AND C.REVISED_DIST IS NOT NULL
                         AND C.REV_STD_STATE_CODE IS NOT NULL 
                         AND C.STD_PIN_CODE IS NOT NULL 
        AND rw=1
                       GROUP BY C.REV_STD_STATE_CODE,
                                C.REVISED_DIST,
                                CLST_ID    
  """)                              
                                

twodf.createOrReplaceTempView("HM_MFI_ASSOC_DIST_BFIL")



   val threedf =spark.sql("""
   SELECT A.*,C.MAX_DPD_ACTIV,PORTFOLIO_OUTSTANDING,ACTIVE_MFI_ASSOCIATION,
                               MIN_DISB,MIN_DISB_ACTIV,
                                (ROUND(MONTHS_BETWEEN(LAST_DAY(as_of),LAST_DAY(MIN_DISB)),0)+1)  VINTAGE_MOB
                        FROM HM_ACTIVE_DATA_BFIL A,
                               HM_MFI_ASSOC_DIST_BFIL C
                         WHERE A.CLST_ID=C.CLST_ID
                           AND A.REV_STD_STATE_CODE=C.STD_STATE_CODE
                           and A.REVISED_DIST=C.DISTRICT
""")						   
threedf.createOrReplaceTempView("HM_ACTIVE_BFIL_1")
                           
val fourdf =spark.sql(""" 
SELECT CAST(date_format(as_of,'yyyyMM') as STRING) as LOAD_YYYYMM,
                                     'All' product_ind,
                                     A.REV_STD_STATE_CODE STD_STATE_CODE,
                                     A.REVISED_DIST DISTRICT,
                                     concat_ws('|',A.REV_STD_STATE_CODE,A.REVISED_DIST,'DIST_CREDITEXP_MAXDPD_MFI_ASS_ACTIV_CLUSTER') COMBO,
                                     CASE WHEN VINTAGE_MOB>=0 
                                     THEN 
                                         CASE WHEN VINTAGE_MOB BETWEEN 0 AND 3 THEN '0-3'
                                             WHEN VINTAGE_MOB BETWEEN 4 AND 6 THEN '3-6'
                                             WHEN VINTAGE_MOB BETWEEN 7 AND 12 THEN '6-12'
                                             WHEN VINTAGE_MOB BETWEEN 13 AND 24 THEN '12-24'
                                             WHEN VINTAGE_MOB BETWEEN 25 AND 36 THEN '24-36'
                                             WHEN VINTAGE_MOB BETWEEN 37 AND 48 THEN '36-48'
                                             WHEN VINTAGE_MOB BETWEEN 49 AND 60 THEN '48-60'
                                             WHEN VINTAGE_MOB BETWEEN 61 AND 120 THEN '60-120'
                                             WHEN VINTAGE_MOB >120 THEN '>120'
                                          ELSE NULL
                                          END
                                      ELSE NULL 
                                      END VINTAGE,
                                  CASE WHEN ABS(PORTFOLIO_OUTSTANDING) BETWEEN 0  AND 10000 THEN '0_10K'
                                         WHEN ABS(PORTFOLIO_OUTSTANDING) BETWEEN 10001 AND 15000 THEN '10K_15K'
                                          WHEN ABS(PORTFOLIO_OUTSTANDING) BETWEEN 15001 AND 20000 THEN '15K_20K'
                                          WHEN ABS(PORTFOLIO_OUTSTANDING) BETWEEN 20001 AND 25000 THEN '20K_25K'
                                          WHEN ABS(PORTFOLIO_OUTSTANDING) BETWEEN 25001 AND 30000 THEN '25K_30K'
                                          WHEN ABS(PORTFOLIO_OUTSTANDING) BETWEEN 30001 AND 40000 THEN '30K_40K'
                                          WHEN ABS(PORTFOLIO_OUTSTANDING) BETWEEN 40001 AND 60000 THEN '40K_60K'
                                          WHEN ABS(PORTFOLIO_OUTSTANDING) BETWEEN 60001 AND 80000 THEN '60K_80K'
                                          WHEN ABS(PORTFOLIO_OUTSTANDING) BETWEEN 80001 AND 100000 THEN '80K_100K'
                                          WHEN ABS(PORTFOLIO_OUTSTANDING) BETWEEN 100001 AND 150000 THEN '100K_150K'
                                          WHEN ABS(PORTFOLIO_OUTSTANDING) > 150000 THEN '>150K'  
                                          WHEN ABS(PORTFOLIO_OUTSTANDING) IS NULL THEN 'BLANK'
                                     ELSE 'NA'
                                     END OUTSTANDING_AMOUNT,
                                CASE  WHEN DAS IN ('S04','S05') AND (MAX_DPD_ACTIV=0 OR MAX_DPD_ACTIV IS NULL) AND NVL(ABS(AMT_DUE),0) <= (0.01 * NVL(ABS(DISB_AMT),0)) THEN 'Not Deliquent(Good)'
                                           WHEN DAS IN ('S04','S05') AND (MAX_DPD_ACTIV BETWEEN 1 AND 30) THEN '1_30'
                                           WHEN DAS IN ('S04','S05') AND (MAX_DPD_ACTIV BETWEEN 31 AND 60) THEN '31_60'
                                           WHEN DAS IN ('S04','S05') AND (MAX_DPD_ACTIV BETWEEN 61 AND 90) THEN '61_90'
                                           WHEN DAS IN ('S04','S05') AND (MAX_DPD_ACTIV BETWEEN 91 AND 180) THEN '91_180'
                                           WHEN DAS IN ('S04','S05') AND (MAX_DPD_ACTIV > 180) THEN '>180'
                                           WHEN DAS IN ('S06') THEN 'WRITEOFF'
                                           WHEN DAS IN ('S07') THEN 'CLOSED'
                                           WHEN (MAX_DPD_ACTIV=0 OR MAX_DPD_ACTIV IS NULL) AND NVL(ABS(AMT_DUE),0) > (0.01 * NVL(ABS(DISB_AMT),0)) THEN 'Not Stipulated(Bad)'
                                           ELSE 'NA'
                                     END DPD,
                                     CASE WHEN ACTIVE_MFI_ASSOCIATION=1 THEN '1'
                                          WHEN ACTIVE_MFI_ASSOCIATION=2  THEN '2'
                                          WHEN ACTIVE_MFI_ASSOCIATION=3 THEN '3'
                                          WHEN ACTIVE_MFI_ASSOCIATION=4 THEN '4'
                                          WHEN ACTIVE_MFI_ASSOCIATION>=5 THEN '>=5'
                                     ELSE 'NA'
                                     END ACTIVE_MFI_ASSOCIATION,
                                  CASE WHEN disb_dt < '2017-04-30' THEN 'Pre-Apr 2017'
                                       WHEN disb_dt >='2017-04-30' THEN 'Since Apr 2017'
                                  ELSE 'NA'
                                  END DISBURSED_PERIOD,   
                                  COUNT(DISTINCT a.CLST_ID) UNIQUE_CLST,
                                  COUNT(DISTINCT(CASE WHEN ((CLOSED_DT IS NULL OR CLOSED_DT>as_of) AND DAS IN ('S04','S05')) THEN A.CLST_ID ELSE NULL END)) ACTIVE_CLST,
                                  COUNT(DISTINCT ACT_KEY) TOTAL_LOANS,
                                  COUNT(DISTINCT(CASE WHEN ((CLOSED_DT IS NULL OR CLOSED_DT>as_of) AND DAS IN ('S04','S05')) THEN ACT_KEY ELSE NULL END)) ACTIVE_LOANS,
                                  SUM(ABS(DISB_AMT)) TOTAL_DISB_AMOUNT,
                                  SUM(CASE WHEN ((CLOSED_DT IS NULL OR CLOSED_DT>as_of) AND DAS IN ('S04','S05')) THEN ABS(DISB_AMT) ELSE 0 END) ACTIVE_DISB_AMOUNT,
                                  SUM(ABS(AMT_DUE)) TOTAL_OVERDUE_AMOUNT,
                                  SUM(ABS(CUR_BAL)) TOTAL_OUTSTANDING_AMOUNT,
                                  SUM(CASE WHEN ((CLOSED_DT IS NULL OR CLOSED_DT>as_of) AND DAS IN ('S04','S05')) THEN ABS(CUR_BAL) ELSE 0 END) ACTIVE_OUTSTANDING_AMOUNT
                              FROM HM_ACTIVE_BFIL_1 A
                            GROUP BY as_of,
                                      A.REV_STD_STATE_CODE,
                                      A.REVISED_DIST,
                                      CASE WHEN VINTAGE_MOB>=0 
                                      THEN
                                            CASE WHEN VINTAGE_MOB BETWEEN 0 AND 3 THEN '0-3'
                                               WHEN VINTAGE_MOB BETWEEN 4 AND 6 THEN '3-6'
                                               WHEN VINTAGE_MOB BETWEEN 7 AND 12 THEN '6-12'
                                               WHEN VINTAGE_MOB BETWEEN 13 AND 24 THEN '12-24'
                                               WHEN VINTAGE_MOB BETWEEN 25 AND 36 THEN '24-36'
                                               WHEN VINTAGE_MOB BETWEEN 37 AND 48 THEN '36-48'
                                               WHEN VINTAGE_MOB BETWEEN 49 AND 60 THEN '48-60'
                                               WHEN VINTAGE_MOB BETWEEN 61 AND 120 THEN '60-120'
                                               WHEN VINTAGE_MOB >120 THEN '>120'
                                            ELSE NULL
                                            END
                                      ELSE NULL
                                      END,
                                     CASE WHEN ABS(PORTFOLIO_OUTSTANDING) BETWEEN 0  AND 10000 THEN '0_10K'
                                         WHEN ABS(PORTFOLIO_OUTSTANDING) BETWEEN 10001 AND 15000 THEN '10K_15K'
                                          WHEN ABS(PORTFOLIO_OUTSTANDING) BETWEEN 15001 AND 20000 THEN '15K_20K'
                                          WHEN ABS(PORTFOLIO_OUTSTANDING) BETWEEN 20001 AND 25000 THEN '20K_25K'
                                          WHEN ABS(PORTFOLIO_OUTSTANDING) BETWEEN 25001 AND 30000 THEN '25K_30K'
                                          WHEN ABS(PORTFOLIO_OUTSTANDING) BETWEEN 30001 AND 40000 THEN '30K_40K'
                                          WHEN ABS(PORTFOLIO_OUTSTANDING) BETWEEN 40001 AND 60000 THEN '40K_60K'
                                          WHEN ABS(PORTFOLIO_OUTSTANDING) BETWEEN 60001 AND 80000 THEN '60K_80K'
                                          WHEN ABS(PORTFOLIO_OUTSTANDING) BETWEEN 80001 AND 100000 THEN '80K_100K'
                                          WHEN ABS(PORTFOLIO_OUTSTANDING) BETWEEN 100001 AND 150000 THEN '100K_150K'
                                          WHEN ABS(PORTFOLIO_OUTSTANDING) > 150000 THEN '>150K'  
                                          WHEN ABS(PORTFOLIO_OUTSTANDING) IS NULL THEN 'BLANK'
                                     ELSE 'NA'
                                     END,
                                        CASE  WHEN DAS IN ('S04','S05') AND (MAX_DPD_ACTIV=0 OR MAX_DPD_ACTIV IS NULL) AND NVL(ABS(AMT_DUE),0) <= (0.01 * NVL(ABS(DISB_AMT),0)) THEN 'Not Deliquent(Good)'
                                           WHEN DAS IN ('S04','S05') AND (MAX_DPD_ACTIV BETWEEN 1 AND 30) THEN '1_30'
                                           WHEN DAS IN ('S04','S05') AND (MAX_DPD_ACTIV BETWEEN 31 AND 60) THEN '31_60'
                                           WHEN DAS IN ('S04','S05') AND (MAX_DPD_ACTIV BETWEEN 61 AND 90) THEN '61_90'
                                           WHEN DAS IN ('S04','S05') AND (MAX_DPD_ACTIV BETWEEN 91 AND 180) THEN '91_180'
                                           WHEN DAS IN ('S04','S05') AND (MAX_DPD_ACTIV > 180) THEN '>180'
                                           WHEN DAS IN ('S06') THEN 'WRITEOFF'
                                           WHEN DAS IN ('S07') THEN 'CLOSED'
                                           WHEN (MAX_DPD_ACTIV=0 OR MAX_DPD_ACTIV IS NULL) AND NVL(ABS(AMT_DUE),0) > (0.01 * NVL(ABS(DISB_AMT),0)) THEN 'Not Stipulated(Bad)'
                                           ELSE 'NA'
                                     END,
                                       CASE WHEN ACTIVE_MFI_ASSOCIATION=1 THEN '1'
                                            WHEN ACTIVE_MFI_ASSOCIATION=2  THEN '2'
                                            WHEN ACTIVE_MFI_ASSOCIATION=3 THEN '3'
                                            WHEN ACTIVE_MFI_ASSOCIATION=4 THEN '4'
                                            WHEN ACTIVE_MFI_ASSOCIATION>=5 THEN '>=5'
                                       ELSE 'NA'
                                       END,
                                       CASE WHEN disb_dt<'2017-04-30' THEN 'Pre-Apr 2017'
                                       WHEN disb_dt>='2017-04-30' THEN 'Since Apr 2017'
                                       ELSE 'NA'
                                       END
""")									   
fourdf.createOrReplaceTempView("HM_DIST_BFIL_DATA")

fourdf.write.mode("overwrite").saveAsTable("hmanalytics.HM_DIST_BFIL_DATA")


/* 2. Self cluster portfolio*/


val oneSldf = spark.sql("""
SELECT * FROM hmanalytics.hm_mfi_step2_fact    A
Left Semi Join
(SELECT DISTINCT CLST_ID FROM hmanalytics.hm_mfi_step2_fact WHERE MFI_ID IN ('PRB0000016') and RW=1)B
ON(A.clst_id = B.clst_id)
 WHERE NVL(A.SUPPRESS_INDICATOR,'N')<>'Y'
AND RW=1
""")

oneSldf.createOrReplaceTempView("HM_BFIL_SELFBASE_DATA")


 val twoSldf = spark.sql("""
SELECT 
       B.REV_STD_STATE_CODE STD_STATE_CODE,
       B.REVISED_DIST DISTRICT,
       CLST_ID,
       COUNT(DISTINCT CASE WHEN ((CLOSED_DT IS NULL OR CLOSED_DT>as_of) AND DAS IN ('S04','S05','S11','S12','S16','S18','S20')) THEN ACT_KEY ELSE NULL END) ACTIVE_ACCOUNTS,
       COUNT(DISTINCT MFI_ID) OVERALL_MFI_ASSOCIATION,
       COUNT(DISTINCT CASE WHEN ((CLOSED_DT IS NULL OR CLOSED_DT>as_of) AND DAS IN ('S04','S05','S11','S12','S16','S18','S20')) THEN MFI_ID ELSE NULL END) ACTIVE_MFI_ASSOCIATION,
       SUM(CUR_BAL) TOTAL_OUTSTANDING_AMOUNT,
       SUM(CASE WHEN ((CLOSED_DT IS NULL OR CLOSED_DT>as_of) AND DAS IN ('S04','S05','S11','S12','S16','S18','S20')) THEN CUR_BAL ELSE 0 END) PORTFOLIO_OUTSTANDING,
       MAX(DPD) MAX_DPD,
       MAX(CASE WHEN ((CLOSED_DT IS NULL OR CLOSED_DT>as_of) AND DAS IN ('S04','S05','S11','S12','S16','S18','S20')) THEN DPD ELSE NULL END) MAX_DPD_ACTIV,
       MIN(A.DISB_DT) MIN_DISB,
       MIN(CASE WHEN ((CLOSED_DT IS NULL OR CLOSED_DT>as_of) AND DAS IN ('S04','S05','S11','S12','S16','S18','S20')) THEN A.DISB_DT ELSE NULL END) MIN_DISB_ACTIV
  FROM HM_BFIL_SELFBASE_DATA  A
     JOIN (SELECT DISTINCT KEY,STD_STATE_CODE,DISTRICT,STD_PIN_CODE,REV_STD_STATE_CODE,REVISED_DIST  FROM hmanalytics.HM_SDP_DIM2 WHERE ACTIVE=1) B
      ON (A.sdp_key = B.key)
WHERE  B.REVISED_DIST IS NOT NULL
   AND B.REV_STD_STATE_CODE IS NOT NULL
 GROUP BY B.REV_STD_STATE_CODE,
          B.REVISED_DIST,
          CLST_ID
""")
twoSldf.createOrReplaceTempView("HM_MFIASS_CLST_SELF")
		  
          


 val threeSldf = spark.sql("""
  SELECT  A.*,
     B.REV_STD_STATE_CODE,B.REVISED_DIST
  FROM HM_BFIL_SELFBASE_DATA  A
  JOIN (SELECT DISTINCT KEY,STD_STATE_CODE,DISTRICT,STD_PIN_CODE,REV_STD_STATE_CODE,REVISED_DIST  FROM hmanalytics.HM_SDP_DIM2 WHERE ACTIVE=1) B
    ON (A.sdp_key = B.key)
 WHERE B.REVISED_DIST IS NOT NULL
   AND B.REV_STD_STATE_CODE IS NOT NULL
   AND (((CLOSED_DT IS NULL OR CLOSED_DT>as_of) AND DAS IN ('S04','S05')) OR DAS IN ('S06'))
   AND NVL(SUPPRESS_INDICATOR,'N')<>'Y'
""")   
 threeSldf.createOrReplaceTempView("HM_ACTIVE_BFIL_SELF")

 val fourSldf = spark.sql("""
SELECT A.*,C.MAX_DPD_ACTIV,PORTFOLIO_OUTSTANDING,ACTIVE_MFI_ASSOCIATION,
       MIN_DISB,MIN_DISB_ACTIV,
       (ROUND(MONTHS_BETWEEN(LAST_DAY(as_of),LAST_DAY(MIN_DISB)),0)+1)  VINTAGE_MOB
FROM HM_ACTIVE_BFIL_SELF A,
     HM_MFIASS_CLST_SELF C
 WHERE A.CLST_ID=C.CLST_ID
   AND A.REV_STD_STATE_CODE=C.STD_STATE_CODE
   AND A.REVISED_DIST=C.DISTRICT           
""")         
          
fourSldf.createOrReplaceTempView("HM_ACTIVE_SELF_BFIL_1")


val fiveSldf =spark.sql(""" 
SELECT CAST(date_format(as_of,'yyyyMM') as STRING) as LOAD_YYYYMM,
                                     'Self' product_ind,
                                     A.REV_STD_STATE_CODE STD_STATE_CODE,
                                     A.REVISED_DIST DISTRICT,
                                     concat_ws('|',A.REV_STD_STATE_CODE,A.REVISED_DIST,'DIST_CREDITEXP_MAXDPD_MFI_ASS_ACTIV_CLUSTER') COMBO,
                                     CASE WHEN VINTAGE_MOB>=0 
                                     THEN 
                                         CASE WHEN VINTAGE_MOB BETWEEN 0 AND 3 THEN '0-3'
                                             WHEN VINTAGE_MOB BETWEEN 4 AND 6 THEN '3-6'
                                             WHEN VINTAGE_MOB BETWEEN 7 AND 12 THEN '6-12'
                                             WHEN VINTAGE_MOB BETWEEN 13 AND 24 THEN '12-24'
                                             WHEN VINTAGE_MOB BETWEEN 25 AND 36 THEN '24-36'
                                             WHEN VINTAGE_MOB BETWEEN 37 AND 48 THEN '36-48'
                                             WHEN VINTAGE_MOB BETWEEN 49 AND 60 THEN '48-60'
                                             WHEN VINTAGE_MOB BETWEEN 61 AND 120 THEN '60-120'
                                             WHEN VINTAGE_MOB >120 THEN '>120'
                                          ELSE NULL
                                          END
                                      ELSE NULL 
                                      END VINTAGE,
                                  CASE WHEN ABS(PORTFOLIO_OUTSTANDING) BETWEEN 0  AND 10000 THEN '0_10K'
                                         WHEN ABS(PORTFOLIO_OUTSTANDING) BETWEEN 10001 AND 15000 THEN '10K_15K'
                                          WHEN ABS(PORTFOLIO_OUTSTANDING) BETWEEN 15001 AND 20000 THEN '15K_20K'
                                          WHEN ABS(PORTFOLIO_OUTSTANDING) BETWEEN 20001 AND 25000 THEN '20K_25K'
                                          WHEN ABS(PORTFOLIO_OUTSTANDING) BETWEEN 25001 AND 30000 THEN '25K_30K'
                                          WHEN ABS(PORTFOLIO_OUTSTANDING) BETWEEN 30001 AND 40000 THEN '30K_40K'
                                          WHEN ABS(PORTFOLIO_OUTSTANDING) BETWEEN 40001 AND 60000 THEN '40K_60K'
                                          WHEN ABS(PORTFOLIO_OUTSTANDING) BETWEEN 60001 AND 80000 THEN '60K_80K'
                                          WHEN ABS(PORTFOLIO_OUTSTANDING) BETWEEN 80001 AND 100000 THEN '80K_100K'
                                          WHEN ABS(PORTFOLIO_OUTSTANDING) BETWEEN 100001 AND 150000 THEN '100K_150K'
                                          WHEN ABS(PORTFOLIO_OUTSTANDING) > 150000 THEN '>150K'  
                                          WHEN ABS(PORTFOLIO_OUTSTANDING) IS NULL THEN 'BLANK'
                                     ELSE 'NA'
                                     END OUTSTANDING_AMOUNT,
                                CASE  WHEN DAS IN ('S04','S05') AND (MAX_DPD_ACTIV=0 OR MAX_DPD_ACTIV IS NULL) AND NVL(ABS(AMT_DUE),0) <= (0.01 * NVL(ABS(DISB_AMT),0)) THEN 'Not Deliquent(Good)'
                                           WHEN DAS IN ('S04','S05') AND (MAX_DPD_ACTIV BETWEEN 1 AND 30) THEN '1_30'
                                           WHEN DAS IN ('S04','S05') AND (MAX_DPD_ACTIV BETWEEN 31 AND 60) THEN '31_60'
                                           WHEN DAS IN ('S04','S05') AND (MAX_DPD_ACTIV BETWEEN 61 AND 90) THEN '61_90'
                                           WHEN DAS IN ('S04','S05') AND (MAX_DPD_ACTIV BETWEEN 91 AND 180) THEN '91_180'
                                           WHEN DAS IN ('S04','S05') AND (MAX_DPD_ACTIV > 180) THEN '>180'
                                           WHEN DAS IN ('S06') THEN 'WRITEOFF'
                                           WHEN DAS IN ('S07') THEN 'CLOSED'
                                           WHEN (MAX_DPD_ACTIV=0 OR MAX_DPD_ACTIV IS NULL) AND NVL(ABS(AMT_DUE),0) > (0.01 * NVL(ABS(DISB_AMT),0)) THEN 'Not Stipulated(Bad)'
                                           ELSE 'NA'
                                     END DPD,
                                     CASE WHEN ACTIVE_MFI_ASSOCIATION=1 THEN '1'
                                          WHEN ACTIVE_MFI_ASSOCIATION=2  THEN '2'
                                          WHEN ACTIVE_MFI_ASSOCIATION=3 THEN '3'
                                          WHEN ACTIVE_MFI_ASSOCIATION=4 THEN '4'
                                          WHEN ACTIVE_MFI_ASSOCIATION>=5 THEN '>=5'
                                     ELSE 'NA'
                                     END ACTIVE_MFI_ASSOCIATION,
                                  CASE WHEN disb_dt < '2017-04-30' THEN 'Pre-Apr 2017'
                                       WHEN disb_dt >='2017-04-30' THEN 'Since Apr 2017'
                                  ELSE 'NA'
                                  END DISBURSED_PERIOD,   
                                  COUNT(DISTINCT a.CLST_ID) UNIQUE_CLST,
                                  COUNT(DISTINCT(CASE WHEN ((CLOSED_DT IS NULL OR CLOSED_DT>as_of) AND DAS IN ('S04','S05')) THEN A.CLST_ID ELSE NULL END)) ACTIVE_CLST,
                                  COUNT(DISTINCT ACT_KEY) TOTAL_LOANS,
                                  COUNT(DISTINCT(CASE WHEN ((CLOSED_DT IS NULL OR CLOSED_DT>as_of) AND DAS IN ('S04','S05')) THEN ACT_KEY ELSE NULL END)) ACTIVE_LOANS,
                                  SUM(ABS(DISB_AMT)) TOTAL_DISB_AMOUNT,
                                  SUM(CASE WHEN ((CLOSED_DT IS NULL OR CLOSED_DT>as_of) AND DAS IN ('S04','S05')) THEN ABS(DISB_AMT) ELSE 0 END) ACTIVE_DISB_AMOUNT,
                                  SUM(ABS(AMT_DUE)) TOTAL_OVERDUE_AMOUNT,
                                  SUM(ABS(CUR_BAL)) TOTAL_OUTSTANDING_AMOUNT,
                                  SUM(CASE WHEN ((CLOSED_DT IS NULL OR CLOSED_DT>as_of) AND DAS IN ('S04','S05')) THEN ABS(CUR_BAL) ELSE 0 END) ACTIVE_OUTSTANDING_AMOUNT
                              FROM HM_ACTIVE_SELF_BFIL_1 A
                            GROUP BY as_of,
                                      A.REV_STD_STATE_CODE,
                                      A.REVISED_DIST,
                                      CASE WHEN VINTAGE_MOB>=0 
                                      THEN
                                            CASE WHEN VINTAGE_MOB BETWEEN 0 AND 3 THEN '0-3'
                                               WHEN VINTAGE_MOB BETWEEN 4 AND 6 THEN '3-6'
                                               WHEN VINTAGE_MOB BETWEEN 7 AND 12 THEN '6-12'
                                               WHEN VINTAGE_MOB BETWEEN 13 AND 24 THEN '12-24'
                                               WHEN VINTAGE_MOB BETWEEN 25 AND 36 THEN '24-36'
                                               WHEN VINTAGE_MOB BETWEEN 37 AND 48 THEN '36-48'
                                               WHEN VINTAGE_MOB BETWEEN 49 AND 60 THEN '48-60'
                                               WHEN VINTAGE_MOB BETWEEN 61 AND 120 THEN '60-120'
                                               WHEN VINTAGE_MOB >120 THEN '>120'
                                            ELSE NULL
                                            END
                                      ELSE NULL
                                      END,
                                     CASE WHEN ABS(PORTFOLIO_OUTSTANDING) BETWEEN 0  AND 10000 THEN '0_10K'
                                         WHEN ABS(PORTFOLIO_OUTSTANDING) BETWEEN 10001 AND 15000 THEN '10K_15K'
                                          WHEN ABS(PORTFOLIO_OUTSTANDING) BETWEEN 15001 AND 20000 THEN '15K_20K'
                                          WHEN ABS(PORTFOLIO_OUTSTANDING) BETWEEN 20001 AND 25000 THEN '20K_25K'
                                          WHEN ABS(PORTFOLIO_OUTSTANDING) BETWEEN 25001 AND 30000 THEN '25K_30K'
                                          WHEN ABS(PORTFOLIO_OUTSTANDING) BETWEEN 30001 AND 40000 THEN '30K_40K'
                                          WHEN ABS(PORTFOLIO_OUTSTANDING) BETWEEN 40001 AND 60000 THEN '40K_60K'
                                          WHEN ABS(PORTFOLIO_OUTSTANDING) BETWEEN 60001 AND 80000 THEN '60K_80K'
                                          WHEN ABS(PORTFOLIO_OUTSTANDING) BETWEEN 80001 AND 100000 THEN '80K_100K'
                                          WHEN ABS(PORTFOLIO_OUTSTANDING) BETWEEN 100001 AND 150000 THEN '100K_150K'
                                          WHEN ABS(PORTFOLIO_OUTSTANDING) > 150000 THEN '>150K'  
                                          WHEN ABS(PORTFOLIO_OUTSTANDING) IS NULL THEN 'BLANK'
                                     ELSE 'NA'
                                     END,
                                        CASE  WHEN DAS IN ('S04','S05') AND (MAX_DPD_ACTIV=0 OR MAX_DPD_ACTIV IS NULL) AND NVL(ABS(AMT_DUE),0) <= (0.01 * NVL(ABS(DISB_AMT),0)) THEN 'Not Deliquent(Good)'
                                           WHEN DAS IN ('S04','S05') AND (MAX_DPD_ACTIV BETWEEN 1 AND 30) THEN '1_30'
                                           WHEN DAS IN ('S04','S05') AND (MAX_DPD_ACTIV BETWEEN 31 AND 60) THEN '31_60'
                                           WHEN DAS IN ('S04','S05') AND (MAX_DPD_ACTIV BETWEEN 61 AND 90) THEN '61_90'
                                           WHEN DAS IN ('S04','S05') AND (MAX_DPD_ACTIV BETWEEN 91 AND 180) THEN '91_180'
                                           WHEN DAS IN ('S04','S05') AND (MAX_DPD_ACTIV > 180) THEN '>180'
                                           WHEN DAS IN ('S06') THEN 'WRITEOFF'
                                           WHEN DAS IN ('S07') THEN 'CLOSED'
                                           WHEN (MAX_DPD_ACTIV=0 OR MAX_DPD_ACTIV IS NULL) AND NVL(ABS(AMT_DUE),0) > (0.01 * NVL(ABS(DISB_AMT),0)) THEN 'Not Stipulated(Bad)'
                                           ELSE 'NA'
                                     END,
                                       CASE WHEN ACTIVE_MFI_ASSOCIATION=1 THEN '1'
                                            WHEN ACTIVE_MFI_ASSOCIATION=2  THEN '2'
                                            WHEN ACTIVE_MFI_ASSOCIATION=3 THEN '3'
                                            WHEN ACTIVE_MFI_ASSOCIATION=4 THEN '4'
                                            WHEN ACTIVE_MFI_ASSOCIATION>=5 THEN '>=5'
                                       ELSE 'NA'
                                       END,
                                       CASE WHEN disb_dt<'2017-04-30' THEN 'Pre-Apr 2017'
                                       WHEN disb_dt>='2017-04-30' THEN 'Since Apr 2017'
                                       ELSE 'NA'
                                       END
""")							
fiveSldf.createOrReplaceTempView("HM_DIST_BFIL_SELF")
fiveSldf.write.mode("append").saveAsTable("hmanalytics.HM_DIST_BFIL_DATA")

val allfileDF = spark.sql("""
SELECT '"""+load_dt+ """' CLOSING_AS_OF,
         STD_STATE_CODE STATE,
         DISTRICT,
         VINTAGE BORROWER_VINTAGE,
         CASE WHEN ACTIVE_MFI_ASSOCIATION IN ('1') THEN '1 Lender'
              WHEN ACTIVE_MFI_ASSOCIATION IN ('2') THEN '2 Lenders'
              WHEN ACTIVE_MFI_ASSOCIATION IN ('3') THEN '3 Lenders'
              WHEN ACTIVE_MFI_ASSOCIATION IN ('4') THEN '4 Lenders'
              WHEN ACTIVE_MFI_ASSOCIATION IN ('>=5') THEN '>=5 Lenders'
              ELSE 'NA'
         END ACTIVE_LENDER,
         CASE WHEN OUTSTANDING_AMOUNT IN ('0_10K','10K_15K') THEN '<=15'
              WHEN OUTSTANDING_AMOUNT IN ('15K_20K','20K_25K','25K_30K') THEN '15K_30K'
              WHEN OUTSTANDING_AMOUNT IN ('30K_40K','40K_60K') THEN '30K-60K'
              WHEN OUTSTANDING_AMOUNT IN ('60K_80K') THEN '60K-80K'
              WHEN OUTSTANDING_AMOUNT IN ('80K_100K') THEN '80K-100K'
              WHEN OUTSTANDING_AMOUNT IN ('100K_150K') THEN '100K_150K'
              WHEN OUTSTANDING_AMOUNT IN ('>150K') THEN '>150K'  
         ELSE 'NA'
         END CREDIT_EXPOSURE,
         CASE WHEN DPD = 'Not Deliquent(Good)' THEN 'CURRENT'
              WHEN DPD = '1_30'                THEN '1-30 DPD'
              WHEN DPD = '31_60'               THEN '31-60 DPD'
              WHEN DPD = '61_90'               THEN '61-90 DPD'
              WHEN DPD = '91_180'              THEN '91-180 DPD'
              WHEN DPD = '>180'                THEN '>180 DPD'
              WHEN DPD = 'Not Stipulated(Bad)' THEN 'Not stipulated'
              WHEN DPD = 'WRITEOFF' THEN 'Written-Off'
         ELSE 'NA'
         END WORST_DPD,
         DISBURSED_PERIOD,
         SUM(UNIQUE_CLST)  Active_Customers,
         SUM(TOTAL_LOANS) Active_Loans,
         SUM(TOTAL_DISB_AMOUNT) Disbursed_Amount_Active,
         SUM(TOTAL_OUTSTANDING_AMOUNT) Portfolio_Outstanding,
         SUM(TOTAL_OVERDUE_AMOUNT) Overdue_Amount
  FROM hmanalytics.HM_DIST_BFIL_DATA   A
  WHERE COMBO LIKE '%DIST_CREDITEXP_MAXDPD_MFI_ASS_ACTIV_CLUSTER%'
  AND product_ind='All'
   GROUP BY 
           STD_STATE_CODE,
           DISTRICT,
           VINTAGE,
           CASE WHEN ACTIVE_MFI_ASSOCIATION IN ('1') THEN '1 Lender'
              WHEN ACTIVE_MFI_ASSOCIATION IN ('2') THEN '2 Lenders'
              WHEN ACTIVE_MFI_ASSOCIATION IN ('3') THEN '3 Lenders'
              WHEN ACTIVE_MFI_ASSOCIATION IN ('4') THEN '4 Lenders'
              WHEN ACTIVE_MFI_ASSOCIATION IN ('>=5') THEN '>=5 Lenders'
              ELSE 'NA'
           END,
           CASE WHEN OUTSTANDING_AMOUNT IN ('0_10K','10K_15K') THEN '<=15'
                WHEN OUTSTANDING_AMOUNT IN ('15K_20K','20K_25K','25K_30K') THEN '15K_30K'
                WHEN OUTSTANDING_AMOUNT IN ('30K_40K','40K_60K') THEN '30K-60K'
                WHEN OUTSTANDING_AMOUNT IN ('60K_80K') THEN '60K-80K'
                WHEN OUTSTANDING_AMOUNT IN ('80K_100K') THEN '80K-100K'
                WHEN OUTSTANDING_AMOUNT IN ('100K_150K') THEN '100K_150K'
                WHEN OUTSTANDING_AMOUNT IN ('>150K') THEN '>150K'  
           ELSE 'NA'
           END,
           CASE WHEN DPD = 'Not Deliquent(Good)' THEN 'CURRENT'
                                              WHEN DPD = '1_30'                THEN '1-30 DPD'
                                              WHEN DPD = '31_60'               THEN '31-60 DPD'
                                              WHEN DPD = '61_90'               THEN '61-90 DPD'
                                              WHEN DPD = '91_180'              THEN '91-180 DPD'
                                              WHEN DPD = '>180'                THEN '>180 DPD'
                                              WHEN DPD = 'Not Stipulated(Bad)' THEN 'Not stipulated'
                                              WHEN DPD = 'WRITEOFF' THEN 'Written-Off'
                                         ELSE 'NA'
         END,
         DISBURSED_PERIOD
""")  
allfileDF.orderBy(col("STATE")).repartition(1).write.mode("overwrite").option("header", "true").option("delimiter", "|").csv("/tmp/prateek/BFIL")	
var file = fs.globStatus(new Path("/tmp/prateek/BFIL/part*"))(0).getPath().getName()
fs.rename(new Path("/tmp/prateek/BFIL/" +file), new Path("/tmp/prateek/BFIL_NEW/CrifHighmark-BFIL_Unique_Cluster_Rev_MFI_"+load_dt+"_V2.csv"))


val selffileDF = spark.sql("""
SELECT '"""+load_dt+ """' CLOSING_AS_OF,
         STD_STATE_CODE STATE,
         DISTRICT,
         VINTAGE BORROWER_VINTAGE,
         CASE WHEN ACTIVE_MFI_ASSOCIATION IN ('1') THEN '1 Lender'
              WHEN ACTIVE_MFI_ASSOCIATION IN ('2') THEN '2 Lenders'
              WHEN ACTIVE_MFI_ASSOCIATION IN ('3') THEN '3 Lenders'
              WHEN ACTIVE_MFI_ASSOCIATION IN ('4') THEN '4 Lenders'
              WHEN ACTIVE_MFI_ASSOCIATION IN ('>=5') THEN '>=5 Lenders'
              ELSE 'NA'
         END ACTIVE_LENDER,
         CASE WHEN OUTSTANDING_AMOUNT IN ('0_10K','10K_15K') THEN '<=15'
              WHEN OUTSTANDING_AMOUNT IN ('15K_20K','20K_25K','25K_30K') THEN '15K_30K'
              WHEN OUTSTANDING_AMOUNT IN ('30K_40K','40K_60K') THEN '30K-60K'
              WHEN OUTSTANDING_AMOUNT IN ('60K_80K') THEN '60K-80K'
              WHEN OUTSTANDING_AMOUNT IN ('80K_100K') THEN '80K-100K'
              WHEN OUTSTANDING_AMOUNT IN ('100K_150K') THEN '100K_150K'
              WHEN OUTSTANDING_AMOUNT IN ('>150K') THEN '>150K'  
         ELSE 'NA'
         END CREDIT_EXPOSURE,
         CASE WHEN DPD = 'Not Deliquent(Good)' THEN 'CURRENT'
              WHEN DPD = '1_30'                THEN '1-30 DPD'
              WHEN DPD = '31_60'               THEN '31-60 DPD'
              WHEN DPD = '61_90'               THEN '61-90 DPD'
              WHEN DPD = '91_180'              THEN '91-180 DPD'
              WHEN DPD = '>180'                THEN '>180 DPD'
              WHEN DPD = 'Not Stipulated(Bad)' THEN 'Not stipulated'
              WHEN DPD = 'WRITEOFF' THEN 'Written-Off'
         ELSE 'NA'
         END WORST_DPD,
         DISBURSED_PERIOD,
         SUM(UNIQUE_CLST)  Active_Customers,
         SUM(TOTAL_LOANS) Active_Loans,
         SUM(TOTAL_DISB_AMOUNT) Disbursed_Amount_Active,
         SUM(TOTAL_OUTSTANDING_AMOUNT) Portfolio_Outstanding,
         SUM(TOTAL_OVERDUE_AMOUNT) Overdue_Amount
  FROM hmanalytics.HM_DIST_BFIL_DATA   A
  WHERE COMBO LIKE '%DIST_CREDITEXP_MAXDPD_MFI_ASS_ACTIV_CLUSTER%'
  AND product_ind='Self'
   GROUP BY 
           STD_STATE_CODE,
           DISTRICT,
           VINTAGE,
           CASE WHEN ACTIVE_MFI_ASSOCIATION IN ('1') THEN '1 Lender'
              WHEN ACTIVE_MFI_ASSOCIATION IN ('2') THEN '2 Lenders'
              WHEN ACTIVE_MFI_ASSOCIATION IN ('3') THEN '3 Lenders'
              WHEN ACTIVE_MFI_ASSOCIATION IN ('4') THEN '4 Lenders'
              WHEN ACTIVE_MFI_ASSOCIATION IN ('>=5') THEN '>=5 Lenders'
              ELSE 'NA'
           END,
           CASE WHEN OUTSTANDING_AMOUNT IN ('0_10K','10K_15K') THEN '<=15'
                WHEN OUTSTANDING_AMOUNT IN ('15K_20K','20K_25K','25K_30K') THEN '15K_30K'
                WHEN OUTSTANDING_AMOUNT IN ('30K_40K','40K_60K') THEN '30K-60K'
                WHEN OUTSTANDING_AMOUNT IN ('60K_80K') THEN '60K-80K'
                WHEN OUTSTANDING_AMOUNT IN ('80K_100K') THEN '80K-100K'
                WHEN OUTSTANDING_AMOUNT IN ('100K_150K') THEN '100K_150K'
                WHEN OUTSTANDING_AMOUNT IN ('>150K') THEN '>150K'  
           ELSE 'NA'
           END,
           CASE WHEN DPD = 'Not Deliquent(Good)' THEN 'CURRENT'
                                              WHEN DPD = '1_30'                THEN '1-30 DPD'
                                              WHEN DPD = '31_60'               THEN '31-60 DPD'
                                              WHEN DPD = '61_90'               THEN '61-90 DPD'
                                              WHEN DPD = '91_180'              THEN '91-180 DPD'
                                              WHEN DPD = '>180'                THEN '>180 DPD'
                                              WHEN DPD = 'Not Stipulated(Bad)' THEN 'Not stipulated'
                                              WHEN DPD = 'WRITEOFF' THEN 'Written-Off'
                                         ELSE 'NA'
         END,
         DISBURSED_PERIOD
""")
  
selffileDF.orderBy(col("STATE")).repartition(1).write.mode("overwrite").option("header", "true").option("delimiter", "|").csv("/tmp/prateek/BFIL")
file = fs.globStatus(new Path("/tmp/prateek/BFIL/part*"))(0).getPath().getName()

fs.rename(new Path("/tmp/prateek/BFIL/" +file), new Path("/tmp/prateek/BFIL_NEW/CrifHighmark-BFIL_Self_Unique_Clust_Rev_MFI_"+load_dt+"_V2.csv"))




/* Account Level Data File */

val activMfiDF = spark.sql("""
SELECT SDP_KEY,
CONTRI_KEY,
SUM(TOTAL_CONSUMERS) TOTAL_CONSUMERS,
SUM(CASE WHEN LOAN_ACCOUNT_STATUS_KEY=1 THEN TOTAL_CONSUMERS ELSE 0 END) TOTAL_ACTIVE_CONSUMERS
FROM
hmanalytics.HM_MFI_ACT_FACT_TBL
WHERE
LOAD_YYYYMM= '"""+load_dt+ """'
GROUP BY
SDP_KEY,
CONTRI_KEY
""")

activMfiDF.createOrReplaceTempView("activMfiDF")

val bfilStd1Df = spark.sql("""
SELECT    'RPT1' RPT_FMT,                                  
         A.LOAD_YYYYMM QUARTER,
         REV_STD_STATE_CODE STATE,
         REVISED_DIST DISTRICT,
         CAST(STD_PIN_CODE AS INT),
        CASE WHEN NVL(ACTIVE_MFI,0)<=3 THEN '<=3'
        WHEN NVL(ACTIVE_MFI,0) BETWEEN 4 AND 6 THEN '4-6'
        WHEN NVL(ACTIVE_MFI,0) BETWEEN 7 AND 10 THEN '7-10'
        WHEN NVL(ACTIVE_MFI,0) BETWEEN 11 AND 15 THEN '11-15'
        WHEN NVL(ACTIVE_MFI,0) BETWEEN 16 AND 20 THEN '16-20'
        WHEN NVL(ACTIVE_MFI,0) BETWEEN 21 AND 30 THEN '21-30'
        WHEN NVL(ACTIVE_MFI,0) BETWEEN 31 AND 40 THEN '31-40'
        WHEN NVL(ACTIVE_MFI,0)>40 THEN '>40'
        ELSE NULL
        END ACTIVE_MFI_BUCKETS,
        CASE WHEN MOB BETWEEN 0 AND 2 THEN '0-3M'
             WHEN MOB BETWEEN 3 AND 5 THEN '3-6M'
             WHEN MOB BETWEEN 6 AND 11 THEN '6-12M'
             WHEN MOB BETWEEN 12 AND 23 THEN '12-24M'
             WHEN MOB BETWEEN 24 AND 35 THEN '24-36M'
             WHEN MOB BETWEEN 36 AND 47 THEN '36-48M'
             WHEN MOB BETWEEN 48 AND 59 THEN '48-60M'
             WHEN MOB BETWEEN 60 AND 119 THEN '60-120M'
             WHEN MOB >= 120 THEN '>120M'
         ELSE NULL
        END BORROWER_VINTAGE ,
           CASE WHEN CONTRIBUTOR_ID IN('MFI0000002','PRB0000016') THEN 'BFIL'
                 WHEN CONTRIBUTOR_ID IN ('MFI0000004','MFI0000001','SFB0000005','PRB0000006','MFI0000014','MFI0000009','MFI0000097') THEN 'Peer Group 1'
                 WHEN CONTRIBUTOR_ID IN ('MFI0000005','MFI0000043','MFI0000008','MFI0000030','MFI0000015','SFB0000002') THEN 'Peer Group 2'
                 WHEN CONTRIBUTOR_ID IN ('MFI0000048','MFI0000049','MFI0000021','PRB0000033','PRB0000001','PRB0000003','MFI0000020') THEN 'Peer Group 3'
                 WHEN CONTRIBUTOR_ID IN ('MFI0000012','MFI0000019','MFI0000114','MFI0000011','MFI0000089','PRB0000016') THEN 'Peer Group 4'
                 ELSE 'Peer Group 5'
            END  LENDER_TYPE,
                       'MFI' PRODUCT,
                       ACCOUNT_TYPE,
                       ACCOUNT_NAME, 
                       CASE WHEN LOAN_TICKET_SIZE_KEY <= 2 THEN '<=10K'
                            WHEN LOAN_TICKET_SIZE_KEY=3 THEN '10K-15K'
                            WHEN LOAN_TICKET_SIZE_KEY=4 THEN '15K-20K'
                            WHEN LOAN_TICKET_SIZE_KEY=5 THEN '20K-25K'
                            WHEN LOAN_TICKET_SIZE_KEY=6 THEN '25K-30K'
                            WHEN LOAN_TICKET_SIZE_KEY BETWEEN 7 AND 8 THEN '30K-40K'
                            WHEN LOAN_TICKET_SIZE_KEY BETWEEN 9 AND 12 THEN '40K-60K'
                            WHEN LOAN_TICKET_SIZE_KEY > 12 THEN '60K+'
                       ELSE 'NA'
                       END TICKET_SIZE,
                         CASE WHEN DISB_YYYYMM<=201403  THEN 'UPTO FY 2013-2014'
                                   WHEN DISB_YYYYMM >=201404 THEN FYQ
                                   ELSE 'OTHERS'
                              END DISBURSAL_PERIOD,
                       CASE WHEN LOAN_INSTALL_FREQ_KEY = 1 THEN 'Weekly'
                                WHEN LOAN_INSTALL_FREQ_KEY = 2 THEN 'BiWeekly'
                                WHEN LOAN_INSTALL_FREQ_KEY = 3 THEN 'Monthly'
                                WHEN LOAN_INSTALL_FREQ_KEY = 4 THEN 'Bimonthly'
                           ELSE 'Others'
                           END LOAN_INSTALL_FREQ,
                           CASE WHEN LOAN_NUM_INSTAL_BUCKET_NEW_KEY =1 THEN '0_6'
                                WHEN LOAN_NUM_INSTAL_BUCKET_NEW_KEY =2 THEN '7_12'
                                WHEN LOAN_NUM_INSTAL_BUCKET_NEW_KEY =3 THEN '13_24'
                                WHEN LOAN_NUM_INSTAL_BUCKET_NEW_KEY =4 THEN 'GRTR24'
                           ELSE 'NA'
                           END  TENURE,
                           CASE WHEN supress_ind_key=1 THEN 1
                           ELSE 0 END SUPPRESS_INDICATOR,
                           SUM(TOTAL_ACCOUNTS) TOTAL_LOANS,
                           SUM(CASE WHEN LOAN_ACCOUNT_STATUS_KEY=1 THEN TOTAL_ACCOUNTS ELSE 0 END) TOTAL_ACTIV_LOANS,
                           SUM(TOTAL_DISBURSED_AMOUNT) TOTAL_DISB_AMOUNT,
                           SUM(CASE WHEN LOAN_ACCOUNT_STATUS_KEY=1 THEN TOTAL_DISBURSED_AMOUNT ELSE 0 END) TOTAL_ACTIV_DISB_AMOUNT,
                           SUM(CASE WHEN LOAN_ACCOUNT_STATUS_KEY=1 THEN TOTAL_OUTSTANDING_AMOUNT ELSE 0 END) TOTAL_ACT_OUTSTANDING_AMOUNT,
                           SUM(CASE WHEN LOAN_ACCOUNT_STATUS_KEY=1 AND LOAN_DPD_BUCKET_KEY IN(15) THEN TOTAL_ACCOUNTS ELSE 0 END) LOANS_CURRENT,
                           SUM(CASE WHEN LOAN_ACCOUNT_STATUS_KEY=1 AND LOAN_DPD_BUCKET_KEY IN(1) THEN TOTAL_ACCOUNTS ELSE 0 END) LOANS_NS,
                           SUM(CASE WHEN LOAN_ACCOUNT_STATUS_KEY=1 AND OVERDUE_IND=1 AND LOAN_DPD_BUCKET_KEY IN(2) THEN TOTAL_ACCOUNTS ELSE 0 END) TOTAL_LOAN_DPD_1_30,
                           SUM(CASE WHEN LOAN_ACCOUNT_STATUS_KEY=1 AND OVERDUE_IND=1 AND LOAN_DPD_BUCKET_KEY IN(3) THEN TOTAL_ACCOUNTS ELSE 0 END) TOTAL_LOAN_DPD_31_60,
                           SUM(CASE WHEN LOAN_ACCOUNT_STATUS_KEY=1 AND OVERDUE_IND=1 AND LOAN_DPD_BUCKET_KEY IN(4) THEN TOTAL_ACCOUNTS ELSE 0 END) TOTAL_LOAN_DPD_61_90,
                           SUM(CASE WHEN LOAN_ACCOUNT_STATUS_KEY=1 AND OVERDUE_IND=1 AND LOAN_DPD_BUCKET_KEY IN(3,4,5,6,7) THEN TOTAL_ACCOUNTS ELSE 0 END) TOTAL_LOAN_DPD_31_180,
                           SUM(CASE WHEN LOAN_ACCOUNT_STATUS_KEY=1 AND OVERDUE_IND=1 AND LOAN_DPD_BUCKET_KEY IN(5,6,7) THEN TOTAL_ACCOUNTS ELSE 0 END) TOTAL_LOAN_DPD_91_180,
                           SUM(CASE WHEN LOAN_ACCOUNT_STATUS_KEY=1 AND OVERDUE_IND=1 AND LOAN_DPD_BUCKET_KEY IN(5) THEN TOTAL_ACCOUNTS ELSE 0 END) TOTAL_LOAN_DPD_91_120,
                           SUM(CASE WHEN LOAN_ACCOUNT_STATUS_KEY=1 AND OVERDUE_IND=1 AND LOAN_DPD_BUCKET_KEY IN(6) THEN TOTAL_ACCOUNTS ELSE 0 END) TOTAL_LOAN_DPD_121_150,
                           SUM(CASE WHEN LOAN_ACCOUNT_STATUS_KEY=1 AND OVERDUE_IND=1 AND LOAN_DPD_BUCKET_KEY IN(7) THEN TOTAL_ACCOUNTS ELSE 0 END) TOTAL_LOAN_DPD_151_180,
                           SUM(CASE WHEN LOAN_ACCOUNT_STATUS_KEY=1 AND OVERDUE_IND=1 AND LOAN_DPD_BUCKET_KEY BETWEEN 8 AND 13 THEN TOTAL_ACCOUNTS ELSE 0 END) TOTAL_LOAN_DPD_181_360,
                           SUM(CASE WHEN LOAN_ACCOUNT_STATUS_KEY=1 AND OVERDUE_IND=1 AND LOAN_DPD_BUCKET_KEY IN(14)THEN TOTAL_ACCOUNTS ELSE 0 END) TOTAL_LOAN_DPD_GRTR_360,
                           SUM(CASE WHEN LOAN_ACCOUNT_STATUS_KEY=1 AND LOAN_DPD_BUCKET_KEY IN(15) THEN TOTAL_OUTSTANDING_AMOUNT ELSE 0 END) POS_CURRENT,
                           SUM(CASE WHEN LOAN_ACCOUNT_STATUS_KEY=1 AND LOAN_DPD_BUCKET_KEY IN(1) THEN TOTAL_OUTSTANDING_AMOUNT ELSE 0 END) POS_NS,
                           SUM(CASE WHEN LOAN_ACCOUNT_STATUS_KEY=1 AND OVERDUE_IND=1 AND LOAN_DPD_BUCKET_KEY IN(2) THEN TOTAL_OUTSTANDING_AMOUNT ELSE 0 END) OUTSTANDING_AMT_1_30,
                           SUM(CASE WHEN LOAN_ACCOUNT_STATUS_KEY=1 AND OVERDUE_IND=1 AND LOAN_DPD_BUCKET_KEY IN(3) THEN TOTAL_OUTSTANDING_AMOUNT ELSE 0 END) OUTSTANDING_AMT_31_60,
                           SUM(CASE WHEN LOAN_ACCOUNT_STATUS_KEY=1 AND OVERDUE_IND=1 AND LOAN_DPD_BUCKET_KEY IN(4) THEN TOTAL_OUTSTANDING_AMOUNT ELSE 0 END) OUTSTANDING_AMT_61_90,
                           SUM(CASE WHEN LOAN_ACCOUNT_STATUS_KEY=1 AND OVERDUE_IND=1 AND LOAN_DPD_BUCKET_KEY IN(3,4,5,6,7) THEN TOTAL_OUTSTANDING_AMOUNT ELSE 0 END) OUTSTANDING_AMT_31_180,
                           SUM(CASE WHEN LOAN_ACCOUNT_STATUS_KEY=1 AND OVERDUE_IND=1 AND LOAN_DPD_BUCKET_KEY IN(5,6,7) THEN TOTAL_OUTSTANDING_AMOUNT ELSE 0 END) OUTSTANDING_AMT_91_180,
                           SUM(CASE WHEN LOAN_ACCOUNT_STATUS_KEY=1 AND OVERDUE_IND=1 AND LOAN_DPD_BUCKET_KEY IN(5) THEN TOTAL_OUTSTANDING_AMOUNT ELSE 0 END) OUTSTANDING_AMT_91_120,
                           SUM(CASE WHEN LOAN_ACCOUNT_STATUS_KEY=1 AND OVERDUE_IND=1 AND LOAN_DPD_BUCKET_KEY IN(6) THEN TOTAL_OUTSTANDING_AMOUNT ELSE 0 END) OUTSTANDING_AMT_121_150,
                           SUM(CASE WHEN LOAN_ACCOUNT_STATUS_KEY=1 AND OVERDUE_IND=1 AND LOAN_DPD_BUCKET_KEY IN(7) THEN TOTAL_OUTSTANDING_AMOUNT ELSE 0 END) OUTSTANDING_AMT_151_180,
                           SUM(CASE WHEN LOAN_ACCOUNT_STATUS_KEY=1 AND OVERDUE_IND=1 AND LOAN_DPD_BUCKET_KEY BETWEEN 8 AND 13 THEN TOTAL_OUTSTANDING_AMOUNT ELSE 0 END) OUTSTANDING_AMT_181_360,
                           SUM(CASE WHEN LOAN_ACCOUNT_STATUS_KEY=1 AND OVERDUE_IND=1 AND LOAN_DPD_BUCKET_KEY IN(14)THEN TOTAL_OUTSTANDING_AMOUNT ELSE 0 END) OUTSTANDING_AMT_GRTR_360,
                           SUM(CASE WHEN from_unixtime(unix_timestamp(disb_yyyymm ,'yyyyMM'),'yyyy-MM-dd') BETWEEN add_months(from_unixtime(unix_timestamp(A.LOAD_YYYYMM ,'yyyyMM'),'yyyy-MM-dd'),-6) AND from_unixtime(unix_timestamp(A.LOAD_YYYYMM ,'yyyyMM'),'yyyy-MM-dd') THEN TOTAL_ACCOUNTS ELSE 0 END) TOTAL_LOANS_6M,
                            SUM(CASE WHEN from_unixtime(unix_timestamp(disb_yyyymm ,'yyyyMM'),'yyyy-MM-dd') BETWEEN add_months(from_unixtime(unix_timestamp(A.LOAD_YYYYMM ,'yyyyMM'),'yyyy-MM-dd'),-6) AND from_unixtime(unix_timestamp(A.LOAD_YYYYMM ,'yyyyMM'),'yyyy-MM-dd')  THEN TOTAL_DISBURSED_AMOUNT ELSE 0 END) TOTAL_DISBURSED_AMOUNT_6M,
                            SUM(CASE WHEN from_unixtime(unix_timestamp(disb_yyyymm ,'yyyyMM'),'yyyy-MM-dd') BETWEEN add_months(from_unixtime(unix_timestamp(A.LOAD_YYYYMM ,'yyyyMM'),'yyyy-MM-dd'),-12) AND from_unixtime(unix_timestamp(A.LOAD_YYYYMM ,'yyyyMM'),'yyyy-MM-dd')  THEN TOTAL_ACCOUNTS ELSE 0 END) LOANS_DISB_IN_LAST_12_MONTHS,
                            SUM(CASE WHEN from_unixtime(unix_timestamp(disb_yyyymm ,'yyyyMM'),'yyyy-MM-dd') BETWEEN add_months(from_unixtime(unix_timestamp(A.LOAD_YYYYMM ,'yyyyMM'),'yyyy-MM-dd'),-12) AND from_unixtime(unix_timestamp(A.LOAD_YYYYMM ,'yyyyMM'),'yyyy-MM-dd')  THEN TOTAL_DISBURSED_AMOUNT ELSE 0 END) AMT_DISB_IN_LAST_12_MONTHS,
                           SUM(CASE WHEN from_unixtime(unix_timestamp(disb_yyyymm ,'yyyyMM'),'yyyy-MM-dd') BETWEEN add_months(from_unixtime(unix_timestamp(A.LOAD_YYYYMM ,'yyyyMM'),'yyyy-MM-dd'),-24) AND from_unixtime(unix_timestamp(A.LOAD_YYYYMM ,'yyyyMM'),'yyyy-MM-dd')  THEN TOTAL_ACCOUNTS ELSE 0 END) LOANS_DISB_IN_LAST_24_MONTHS,
                            SUM(CASE WHEN from_unixtime(unix_timestamp(disb_yyyymm ,'yyyyMM'),'yyyy-MM-dd') BETWEEN add_months(from_unixtime(unix_timestamp(A.LOAD_YYYYMM ,'yyyyMM'),'yyyy-MM-dd'),-24) AND from_unixtime(unix_timestamp(A.LOAD_YYYYMM ,'yyyyMM'),'yyyy-MM-dd')  THEN TOTAL_DISBURSED_AMOUNT ELSE 0 END) AMT_DISB_IN_LAST_24_MONTHS,
                           SUM(CASE WHEN DAS_ID IN (6,10) THEN TOTAL_ACCOUNTS ELSE 0 END) TOTAL_WRITTEN_OFF_LOANS,
                           SUM(CASE WHEN DAS_ID IN (6,10) THEN TOTAL_WRITE_OFF_AMOUNT ELSE 0 END) TOTAL_WRITTEN_OFF_AMOUNT
                      FROM  (   
                             SELECT A.*,
                                     B.REV_STD_STATE_CODE,
                                     B.REVISED_DIST,
                                     B.STD_PIN_CODE,
                                     ACTIVE_MFI,
									 MONTHS_BETWEEN(from_unixtime(unix_timestamp(A.LOAD_YYYYMM ,'yyyyMM'),'yyyy-MM-dd') ,from_unixtime(unix_timestamp(A.min_disb_yyyymm ,'yyyyMM'), 'yyyy-MM-dd')) MOB,
                                     C.CONTRIBUTOR_ID,
                                     C.INSTITUTION_NAME,
                                     E.FYQ,
                                     F.ACCOUNT_NAME,
                                     F.ACCOUNT_TYPE
                               FROM (SELECT A.*
                                       FROM HMANALYTICS.HM_MFI_ACT_FACT_TBL A
                                      WHERE CONTRI_KEY NOT IN (1019) AND LOAD_YYYYMM= '"""+load_dt+ """'
                                     ) A
                               JOIN (SELECT 
                                      REV_STD_STATE_CODE,
                                      REVISED_DIST,
                                   STD_PIN_CODE,
								   SDP_KEY,
                                   count(DISTINCT CASE WHEN TOTAL_CONSUMERS >=10  AND TOTAL_ACTIVE_CONSUMERS>0 THEN  CONTRI_KEY  ELSE NULL END) ACTIVE_MFI
                                   FROM
                                   activMfiDF K
                                      JOIN hmanalytics.HM_SDP_DIM2_V1 Z
                                      ON (K.SDP_KEY=Z.KEY
                                          AND Z.ACTIVE=1)
                                       group by 
                                      REV_STD_STATE_CODE,
                                      REVISED_DIST,
									  SDP_KEY,
                                   STD_PIN_CODE) B
                                   ON (A.SDP_KEY = B.SDP_KEY)
                               JOIN HMANALYTICS.HM_CONTRIBUTOR_DIM C
                                 ON (C.ACTIVE=1
                                     AND A.CONTRI_KEY =C.CONTRI_KEY
                                     )
                          LEFT JOIN HMANALYTICS.HM_DATE_DIM2 E
                                 ON (CAST(A.DISB_YYYYMM AS INT) =E.YYYYMM)
                    LEFT OUTER JOIN (SELECT LOAN_ACCOUNT_TYPE_KEY,ACCOUNT_TYPE,ACCOUNT_NAME 
                                       FROM hmanalytics.HM_LOAN_ACCOUNT_TYPE_DIM
                                      WHERE BUREAU_FLAG='MFI'
                                        AND ACTIVE=1 
                                     ) F
                                 ON (A.LOAN_ACCOUNT_TYPE_KEY=F.LOAN_ACCOUNT_TYPE_KEY)     
                            ) A
                       WHERE REV_STD_STATE_CODE IS NOT NULL
               GROUP BY   A.LOAD_YYYYMM ,
         REV_STD_STATE_CODE ,
         REVISED_DIST ,
         STD_PIN_CODE,
        CASE WHEN NVL(ACTIVE_MFI,0)<=3 THEN '<=3'
        WHEN NVL(ACTIVE_MFI,0) BETWEEN 4 AND 6 THEN '4-6'
        WHEN NVL(ACTIVE_MFI,0) BETWEEN 7 AND 10 THEN '7-10'
        WHEN NVL(ACTIVE_MFI,0) BETWEEN 11 AND 15 THEN '11-15'
        WHEN NVL(ACTIVE_MFI,0) BETWEEN 16 AND 20 THEN '16-20'
        WHEN NVL(ACTIVE_MFI,0) BETWEEN 21 AND 30 THEN '21-30'
        WHEN NVL(ACTIVE_MFI,0) BETWEEN 31 AND 40 THEN '31-40'
        WHEN NVL(ACTIVE_MFI,0)>40 THEN '>40'
        ELSE NULL
        END ,
        CASE WHEN MOB BETWEEN 0 AND 2 THEN '0-3M'
             WHEN MOB BETWEEN 3 AND 5 THEN '3-6M'
             WHEN MOB BETWEEN 6 AND 11 THEN '6-12M'
             WHEN MOB BETWEEN 12 AND 23 THEN '12-24M'
             WHEN MOB BETWEEN 24 AND 35 THEN '24-36M'
             WHEN MOB BETWEEN 36 AND 47 THEN '36-48M'
             WHEN MOB BETWEEN 48 AND 59 THEN '48-60M'
             WHEN MOB BETWEEN 60 AND 119 THEN '60-120M'
             WHEN MOB >= 120 THEN '>120M'
         ELSE NULL
        END  ,
           CASE WHEN CONTRIBUTOR_ID IN('MFI0000002','PRB0000016') THEN 'BFIL'
                 WHEN CONTRIBUTOR_ID IN ('MFI0000004','MFI0000001','SFB0000005','PRB0000006','MFI0000014','MFI0000009','MFI0000097') THEN 'Peer Group 1'
                 WHEN CONTRIBUTOR_ID IN ('MFI0000005','MFI0000043','MFI0000008','MFI0000030','MFI0000015','SFB0000002') THEN 'Peer Group 2'
                 WHEN CONTRIBUTOR_ID IN ('MFI0000048','MFI0000049','MFI0000021','PRB0000033','PRB0000001','PRB0000003','MFI0000020') THEN 'Peer Group 3'
                 WHEN CONTRIBUTOR_ID IN ('MFI0000012','MFI0000019','MFI0000114','MFI0000011','MFI0000089','PRB0000016') THEN 'Peer Group 4'
                 ELSE 'Peer Group 5'
            END  ,
                        'MFI',
                       ACCOUNT_TYPE,
                       ACCOUNT_NAME, 
                       CASE WHEN LOAN_TICKET_SIZE_KEY <= 2 THEN '<=10K'
                            WHEN LOAN_TICKET_SIZE_KEY=3 THEN '10K-15K'
                            WHEN LOAN_TICKET_SIZE_KEY=4 THEN '15K-20K'
                            WHEN LOAN_TICKET_SIZE_KEY=5 THEN '20K-25K'
                            WHEN LOAN_TICKET_SIZE_KEY=6 THEN '25K-30K'
                            WHEN LOAN_TICKET_SIZE_KEY BETWEEN 7 AND 8 THEN '30K-40K'
                            WHEN LOAN_TICKET_SIZE_KEY BETWEEN 9 AND 12 THEN '40K-60K'
                            WHEN LOAN_TICKET_SIZE_KEY > 12 THEN '60K+'
                       ELSE 'NA'
                       END ,
                         CASE WHEN DISB_YYYYMM<=201403  THEN 'UPTO FY 2013-2014'
                                   WHEN DISB_YYYYMM >=201404 THEN FYQ
                                   ELSE 'OTHERS'
                              END ,
                       CASE WHEN LOAN_INSTALL_FREQ_KEY = 1 THEN 'Weekly'
                                WHEN LOAN_INSTALL_FREQ_KEY = 2 THEN 'BiWeekly'
                                WHEN LOAN_INSTALL_FREQ_KEY = 3 THEN 'Monthly'
                                WHEN LOAN_INSTALL_FREQ_KEY = 4 THEN 'Bimonthly'
                           ELSE 'Others'
                           END ,
                           CASE WHEN LOAN_NUM_INSTAL_BUCKET_NEW_KEY =1 THEN '0_6'
                                WHEN LOAN_NUM_INSTAL_BUCKET_NEW_KEY =2 THEN '7_12'
                                WHEN LOAN_NUM_INSTAL_BUCKET_NEW_KEY =3 THEN '13_24'
                                WHEN LOAN_NUM_INSTAL_BUCKET_NEW_KEY =4 THEN 'GRTR24'
                           ELSE 'NA'
                           END  ,
                           CASE WHEN supress_ind_key=1 THEN 1
                           ELSE 0 END
						   """)
						   
bfilStd1Df.write.mode("overwrite").saveAsTable("hmanalytics.hm_mfi_bfil_aggr")
	
	    
			
			
val bfilStd2Df = spark.sql("""
SELECT    'RPT2' RPT_FMT,                                  
         A.LOAD_YYYYMM QUARTER,
         REV_STD_STATE_CODE STATE,
         REVISED_DIST DISTRICT,
         CAST(STD_PIN_CODE AS INT),
        CASE WHEN NVL(ACTIVE_MFI,0)<=3 THEN '<=3'
        WHEN NVL(ACTIVE_MFI,0) BETWEEN 4 AND 6 THEN '4-6'
        WHEN NVL(ACTIVE_MFI,0) BETWEEN 7 AND 10 THEN '7-10'
        WHEN NVL(ACTIVE_MFI,0) BETWEEN 11 AND 15 THEN '11-15'
        WHEN NVL(ACTIVE_MFI,0) BETWEEN 16 AND 20 THEN '16-20'
        WHEN NVL(ACTIVE_MFI,0) BETWEEN 21 AND 30 THEN '21-30'
        WHEN NVL(ACTIVE_MFI,0) BETWEEN 31 AND 40 THEN '31-40'
        WHEN NVL(ACTIVE_MFI,0)>40 THEN '>40'
        ELSE NULL
        END ACTIVE_MFI_BUCKETS,
        CASE WHEN MOB BETWEEN 0 AND 2 THEN '0-3M'
             WHEN MOB BETWEEN 3 AND 5 THEN '3-6M'
             WHEN MOB BETWEEN 6 AND 11 THEN '6-12M'
             WHEN MOB BETWEEN 12 AND 23 THEN '12-24M'
             WHEN MOB BETWEEN 24 AND 35 THEN '24-36M'
             WHEN MOB BETWEEN 36 AND 47 THEN '36-48M'
             WHEN MOB BETWEEN 48 AND 59 THEN '48-60M'
             WHEN MOB BETWEEN 60 AND 119 THEN '60-120M'
             WHEN MOB >= 120 THEN '>120M'
         ELSE NULL
        END BORROWER_VINTAGE ,
           CASE  WHEN CONTRIBUTOR_ID IN ('MFI0000002','PRB0000016')  THEN 'SELF'
            WHEN CONTRIBUTOR_ID IN ('NBF0000275','MFI0000001','MFI0000009','MFI0000042','MFI0000043','MFI0000053','MFI0000089','MFI0000114','MFI0000009',
            'MFI0000005','SFB0000001','SFB0000002','SFB0000005','SFB0000004','SFB0000003')
            THEN 'SFBs'
            WHEN  CONTRIBUTOR_ID IN ('MFI0000052','NBF0001459','MFI0000109','NBF0001359','NBF0000233','NBF0001581','MFI0000049','MFI0000020',
                                      'MFI0000101','MFI0000038','MFI0000015','MFI0000081','MFI0000217','NBF0001209','MFI0000063','MFI0000002',
                                      'MFI0000018','NBF0000805','MFI0000233','MFI0000221','MFI0000029','MFI0000004','NBF0001762','MFI0000124',
                                      'MFI0000019','MFI0000099','MFI0000044','NBF0001225','MFI0000135','NBF0000833','MFI0000039','MFI0000104',
                                      'NBF0001832','MFI0000040','MFI0000204','MFI0000128','MFI0000201','MFI0000197','MFI0000214','MFI0000146',
                                      'MFI0000116','MFI0000037','MFI0000195','NBF0001775','MFI0000059','MFI0000156','NBF0001448','MFI0000126',
                                      'MFI0000105','MFI0000045','NBF0000830','NBF0001166','MFI0000057','MFI0000068','MFI0000134','MFI0000092',
                                      'NBF0001049','MFI0000231','NBF0001323','NBF0000100','MFI0000089','MFI0000032','MFI0000048','MFI0000055',
                                      'MFI0000071','MFI0000056','MFI0000016','MFI0000060','MFI0000180','NBF0001858','MFI0000021','MFI0000162',
                                      'NBF0001086','MFI0000232','MFI0000008','MFI0000097','MFI0000211','MFI0000106','MFI0000059','MFI0000023',
                                      'MFI0000011','NBF0002173','MFI0000079','MFI0000125','MFI0000094','MFI0000117','MFI0000100','MFI0000159',
                                      'MFI0000115','MFI0000210','MFI0000078','MFI0000025','MFI0000127','NBF0001686','NBF0000515') THEN 'NBFC MFI'
            WHEN CONTRIBUTOR_ID LIKE 'PRB%' OR UPPER(INSTITUTION_NAME) LIKE '%BANK%' THEN 'Banks'
            ELSE 'Others'
            END LENDER_TYPE,
                       'MFI' PRODUCT,
                       ACCOUNT_TYPE,
                       ACCOUNT_NAME, 
                       CASE WHEN LOAN_TICKET_SIZE_KEY <= 2 THEN '<=10K'
                            WHEN LOAN_TICKET_SIZE_KEY=3 THEN '10K-15K'
                            WHEN LOAN_TICKET_SIZE_KEY=4 THEN '15K-20K'
                            WHEN LOAN_TICKET_SIZE_KEY=5 THEN '20K-25K'
                            WHEN LOAN_TICKET_SIZE_KEY=6 THEN '25K-30K'
                            WHEN LOAN_TICKET_SIZE_KEY BETWEEN 7 AND 8 THEN '30K-40K'
                            WHEN LOAN_TICKET_SIZE_KEY BETWEEN 9 AND 12 THEN '40K-60K'
                            WHEN LOAN_TICKET_SIZE_KEY > 12 THEN '60K+'
                       ELSE 'NA'
                       END TICKET_SIZE,
                         CASE WHEN DISB_YYYYMM<=201403  THEN 'UPTO FY 2013-2014'
                                   WHEN DISB_YYYYMM >=201404 THEN FYQ
                                   ELSE 'OTHERS'
                              END DISBURSAL_PERIOD,
                       CASE WHEN LOAN_INSTALL_FREQ_KEY = 1 THEN 'Weekly'
                                WHEN LOAN_INSTALL_FREQ_KEY = 2 THEN 'BiWeekly'
                                WHEN LOAN_INSTALL_FREQ_KEY = 3 THEN 'Monthly'
                                WHEN LOAN_INSTALL_FREQ_KEY = 4 THEN 'Bimonthly'
                           ELSE 'Others'
                           END LOAN_INSTALL_FREQ,
                           CASE WHEN LOAN_NUM_INSTAL_BUCKET_NEW_KEY =1 THEN '0_6'
                                WHEN LOAN_NUM_INSTAL_BUCKET_NEW_KEY =2 THEN '7_12'
                                WHEN LOAN_NUM_INSTAL_BUCKET_NEW_KEY =3 THEN '13_24'
                                WHEN LOAN_NUM_INSTAL_BUCKET_NEW_KEY =4 THEN 'GRTR24'
                           ELSE 'NA'
                           END  TENURE,
                           CASE WHEN supress_ind_key=1 THEN 1
                           ELSE 0 END SUPPRESS_INDICATOR,
                           SUM(TOTAL_ACCOUNTS) TOTAL_LOANS,
                           SUM(CASE WHEN LOAN_ACCOUNT_STATUS_KEY=1 THEN TOTAL_ACCOUNTS ELSE 0 END) TOTAL_ACTIV_LOANS,
                           SUM(TOTAL_DISBURSED_AMOUNT) TOTAL_DISB_AMOUNT,
                           SUM(CASE WHEN LOAN_ACCOUNT_STATUS_KEY=1 THEN TOTAL_DISBURSED_AMOUNT ELSE 0 END) TOTAL_ACTIV_DISB_AMOUNT,
                           SUM(CASE WHEN LOAN_ACCOUNT_STATUS_KEY=1 THEN TOTAL_OUTSTANDING_AMOUNT ELSE 0 END) TOTAL_ACT_OUTSTANDING_AMOUNT,
                           SUM(CASE WHEN LOAN_ACCOUNT_STATUS_KEY=1 AND LOAN_DPD_BUCKET_KEY IN(15) THEN TOTAL_ACCOUNTS ELSE 0 END) LOANS_CURRENT,
                           SUM(CASE WHEN LOAN_ACCOUNT_STATUS_KEY=1 AND LOAN_DPD_BUCKET_KEY IN(1) THEN TOTAL_ACCOUNTS ELSE 0 END) LOANS_NS,
                           SUM(CASE WHEN LOAN_ACCOUNT_STATUS_KEY=1 AND OVERDUE_IND=1 AND LOAN_DPD_BUCKET_KEY IN(2) THEN TOTAL_ACCOUNTS ELSE 0 END) TOTAL_LOAN_DPD_1_30,
                           SUM(CASE WHEN LOAN_ACCOUNT_STATUS_KEY=1 AND OVERDUE_IND=1 AND LOAN_DPD_BUCKET_KEY IN(3) THEN TOTAL_ACCOUNTS ELSE 0 END) TOTAL_LOAN_DPD_31_60,
                           SUM(CASE WHEN LOAN_ACCOUNT_STATUS_KEY=1 AND OVERDUE_IND=1 AND LOAN_DPD_BUCKET_KEY IN(4) THEN TOTAL_ACCOUNTS ELSE 0 END) TOTAL_LOAN_DPD_61_90,
                           SUM(CASE WHEN LOAN_ACCOUNT_STATUS_KEY=1 AND OVERDUE_IND=1 AND LOAN_DPD_BUCKET_KEY IN(3,4,5,6,7) THEN TOTAL_ACCOUNTS ELSE 0 END) TOTAL_LOAN_DPD_31_180,
                           SUM(CASE WHEN LOAN_ACCOUNT_STATUS_KEY=1 AND OVERDUE_IND=1 AND LOAN_DPD_BUCKET_KEY IN(5,6,7) THEN TOTAL_ACCOUNTS ELSE 0 END) TOTAL_LOAN_DPD_91_180,
                           SUM(CASE WHEN LOAN_ACCOUNT_STATUS_KEY=1 AND OVERDUE_IND=1 AND LOAN_DPD_BUCKET_KEY IN(5) THEN TOTAL_ACCOUNTS ELSE 0 END) TOTAL_LOAN_DPD_91_120,
                           SUM(CASE WHEN LOAN_ACCOUNT_STATUS_KEY=1 AND OVERDUE_IND=1 AND LOAN_DPD_BUCKET_KEY IN(6) THEN TOTAL_ACCOUNTS ELSE 0 END) TOTAL_LOAN_DPD_121_150,
                           SUM(CASE WHEN LOAN_ACCOUNT_STATUS_KEY=1 AND OVERDUE_IND=1 AND LOAN_DPD_BUCKET_KEY IN(7) THEN TOTAL_ACCOUNTS ELSE 0 END) TOTAL_LOAN_DPD_151_180,
                           SUM(CASE WHEN LOAN_ACCOUNT_STATUS_KEY=1 AND OVERDUE_IND=1 AND LOAN_DPD_BUCKET_KEY BETWEEN 8 AND 13 THEN TOTAL_ACCOUNTS ELSE 0 END) TOTAL_LOAN_DPD_181_360,
                           SUM(CASE WHEN LOAN_ACCOUNT_STATUS_KEY=1 AND OVERDUE_IND=1 AND LOAN_DPD_BUCKET_KEY IN(14)THEN TOTAL_ACCOUNTS ELSE 0 END) TOTAL_LOAN_DPD_GRTR_360,
                           SUM(CASE WHEN LOAN_ACCOUNT_STATUS_KEY=1 AND LOAN_DPD_BUCKET_KEY IN(15) THEN TOTAL_OUTSTANDING_AMOUNT ELSE 0 END) POS_CURRENT,
                           SUM(CASE WHEN LOAN_ACCOUNT_STATUS_KEY=1 AND LOAN_DPD_BUCKET_KEY IN(1) THEN TOTAL_OUTSTANDING_AMOUNT ELSE 0 END) POS_NS,
                           SUM(CASE WHEN LOAN_ACCOUNT_STATUS_KEY=1 AND OVERDUE_IND=1 AND LOAN_DPD_BUCKET_KEY IN(2) THEN TOTAL_OUTSTANDING_AMOUNT ELSE 0 END) OUTSTANDING_AMT_1_30,
                           SUM(CASE WHEN LOAN_ACCOUNT_STATUS_KEY=1 AND OVERDUE_IND=1 AND LOAN_DPD_BUCKET_KEY IN(3) THEN TOTAL_OUTSTANDING_AMOUNT ELSE 0 END) OUTSTANDING_AMT_31_60,
                           SUM(CASE WHEN LOAN_ACCOUNT_STATUS_KEY=1 AND OVERDUE_IND=1 AND LOAN_DPD_BUCKET_KEY IN(4) THEN TOTAL_OUTSTANDING_AMOUNT ELSE 0 END) OUTSTANDING_AMT_61_90,
                           SUM(CASE WHEN LOAN_ACCOUNT_STATUS_KEY=1 AND OVERDUE_IND=1 AND LOAN_DPD_BUCKET_KEY IN(3,4,5,6,7) THEN TOTAL_OUTSTANDING_AMOUNT ELSE 0 END) OUTSTANDING_AMT_31_180,
                           SUM(CASE WHEN LOAN_ACCOUNT_STATUS_KEY=1 AND OVERDUE_IND=1 AND LOAN_DPD_BUCKET_KEY IN(5,6,7) THEN TOTAL_OUTSTANDING_AMOUNT ELSE 0 END) OUTSTANDING_AMT_91_180,
                           SUM(CASE WHEN LOAN_ACCOUNT_STATUS_KEY=1 AND OVERDUE_IND=1 AND LOAN_DPD_BUCKET_KEY IN(5) THEN TOTAL_OUTSTANDING_AMOUNT ELSE 0 END) OUTSTANDING_AMT_91_120,
                           SUM(CASE WHEN LOAN_ACCOUNT_STATUS_KEY=1 AND OVERDUE_IND=1 AND LOAN_DPD_BUCKET_KEY IN(6) THEN TOTAL_OUTSTANDING_AMOUNT ELSE 0 END) OUTSTANDING_AMT_121_150,
                           SUM(CASE WHEN LOAN_ACCOUNT_STATUS_KEY=1 AND OVERDUE_IND=1 AND LOAN_DPD_BUCKET_KEY IN(7) THEN TOTAL_OUTSTANDING_AMOUNT ELSE 0 END) OUTSTANDING_AMT_151_180,
                           SUM(CASE WHEN LOAN_ACCOUNT_STATUS_KEY=1 AND OVERDUE_IND=1 AND LOAN_DPD_BUCKET_KEY BETWEEN 8 AND 13 THEN TOTAL_OUTSTANDING_AMOUNT ELSE 0 END) OUTSTANDING_AMT_181_360,
                           SUM(CASE WHEN LOAN_ACCOUNT_STATUS_KEY=1 AND OVERDUE_IND=1 AND LOAN_DPD_BUCKET_KEY IN(14)THEN TOTAL_OUTSTANDING_AMOUNT ELSE 0 END) OUTSTANDING_AMT_GRTR_360,
                           SUM(CASE WHEN from_unixtime(unix_timestamp(disb_yyyymm ,'yyyyMM'),'yyyy-MM-dd') BETWEEN add_months(from_unixtime(unix_timestamp(A.LOAD_YYYYMM ,'yyyyMM'),'yyyy-MM-dd'),-6) AND from_unixtime(unix_timestamp(A.LOAD_YYYYMM ,'yyyyMM'),'yyyy-MM-dd') THEN TOTAL_ACCOUNTS ELSE 0 END) TOTAL_LOANS_6M,
                            SUM(CASE WHEN from_unixtime(unix_timestamp(disb_yyyymm ,'yyyyMM'),'yyyy-MM-dd') BETWEEN add_months(from_unixtime(unix_timestamp(A.LOAD_YYYYMM ,'yyyyMM'),'yyyy-MM-dd'),-6) AND from_unixtime(unix_timestamp(A.LOAD_YYYYMM ,'yyyyMM'),'yyyy-MM-dd')  THEN TOTAL_DISBURSED_AMOUNT ELSE 0 END) TOTAL_DISBURSED_AMOUNT_6M,
                            SUM(CASE WHEN from_unixtime(unix_timestamp(disb_yyyymm ,'yyyyMM'),'yyyy-MM-dd') BETWEEN add_months(from_unixtime(unix_timestamp(A.LOAD_YYYYMM ,'yyyyMM'),'yyyy-MM-dd'),-12) AND from_unixtime(unix_timestamp(A.LOAD_YYYYMM ,'yyyyMM'),'yyyy-MM-dd')  THEN TOTAL_ACCOUNTS ELSE 0 END) LOANS_DISB_IN_LAST_12_MONTHS,
                            SUM(CASE WHEN from_unixtime(unix_timestamp(disb_yyyymm ,'yyyyMM'),'yyyy-MM-dd') BETWEEN add_months(from_unixtime(unix_timestamp(A.LOAD_YYYYMM ,'yyyyMM'),'yyyy-MM-dd'),-12) AND from_unixtime(unix_timestamp(A.LOAD_YYYYMM ,'yyyyMM'),'yyyy-MM-dd')  THEN TOTAL_DISBURSED_AMOUNT ELSE 0 END) AMT_DISB_IN_LAST_12_MONTHS,
                           SUM(CASE WHEN from_unixtime(unix_timestamp(disb_yyyymm ,'yyyyMM'),'yyyy-MM-dd') BETWEEN add_months(from_unixtime(unix_timestamp(A.LOAD_YYYYMM ,'yyyyMM'),'yyyy-MM-dd'),-24) AND from_unixtime(unix_timestamp(A.LOAD_YYYYMM ,'yyyyMM'),'yyyy-MM-dd')  THEN TOTAL_ACCOUNTS ELSE 0 END) LOANS_DISB_IN_LAST_24_MONTHS,
                            SUM(CASE WHEN from_unixtime(unix_timestamp(disb_yyyymm ,'yyyyMM'),'yyyy-MM-dd') BETWEEN add_months(from_unixtime(unix_timestamp(A.LOAD_YYYYMM ,'yyyyMM'),'yyyy-MM-dd'),-24) AND from_unixtime(unix_timestamp(A.LOAD_YYYYMM ,'yyyyMM'),'yyyy-MM-dd')  THEN TOTAL_DISBURSED_AMOUNT ELSE 0 END) AMT_DISB_IN_LAST_24_MONTHS,
                           SUM(CASE WHEN DAS_ID IN (6,10) THEN TOTAL_ACCOUNTS ELSE 0 END) TOTAL_WRITTEN_OFF_LOANS,
                           SUM(CASE WHEN DAS_ID IN (6,10) THEN TOTAL_WRITE_OFF_AMOUNT ELSE 0 END) TOTAL_WRITTEN_OFF_AMOUNT
                      FROM  (   
                             SELECT A.*,
                                     B.REV_STD_STATE_CODE,
                                     B.REVISED_DIST,
                                     B.STD_PIN_CODE,
                                     ACTIVE_MFI,
									 MONTHS_BETWEEN(from_unixtime(unix_timestamp(A.LOAD_YYYYMM ,'yyyyMM'),'yyyy-MM-dd') ,from_unixtime(unix_timestamp(A.min_disb_yyyymm ,'yyyyMM'), 'yyyy-MM-dd')) MOB,
                                     C.CONTRIBUTOR_ID,
                                     C.INSTITUTION_NAME,
                                     E.FYQ,
                                     F.ACCOUNT_NAME,
                                     F.ACCOUNT_TYPE
                               FROM (SELECT A.*
                                       FROM HMANALYTICS.HM_MFI_ACT_FACT_TBL A
                                      WHERE CONTRI_KEY NOT IN (1019) AND LOAD_YYYYMM= '"""+load_dt+ """'
                                     ) A
                               JOIN (SELECT 
                                      REV_STD_STATE_CODE,
                                      REVISED_DIST,
                                   STD_PIN_CODE,
								   SDP_KEY,
                                   count(DISTINCT CASE WHEN TOTAL_CONSUMERS >=10  AND TOTAL_ACTIVE_CONSUMERS>0 THEN  CONTRI_KEY  ELSE NULL END) ACTIVE_MFI
                                   FROM
                                   activMfiDF K
                                      JOIN hmanalytics.HM_SDP_DIM2_V1 Z
                                      ON (K.SDP_KEY=Z.KEY
                                          AND Z.ACTIVE=1)
                                       group by 
                                      REV_STD_STATE_CODE,
                                      REVISED_DIST,
									  SDP_KEY,
                                   STD_PIN_CODE) B
                                   ON (A.SDP_KEY = B.SDP_KEY)
                               JOIN HMANALYTICS.HM_CONTRIBUTOR_DIM C
                                 ON (C.ACTIVE=1
                                     AND A.CONTRI_KEY =C.CONTRI_KEY
                                     )
                          LEFT JOIN HMANALYTICS.HM_DATE_DIM2 E
                                 ON (CAST(A.DISB_YYYYMM AS INT) =E.YYYYMM)
                    LEFT OUTER JOIN (SELECT LOAN_ACCOUNT_TYPE_KEY,ACCOUNT_TYPE,ACCOUNT_NAME 
                                       FROM hmanalytics.HM_LOAN_ACCOUNT_TYPE_DIM
                                      WHERE BUREAU_FLAG='MFI'
                                        AND ACTIVE=1 
                                     ) F
                                 ON (A.LOAN_ACCOUNT_TYPE_KEY=F.LOAN_ACCOUNT_TYPE_KEY)     
                            ) A
                       WHERE REV_STD_STATE_CODE IS NOT NULL
               GROUP BY   A.LOAD_YYYYMM ,
         REV_STD_STATE_CODE ,
         REVISED_DIST ,
         STD_PIN_CODE,
        CASE WHEN NVL(ACTIVE_MFI,0)<=3 THEN '<=3'
        WHEN NVL(ACTIVE_MFI,0) BETWEEN 4 AND 6 THEN '4-6'
        WHEN NVL(ACTIVE_MFI,0) BETWEEN 7 AND 10 THEN '7-10'
        WHEN NVL(ACTIVE_MFI,0) BETWEEN 11 AND 15 THEN '11-15'
        WHEN NVL(ACTIVE_MFI,0) BETWEEN 16 AND 20 THEN '16-20'
        WHEN NVL(ACTIVE_MFI,0) BETWEEN 21 AND 30 THEN '21-30'
        WHEN NVL(ACTIVE_MFI,0) BETWEEN 31 AND 40 THEN '31-40'
        WHEN NVL(ACTIVE_MFI,0)>40 THEN '>40'
        ELSE NULL
        END ,
        CASE WHEN MOB BETWEEN 0 AND 2 THEN '0-3M'
             WHEN MOB BETWEEN 3 AND 5 THEN '3-6M'
             WHEN MOB BETWEEN 6 AND 11 THEN '6-12M'
             WHEN MOB BETWEEN 12 AND 23 THEN '12-24M'
             WHEN MOB BETWEEN 24 AND 35 THEN '24-36M'
             WHEN MOB BETWEEN 36 AND 47 THEN '36-48M'
             WHEN MOB BETWEEN 48 AND 59 THEN '48-60M'
             WHEN MOB BETWEEN 60 AND 119 THEN '60-120M'
             WHEN MOB >= 120 THEN '>120M'
         ELSE NULL
        END  ,
           CASE  WHEN CONTRIBUTOR_ID IN ('MFI0000002','PRB0000016')  THEN 'SELF'
            WHEN CONTRIBUTOR_ID IN ('NBF0000275','MFI0000001','MFI0000009','MFI0000042','MFI0000043','MFI0000053','MFI0000089','MFI0000114','MFI0000009',
            'MFI0000005','SFB0000001','SFB0000002','SFB0000005','SFB0000004','SFB0000003')
            THEN 'SFBs'
            WHEN  CONTRIBUTOR_ID IN ('MFI0000052','NBF0001459','MFI0000109','NBF0001359','NBF0000233','NBF0001581','MFI0000049','MFI0000020',
                                      'MFI0000101','MFI0000038','MFI0000015','MFI0000081','MFI0000217','NBF0001209','MFI0000063','MFI0000002',
                                      'MFI0000018','NBF0000805','MFI0000233','MFI0000221','MFI0000029','MFI0000004','NBF0001762','MFI0000124',
                                      'MFI0000019','MFI0000099','MFI0000044','NBF0001225','MFI0000135','NBF0000833','MFI0000039','MFI0000104',
                                      'NBF0001832','MFI0000040','MFI0000204','MFI0000128','MFI0000201','MFI0000197','MFI0000214','MFI0000146',
                                      'MFI0000116','MFI0000037','MFI0000195','NBF0001775','MFI0000059','MFI0000156','NBF0001448','MFI0000126',
                                      'MFI0000105','MFI0000045','NBF0000830','NBF0001166','MFI0000057','MFI0000068','MFI0000134','MFI0000092',
                                      'NBF0001049','MFI0000231','NBF0001323','NBF0000100','MFI0000089','MFI0000032','MFI0000048','MFI0000055',
                                      'MFI0000071','MFI0000056','MFI0000016','MFI0000060','MFI0000180','NBF0001858','MFI0000021','MFI0000162',
                                      'NBF0001086','MFI0000232','MFI0000008','MFI0000097','MFI0000211','MFI0000106','MFI0000059','MFI0000023',
                                      'MFI0000011','NBF0002173','MFI0000079','MFI0000125','MFI0000094','MFI0000117','MFI0000100','MFI0000159',
                                      'MFI0000115','MFI0000210','MFI0000078','MFI0000025','MFI0000127','NBF0001686','NBF0000515') THEN 'NBFC MFI'
            WHEN CONTRIBUTOR_ID LIKE 'PRB%' OR UPPER(INSTITUTION_NAME) LIKE '%BANK%' THEN 'Banks'
            ELSE 'Others'
            END  ,
                        'MFI',
                       ACCOUNT_TYPE,
                       ACCOUNT_NAME, 
                       CASE WHEN LOAN_TICKET_SIZE_KEY <= 2 THEN '<=10K'
                            WHEN LOAN_TICKET_SIZE_KEY=3 THEN '10K-15K'
                            WHEN LOAN_TICKET_SIZE_KEY=4 THEN '15K-20K'
                            WHEN LOAN_TICKET_SIZE_KEY=5 THEN '20K-25K'
                            WHEN LOAN_TICKET_SIZE_KEY=6 THEN '25K-30K'
                            WHEN LOAN_TICKET_SIZE_KEY BETWEEN 7 AND 8 THEN '30K-40K'
                            WHEN LOAN_TICKET_SIZE_KEY BETWEEN 9 AND 12 THEN '40K-60K'
                            WHEN LOAN_TICKET_SIZE_KEY > 12 THEN '60K+'
                       ELSE 'NA'
                       END ,
                         CASE WHEN DISB_YYYYMM<=201403  THEN 'UPTO FY 2013-2014'
                                   WHEN DISB_YYYYMM >=201404 THEN FYQ
                                   ELSE 'OTHERS'
                              END ,
                       CASE WHEN LOAN_INSTALL_FREQ_KEY = 1 THEN 'Weekly'
                                WHEN LOAN_INSTALL_FREQ_KEY = 2 THEN 'BiWeekly'
                                WHEN LOAN_INSTALL_FREQ_KEY = 3 THEN 'Monthly'
                                WHEN LOAN_INSTALL_FREQ_KEY = 4 THEN 'Bimonthly'
                           ELSE 'Others'
                           END ,
                           CASE WHEN LOAN_NUM_INSTAL_BUCKET_NEW_KEY =1 THEN '0_6'
                                WHEN LOAN_NUM_INSTAL_BUCKET_NEW_KEY =2 THEN '7_12'
                                WHEN LOAN_NUM_INSTAL_BUCKET_NEW_KEY =3 THEN '13_24'
                                WHEN LOAN_NUM_INSTAL_BUCKET_NEW_KEY =4 THEN 'GRTR24'
                           ELSE 'NA'
                           END  ,
                           CASE WHEN supress_ind_key=1 THEN 1
                           ELSE 0 END
						   """)
	bfilStd2Df.write.mode("append").saveAsTable("hmanalytics.hm_mfi_bfil_aggr")
	
	
	val std1DF = spark.sql("""
SELECT  date_format(last_day(from_unixtime(unix_timestamp(QUARTER,'yyyyMM'),'yyyy-MM-dd')),'dd-MMM-yy') Book_Closing_Period,
        STATE,
        DISTRICT,
        STD_PIN_CODE Pincode,
        ACTIVE_MFI_BUCKETS Active_Lenders,
		borrower_vintage,
        LENDER_TYPE,
        TICKET_SIZE,
        DISBURSAL_PERIOD,
        TENURE,
        LOAN_INSTALL_FREQ Repayment_Frequency,
        SUM(TOTAL_LOANS) Loans_Sanctioned,
        SUM(TOTAL_ACTIV_LOANS) Live_Loans,
        SUM(TOTAL_DISB_AMOUNT) Total_Sanctioned_Amount,
        SUM(TOTAL_ACTIV_DISB_AMOUNT) Active_Sanctioned_Amount,
        SUM(TOTAL_ACT_OUTSTANDING_AMOUNT) Portfolio_Outstanding,
        sum(LOANS_CURRENT) Loans_STD,
        SUM(TOTAL_LOAN_DPD_1_30) Loans_1_30,
        SUM(TOTAL_LOAN_DPD_31_60) Loans_31_60,
        SUM(TOTAL_LOAN_DPD_61_90) Loans_61_90,
        SUM(TOTAL_LOAN_DPD_31_60+TOTAL_LOAN_DPD_61_90+TOTAL_LOAN_DPD_91_180) Loans_31_180,
        SUM(TOTAL_LOAN_DPD_91_180) Loans_91_180,
        SUM(TOTAL_LOAN_DPD_181_360+TOTAL_LOAN_DPD_GRTR_360) Loans_180plus,
        SUM(TOTAL_LOAN_DPD_181_360) Loans_181_360,
        SUM(TOTAL_LOAN_DPD_GRTR_360) Loans_360plus,
        SUM(LOANS_NS) Loans_Not_Stipualted,
        sum(POS_CURRENT) Portfolio_STD,
        SUM(OUTSTANDING_AMT_1_30) PAR_1_30,
        SUM(OUTSTANDING_AMT_31_60) PAR_31_60,
        SUM(OUTSTANDING_AMT_61_90) PAR_61_90,
        SUM(OUTSTANDING_AMT_31_60+OUTSTANDING_AMT_61_90+OUTSTANDING_AMT_91_180) PAR_31_180,
        SUM(OUTSTANDING_AMT_91_180) PAR_91_180,
        SUM(OUTSTANDING_AMT_181_360+OUTSTANDING_AMT_GRTR_360) PAR_180plus,
        SUM(OUTSTANDING_AMT_181_360) PAR_181_360,
        SUM(OUTSTANDING_AMT_GRTR_360) PAR_360plus,
        SUM(POS_NS) PAR_Not_Stipulated,
        SUM(TOTAL_LOANS_6M) Loans_6M,
        SUM(TOTAL_DISBURSED_AMOUNT_6M) Amount_6M,
        SUM(LOANS_DISB_IN_LAST_12_MONTHS) Loans_12M,
        SUM(AMT_DISB_IN_LAST_12_MONTHS) Amount_12M,
        SUM(LOANS_DISB_IN_LAST_24_MONTHS) Loans_24M,
        SUM(AMT_DISB_IN_LAST_24_MONTHS) Amount_24M,
        SUM(TOTAL_WRITTEN_OFF_LOANS) Total_Written_Off_Loans,
        SUM(TOTAL_WRITTEN_OFF_AMOUNT) Total_Written_Off_Amount

   FROM hmanalytics.hm_mfi_bfil_aggr
  WHERE QUARTER ='"""+load_dt+ """'
    AND SUPPRESS_INDICATOR <> 1 AND RPT_FMT='RPT1'
  GROUP BY QUARTER,
        STATE,
        DISTRICT,
        STD_PIN_CODE,
        ACTIVE_MFI_BUCKETS,
		borrower_vintage,
        LENDER_TYPE,
        TICKET_SIZE,
        DISBURSAL_PERIOD,
        TENURE,
        LOAN_INSTALL_FREQ
""")  
      
std1DF.orderBy(col("STATE")).repartition(1).write.mode("overwrite").option("header", "true").option("delimiter", "|").csv("/tmp/prateek/BFIL")
file = fs.globStatus(new Path("/tmp/prateek/BFIL/part*"))(0).getPath().getName()

fs.rename(new Path("/tmp/prateek/BFIL/"+file), new Path("/tmp/prateek/BFIL_NEW/CrifHighmark-BFIL-MFI_Report1_"+load_dt+".csv"))


val std2DF = spark.sql("""
SELECT  date_format(last_day(from_unixtime(unix_timestamp(QUARTER,'yyyyMM'),'yyyy-MM-dd')),'dd-MMM-yy') Book_Closing_Period,
        STATE,
        DISTRICT,
        STD_PIN_CODE Pincode,
        ACTIVE_MFI_BUCKETS Active_Lenders,
		borrower_vintage,
        LENDER_TYPE,
        TICKET_SIZE,
        DISBURSAL_PERIOD,
        TENURE,
        LOAN_INSTALL_FREQ Repayment_Frequency,
        SUM(TOTAL_LOANS) Loans_Sanctioned,
        SUM(TOTAL_ACTIV_LOANS) Live_Loans,
        SUM(TOTAL_DISB_AMOUNT) Total_Sanctioned_Amount,
        SUM(TOTAL_ACTIV_DISB_AMOUNT) Active_Sanctioned_Amount,
        SUM(TOTAL_ACT_OUTSTANDING_AMOUNT) Portfolio_Outstanding,
        sum(LOANS_CURRENT) Loans_STD,
        SUM(TOTAL_LOAN_DPD_1_30) Loans_1_30,
        SUM(TOTAL_LOAN_DPD_31_60) Loans_31_60,
        SUM(TOTAL_LOAN_DPD_61_90) Loans_61_90,
        SUM(TOTAL_LOAN_DPD_31_60+TOTAL_LOAN_DPD_61_90+TOTAL_LOAN_DPD_91_180) Loans_31_180,
        SUM(TOTAL_LOAN_DPD_91_180) Loans_91_180,
        SUM(TOTAL_LOAN_DPD_181_360+TOTAL_LOAN_DPD_GRTR_360) Loans_180plus,
        SUM(TOTAL_LOAN_DPD_181_360) Loans_181_360,
        SUM(TOTAL_LOAN_DPD_GRTR_360) Loans_360plus,
        SUM(LOANS_NS) Loans_Not_Stipualted,
        sum(POS_CURRENT) Portfolio_STD,
        SUM(OUTSTANDING_AMT_1_30) PAR_1_30,
        SUM(OUTSTANDING_AMT_31_60) PAR_31_60,
        SUM(OUTSTANDING_AMT_61_90) PAR_61_90,
        SUM(OUTSTANDING_AMT_31_60+OUTSTANDING_AMT_61_90+OUTSTANDING_AMT_91_180) PAR_31_180,
        SUM(OUTSTANDING_AMT_91_180) PAR_91_180,
        SUM(OUTSTANDING_AMT_181_360+OUTSTANDING_AMT_GRTR_360) PAR_180plus,
        SUM(OUTSTANDING_AMT_181_360) PAR_181_360,
        SUM(OUTSTANDING_AMT_GRTR_360) PAR_360plus,
        SUM(POS_NS) PAR_Not_Stipulated,
        SUM(TOTAL_LOANS_6M) Loans_6M,
        SUM(TOTAL_DISBURSED_AMOUNT_6M) Amount_6M,
        SUM(LOANS_DISB_IN_LAST_12_MONTHS) Loans_12M,
        SUM(AMT_DISB_IN_LAST_12_MONTHS) Amount_12M,
        SUM(LOANS_DISB_IN_LAST_24_MONTHS) Loans_24M,
        SUM(AMT_DISB_IN_LAST_24_MONTHS) Amount_24M,
        SUM(TOTAL_WRITTEN_OFF_LOANS) Total_Written_Off_Loans,
        SUM(TOTAL_WRITTEN_OFF_AMOUNT) Total_Written_Off_Amount

   FROM hmanalytics.hm_mfi_bfil_aggr
  WHERE QUARTER = '"""+load_dt+ """'
    AND SUPPRESS_INDICATOR <> 1 AND RPT_FMT='RPT2'
  GROUP BY QUARTER,
        STATE,
        DISTRICT,
        STD_PIN_CODE,
        ACTIVE_MFI_BUCKETS,
		borrower_vintage,
        LENDER_TYPE,
        TICKET_SIZE,
        DISBURSAL_PERIOD,
        TENURE,
        LOAN_INSTALL_FREQ
""")  
      
		
std2DF.orderBy(col("STATE")).repartition(1).write.mode("overwrite").option("header", "true").option("delimiter", "|").csv("/tmp/prateek/BFIL")
file = fs.globStatus(new Path("/tmp/prateek/BFIL/part*"))(0).getPath().getName()

fs.rename(new Path("/tmp/prateek/BFIL/" +file), new Path("/tmp/prateek/BFIL_NEW/CrifHighmark-BFIL-MFI_Report2_"+load_dt+".csv"))


  }
}