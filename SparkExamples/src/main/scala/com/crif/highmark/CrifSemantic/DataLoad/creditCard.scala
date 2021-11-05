package com.crif.highmark.CrifSemantic.DataLoad

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import scala.concurrent.{Await, Future}
import scala.concurrent.duration._
import scala.concurrent.ExecutionContext.Implicits.global


object creditCard extends App {
 
val spark = SparkSession.builder().appName("CreditCard")
.config("hive.metastore.uris", "thrift://chmcisprbdmn01.chm.intra:9083")
.config("hive.exec.dynamic.partition", "true")
.config("hive.exec.dynamic.partition.mode", "nonstrict")
.config("hive.enforce.bucketing", "true")
.config("hive.exec.max.dynamic.partitions", "20000")
.enableHiveSupport().getOrCreate()

val argument = spark.sparkContext.getConf.get("spark.driver.args").split("\\s+")

val self=argument(0)
val end_dt=argument(1)
val mon_rn=argument(2)


val trendDataDF = spark.sql(s"""
SELECT 
  cast(
    CASE WHEN regexp_extract(A.DAYS_PAST_DUE, '[0-9]+', 0) IS NOT NULL 
    AND TRIM(A.ASSET_CLASS) IS NULL THEN nvl(
      cast(
        regexp_extract(A.DAYS_PAST_DUE, '[0-9]+', 0) as int
      ), 
      0
    ) WHEN regexp_extract(A.DAYS_PAST_DUE, '[0-9]+', 0) IS NULL 
    AND TRIM(A.ASSET_CLASS) IS NOT NULL THEN CASE WHEN TRIM(A.ASSET_CLASS) = 'L01' THEN 0 WHEN TRIM(A.ASSET_CLASS) IN ('L02') THEN 91 WHEN TRIM(A.ASSET_CLASS) IN ('L03', 'L04') THEN 361 WHEN TRIM(A.ASSET_CLASS) IN ('L05') THEN 1 END WHEN regexp_extract(A.DAYS_PAST_DUE, '[0-9]+', 0) IS NOT NULL 
    AND TRIM(A.ASSET_CLASS) IS NOT NULL THEN CASE WHEN NVL(
      cast(
        CASE WHEN TRIM(A.ASSET_CLASS) = 'L01' THEN 0 WHEN TRIM(A.ASSET_CLASS) IN ('L02') THEN 91 WHEN TRIM(A.ASSET_CLASS) IN ('L03', 'L04') THEN 361 WHEN TRIM(A.ASSET_CLASS) IN ('L05') THEN 1 END as int
      ), 
      0
    ) <= nvl(
      cast(
        regexp_extract(A.DAYS_PAST_DUE, '[0-9]+', 0) as int
      ), 
      0
    ) THEN nvl(
      cast(
        regexp_extract(A.DAYS_PAST_DUE, '[0-9]+', 0) as int
      ), 
      0
    ) ELSE NVL(
      cast(
        CASE WHEN TRIM(A.ASSET_CLASS) = 'L01' THEN 0 WHEN TRIM(A.ASSET_CLASS) IN ('L02') THEN 91 WHEN TRIM(A.ASSET_CLASS) IN ('L03', 'L04') THEN 361 WHEN TRIM(A.ASSET_CLASS) IN ('L05') THEN 1 END as int
      ), 
      0
    ) END WHEN regexp_extract(A.DAYS_PAST_DUE, '[0-9]+', 0) IS NULL 
    AND TRIM(A.ASSET_CLASS) IS NULL THEN CASE WHEN A.DAS = 'S04' THEN 0 WHEN A.DAS = 'S05' THEN 1 ELSE 0 END END as int
  ) DERIVED_DPD, 
  A.DAYS_PAST_DUE, 
  A.MFI_ID, 
  LAST_DAY(cast(A.ACT_REPORTED_DT as date)) ACT_REPORTED_DT, 
  NVL(
    ABS(A.CURRENT_BALANCE), 
    0
  ) CURRENT_BALANCE, 
  NVL(
    CAST(
      ABS(A.AMOUNT_OVERDUE_TOTAL) as double
    ), 
    0
  ) AMOUNT_OVERDUE_TOTAL, 
  ABS(A.CHARGEOFF_AMT) CHARGEOFF_AMT, 
  A.DAS DAS, 
  CLOSED_DT CLOSED_DT, 
  A.ACCOUNT_KEY, 
  CASE WHEN CLOSED_DT IS NULL 
  OR CLOSED_DT > cast(A.ACT_REPORTED_DT as date) THEN 'Y' ELSE 'N' END NOT_CLOSED_IND, 
  CASE WHEN ABS(
    COALESCE(
      B.HIGH_CREDIT, B.DISBURSED_AMOUNT, 
      B.CREDIT_LIMIT, A.CURRENT_BALANCE
    )
  )<= ABS(A.CURRENT_BALANCE) THEN ABS(A.CURRENT_BALANCE) ELSE ABS(
    COALESCE(
      B.HIGH_CREDIT, B.DISBURSED_AMOUNT, 
      B.CREDIT_LIMIT, A.CURRENT_BALANCE
    )
  ) END SANCTIONED_AMOUNT, 
  B.ACCOUNT_TYPE ACCOUNT_TYPE, 
  LAST_DAY(B.DISBURSED_DT) DISBURSED_DT, 
  MONTHS_BETWEEN(
    LAST_DAY(A.ACT_REPORTED_DT), 
    LAST_DAY(B.DISBURSED_DT)
  ) MOB

FROM 
  hmanalytics.hm_mfi_account_trend A 
  JOIN hmanalytics.hm_cns_account_prod  B ON(
    A.ACCOUNT_KEY = B.ACCOUNT_KEY 
    AND B.ACCOUNT_TYPE = 'A15'
    AND B.ACTIVE = 1 
    AND B.SUPPRESS_INDICATOR is null
  
    AND cast(A.ACT_REPORTED_DT as date) <= '"""+end_dt+"""'
 )
""")

trendDataDF.createOrReplaceTempView("tempTrend")

val trendDF = spark.sql(s"""
select distinct
A.*,B.city,B.max_clst_id
from tempTrend A
Left Outer JOIN 
(select ACCOUNT_KEY,
COALESCE(C.std_derived_city,C.std_city_1,C.std_city_2) city,
COALESCE(max_clst_id,consumer_key) max_clst_id,
ownership_ind
from hmanalytics.hm_cns_unique_row C
 LEFT OUTER JOIN 
(SELECT distinct candidate_id,max_clst_id FROM hmanalytics.hm_cns_clst_"""+mon_rn+""") D
ON(C.CONSUMER_KEY=D.candidate_id)
) B
ON(A.ACCOUNT_KEY = B.ACCOUNT_KEY
)
WHERE B.ownership_ind <> 'O04'
""")



val prodDataDF = spark.sql(s"""
SELECT 
      cast(
        CASE WHEN regexp_extract(DAYS_PAST_DUE, '[0-9]+', 0) IS NOT NULL 
        AND TRIM(ASSET_CLASS) IS NULL THEN nvl(
          cast(
            regexp_extract(DAYS_PAST_DUE, '[0-9]+', 0) as int
          ), 
          0
        ) WHEN regexp_extract(DAYS_PAST_DUE, '[0-9]+', 0) IS NULL 
        AND TRIM(ASSET_CLASS) IS NOT NULL THEN CASE WHEN TRIM(ASSET_CLASS) = 'L01' THEN 0 WHEN TRIM(ASSET_CLASS) IN ('L02') THEN 91 WHEN TRIM(ASSET_CLASS) IN ('L03', 'L04') THEN 361 WHEN TRIM(ASSET_CLASS) IN ('L05') THEN 1 END WHEN regexp_extract(DAYS_PAST_DUE, '[0-9]+', 0) IS NOT NULL 
        AND TRIM(ASSET_CLASS) IS NOT NULL THEN CASE WHEN NVL(
          cast(
            CASE WHEN TRIM(ASSET_CLASS) = 'L01' THEN 0 WHEN TRIM(ASSET_CLASS) IN ('L02') THEN 91 WHEN TRIM(ASSET_CLASS) IN ('L03', 'L04') THEN 361 WHEN TRIM(ASSET_CLASS) IN ('L05') THEN 1 END as int
          ), 
          0
        ) <= nvl(
          cast(
            regexp_extract(DAYS_PAST_DUE, '[0-9]+', 0) as int
          ), 
          0
        ) THEN nvl(
          cast(
            regexp_extract(DAYS_PAST_DUE, '[0-9]+', 0) as int
          ), 
          0
        ) ELSE NVL(
          cast(
            CASE WHEN TRIM(ASSET_CLASS) = 'L01' THEN 0 WHEN TRIM(ASSET_CLASS) IN ('L02') THEN 91 WHEN TRIM(ASSET_CLASS) IN ('L03', 'L04') THEN 361 WHEN TRIM(ASSET_CLASS) IN ('L05') THEN 1 END as int
          ), 
          0
        ) END WHEN regexp_extract(DAYS_PAST_DUE, '[0-9]+', 0) IS NULL 
        AND TRIM(ASSET_CLASS) IS NULL THEN CASE WHEN DAS = 'S04' THEN 0 WHEN DAS = 'S05' THEN 1 ELSE 0 END END as int
      ) DERIVED_DPD, 
      DAYS_PAST_DUE, 
      MFI_ID, 
      LAST_DAY(ACT_REPORTED_DT) ACT_REPORTED_DT, 
      NVL(
        ABS(CURRENT_BALANCE), 
        0
      ) CURRENT_BALANCE, 
      NVL(
        CAST(
          ABS(AMOUNT_OVERDUE_TOTAL) as double
        ), 
        0
      ) AMOUNT_OVERDUE_TOTAL, 
      ABS(CHARGEOFF_AMT) CHARGEOFF_AMT, 
      DAS, 
      CLOSED_DT CLOSED_DT, 
      ACCOUNT_KEY, 
      CASE WHEN CLOSED_DT IS NULL 
      OR CLOSED_DT > ACT_REPORTED_DT THEN 'Y' ELSE 'N' END NOT_CLOSED_IND, 
      CASE WHEN ABS(
        COALESCE(
          HIGH_CREDIT, DISBURSED_AMOUNT, CREDIT_LIMIT, 
          CURRENT_BALANCE
        )
      )<= ABS(CURRENT_BALANCE) THEN ABS(CURRENT_BALANCE) ELSE ABS(
        COALESCE(
          HIGH_CREDIT, DISBURSED_AMOUNT, CREDIT_LIMIT, 
          CURRENT_BALANCE
        )
      ) END SANCTIONED_AMOUNT, 
      ACCOUNT_TYPE, 
      LAST_DAY(DISBURSED_DT) DISBURSED_DT, 
      MONTHS_BETWEEN(
        LAST_DAY(ACT_REPORTED_DT), 
        LAST_DAY(DISBURSED_DT)
      ) MOB

    FROM 
      hmanalytics.hm_cns_account_prod 
 
    WHERE 
      ACCOUNT_TYPE = 'A15'
      AND ACTIVE = 1 
      AND SUPPRESS_INDICATOR is null

""")

prodDataDF.createOrReplaceTempView("tempProd")

val prodDF = spark.sql(s"""
select distinct
A.*,B.city,B.max_clst_id
from tempProd A
Left Outer JOIN 
(select ACCOUNT_KEY,
COALESCE(C.std_derived_city,C.std_city_1,C.std_city_2) city,
COALESCE(max_clst_id,consumer_key) max_clst_id,
ownership_ind
from hmanalytics.hm_cns_unique_row C
LEFT OUTER JOIN 
(SELECT distinct candidate_id,max_clst_id FROM hmanalytics.hm_cns_clst_"""+mon_rn+""") D
ON(C.CONSUMER_KEY=D.candidate_id)
) B
ON(A.ACCOUNT_KEY = B.ACCOUNT_KEY
)
WHERE B.ownership_ind <> 'O04'
""")

val thresDF = spark.sql(s"""select * from hmanalytics.hm_cns_threshold_sep_19 where account_type='A15' and active=1""")
val minTrendValDF = trendDF.crossJoin(thresDF) .withColumn("THRESHOLD_ACNT_TYPE", when((col("CUR_BAL_99PT996").isNull) || (col("CUR_BAL_99PT996").isNotNull && coalesce(abs(col("CURRENT_BALANCE")),lit(0))<= col("CUR_BAL_99PT996") ),1).otherwise(0)) .select(trendDF("*"), col("THRESHOLD_ACNT_TYPE"))
val newTrendDF = minTrendValDF.where(col("THRESHOLD_ACNT_TYPE") === 1).drop(col("THRESHOLD_ACNT_TYPE"))


val minProdValDF = prodDF.crossJoin(thresDF) .withColumn("THRESHOLD_ACNT_TYPE", when((col("CUR_BAL_99PT996").isNull) || (col("CUR_BAL_99PT996").isNotNull && coalesce(abs(col("CURRENT_BALANCE")),lit(0))<= col("CUR_BAL_99PT996") ),1).otherwise(0)) .select(prodDF("*"), col("THRESHOLD_ACNT_TYPE"))
val newProdDF = minProdValDF.where(col("THRESHOLD_ACNT_TYPE") === 1).drop(col("THRESHOLD_ACNT_TYPE"))

val f1 = Future{
  newTrendDF.write.mode("overwrite").saveAsTable("hmanalytics.hm_cc_cns_trend_data")
   }
val f2 = Future{
   newProdDF.write.mode("overwrite").saveAsTable("hmanalytics.hm_cc_cns_prd_data")
  }
   Await.ready(f1, Duration.Inf)
   Await.ready(f2, Duration.Inf)
   
  creditCardTab.DataAggregation(spark,argument)
}