package com.crif.highmark.CrifSemantic.DataLoad
import java.util.{Properties, UUID}
import java.io.FileInputStream
import scala.concurrent.{Await, Future}
import scala.concurrent.duration._
import scala.concurrent.ExecutionContext.Implicits.global
import java.text.SimpleDateFormat
import org.apache.commons.lang.time.DateUtils
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import scala.concurrent.{Await, Future}
import scala.concurrent.duration._
import scala.concurrent.ExecutionContext.Implicits.global


object collectionFlowRate extends App{
val spark = SparkSession.builder().appName("collectionFlowRate")
.config("hive.metastore.uris", "thrift://chmcisprbdmn01.chm.intra:9083")
.config("hive.exec.dynamic.partition", "true")
.config("hive.exec.dynamic.partition.mode", "nonstrict")
.config("hive.enforce.bucketing", "true")
.config("hive.exec.max.dynamic.partitions", "20000")
.enableHiveSupport().getOrCreate()

val argument = spark.sparkContext.getConf.get("spark.driver.args").split("\\s+")
import scala.collection.JavaConverters._
val prop = new Properties()
prop.load(new FileInputStream("collectionFlowRateProp.txt"))
val dateFormat = new SimpleDateFormat("yyyy-MM-dd")

val self=argument(0)
val end_dt=argument(1)
val act_type=argument(2)
val tl=argument(3)
val mon_rn=argument(4)

val lender_type=prop.get("lender_type").toString().replaceAll("self", s"'$self'")
val ticket_size=prop.get(s"ticket_size_${act_type}").toString()


val trendDataDF = spark.sql(s"""
SELECT 
 'T1' data_ind,
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
  LAST_DAY(B.DISBURSED_DT) DISBURSED_DT

FROM 
  hmanalytics.hm_mfi_account_trend A 
  JOIN hmanalytics.hm_cns_account_prod  B ON(
    A.ACCOUNT_KEY = B.ACCOUNT_KEY 
    AND B.ACCOUNT_TYPE IN ('"""+act_type+"""')
    AND B.ACTIVE = 1 
    AND B.SUPPRESS_INDICATOR is null
    AND last_day(cast(A.ACT_REPORTED_DT as date)) ='"""+end_dt+"""'
		AND B.DISBURSED_DT <='"""+end_dt+"""'
 )
 
 UNION ALL
   
   SELECT 
 'T2' data_ind,
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
  LAST_DAY(B.DISBURSED_DT) DISBURSED_DT

FROM 
  hmanalytics.hm_mfi_account_trend A 
  JOIN hmanalytics.hm_cns_account_prod  B ON(
    A.ACCOUNT_KEY = B.ACCOUNT_KEY 
    AND B.ACCOUNT_TYPE IN ('"""+act_type+"""')
    AND B.ACTIVE = 1 
    AND B.SUPPRESS_INDICATOR is null
    AND last_day(cast(A.ACT_REPORTED_DT as date)) =add_months('"""+end_dt+"""',"""+tl+""")

 )
""")

trendDataDF.createOrReplaceTempView("temp")

val clstDF=spark.sql("""
select  distinct std_state_code,account_key,consumer_key,OWNERSHIP_IND,COALESCE(max_clst_id,A.CONSUMER_KEY) max_clst_id
 from hmanalytics.hm_cns_unique_row A
LEFT OUTER JOIN
 hmanalytics.hm_cns_clst_"""+mon_rn+""" B
 ON(A.consumer_key = B.candidate_id)
""")

clstDF.createOrReplaceTempView("clst")

val stCdDF= spark.sql("""
select A.*,
B.std_state_code,B.consumer_key,B.max_clst_id
from
temp A
left outer Join clst B
ON(A.account_key = B.ACCOUNT_KEY)
WHERE !(ACCOUNT_TYPE ='A15' AND OWNERSHIP_IND = 'O04')
""")

stCdDF.createOrReplaceTempView("temp1")

val thresDF= spark.sql("""
select A.* from temp1 A 
join 
hmanalytics.hm_cns_threshold_sep_19 B
ON(A.ACCOUNT_TYPE=B.ACCOUNT_TYPE
AND B.ACTIVE=1)
where current_balance <= cur_bal_99pt996 and sanctioned_amount <= disb_amt_99pt996
""")

thresDF.write.mode("overwrite").saveAsTable("hmanalytics.hm_cns_flow_rate_base")

val df = spark.sql("""
select
A.ACCOUNT_TYPE ACCOUNT_TYPE,
A.MFI_ID MFI_ID,
A.disbursed_dt disbursed_dt,
last_day(A.act_reported_dt) Loan_Book_Obs_Prd,
last_day(B.act_reported_dt) Time_Lapsed,
A.std_state_code std_state_code,
A.ACCOUNT_KEY ACCOUNT_KEY,
CASE WHEN (A.NOT_CLOSED_IND = 'Y') AND A.DAS IN('S04', 'S05', 'S11', 'S12', 'S16', 'S18','S20') AND ( A.DERIVED_DPD = 0 OR A.DERIVED_DPD IS NULL ) THEN 'No Due'
WHEN  (A.NOT_CLOSED_IND = 'Y') AND A.DAS IN('S04', 'S05', 'S11', 'S12', 'S16', 'S18','S20') AND A.DERIVED_DPD BETWEEN  1 AND 30 THEN '1_30'
WHEN  (A.NOT_CLOSED_IND = 'Y') AND A.DAS IN('S04', 'S05', 'S11', 'S12', 'S16', 'S18','S20') AND A.DERIVED_DPD BETWEEN 31 AND 60 THEN '30_60'
WHEN  (A.NOT_CLOSED_IND = 'Y') AND A.DAS IN('S04', 'S05', 'S11', 'S12', 'S16', 'S18','S20') AND A.DERIVED_DPD BETWEEN 61 AND 90 THEN '60_90' 
WHEN  (A.NOT_CLOSED_IND = 'Y') AND A.DAS IN('S04', 'S05', 'S11', 'S12', 'S16', 'S18','S20') AND A.DERIVED_DPD BETWEEN 91 AND 120 THEN '90_120'
WHEN  (A.NOT_CLOSED_IND = 'Y') AND A.DAS IN('S04', 'S05', 'S11', 'S12', 'S16', 'S18','S20') AND A.DERIVED_DPD BETWEEN 121 AND 150 THEN '120_150'
WHEN  (A.NOT_CLOSED_IND = 'Y') AND A.DAS IN('S04', 'S05', 'S11', 'S12', 'S16', 'S18','S20') AND A.DERIVED_DPD BETWEEN 151 AND 180 THEN '150_180'
WHEN  (A.NOT_CLOSED_IND = 'Y') AND A.DAS IN('S04', 'S05', 'S11', 'S12', 'S16', 'S18','S20') AND A.DERIVED_DPD BETWEEN 181 AND 360 THEN '180_360'
WHEN  (A.NOT_CLOSED_IND = 'Y') AND A.DAS IN('S04', 'S05', 'S11', 'S12', 'S16', 'S18','S20') AND A.DERIVED_DPD > 360 THEN '360+'  END  DPD_LBOP,
CASE WHEN (B.NOT_CLOSED_IND = 'Y') AND B.DAS IN('S04', 'S05', 'S11', 'S12', 'S16', 'S18','S20') AND ( B.DERIVED_DPD = 0 OR B.DERIVED_DPD IS NULL ) THEN 'No Due'
WHEN  (B.NOT_CLOSED_IND = 'Y') AND B.DAS IN('S04', 'S05', 'S11', 'S12', 'S16', 'S18','S20') AND B.DERIVED_DPD BETWEEN 1 AND 30 THEN '1_30'
WHEN  (B.NOT_CLOSED_IND = 'Y') AND B.DAS IN('S04', 'S05', 'S11', 'S12', 'S16', 'S18','S20') AND  B.DERIVED_DPD BETWEEN 31 AND 60 THEN '30_60'
WHEN  (B.NOT_CLOSED_IND = 'Y') AND B.DAS IN('S04', 'S05', 'S11', 'S12', 'S16', 'S18','S20') AND  B.DERIVED_DPD BETWEEN 61 AND 90 THEN '60_90' 
WHEN  (B.NOT_CLOSED_IND = 'Y') AND B.DAS IN('S04', 'S05', 'S11', 'S12', 'S16', 'S18','S20') AND  B.DERIVED_DPD BETWEEN 91 AND 120 THEN '90_120'
WHEN  (B.NOT_CLOSED_IND = 'Y') AND B.DAS IN('S04', 'S05', 'S11', 'S12', 'S16', 'S18','S20') AND  B.DERIVED_DPD BETWEEN 121 AND 150 THEN '120_150'
WHEN  (B.NOT_CLOSED_IND = 'Y') AND B.DAS IN('S04', 'S05', 'S11', 'S12', 'S16', 'S18','S20') AND  B.DERIVED_DPD BETWEEN 151 AND 180 THEN '150_180'
WHEN  (B.NOT_CLOSED_IND = 'Y') AND B.DAS IN('S04', 'S05', 'S11', 'S12', 'S16', 'S18','S20') AND  B.DERIVED_DPD BETWEEN 181 AND 360 THEN '180_360'
WHEN (B.NOT_CLOSED_IND = 'Y') AND B.DAS IN('S04', 'S05', 'S11', 'S12', 'S16', 'S18','S20') AND   B.DERIVED_DPD > 360 THEN '360+'
WHEN B.DAS IN ('S06', 'S13') Then 'Written Off' END   DPD_TL,
A.current_balance current_balance_lbop,
B.current_balance current_balance_tl,
A.sanctioned_amount sanctioned_amount_lbop,
B.sanctioned_amount sanctioned_amount_tl,
A.chargeoff_amt chargeoff_amt_lbop,
B.chargeoff_amt chargeoff_amt_tl,
A.NOT_CLOSED_IND NOT_CLOSED_IND,
A.DAS DAS
FROM
hmanalytics.hm_cns_flow_rate_base A
LEFT OUTER JOIN hmanalytics.hm_cns_flow_rate_base B
ON(A.ACCOUNT_KEY = B.ACCOUNT_KEY
AND (add_months(last_day(A.act_reported_dt),"""+tl+""") = last_day(B.act_reported_dt)))
WHERE last_day(A.act_reported_dt) ='"""+end_dt+"""'
""")



								 
df.write.mode("overwrite").saveAsTable("hmanalytics.hm_cns_flow_rate_base_new")

 
val fnlDF= spark.sql("""
INSERT INTO TABLE hmanalytics.hm_cns_flow_rate_base_fnl
PARTITION (batch_run) 
select 
Product,
LENDER_TYPE,
TICKET_SIZE_LBOP,
std_state_code STATE,
Loan_Book_Obs_Prd,
Time_Lapsed,
DPD_LBOP,
DPD_TL,
origination_period,
case when MOB between 0 and 2  THEN '1_3'
when MOB between 3 and 5  THEN '4_6'
when MOB between 6 and 8  THEN '7_9'
when MOB between 9 and 11  THEN '10_12'
when MOB between 12 and 14  THEN '13_15'
when MOB between 15 and 17  THEN '16_18'
when MOB between 18 and 20  THEN '19_21'
when MOB between 21 and 23  THEN '22_24'
when MOB between 24 and 26  THEN '25_27'
when MOB between 27 and 29  THEN '28_30'
when MOB between 30 and 32  THEN '31_33'
when MOB between 33 and 35  THEN '34_36'
when MOB > 35  THEN '>36'
END MOB_BUCKETS,
count(distinct ACCOUNT_KEY ) Live_Loans,
sum(current_balance_tl) Current_Balance_TL,
Sum(chargeoff_amt_tl) TOTAL_WRITTEN_OFF_AMOUNT,
concat_ws('|','"""+act_type+"""','"""+self+"""',CAST(date_format(Loan_Book_Obs_Prd,'yyyyMM') as INT),'"""+tl+"""') batch_run
FROM
(select distinct 
ACCOUNT_NAME Product,
Loan_Book_Obs_Prd,
Time_Lapsed,
NOT_CLOSED_IND,
DAS,
CASE WHEN disbursed_dt < add_months('"""+end_dt+"""',-36) THEN '<36 Months' ELSE E.FYQ END origination_period,
MONTHS_BETWEEN(LAST_DAY(Loan_Book_Obs_Prd),LAST_DAY(DISBURSED_DT)) MOB,
ACCOUNT_KEY,	
"""+lender_type+""" LENDER_TYPE,
"""+ticket_size+""" TICKET_SIZE_LBOP,
current_balance_lbop,
current_balance_tl,
sanctioned_amount_lbop,
sanctioned_amount_tl,
chargeoff_amt_lbop,
chargeoff_amt_tl,
std_state_code,
DPD_LBOP,
DPD_TL 
from 
hmanalytics.hm_cns_flow_rate_base_new A
JOIN
(SELECT ACCOUNT_TYPE,ACCOUNT_NAME FROM hmanalytics.HM_LOAN_ACCOUNT_TYPE_DIM
WHERE BUREAU_FLAG='CNS'AND ACTIVE=1) F
ON (A.ACCOUNT_TYPE=F.ACCOUNT_TYPE) 										
LEFT JOIN HMANALYTICS.HM_DATE_DIM2 E
      ON (CAST(date_format(disbursed_dt,'yyyyMM') as INT) =E.YYYYMM)) A
WHERE time_lapsed is not null
GROUP BY
Product,
LENDER_TYPE,
TICKET_SIZE_LBOP,
std_state_code ,
Loan_Book_Obs_Prd,
Time_Lapsed,
DPD_LBOP,
DPD_TL,
origination_period,
case when MOB between 0 and 2  THEN '1_3'
when MOB between 3 and 5  THEN '4_6'
when MOB between 6 and 8  THEN '7_9'
when MOB between 9 and 11  THEN '10_12'
when MOB between 12 and 14  THEN '13_15'
when MOB between 15 and 17  THEN '16_18'
when MOB between 18 and 20  THEN '19_21'
when MOB between 21 and 23  THEN '22_24'
when MOB between 24 and 26  THEN '25_27'
when MOB between 27 and 29  THEN '28_30'
when MOB between 30 and 32  THEN '31_33'
when MOB between 33 and 35  THEN '34_36'
when MOB > 35  THEN '>36'
END 
""")

//fnlDF.write.partitionBy("batch_run").mode("append").saveAsTable("hmanalytics.hm_cns_flow_rate_base_fnl")


  
}