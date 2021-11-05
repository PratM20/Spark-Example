package com.crif.highmark.CrifSemantic.DataLoad
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import scala.concurrent.{Await, Future}
import scala.concurrent.duration._
import scala.concurrent.ExecutionContext.Implicits.global

object nocReport extends App{
	
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
val rpt_dt=argument(2)	
val as_of=argument(3)

val nocDF = spark.sql(s"""SELECT 
  MFI_ID, 
  C.institution_name INSTITUTION, 
  From_unixtime(
    unix_timestamp('"""+as_of+ """', 'dd-MMM-yy'), 
    'yyyy-MM-dd'
  ) REPORTED_DT, 
  ACCOUNT_TYPE,
  Sum(total_loans) TOTAL_LOANS, 
  Sum(total_disb_amount) TOTAL_DISB_AMOUNT, 
  Sum(active_loans) ACTIVE_LOANS, 
  Sum(active_disb_amt) ACTIVE_DISB_AMOUNT, 
  Sum(active_outstanding) ACTIVE_OUTSTANDING,
  Sum(lar_0) LAR_0,
  Sum(lar_1_30) LAR_1_30, 
  Sum(lar_31_60) LAR_31_60, 
  Sum(lar_61_90) LAR_61_90, 
  Sum(lar_91_180) LAR_91_180, 
  Sum(lar_181_360) LAR_181_360, 
  Sum(lar_360_plus) LAR_360_plus, 
  Sum(Par_0) PAR_0,
  Sum(par_1_30) PAR_1_30, 
  Sum(par_31_60) PAR_31_60, 
  Sum(par_61_90) PAR_61_90, 
  Sum(par_91_180) PAR_91_180, 
  Sum(par_181_360) PAR_181_360, 
  Sum(par_360_plus) PAR_360_PLUS, 
  Sum(total_written_off_loans) TOTAL_WRITTEN_OFF_LOANS, 
  Sum(total_written_off_amount) TOTAL_WRITTEN_OFF_AMOUNT 
FROM 
  (
    SELECT 
      mfi_id, 
      account_type, 
      Last_day(reported_dt) ACT_REPORTED_DT, 
      Count(DISTINCT account_key) TOTAL_LOANS, 
      Sum(disbursed_amount) TOTAL_DISB_AMOUNT, 
      Count( DISTINCT CASE WHEN das_active_flag = 1 AND closed_dt_ind = 'N' THEN account_key ELSE NULL END ) ACTIVE_LOANS, 
      Sum(CASE WHEN das_active_flag = 1 AND closed_dt_ind = 'N' THEN disbursed_amount ELSE 0 END) ACTIVE_DISB_AMT, 
      Sum(CASE WHEN das_active_flag = 1 AND closed_dt_ind = 'N' THEN current_balance ELSE 0 END) ACTIVE_OUTSTANDING,
   Count(DISTINCT CASE WHEN das_active_flag = 1  AND closed_dt_ind = 'N' AND (dpd = 0 OR dpd IS NULL) THEN account_key ELSE NULL END) LAR_0, 
      Count(DISTINCT CASE WHEN das_active_flag = 1 AND ((dpd >0 AND dpd IS NOT NULL) OR Nvl(Abs(amount_overdue_total),0) > (0.01 * DISBURSED_AMOUNT)) AND closed_dt_ind = 'N' AND dpd BETWEEN 1 AND 30 THEN account_key ELSE NULL END) LAR_1_30, 
      Count(DISTINCT CASE WHEN das_active_flag = 1 AND ((dpd >0 AND dpd IS NOT NULL) OR Nvl(Abs(amount_overdue_total),0) > (0.01 * DISBURSED_AMOUNT)) AND closed_dt_ind = 'N' AND dpd BETWEEN 31 AND 60 THEN account_key ELSE NULL END) LAR_31_60, 
      Count(DISTINCT CASE WHEN das_active_flag = 1 AND ((dpd >0 AND dpd IS NOT NULL) OR Nvl(Abs(amount_overdue_total),0) > (0.01 * DISBURSED_AMOUNT)) AND closed_dt_ind = 'N' AND dpd BETWEEN 61 AND 90 THEN account_key ELSE NULL END) LAR_61_90, 
      Count(DISTINCT CASE WHEN das_active_flag = 1 AND ((dpd >0 AND dpd IS NOT NULL) OR Nvl(Abs(amount_overdue_total),0) > (0.01 * DISBURSED_AMOUNT)) AND closed_dt_ind = 'N' AND dpd BETWEEN 91 AND 180 THEN account_key ELSE NULL END) LAR_91_180, 
      Count(DISTINCT CASE WHEN das_active_flag = 1 AND ((dpd >0 AND dpd IS NOT NULL) OR Nvl(Abs(amount_overdue_total),0) > (0.01 * DISBURSED_AMOUNT)) AND closed_dt_ind = 'N' AND dpd BETWEEN 181 AND 360 THEN account_key ELSE NULL END) LAR_181_360, 
      Count(DISTINCT CASE WHEN das_active_flag = 1 AND ((dpd >0 AND dpd IS NOT NULL) OR Nvl(Abs(amount_overdue_total),0) > (0.01 * DISBURSED_AMOUNT)) AND closed_dt_ind = 'N' AND dpd >= 361 THEN account_key ELSE NULL END) LAR_360_PLUS,
   Sum(CASE WHEN das_active_flag = 1   AND closed_dt_ind = 'N' AND (dpd = 0 OR dpd IS NULL)  THEN current_balance ELSE 0 END) PAR_0,
      Sum(CASE WHEN das_active_flag = 1 AND ((dpd >0 AND dpd IS NOT NULL) OR Nvl(Abs(amount_overdue_total),0) > (0.01 * DISBURSED_AMOUNT)) AND closed_dt_ind = 'N' AND dpd BETWEEN 1 AND 30 THEN current_balance ELSE 0 END) PAR_1_30, 
      Sum(CASE WHEN das_active_flag = 1 AND ((dpd >0 AND dpd IS NOT NULL) OR Nvl(Abs(amount_overdue_total),0) > (0.01 * DISBURSED_AMOUNT)) AND closed_dt_ind = 'N' AND dpd BETWEEN 31 AND 60 THEN current_balance ELSE 0 END) PAR_31_60, 
      Sum(CASE WHEN das_active_flag = 1 AND ((dpd >0 AND dpd IS NOT NULL) OR Nvl(Abs(amount_overdue_total),0) > (0.01 * DISBURSED_AMOUNT)) AND closed_dt_ind = 'N' AND dpd BETWEEN 61 AND 90 THEN current_balance ELSE 0 END) PAR_61_90, 
      Sum(CASE WHEN das_active_flag = 1 AND ((dpd >0 AND dpd IS NOT NULL) OR Nvl(Abs(amount_overdue_total),0) > (0.01 * DISBURSED_AMOUNT)) AND closed_dt_ind = 'N' AND dpd BETWEEN 91 AND 180 THEN current_balance ELSE 0 END) PAR_91_180, 
      Sum(CASE WHEN das_active_flag = 1 AND ((dpd >0 AND dpd IS NOT NULL) OR Nvl(Abs(amount_overdue_total),0) > (0.01 * DISBURSED_AMOUNT)) AND closed_dt_ind = 'N' AND dpd BETWEEN 181 AND 360 THEN current_balance ELSE 0 END) PAR_181_360, 
      Sum(CASE WHEN das_active_flag = 1 AND ((dpd >0 AND dpd IS NOT NULL) OR Nvl(Abs(amount_overdue_total),0) > (0.01 * DISBURSED_AMOUNT)) AND closed_dt_ind = 'N' AND dpd >= 361 THEN current_balance ELSE 0 END) PAR_360_PLUS, 
      Count(DISTINCT CASE WHEN das IN ('S06', 'S13') THEN account_key ELSE NULL END) TOTAL_WRITTEN_OFF_LOANS, 
      Sum(CASE WHEN das IN ('S06', 'S13') THEN chargeoff_amt ELSE 0 END) TOTAL_WRITTEN_OFF_AMOUNT 
    FROM 
      (
        SELECT 
          DISTINCT account_key, 
          account_type, 
          mfi_id, 
          reported_dt, 
          closed_dt, 
          Abs(current_balance) CURRENT_BALANCE, 
          Abs(amount_overdue_total) AMOUNT_OVERDUE_TOTAL, 
          cast(CASE WHEN regexp_extract(DAYS_PAST_DUE, '[0-9]+', 0) IS NOT NULL AND TRIM(DAC) IS NULL THEN nvl(cast(regexp_extract(DAYS_PAST_DUE, '[0-9]+', 0) as int), 0)
    WHEN regexp_extract(DAYS_PAST_DUE, '[0-9]+', 0) IS NULL 
          AND TRIM(DAC) IS NOT NULL THEN CASE WHEN TRIM(DAC) = 'L01' THEN 0 WHEN TRIM(DAC) IN ('L02') THEN 91 WHEN TRIM(DAC) IN ('L03', 'L04') THEN 361 WHEN TRIM(DAC) IN ('L05') THEN 1 END WHEN regexp_extract(DAYS_PAST_DUE, '[0-9]+', 0) IS NOT NULL 
          AND TRIM(DAC) IS NOT NULL THEN CASE WHEN NVL(cast(CASE WHEN TRIM(DAC) = 'L01' THEN 0 WHEN TRIM(DAC) IN ('L02') THEN 91 WHEN TRIM(DAC) IN ('L03', 'L04') THEN 361 WHEN TRIM(DAC) IN ('L05') THEN 1 END as int), 0) <= nvl(cast(regexp_extract(DAYS_PAST_DUE, '[0-9]+', 0) as int), 0)
    THEN nvl(cast(regexp_extract(DAYS_PAST_DUE, '[0-9]+', 0) as int), 0)
    ELSE NVL(cast(CASE WHEN TRIM(DAC) = 'L01' THEN 0 WHEN TRIM(DAC) IN ('L02') THEN 91 WHEN TRIM(DAC) IN ('L03', 'L04') THEN 361 WHEN TRIM(DAC) IN ('L05') THEN 1 END as int), 0) END
    WHEN regexp_extract(DAYS_PAST_DUE, '[0-9]+', 0) IS NULL AND TRIM(DAC) IS NULL THEN CASE WHEN DAS = 'S04' THEN 0 WHEN DAS = 'S05' THEN 1 ELSE 0 END END as int) DPD,  
          Abs(chargeoff_amt) CHARGEOFF_AMT, 
          das, 
          abs(coalesce(HIGH_CREDIT, DISBURSED_AMOUNT, CREDIT_LIMIT)) DISBURSED_AMOUNT, 
          CASE WHEN (closed_dt IS NULL OR closed_dt > From_unixtime(unix_timestamp('"""+end_dt+ """', 'dd-MMM-yy'), 'yyyy-MM-dd') AND DAS IN ('S04','S05','S11','S12','S16','S18','S20')) THEN 'N' ELSE 'Y' END CLOSED_DT_IND, 
          CASE WHEN ((Cast(Regexp_extract(days_past_due, '[0-9]+', 0) AS INT) > 0 
              AND Cast(Regexp_extract(days_past_due, '[0-9]+', 0) AS INT) IS NOT NULL) 
            OR Nvl(Abs(amount_overdue_total), 0) > (0.01 * Nvl(abs(coalesce(HIGH_CREDIT, DISBURSED_AMOUNT, CREDIT_LIMIT)), 0))) THEN 1
   WHEN (Cast(Regexp_extract(days_past_due, '[0-9]+', 0) AS INT) = 0 OR Cast(Regexp_extract(days_past_due, '[0-9]+', 0) AS INT) IS NULL) 
          AND Nvl(Abs(amount_overdue_total), 0) <= (0.01 * Nvl(abs(coalesce(HIGH_CREDIT, DISBURSED_AMOUNT, CREDIT_LIMIT)), 0))
    THEN 0 ELSE NULL END OVERDUE_IND, 
          CASE WHEN das IN ('S04', 'S05') THEN 1 ELSE 0 END DAS_ACTIVE_FLAG 
        FROM 
     hmanalytics.hm_cns_macro_analysis_"""+mon_rn+ """_parquet 
    
        UNION ALL
  
       SELECT 
          DISTINCT account_key, 
          account_type, 
          mfi_id, 
          reported_dt, 
          closed_dt, 
          Abs(current_balance) CURRENT_BALANCE, 
          Abs(amount_overdue_total) AMOUNT_OVERDUE_TOTAL, 
          cast(CASE WHEN regexp_extract(DAYS_PAST_DUE, '[0-9]+', 0) IS NOT NULL AND TRIM(DAC) IS NULL THEN nvl(cast(regexp_extract(DAYS_PAST_DUE, '[0-9]+', 0) as int), 0)
    WHEN regexp_extract(DAYS_PAST_DUE, '[0-9]+', 0) IS NULL 
          AND TRIM(DAC) IS NOT NULL THEN CASE WHEN TRIM(DAC) = 'L01' THEN 0 WHEN TRIM(DAC) IN ('L02') THEN 91 WHEN TRIM(DAC) IN ('L03', 'L04') THEN 361 WHEN TRIM(DAC) IN ('L05') THEN 1 END WHEN regexp_extract(DAYS_PAST_DUE, '[0-9]+', 0) IS NOT NULL 
          AND TRIM(DAC) IS NOT NULL THEN CASE WHEN NVL(cast(CASE WHEN TRIM(DAC) = 'L01' THEN 0 WHEN TRIM(DAC) IN ('L02') THEN 91 WHEN TRIM(DAC) IN ('L03', 'L04') THEN 361 WHEN TRIM(DAC) IN ('L05') THEN 1 END as int), 0) <= nvl(cast(regexp_extract(DAYS_PAST_DUE, '[0-9]+', 0) as int), 0)
    THEN nvl(cast(regexp_extract(DAYS_PAST_DUE, '[0-9]+', 0) as int), 0)
    ELSE NVL(cast(CASE WHEN TRIM(DAC) = 'L01' THEN 0 WHEN TRIM(DAC) IN ('L02') THEN 91 WHEN TRIM(DAC) IN ('L03', 'L04') THEN 361 WHEN TRIM(DAC) IN ('L05') THEN 1 END as int), 0) END
    WHEN regexp_extract(DAYS_PAST_DUE, '[0-9]+', 0) IS NULL AND TRIM(DAC) IS NULL THEN CASE WHEN DAS = 'S04' THEN 0 WHEN DAS = 'S05' THEN 1 ELSE 0 END END as int) DPD,  
          Abs(chargeoff_amt) CHARGEOFF_AMT, 
          das, 
          abs(coalesce(HIGH_CREDIT, DISBURSED_AMOUNT, CREDIT_LIMIT)) DISBURSED_AMOUNT, 
          CASE WHEN (
            closed_dt IS NULL 
            OR closed_dt > From_unixtime(
              unix_timestamp('"""+end_dt+ """', 'dd-MMM-yy'), 
              'yyyy-MM-dd'
            )
          AND DAS IN ('S04','S05','S11','S12','S16','S18','S20')) THEN 'N' ELSE 'Y' END CLOSED_DT_IND, 
          CASE WHEN (
            (
              Cast(
                Regexp_extract(days_past_due, '[0-9]+', 0) AS INT
              ) > 0 
              AND Cast(
                Regexp_extract(days_past_due, '[0-9]+', 0) AS INT
              ) IS NOT NULL
            ) 
            OR Nvl(
              Abs(amount_overdue_total), 
              0
            ) > (
              0.01 * Nvl(
                abs(coalesce(HIGH_CREDIT, DISBURSED_AMOUNT, CREDIT_LIMIT)), 
                0
              )
            )
          ) THEN 1 WHEN (
            Cast(
              Regexp_extract(days_past_due, '[0-9]+', 0) AS INT
            ) = 0 
            OR Cast(
              Regexp_extract(days_past_due, '[0-9]+', 0) AS INT
            ) IS NULL
          ) 
          AND Nvl(
            Abs(amount_overdue_total), 
            0
          ) <= (
            0.01 * Nvl(
              abs(coalesce(HIGH_CREDIT, DISBURSED_AMOUNT, CREDIT_LIMIT)), 
              0
            )
          ) THEN 0 ELSE NULL END OVERDUE_IND, 
          CASE WHEN das IN ('S04', 'S05') THEN 1 ELSE 0 END DAS_ACTIVE_FLAG 
        FROM 
          hmanalytics.hm_cns_closed_macro_analysis 
where 
  (
    closed_dt IS NULL 
    or closed_dt <= from_unixtime(
      unix_timestamp('"""+end_dt+ """', 'dd-MMM-yy'), 
      'yyyy-MM-dd'
    )
  ) 
  and REPORTED_DT <= from_unixtime(
    unix_timestamp('"""+rpt_dt+ """', 'ddMMyyyy'), 
    'yyyy-MM-dd'
  ) 
  and ACT_INSERT_DT <= from_unixtime(
    unix_timestamp('"""+rpt_dt+ """', 'ddMMyyyy'), 
    'yyyy-MM-dd'
  )

      ) A 
    GROUP BY 
      mfi_id, 
      account_type, 
      reported_dt
  ) B 
  LEFT JOIN hmanalytics.hm_contributor_dim C ON (
    B.mfi_id = C.contributor_id 
    AND C.active = 1
  ) 
GROUP BY 
  mfi_id, 
  C.institution_name,
  account_type  """)
  

/*val nocDF = spark.sql(s"""SELECT 
  mfi_id, 
  C.institution_name institution_name, 
  From_unixtime(
    Unix_timestamp('30-SEP-17', 'dd-MMM-yy'), 
    'yyyy-MM-dd'
  ) reported_dt, 
  account_type,
  Sum(total_loans) total_loans, 
  Sum(total_disb_amount) total_disb_amount, 
  Sum(active_loans) active_loans, 
  Sum(active_disb_amt) active_disb_amt, 
  Sum(active_outstanding) active_outstanding,
  Sum(lar_0) lar_0,
  Sum(lar_1_30) lar_1_30, 
  Sum(lar_31_60) lar_31_60, 
  Sum(lar_61_90) lar_61_90, 
  Sum(lar_91_180) lar_91_180, 
  Sum(lar_181_360) lar_181_360, 
  Sum(lar_360_plus) lar_360_plus, 
  Sum(Par_0) Par_0,
  Sum(par_1_30) par_1_30, 
  Sum(par_31_60) par_31_60, 
  Sum(par_61_90) par_61_90, 
  Sum(par_91_180) par_91_180, 
  Sum(par_181_360) par_181_360, 
  Sum(par_360_plus) par_360_plus, 
  Sum(total_written_off_loans) total_written_off_loans, 
  Sum(total_written_off_amount) total_written_off_amount 
FROM 
  (
    SELECT 
      mfi_id, 
      account_type, 
      Last_day(reported_dt) ACT_REPORTED_DT, 
      Count(DISTINCT account_key) TOTAL_LOANS, 
      Sum(disbursed_amount) TOTAL_DISB_AMOUNT, 
      Count( DISTINCT CASE WHEN das_active_flag = 1 AND closed_dt_ind = 'N' THEN account_key ELSE NULL END ) ACTIVE_LOANS, 
      Sum(CASE WHEN das_active_flag = 1 AND closed_dt_ind = 'N' THEN disbursed_amount ELSE 0 END) ACTIVE_DISB_AMT, 
      Sum(CASE WHEN das_active_flag = 1 AND closed_dt_ind = 'N' THEN current_balance ELSE 0 END) ACTIVE_OUTSTANDING,
   Count(DISTINCT CASE WHEN das_active_flag = 1  AND closed_dt_ind = 'N' AND (dpd = 0 OR dpd IS NULL) THEN account_key ELSE NULL END) LAR_0, 
      Count(DISTINCT CASE WHEN das_active_flag = 1 AND ((dpd >0 AND dpd IS NOT NULL) OR Nvl(Abs(amount_overdue_total),0) > (0.01 * DISBURSED_AMOUNT)) AND closed_dt_ind = 'N' AND dpd BETWEEN 1 AND 30 THEN account_key ELSE NULL END) LAR_1_30, 
      Count(DISTINCT CASE WHEN das_active_flag = 1 AND ((dpd >0 AND dpd IS NOT NULL) OR Nvl(Abs(amount_overdue_total),0) > (0.01 * DISBURSED_AMOUNT)) AND closed_dt_ind = 'N' AND dpd BETWEEN 31 AND 60 THEN account_key ELSE NULL END) LAR_31_60, 
      Count(DISTINCT CASE WHEN das_active_flag = 1 AND ((dpd >0 AND dpd IS NOT NULL) OR Nvl(Abs(amount_overdue_total),0) > (0.01 * DISBURSED_AMOUNT)) AND closed_dt_ind = 'N' AND dpd BETWEEN 61 AND 90 THEN account_key ELSE NULL END) LAR_61_90, 
      Count(DISTINCT CASE WHEN das_active_flag = 1 AND ((dpd >0 AND dpd IS NOT NULL) OR Nvl(Abs(amount_overdue_total),0) > (0.01 * DISBURSED_AMOUNT)) AND closed_dt_ind = 'N' AND dpd BETWEEN 91 AND 180 THEN account_key ELSE NULL END) LAR_91_180, 
      Count(DISTINCT CASE WHEN das_active_flag = 1 AND ((dpd >0 AND dpd IS NOT NULL) OR Nvl(Abs(amount_overdue_total),0) > (0.01 * DISBURSED_AMOUNT)) AND closed_dt_ind = 'N' AND dpd BETWEEN 181 AND 360 THEN account_key ELSE NULL END) LAR_181_360, 
      Count(DISTINCT CASE WHEN das_active_flag = 1 AND ((dpd >0 AND dpd IS NOT NULL) OR Nvl(Abs(amount_overdue_total),0) > (0.01 * DISBURSED_AMOUNT)) AND closed_dt_ind = 'N' AND dpd >= 361 THEN account_key ELSE NULL END) LAR_360_PLUS,
   Sum(CASE WHEN das_active_flag = 1   AND closed_dt_ind = 'N' AND (dpd = 0 OR dpd IS NULL)  THEN current_balance ELSE 0 END) PAR_0,
      Sum(CASE WHEN das_active_flag = 1 AND ((dpd >0 AND dpd IS NOT NULL) OR Nvl(Abs(amount_overdue_total),0) > (0.01 * DISBURSED_AMOUNT)) AND closed_dt_ind = 'N' AND dpd BETWEEN 1 AND 30 THEN current_balance ELSE 0 END) PAR_1_30, 
      Sum(CASE WHEN das_active_flag = 1 AND ((dpd >0 AND dpd IS NOT NULL) OR Nvl(Abs(amount_overdue_total),0) > (0.01 * DISBURSED_AMOUNT)) AND closed_dt_ind = 'N' AND dpd BETWEEN 31 AND 60 THEN current_balance ELSE 0 END) PAR_31_60, 
      Sum(CASE WHEN das_active_flag = 1 AND ((dpd >0 AND dpd IS NOT NULL) OR Nvl(Abs(amount_overdue_total),0) > (0.01 * DISBURSED_AMOUNT)) AND closed_dt_ind = 'N' AND dpd BETWEEN 61 AND 90 THEN current_balance ELSE 0 END) PAR_61_90, 
      Sum(CASE WHEN das_active_flag = 1 AND ((dpd >0 AND dpd IS NOT NULL) OR Nvl(Abs(amount_overdue_total),0) > (0.01 * DISBURSED_AMOUNT)) AND closed_dt_ind = 'N' AND dpd BETWEEN 91 AND 180 THEN current_balance ELSE 0 END) PAR_91_180, 
      Sum(CASE WHEN das_active_flag = 1 AND ((dpd >0 AND dpd IS NOT NULL) OR Nvl(Abs(amount_overdue_total),0) > (0.01 * DISBURSED_AMOUNT)) AND closed_dt_ind = 'N' AND dpd BETWEEN 181 AND 360 THEN current_balance ELSE 0 END) PAR_181_360, 
      Sum(CASE WHEN das_active_flag = 1 AND ((dpd >0 AND dpd IS NOT NULL) OR Nvl(Abs(amount_overdue_total),0) > (0.01 * DISBURSED_AMOUNT)) AND closed_dt_ind = 'N' AND dpd >= 361 THEN current_balance ELSE 0 END) PAR_360_PLUS, 
      Count(DISTINCT CASE WHEN das IN ('S06', 'S13') THEN account_key ELSE NULL END) TOTAL_WRITTEN_OFF_LOANS, 
      Sum(CASE WHEN das IN ('S06', 'S13') THEN chargeoff_amt ELSE 0 END) TOTAL_WRITTEN_OFF_AMOUNT 
    FROM 
      (
        SELECT 
          DISTINCT account_key, 
          account_type, 
          mfi_id, 
          reported_dt, 
          closed_dt, 
          Abs(current_balance) CURRENT_BALANCE, 
          Abs(amount_overdue_total) AMOUNT_OVERDUE_TOTAL, 
          cast(CASE WHEN regexp_extract(DAYS_PAST_DUE, '[0-9]+', 0) IS NOT NULL AND TRIM(DAC) IS NULL THEN nvl(cast(regexp_extract(DAYS_PAST_DUE, '[0-9]+', 0) as int), 0)
    WHEN regexp_extract(DAYS_PAST_DUE, '[0-9]+', 0) IS NULL 
          AND TRIM(DAC) IS NOT NULL THEN CASE WHEN TRIM(DAC) = 'L01' THEN 0 WHEN TRIM(DAC) IN ('L02') THEN 91 WHEN TRIM(DAC) IN ('L03', 'L04') THEN 361 WHEN TRIM(DAC) IN ('L05') THEN 1 END WHEN regexp_extract(DAYS_PAST_DUE, '[0-9]+', 0) IS NOT NULL 
          AND TRIM(DAC) IS NOT NULL THEN CASE WHEN NVL(cast(CASE WHEN TRIM(DAC) = 'L01' THEN 0 WHEN TRIM(DAC) IN ('L02') THEN 91 WHEN TRIM(DAC) IN ('L03', 'L04') THEN 361 WHEN TRIM(DAC) IN ('L05') THEN 1 END as int), 0) <= nvl(cast(regexp_extract(DAYS_PAST_DUE, '[0-9]+', 0) as int), 0)
    THEN nvl(cast(regexp_extract(DAYS_PAST_DUE, '[0-9]+', 0) as int), 0)
    ELSE NVL(cast(CASE WHEN TRIM(DAC) = 'L01' THEN 0 WHEN TRIM(DAC) IN ('L02') THEN 91 WHEN TRIM(DAC) IN ('L03', 'L04') THEN 361 WHEN TRIM(DAC) IN ('L05') THEN 1 END as int), 0) END
    WHEN regexp_extract(DAYS_PAST_DUE, '[0-9]+', 0) IS NULL AND TRIM(DAC) IS NULL THEN CASE WHEN DAS = 'S04' THEN 0 WHEN DAS = 'S05' THEN 1 ELSE 0 END END as int) DPD,  
          Abs(chargeoff_amt) CHARGEOFF_AMT, 
          das, 
          abs(coalesce(HIGH_CREDIT, DISBURSED_AMOUNT, CREDIT_LIMIT)) DISBURSED_AMOUNT, 
          CASE WHEN (closed_dt IS NULL OR closed_dt > From_unixtime(Unix_timestamp('30-SEP-17', 'dd-MMM-yy'), 'yyyy-MM-dd') AND DAS IN ('S04','S05','S11','S12','S16','S18','S20')) THEN 'N' ELSE 'Y' END CLOSED_DT_IND, 
          CASE WHEN ((Cast(Regexp_extract(days_past_due, '[0-9]+', 0) AS INT) > 0 
              AND Cast(Regexp_extract(days_past_due, '[0-9]+', 0) AS INT) IS NOT NULL) 
            OR Nvl(Abs(amount_overdue_total), 0) > (0.01 * Nvl(abs(coalesce(HIGH_CREDIT, DISBURSED_AMOUNT, CREDIT_LIMIT)), 0))) THEN 1
   WHEN (Cast(Regexp_extract(days_past_due, '[0-9]+', 0) AS INT) = 0 OR Cast(Regexp_extract(days_past_due, '[0-9]+', 0) AS INT) IS NULL) 
          AND Nvl(Abs(amount_overdue_total), 0) <= (0.01 * Nvl(abs(coalesce(HIGH_CREDIT, DISBURSED_AMOUNT, CREDIT_LIMIT)), 0))
    THEN 0 ELSE NULL END OVERDUE_IND, 
          CASE WHEN das IN ('S04', 'S05') THEN 1 ELSE 0 END DAS_ACTIVE_FLAG 
        FROM 
          hmanalytics.hm_cns_macro_analysis_nov_17_parquet 
        WHERE mfi_id = 'PRB0000006'

          AND disbursed_dt <= From_unixtime(
            Unix_timestamp('30-SEP-17', 'dd-MMM-yy'), 
            'yyyy-MM-dd'
          ) 
          AND account_type IN ('A06')
    AND abs(coalesce(HIGH_CREDIT,DISBURSED_AMOUNT, CREDIT_LIMIT)) <= 2500000
    
        UNION ALL
  
       SELECT 
          DISTINCT account_key, 
          account_type, 
          mfi_id, 
          reported_dt, 
          closed_dt, 
          Abs(current_balance) CURRENT_BALANCE, 
          Abs(amount_overdue_total) AMOUNT_OVERDUE_TOTAL, 
          cast(CASE WHEN regexp_extract(DAYS_PAST_DUE, '[0-9]+', 0) IS NOT NULL AND TRIM(DAC) IS NULL THEN nvl(cast(regexp_extract(DAYS_PAST_DUE, '[0-9]+', 0) as int), 0)
    WHEN regexp_extract(DAYS_PAST_DUE, '[0-9]+', 0) IS NULL 
          AND TRIM(DAC) IS NOT NULL THEN CASE WHEN TRIM(DAC) = 'L01' THEN 0 WHEN TRIM(DAC) IN ('L02') THEN 91 WHEN TRIM(DAC) IN ('L03', 'L04') THEN 361 WHEN TRIM(DAC) IN ('L05') THEN 1 END WHEN regexp_extract(DAYS_PAST_DUE, '[0-9]+', 0) IS NOT NULL 
          AND TRIM(DAC) IS NOT NULL THEN CASE WHEN NVL(cast(CASE WHEN TRIM(DAC) = 'L01' THEN 0 WHEN TRIM(DAC) IN ('L02') THEN 91 WHEN TRIM(DAC) IN ('L03', 'L04') THEN 361 WHEN TRIM(DAC) IN ('L05') THEN 1 END as int), 0) <= nvl(cast(regexp_extract(DAYS_PAST_DUE, '[0-9]+', 0) as int), 0)
    THEN nvl(cast(regexp_extract(DAYS_PAST_DUE, '[0-9]+', 0) as int), 0)
    ELSE NVL(cast(CASE WHEN TRIM(DAC) = 'L01' THEN 0 WHEN TRIM(DAC) IN ('L02') THEN 91 WHEN TRIM(DAC) IN ('L03', 'L04') THEN 361 WHEN TRIM(DAC) IN ('L05') THEN 1 END as int), 0) END
    WHEN regexp_extract(DAYS_PAST_DUE, '[0-9]+', 0) IS NULL AND TRIM(DAC) IS NULL THEN CASE WHEN DAS = 'S04' THEN 0 WHEN DAS = 'S05' THEN 1 ELSE 0 END END as int) DPD,  
          Abs(chargeoff_amt) CHARGEOFF_AMT, 
          das, 
          abs(coalesce(HIGH_CREDIT, DISBURSED_AMOUNT, CREDIT_LIMIT)) DISBURSED_AMOUNT, 
          CASE WHEN (
            closed_dt IS NULL 
            OR closed_dt > From_unixtime(
              Unix_timestamp('30-SEP-17', 'dd-MMM-yy'), 
              'yyyy-MM-dd'
            )
          AND DAS IN ('S04','S05','S11','S12','S16','S18','S20')) THEN 'N' ELSE 'Y' END CLOSED_DT_IND, 
          CASE WHEN (
            (
              Cast(
                Regexp_extract(days_past_due, '[0-9]+', 0) AS INT
              ) > 0 
              AND Cast(
                Regexp_extract(days_past_due, '[0-9]+', 0) AS INT
              ) IS NOT NULL
            ) 
            OR Nvl(
              Abs(amount_overdue_total), 
              0
            ) > (
              0.01 * Nvl(
                abs(coalesce(HIGH_CREDIT, DISBURSED_AMOUNT, CREDIT_LIMIT)), 
                0
              )
            )
          ) THEN 1 WHEN (
            Cast(
              Regexp_extract(days_past_due, '[0-9]+', 0) AS INT
            ) = 0 
            OR Cast(
              Regexp_extract(days_past_due, '[0-9]+', 0) AS INT
            ) IS NULL
          ) 
          AND Nvl(
            Abs(amount_overdue_total), 
            0
          ) <= (
            0.01 * Nvl(
              abs(coalesce(HIGH_CREDIT, DISBURSED_AMOUNT, CREDIT_LIMIT)), 
              0
            )
          ) THEN 0 ELSE NULL END OVERDUE_IND, 
          CASE WHEN das IN ('S04', 'S05') THEN 1 ELSE 0 END DAS_ACTIVE_FLAG 
        FROM 
          hmanalytics.hm_cns_closed_macro_anal_parquet_new 
        WHERE mfi_id = 'PRB0000006'
        AND reported_dt <= From_unixtime(
            Unix_timestamp('31-OCT-17', 'dd-MMM-yy'), 
            'yyyy-MM-dd'
          ) 
          AND disbursed_dt <= From_unixtime(
            Unix_timestamp('30-SEP-17', 'dd-MMM-yy'), 
            'yyyy-MM-dd'
          ) 
          AND account_type IN ('A06') 
          AND (
            closed_dt IS NULL 
            OR closed_dt <= From_unixtime(
              Unix_timestamp('31-OCT-17', 'dd-MMM-yy'), 
              'yyyy-MM-dd'
            ) 
            AND act_insert_dt <= From_unixtime(
              Unix_timestamp('18112017', 'ddMMyyyy'), 
              'yyyy-MM-dd'
            )
          )
    AND abs(coalesce(HIGH_CREDIT,DISBURSED_AMOUNT, CREDIT_LIMIT)) <= 2500000
      ) A 
    GROUP BY 
      mfi_id, 
      account_type, 
      reported_dt
  ) B 
  LEFT JOIN hmanalytics.hm_contributor_dim C ON (
    B.mfi_id = C.contributor_id 
    AND C.active = 1
  ) 
GROUP BY 
  mfi_id, 
  C.institution_name,
  account_type  """)*/
  //nocDF.repartition(1).write.mode("append").saveAsTable("hmanalytics.hm_nocReport")
 //nocDF.orderBy(col("mfi_id"), col("active_loans").desc).repartition(1).write.mode("overwrite").option("header", "true").csv("/tmp/prateek/NOC")	

 
 val f1 = Future{
  nocDF.repartition(1).write.mode("append").saveAsTable("hmanalytics.hm_nocReport")	}
	
	val f2 = Future{
  nocDF.orderBy(col("mfi_id"), col("active_loans").desc).repartition(1).write.mode("overwrite").option("header", "true").csv("/tmp/prateek/NOC")	
	}
	
	Await.ready(f1, Duration.Inf)
	Await.ready(f2, Duration.Inf)
	
	//NOC Outliers Code
val rowDF = spark.sql("""select *, ROW_NUMBER () OVER (PARTITION BY mfi_id,reported_dt,account_type ORDER BY total_loans desc) rw  from hmanalytics.hm_nocReport""")
rowDF.createOrReplaceTempView("dedupe")

	
val fourMonthsData = spark.sql(""" select distinct * from dedupe where reported_dt <= From_unixtime(unix_timestamp('"""+as_of+ """', 'dd-MMM-yy'),'yyyy-MM-dd') and reported_dt >= add_months(From_unixtime(unix_timestamp('"""+as_of+ """', 'dd-MMM-yy'),'yyyy-MM-dd'),-3) and rw=1""")

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
WHERE (B.reported_dt <> add_months(From_unixtime(unix_timestamp('"""+as_of+ """', 'dd-MMM-yy'),'yyyy-MM-dd'),-3)
OR A.reported_dt <> From_unixtime(unix_timestamp('"""+as_of+ """', 'dd-MMM-yy'),'yyyy-MM-dd'))
AND (A.ACCOUNT_TYPE is not null OR B.ACCOUNT_TYPE is not null)
""")

diffDF.repartition(1).write.mode("overwrite").saveAsTable("hmanalytics.hm_noc_month_diff")

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

finalDF.orderBy(col("mfi_id"), col("account_type"), col("reported_dt_from")).repartition(1).write.mode("overwrite").saveAsTable("hmanalytics.hm_noc_month_outlier")
finalDF.orderBy(col("mfi_id")).repartition(1).write.mode("overwrite").option("header", "true").csv("/tmp/prateek/NOC")	

	

}