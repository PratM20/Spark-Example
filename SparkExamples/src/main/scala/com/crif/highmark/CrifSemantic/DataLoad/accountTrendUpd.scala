package com.crif.highmark.CrifSemantic.DataLoad
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._


object accountTrendUpd extends App {
	
val spark = SparkSession.builder().appName("accountTrendUpd")
.config("hive.metastore.uris", "thrift://chmcisprbdmn01.chm.intra:9083")
.config("hive.exec.dynamic.partition", "true")
.config("hive.exec.dynamic.partition.mode", "nonstrict")
.config("hive.enforce.bucketing", "true")
.config("hive.exec.max.dynamic.partitions", "20000")
.enableHiveSupport().getOrCreate()

/*val accDF = spark.read.format("jdbc") 
.option("url", "jdbc:oracle:thin:@172.16.3.12:1521:HM1")
.option("dbtable", "(SELECT account_key,mfi_id,reported_dt,act_reported_dt,account_nbr,disbursed_dt,sanctioned_amount,disbursed_amount,num_installment,instal_freq,credit_limit,cash_limit,manual_update_dt,manual_update_ind,suppress_indicator,closed_dt,update_dt  FROM CHMCNSTREND.HM_MFI_ACCOUNT_UPD) a")
.option("user", "E312") .option("password", "E312##123") 
.option("driver", "oracle.jdbc.driver.OracleDriver")
.option("fetchSize","10000").load()

accDF.repartition(1000).write.mode("append").saveAsTable("hmanalytics.hm_mfi_account_upd")*/
  
val a13ActDF = spark.sql(s"""SELECT mfi_id, 
       C.institution_name, 
       account_type, 
       From_unixtime(Unix_timestamp('30-SEP-18', 'DD-MMM-YY'),'yyyy-MM-dd'),
       Sum(total_loans), 
       Sum(total_disb_amount), 
       Sum(active_loans), 
       Sum(active_disb_amt), 
       Sum(active_outstanding), 
       Sum(lar_1_30), 
       Sum(lar_31_60), 
       Sum(lar_61_90), 
       Sum(lar_91_180), 
       Sum(lar_181_360), 
       Sum(lar_360_plus), 
       Sum(par_1_30), 
       Sum(par_31_60), 
       Sum(par_61_90), 
       Sum(par_91_180), 
       Sum(par_181_360), 
       Sum(par_360_plus), 
       Sum(total_written_off_loans), 
       Sum(total_written_off_amount) 
FROM   (SELECT mfi_id, 
               account_type, 
               Last_day(reported_dt)       ACT_REPORTED_DT, 
               Count(DISTINCT account_key) TOTAL_LOANS, 
               Sum(disbursed_amount)       TOTAL_DISB_AMOUNT, 
               Count(DISTINCT CASE 
                                WHEN das_active_flag = 1 
                                     AND closed_dt_ind = 'N' THEN account_key 
                                ELSE NULL 
                              END)         ACTIVE_LOANS, 
               Sum(CASE 
                     WHEN das_active_flag = 1 
                          AND closed_dt_ind = 'N' THEN disbursed_amount 
                     ELSE 0 
                   END)                    ACTIVE_DISB_AMT, 
               Sum(CASE 
                     WHEN das_active_flag = 1 
                          AND closed_dt_ind = 'N' THEN current_balance 
                     ELSE 0 
                   END)                    ACTIVE_OUTSTANDING, 
               Count(DISTINCT CASE 
                                WHEN das_active_flag = 1 
                                     AND overdue_ind = 1 
                                     AND closed_dt_ind = 'N' 
                                     AND dpd BETWEEN 1 AND 30 THEN account_key 
                                ELSE NULL 
                              END)         LAR_1_30, 
               Count(DISTINCT CASE 
                                WHEN das_active_flag = 1 
                                     AND overdue_ind = 1 
                                     AND closed_dt_ind = 'N' 
                                     AND dpd BETWEEN 31 AND 60 THEN account_key 
                                ELSE NULL 
                              END)         LAR_31_60, 
               Count(DISTINCT CASE 
                                WHEN das_active_flag = 1 
                                     AND overdue_ind = 1 
                                     AND closed_dt_ind = 'N' 
                                     AND dpd BETWEEN 61 AND 90 THEN account_key 
                                ELSE NULL 
                              END)         LAR_61_90, 
               Count(DISTINCT CASE 
                                WHEN das_active_flag = 1 
                                     AND overdue_ind = 1 
                                     AND closed_dt_ind = 'N' 
                                     AND dpd BETWEEN 91 AND 180 THEN account_key 
                                ELSE NULL 
                              END)         LAR_91_180, 
               Count(DISTINCT CASE 
                                WHEN das_active_flag = 1 
                                     AND overdue_ind = 1 
                                     AND closed_dt_ind = 'N' 
                                     AND dpd BETWEEN 181 AND 360 THEN 
                                account_key 
                                ELSE NULL 
                              END)         LAR_181_360, 
               Count(DISTINCT CASE 
                                WHEN das_active_flag = 1 
                                     AND overdue_ind = 1 
                                     AND closed_dt_ind = 'N' 
                                     AND dpd >= 361 THEN account_key 
                                ELSE NULL 
                              END)         LAR_360_PLUS, 
               Sum(CASE 
                     WHEN das_active_flag = 1 
                          AND overdue_ind = 1 
                          AND closed_dt_ind = 'N' 
                          AND dpd BETWEEN 1 AND 30 THEN current_balance 
                     ELSE 0 
                   END)                    PAR_1_30, 
               Sum(CASE 
                     WHEN das_active_flag = 1 
                          AND overdue_ind = 1 
                          AND closed_dt_ind = 'N' 
                          AND dpd BETWEEN 31 AND 60 THEN current_balance 
                     ELSE 0 
                   END)                    PAR_31_60, 
               Sum(CASE 
                     WHEN das_active_flag = 1 
                          AND overdue_ind = 1 
                          AND closed_dt_ind = 'N' 
                          AND dpd BETWEEN 61 AND 90 THEN current_balance 
                     ELSE 0 
                   END)                    PAR_61_90, 
               Sum(CASE 
                     WHEN das_active_flag = 1 
                          AND overdue_ind = 1 
                          AND closed_dt_ind = 'N' 
                          AND dpd BETWEEN 91 AND 180 THEN current_balance 
                     ELSE 0 
                   END)                    PAR_91_180, 
               Sum(CASE 
                     WHEN das_active_flag = 1 
                          AND overdue_ind = 1 
                          AND closed_dt_ind = 'N' 
                          AND dpd BETWEEN 181 AND 360 THEN current_balance 
                     ELSE 0 
                   END)                    PAR_181_360, 
               Sum(CASE 
                     WHEN das_active_flag = 1 
                          AND overdue_ind = 1 
                          AND closed_dt_ind = 'N' 
                          AND dpd >= 361 THEN current_balance 
                     ELSE 0 
                   END)                    PAR_360_PLUS, 
               Count(DISTINCT CASE 
                                WHEN das IN ( 'S06', 'S13' ) THEN account_key 
                                ELSE NULL 
                              END)         TOTAL_WRITTEN_OFF_LOANS, 
               Sum(CASE 
                     WHEN das IN ( 'S06', 'S13' ) THEN chargeoff_amt 
                     ELSE 0 
                   END)                    TOTAL_WRITTEN_OFF_AMOUNT 
        FROM   (SELECT DISTINCT account_key, 
                                account_type, 
                                mfi_id, 
                                reported_dt, 
                                closed_dt, 
                                Abs(current_balance) 
                                         CURRENT_BALANCE, 
                                Abs(amount_overdue_total) 
                                         AMOUNT_OVERDUE_TOTAL, 
                                Cast(Regexp_extract(days_past_due, '[0-9]+', 0) 
                                     AS INT) 
                                DPD, 
                                Abs(chargeoff_amt) 
                                         CHARGEOFF_AMT, 
                                das, 
                                Abs(disbursed_amount) 
                                         DISBURSED_AMOUNT, 
                                CASE 
                                  WHEN ( closed_dt IS NULL 
                                          OR closed_dt > From_unixtime( 
                                                         Unix_timestamp( 
                                                         '30-SEP-18', 
                                                         'dd-MMM-yy' 
                                                         ) 
                                                         , 
                                                         'yyyy-MM-dd' 
                                                         ) ) THEN 'N' 
                                  ELSE 'Y' 
                                END 
                                         CLOSED_DT_IND, 
                                CASE 
                                  WHEN ( ( Cast(Regexp_extract(days_past_due, 
                                                '[0-9]+', 
                                                0) AS 
                                                INT) > 0 
                                           AND Cast(Regexp_extract(days_past_due 
                                                    , 
                                                    '[0-9]+', 0) 
                                                    AS INT) IS NOT 
                                               NULL ) 
                                          OR Nvl(Abs(amount_overdue_total), 0) > 
                                             ( 
                                             0.01 * Nvl(Abs(disbursed_amount), 0 
                                                    ) 
                                                     ) ) 
                                THEN 1 
                                  WHEN ( Cast(Regexp_extract(days_past_due, 
                                              '[0-9]+', 0 
                                              ) AS INT 
                                         ) 
                                         = 0 
                                          OR Cast(Regexp_extract(days_past_due, 
                                                  '[0-9]+', 0) AS 
                                                  INT) IS NULL ) 
                                       AND Nvl(Abs(amount_overdue_total), 0) <= 
                                           ( 
                                           0.01 * Nvl(Abs(disbursed_amount), 0) 
                                           ) THEN 
                                  0 
                                  ELSE NULL 
                                END 
                                         OVERDUE_IND, 
                                CASE 
                                  WHEN das IN ( 'S04', 'S05' ) THEN 1 
                                  ELSE 0 
                                END 
                                         DAS_ACTIVE_FLAG 
                FROM   hmanalytics.hm_cns_macro_analysis_nov_18_parquet 
                WHERE  mfi_id = 'PRB0000027' 
                       AND reported_dt <= From_unixtime(Unix_timestamp( 
                                                         '30-SEP-18', 
                                                         'dd-MMM-yy' 
                                                         ), 
                                           'yyyy-MM-dd') 
                      AND disbursed_dt <= From_unixtime(Unix_timestamp( 
                                                         '30-SEP-18', 
                                                         'dd-MMM-yy' 
                                                         ), 
                                           'yyyy-MM-dd') 
                       AND account_type IN ( 'A13', 'A07', 'A12', 'A15', 'A06' ) 
                UNION ALL 
                SELECT DISTINCT account_key, 
                                account_type, 
                                mfi_id, 
                                reported_dt, 
                                closed_dt, 
                                Abs(current_balance) 
                                CURRENT_BALANCE, 
                                Abs(amount_overdue_total) 
                                AMOUNT_OVERDUE_TOTAL, 
                                Cast(Regexp_extract(days_past_due, '[0-9]+', 0) 
                                     AS INT) 
                                DPD, 
                                Abs(chargeoff_amt) 
                                CHARGEOFF_AMT, 
                                das, 
                                Abs(disbursed_amount) 
                                DISBURSED_AMOUNT, 
                                CASE 
                                  WHEN ( closed_dt IS NULL 
                                          OR closed_dt > From_unixtime( 
                                                         Unix_timestamp( 
                                                         '30-SEP-18', 
                                                         'dd-MMM-yy' 
                                                         ) 
                                                         , 
                                                         'yyyy-MM-dd' 
                                                         ) ) THEN 'N' 
                                  ELSE 'Y' 
                                END 
                                CLOSED_DT_IND, 
                                CASE 
                                  WHEN ( ( Cast(Regexp_extract(days_past_due, 
                                                '[0-9]+', 
                                                0) AS 
                                                INT) > 0 
                                           AND Cast(Regexp_extract(days_past_due 
                                                    , 
                                                    '[0-9]+', 0) 
                                                    AS INT) IS NOT 
                                               NULL ) 
                                          OR Nvl(Abs(amount_overdue_total), 0) > 
                                             ( 
                                             0.01 * Nvl(Abs(disbursed_amount), 0 
                                                    ) 
                                                     ) ) 
                                THEN 1 
                                  WHEN ( Cast(Regexp_extract(days_past_due, 
                                              '[0-9]+', 0 
                                              ) AS INT 
                                         ) 
                                         = 0 
                                          OR Cast(Regexp_extract(days_past_due, 
                                                  '[0-9]+', 0) AS 
                                                  INT) IS NULL ) 
                                       AND Nvl(Abs(amount_overdue_total), 0) <= 
                                           ( 
                                           0.01 * Nvl(Abs(disbursed_amount), 0) 
                                           ) THEN 
                                  0 
                                  ELSE NULL 
                                END 
                                OVERDUE_IND, 
                                CASE 
                                  WHEN das IN ( 'S04', 'S05' ) THEN 1 
                                  ELSE 0 
                                END 
                                DAS_ACTIVE_FLAG 
                FROM   hmanalytics.hm_hc_cns_macro_closed_parquet 
                WHERE  mfi_id = 'PRB0000027' 
                       AND reported_dt <= From_unixtime(Unix_timestamp( 
                                                         '30-SEP-18', 
                                                         'dd-MMM-yy' 
                                                         ), 
                                          'yyyy-MM-dd') 
                       AND disbursed_dt <= From_unixtime(Unix_timestamp( 
                                                         '30-SEP-18', 
                                                         'dd-MMM-yy' 
                                                         ), 
                                           'yyyy-MM-dd') 
                       AND account_type IN ( 'A13', 'A07', 'A12', 'A15', 'A06' ) 
                       AND (closed_dt IS NULL 
                        OR closed_dt <= From_unixtime(Unix_timestamp('31-OCT-18' 
                                                      , 
                                                      'dd-MMM-yy'), 
                                            'yyyy-MM-dd') 
                           AND act_insert_dt <= From_unixtime(Unix_timestamp( 
                                                              '09112018' 
                                                              , 
                                                              'ddMMyyyy'), 
                                                'yyyy-MM-dd'))) A 
        GROUP  BY mfi_id, 
                  account_type, 
                  reported_dt) B 
       LEFT JOIN hmanalytics.hm_contributor_dim C 
              ON ( B.mfi_id = C.contributor_id 
                   AND C.active = 1 ) 
GROUP  BY mfi_id, 
          C.institution_name, 
          account_type 
			""")
			
a13ActDF.repartition(1).write.mode("append").option("header", "true").csv("/tmp/prateek/NOC")			
}