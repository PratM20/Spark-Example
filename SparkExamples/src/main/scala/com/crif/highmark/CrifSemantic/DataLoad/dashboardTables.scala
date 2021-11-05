package com.crif.highmark.CrifSemantic.DataLoad
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.types._
import org.apache.spark.sql.SQLContext

object dashboardTables extends App {
  
	 val spark = SparkSession.builder().appName("homeCreditLoad")
											.config("hive.metastore.uris", "thrift://chmcisprbdmn01.chm.intra:9083")
											.config("hive.exec.dynamic.partition", "true")
                      .config("hive.exec.dynamic.partition.mode", "nonstrict")
                      .config("hive.enforce.bucketing", "true")
                      .config("hive.exec.max.dynamic.partitions", "20000")
											.enableHiveSupport().getOrCreate()
	import spark.implicits._
	
	val portfolioDF = spark.sql(s"""
SELECT  AS_OF,
        CASE WHEN MAX_DPD=0 THEN '0'
            WHEN MAX_DPD BETWEEN 1 AND 30 THEN '1_30'
            WHEN MAX_DPD BETWEEN 31 AND 60 THEN '31_60'
            WHEN MAX_DPD BETWEEN 61 AND 90 THEN '61_90'
            WHEN MAX_DPD BETWEEN 91 AND 180 THEN '91_180'
            WHEN MAX_DPD BETWEEN 181 AND 360 THEN '181_360'
            WHEN MAX_DPD>360 THEN '>360'
            ELSE NULL
            END MAX_DPD_BUCK,
        CASE WHEN TOTAL_ACT_OUTSTANDING_AMOUNT BETWEEN 0 AND 5000 THEN '0-5K'
            WHEN TOTAL_ACT_OUTSTANDING_AMOUNT BETWEEN 5001 AND 10000 THEN '5K-10K'
            WHEN TOTAL_ACT_OUTSTANDING_AMOUNT BETWEEN 10001 AND 15000 THEN '10K-15K'
            WHEN TOTAL_ACT_OUTSTANDING_AMOUNT BETWEEN 15001 AND 20000 THEN '15K-20K'
            WHEN TOTAL_ACT_OUTSTANDING_AMOUNT BETWEEN 20001 AND 50000 THEN '20K-50K'
            WHEN TOTAL_ACT_OUTSTANDING_AMOUNT BETWEEN 50001 AND 500000 THEN '50K-5LAC'
         ELSE 'OTHERS'
        END TOTAL_ACT_OUTSTANDING_AMT_BUCK,
        CASE WHEN OUTSTANDING_AMOUNT_SECURED BETWEEN 0 AND 5000 THEN '0-5K'
            WHEN OUTSTANDING_AMOUNT_SECURED BETWEEN 5001 AND 10000 THEN '5K-10K'
            WHEN OUTSTANDING_AMOUNT_SECURED BETWEEN 10001 AND 15000 THEN '10K-15K'
            WHEN OUTSTANDING_AMOUNT_SECURED BETWEEN 15001 AND 20000 THEN '15K-20K'
            WHEN OUTSTANDING_AMOUNT_SECURED BETWEEN 20001 AND 50000 THEN '20K-50K'
            WHEN OUTSTANDING_AMOUNT_SECURED BETWEEN 50001 AND 500000 THEN '50K-5LAC'
         ELSE 'OTHERS'
        END OUTSTANDING_SECURED_BUCK ,
        CASE WHEN OUTSTANDING_AMOUNT_UNSECURED BETWEEN 0 AND 5000 THEN '0-5K'
            WHEN OUTSTANDING_AMOUNT_UNSECURED BETWEEN 5001 AND 10000 THEN '5K-10K'
            WHEN OUTSTANDING_AMOUNT_UNSECURED BETWEEN 10001 AND 15000 THEN '10K-15K'
            WHEN OUTSTANDING_AMOUNT_UNSECURED BETWEEN 15001 AND 20000 THEN '15K-20K'
            WHEN OUTSTANDING_AMOUNT_UNSECURED BETWEEN 20001 AND 50000 THEN '20K-50K'
            WHEN OUTSTANDING_AMOUNT_UNSECURED BETWEEN 50001 AND 500000 THEN '50K-5LAC'
         ELSE 'OTHERS'
        END OUTSTANDING_UNSEC_BUCK, 
        CASE WHEN OUTSTANDING_AMOUNT_Othr_Prodct BETWEEN 0 AND 5000 THEN '0-5K'
            WHEN OUTSTANDING_AMOUNT_Othr_Prodct BETWEEN 5001 AND 10000 THEN '5K-10K'
            WHEN OUTSTANDING_AMOUNT_Othr_Prodct BETWEEN 10001 AND 15000 THEN '10K-15K'
            WHEN OUTSTANDING_AMOUNT_Othr_Prodct BETWEEN 15001 AND 20000 THEN '15K-20K'
            WHEN OUTSTANDING_AMOUNT_Othr_Prodct BETWEEN 20001 AND 50000 THEN '20K-50K'
            WHEN OUTSTANDING_AMOUNT_Othr_Prodct BETWEEN 50001 AND 500000 THEN '50K-5LAC'
         ELSE 'OTHERS'
        END OUTS_AMOUNT_Othr_Prdct_BUCK,                           
        CLUST_IND LENDER_TYPE,
        count(distinct MAX_CLST_ID) TOTAL_CLUSTER,
        COUNT(DISTINCT CASE WHEN TOTAL_ACTIV_LOANS<>0 THEN MAX_CLST_ID ELSE NULL END) ACTIVE_CLUSTER,
        COUNT(DISTINCT CASE WHEN CLUST_IND='ONLY HC'  THEN MAX_CLST_ID ELSE NULL END) TOTAL_CLUSTER_HC,
        COUNT(DISTINCT CASE WHEN CLUST_IND='HC_WITH_OTHERS' THEN MAX_CLST_ID ELSE NULL END) TOTAL_CLST_HCWTH_OTHR,
        COUNT(DISTINCT CASE WHEN CLUST_IND='OTHR CLST' THEN MAX_CLST_ID ELSE NULL END) TOTAL_CLUSTER_OTRS,
        count(distinct case when CLST_HC_MIXED is not null then MAX_CLST_ID else null end) TOTAL_CLUSTER_HX_MIXED,
        count(distinct case when CLST_OTHERS_MIXED is not null then MAX_CLST_ID else null end) TOTAL_CLUSTER_OTHR_MIXED,
        count(distinct case when TOTAL_CLST_BANKS is not null then MAX_CLST_ID else null end) TOTAL_CLUSTER_BANKS,
        count(distinct case when TOTAL_CLST_NONBANKS is not null then MAX_CLST_ID else null end) TOTAL_CLUSTER_NONBANKS,
        count(distinct case when TOTAL_CLST_SECURED is not null then MAX_CLST_ID else null end) TOTAL_CLUSTER_SECURED,
        count(distinct case when TOTAL_CLST_UNSECURED is not null then MAX_CLST_ID else null end) TOTAL_CLUSTER_UNSECURED,
        count(distinct case when TOTAL_CLST_Othr_Prodct is not null then MAX_CLST_ID else null end) TOTAL_CLST_Othr_Prodct,
        sum(TOTAL_ACTIV_LOANS) TOTAL_ACTIV_LOANS,
        sum(ACTIV_LOANS_HC) ACTIV_LOANS_HC,
        SUM(ACTIV_LN_HCWTHOTHERS) ACTIV_LN_HCWTHOTHERS,
        sum(ACTIV_LOANS_OTHERS) ACTIV_LOANS_OTHERS,
        sum(ACTIV_LOANS_MIXED) ACTIV_LOANS_MIXED,
        SUM(ACTIV_LNS_HC_MIXED) ACTIV_LNS_HC_MIXED,
        SUM(ACTIV_LNS_OTHR_MIXED) ACTIV_LNS_OTHR_MIXED,
        SUM(ACTIV_LOANS_CL) ACTIV_LOANS_CL,
        SUM(ACTIV_LOANS_NONCL) ACTIV_LOANS_NONCL,
        SUM(ACTIV_LOANS_BANKS) ACTIV_LOANS_BANKS,
        SUM(ACTIV_LOANS_NONBANKS) ACTIV_LOANS_NONBANKS,
        SUM(ACTIV_LOANS_SECURED) ACTIV_LOANS_SECURED,
        SUM(ACTIV_LOANS_UNSECURED) ACTIV_LOANS_UNSECURED,
        SUM(ACTIV_LOANS_Othr_Prodct) ACTIV_LOANS_Othr_Prodct,
        sum(TOTAL_ACT_OUTSTANDING_AMOUNT) TOTAL_ACT_OUTSTANDING_AMOUNT,
        sum(OUTSTANDING_AMOUNT_HC) OUTSTANDING_AMOUNT_HC,
        SUM(OUTSTANDING_AMT_HCWTH_OTRS) OUTSTANDING_AMT_HCWTH_OTRS,
        sum(OUTSTANDING_AMOUNT_OTHERS) OUTSTANDING_AMOUNT_OTHERS,
        sum(OUTSTANDING_AMOUNT_MIXED) OUTSTANDING_AMOUNT_MIXED,
        SUM(OUTSTANDING_HC_MIXED) OUTSTANDING_HC_MIXED,
        SUM(OUTSTANDING_OTHERS_MIXED) OUTSTANDING_OTHERS_MIXED,
        sum(OUTSTANDING_AMOUNT_BANKS) OUTSTANDING_AMOUNT_BANKS,
        sum(OUTSTANDING_AMOUNT_NONBANKS) OUTSTANDING_AMOUNT_NONBANKS,
        sum(OUTSTANDING_AMOUNT_SECURED) OUTSTANDING_AMOUNT_SECURED,
        sum(OUTSTANDING_AMOUNT_UNSECURED) OUTSTANDING_AMOUNT_UNSECURED,
        SUM(OUTSTANDING_AMOUNT_Othr_Prodct) OUTSTANDING_AMOUNT_Othr_Prodct,
        round((sum(TOTAL_ACT_OUTSTANDING_AMOUNT)/(CASE WHEN sum(TOTAL_ACTIV_LOANS) =0 THEN 1 ELSE sum(TOTAL_ACTIV_LOANS) END )),2) Avg_Outstanding_AMt,
        SUM(PORTFOLIO_CL) PORTFOLIO_CL,
        SUM(PORTFOLIO_NONCL) PORTFOLIO_NONCL
  FROM ( 
        SELECT 'JUL19' AS_OF,* FROM hmanalytics.hm_hc_full_aggr_tbl_sep_19_fnl
        UNION ALL
        SELECT 'AUG19' AS_OF,* FROM hmanalytics.hm_hc_full_aggr_tbl_oct_19_fnl
        UNION ALL
        SELECT 'SEP19' AS_OF,* FROM hmanalytics.hm_hc_full_aggr_tbl_nov_19_fnl
 ) B
group by AS_OF,
         CASE WHEN MAX_DPD=0 THEN '0'
            WHEN MAX_DPD BETWEEN 1 AND 30 THEN '1_30'
            WHEN MAX_DPD BETWEEN 31 AND 60 THEN '31_60'
            WHEN MAX_DPD BETWEEN 61 AND 90 THEN '61_90'
            WHEN MAX_DPD BETWEEN 91 AND 180 THEN '91_180'
            WHEN MAX_DPD BETWEEN 181 AND 360 THEN '181_360'
            WHEN MAX_DPD>360 THEN '>360'
            ELSE NULL
            END,
        CASE WHEN TOTAL_ACT_OUTSTANDING_AMOUNT BETWEEN 0 AND 5000 THEN '0-5K'
            WHEN TOTAL_ACT_OUTSTANDING_AMOUNT BETWEEN 5001 AND 10000 THEN '5K-10K'
            WHEN TOTAL_ACT_OUTSTANDING_AMOUNT BETWEEN 10001 AND 15000 THEN '10K-15K'
            WHEN TOTAL_ACT_OUTSTANDING_AMOUNT BETWEEN 15001 AND 20000 THEN '15K-20K'
            WHEN TOTAL_ACT_OUTSTANDING_AMOUNT BETWEEN 20001 AND 50000 THEN '20K-50K'
            WHEN TOTAL_ACT_OUTSTANDING_AMOUNT BETWEEN 50001 AND 500000 THEN '50K-5LAC'
         ELSE 'OTHERS'
        END,
        CASE WHEN OUTSTANDING_AMOUNT_SECURED BETWEEN 0 AND 5000 THEN '0-5K'
            WHEN OUTSTANDING_AMOUNT_SECURED BETWEEN 5001 AND 10000 THEN '5K-10K'
            WHEN OUTSTANDING_AMOUNT_SECURED BETWEEN 10001 AND 15000 THEN '10K-15K'
            WHEN OUTSTANDING_AMOUNT_SECURED BETWEEN 15001 AND 20000 THEN '15K-20K'
            WHEN OUTSTANDING_AMOUNT_SECURED BETWEEN 20001 AND 50000 THEN '20K-50K'
            WHEN OUTSTANDING_AMOUNT_SECURED BETWEEN 50001 AND 500000 THEN '50K-5LAC'
         ELSE 'OTHERS'
        END,
        CASE WHEN OUTSTANDING_AMOUNT_UNSECURED BETWEEN 0 AND 5000 THEN '0-5K'
            WHEN OUTSTANDING_AMOUNT_UNSECURED BETWEEN 5001 AND 10000 THEN '5K-10K'
            WHEN OUTSTANDING_AMOUNT_UNSECURED BETWEEN 10001 AND 15000 THEN '10K-15K'
            WHEN OUTSTANDING_AMOUNT_UNSECURED BETWEEN 15001 AND 20000 THEN '15K-20K'
            WHEN OUTSTANDING_AMOUNT_UNSECURED BETWEEN 20001 AND 50000 THEN '20K-50K'
            WHEN OUTSTANDING_AMOUNT_UNSECURED BETWEEN 50001 AND 500000 THEN '50K-5LAC'
         ELSE 'OTHERS'
        END, 
        CASE WHEN OUTSTANDING_AMOUNT_Othr_Prodct BETWEEN 0 AND 5000 THEN '0-5K'
            WHEN OUTSTANDING_AMOUNT_Othr_Prodct BETWEEN 5001 AND 10000 THEN '5K-10K'
            WHEN OUTSTANDING_AMOUNT_Othr_Prodct BETWEEN 10001 AND 15000 THEN '10K-15K'
            WHEN OUTSTANDING_AMOUNT_Othr_Prodct BETWEEN 15001 AND 20000 THEN '15K-20K'
            WHEN OUTSTANDING_AMOUNT_Othr_Prodct BETWEEN 20001 AND 50000 THEN '20K-50K'
            WHEN OUTSTANDING_AMOUNT_Othr_Prodct BETWEEN 50001 AND 500000 THEN '50K-5LAC'
         ELSE 'OTHERS'
        END,                             
        CLUST_IND
""")
	
portfolioDF.write.mode("overwrite").saveAsTable("hmanalytics.hm_portfolio_data_full_fnl2")

	val actDataDF = spark.sql(s"""
SELECT AS_OF,
        CASE WHEN MAX_DPD=0 THEN '0'
            WHEN MAX_DPD BETWEEN 1 AND 30 THEN '1_30'
            WHEN MAX_DPD BETWEEN 31 AND 60 THEN '31_60'
            WHEN MAX_DPD BETWEEN 61 AND 90 THEN '61_90'
            WHEN MAX_DPD BETWEEN 91 AND 180 THEN '91_180'
            WHEN MAX_DPD BETWEEN 181 AND 360 THEN '181_360'
            WHEN MAX_DPD>360 THEN '>360'
            ELSE NULL
            END MAX_DPD_BUCK,
        case WHEN TOTAL_ACTIV_LOANS=1 THEN '1'
             WHEN TOTAL_ACTIV_LOANS=2 THEN '2'
             WHEN TOTAL_ACTIV_LOANS=3 THEN '3'
             WHEN TOTAL_ACTIV_LOANS=4 THEN '4'
             WHEN TOTAL_ACTIV_LOANS=5 THEN '5'
             WHEN TOTAL_ACTIV_LOANS>5 THEN '>5'
        ELSE NULL
        END TOTAL_ACTIV_LOANS_BUCK,
        case WHEN ACTIV_LOANS_SECURED=1 THEN '1'
             WHEN ACTIV_LOANS_SECURED=2 THEN '2'
             WHEN ACTIV_LOANS_SECURED=3 THEN '3'
             WHEN ACTIV_LOANS_SECURED=4 THEN '4'
             WHEN ACTIV_LOANS_SECURED=5 THEN '5'
             WHEN ACTIV_LOANS_SECURED>5 THEN '>5'
        ELSE NULL
        END ACTIV_LOANS_SECURED_BUCK,
        case WHEN ACTIV_LOANS_UNSECURED=1 THEN '1'
             WHEN ACTIV_LOANS_UNSECURED=2 THEN '2'
             WHEN ACTIV_LOANS_UNSECURED=3 THEN '3'
             WHEN ACTIV_LOANS_UNSECURED=4 THEN '4'
             WHEN ACTIV_LOANS_UNSECURED=5 THEN '5'
             WHEN ACTIV_LOANS_UNSECURED>5 THEN '>5'
        ELSE NULL
        END ACTIV_LOANS_UNSECURED_BUCK,
        CLUST_IND,
        count(distinct MAX_CLST_ID) TOTAL_CLUSTER,
        COUNT(DISTINCT CASE WHEN TOTAL_ACTIV_LOANS<>0 THEN MAX_CLST_ID ELSE NULL END) ACTIVE_CLUSTER,
        COUNT(DISTINCT CASE WHEN CLUST_IND='ONLY HC'  THEN MAX_CLST_ID ELSE NULL END) TOTAL_CLUSTER_HC,
        COUNT(DISTINCT CASE WHEN CLUST_IND='HC_WITH_OTHERS' THEN MAX_CLST_ID ELSE NULL END) TOTAL_CLST_HCWTH_OTHR,
        COUNT(DISTINCT CASE WHEN CLUST_IND='OTHR CLST' THEN MAX_CLST_ID ELSE NULL END) TOTAL_CLUSTER_OTRS,
        count(distinct case when CLST_HC_MIXED is not null then MAX_CLST_ID else null end) TOTAL_CLUSTER_HX_MIXED,
        count(distinct case when CLST_OTHERS_MIXED is not null then MAX_CLST_ID else null end) TOTAL_CLUSTER_OTHR_MIXED,
        count(distinct case when TOTAL_CLST_BANKS is not null then MAX_CLST_ID else null end) TOTAL_CLUSTER_BANKS,
        count(distinct case when TOTAL_CLST_NONBANKS is not null then MAX_CLST_ID else null end) TOTAL_CLUSTER_NONBANKS,
        count(distinct case when TOTAL_CLST_SECURED is not null then MAX_CLST_ID else null end) TOTAL_CLUSTER_SECURED,
        count(distinct case when TOTAL_CLST_UNSECURED is not null then MAX_CLST_ID else null end) TOTAL_CLUSTER_UNSECURED,
        count(distinct case when TOTAL_CLST_Othr_Prodct is not null then MAX_CLST_ID else null end) TOTAL_CLST_Othr_Prodct,
        sum(TOTAL_ACTIV_LOANS) TOTAL_ACTIV_LOANS,
        SUM(ACTIV_LOANS_BANKS) ACTIV_LOANS_BANKS,
        SUM(ACTIV_LOANS_NONBANKS) ACTIV_LOANS_NONBANKS,
        SUM(ACTIV_LOANS_SECURED) ACTIV_LOANS_SECURED,
        SUM(ACTIV_LOANS_UNSECURED) ACTIV_LOANS_UNSECURED,
        SUM(ACTIV_LOANS_Othr_Prodct) ACTIV_LOANS_Othr_Prodct
  FROM (
        
        SELECT 'JUL19' AS_OF,* FROM hmanalytics.hm_hc_full_aggr_tbl_sep_19_fnl
        UNION ALL
        SELECT 'AUG19' AS_OF,* FROM hmanalytics.hm_hc_full_aggr_tbl_oct_19_fnl
        UNION ALL
        SELECT 'SEP19' AS_OF,* FROM hmanalytics.hm_hc_full_aggr_tbl_nov_19_fnl
) B
group by AS_OF,
         CASE WHEN MAX_DPD=0 THEN '0'
            WHEN MAX_DPD BETWEEN 1 AND 30 THEN '1_30'
            WHEN MAX_DPD BETWEEN 31 AND 60 THEN '31_60'
            WHEN MAX_DPD BETWEEN 61 AND 90 THEN '61_90'
            WHEN MAX_DPD BETWEEN 91 AND 180 THEN '91_180'
            WHEN MAX_DPD BETWEEN 181 AND 360 THEN '181_360'
            WHEN MAX_DPD>360 THEN '>360'
            ELSE NULL
            END,
         case WHEN TOTAL_ACTIV_LOANS=1 THEN '1'
             WHEN TOTAL_ACTIV_LOANS=2 THEN '2'
             WHEN TOTAL_ACTIV_LOANS=3 THEN '3'
             WHEN TOTAL_ACTIV_LOANS=4 THEN '4'
             WHEN TOTAL_ACTIV_LOANS=5 THEN '5'
             WHEN TOTAL_ACTIV_LOANS>5 THEN '>5'
        ELSE NULL
        END,
        case WHEN ACTIV_LOANS_SECURED=1 THEN '1'
             WHEN ACTIV_LOANS_SECURED=2 THEN '2'
             WHEN ACTIV_LOANS_SECURED=3 THEN '3'
             WHEN ACTIV_LOANS_SECURED=4 THEN '4'
             WHEN ACTIV_LOANS_SECURED=5 THEN '5'
             WHEN ACTIV_LOANS_SECURED>5 THEN '>5'
        ELSE NULL
        END,
        case WHEN ACTIV_LOANS_UNSECURED=1 THEN '1'
             WHEN ACTIV_LOANS_UNSECURED=2 THEN '2'
             WHEN ACTIV_LOANS_UNSECURED=3 THEN '3'
             WHEN ACTIV_LOANS_UNSECURED=4 THEN '4'
             WHEN ACTIV_LOANS_UNSECURED=5 THEN '5'
             WHEN ACTIV_LOANS_UNSECURED>5 THEN '>5'
        ELSE NULL
        END,
        CLUST_IND
""")

actDataDF.write.mode("overwrite").saveAsTable("hmanalytics.hm_acnt_data_full_final2")

val ActDF = spark.sql(s"""
SELECT A.AS_OF,
        A.CLUST_IND,
        CASE WHEN ABS(COALESCE(HIGH_CREDIT,DISBURSED_AMOUNT,CREDIT_LIMIT)) BETWEEN 0 AND 5000 THEN '0-5K'
        WHEN ABS(COALESCE(HIGH_CREDIT,DISBURSED_AMOUNT,CREDIT_LIMIT)) BETWEEN 5001 AND 10000 THEN '5K-10K'
        WHEN ABS(COALESCE(HIGH_CREDIT,DISBURSED_AMOUNT,CREDIT_LIMIT)) BETWEEN 10001 AND 15000 THEN '10K-15K'
        WHEN ABS(COALESCE(HIGH_CREDIT,DISBURSED_AMOUNT,CREDIT_LIMIT)) BETWEEN 15001 AND 20000 THEN '15K-20K'
        WHEN ABS(COALESCE(HIGH_CREDIT,DISBURSED_AMOUNT,CREDIT_LIMIT)) BETWEEN 20001 AND 50000 THEN '20K-50K'
        WHEN ABS(COALESCE(HIGH_CREDIT,DISBURSED_AMOUNT,CREDIT_LIMIT)) BETWEEN 50001 AND 500000 THEN '50K-5LAC'
     ELSE 'OTHERS'
    END TICKET_SIZE,
    CASE WHEN MFI_ID='NBF0000399' THEN 'Home Credit'
         WHEN (MFI_ID LIKE 'ARC%' OR MFI_ID LIKE 'CCC%' OR MFI_ID LIKE 'NBF%' OR MFI_ID LIKE 'HFC%')  THEN 'Non Banks'
         ELSE 'Banks'
    END Lender_Type,
    CASE WHEN ACCOUNT_TYPE IN ('A01','A04','A09','A08','A07','A24','A06','A25','A20','A13',
                               'A31','A67','A03','A41','A61','A05','A42','A11') THEN 'Secured'
         WHEN ACCOUNT_TYPE IN ('A15','A62','A19','A28','A12','A22','A15','A63','A29','A27') Then 'Unsecured'
        else 'Others'
    END Account_type,
    case WHEN TOTAL_LOANS=1 THEN '1'
         WHEN TOTAL_LOANS=2 THEN '2'
         WHEN TOTAL_LOANS=3 THEN '3'
         WHEN TOTAL_LOANS=4 THEN '4'
         WHEN TOTAL_LOANS=5 THEN '5'
         WHEN TOTAL_LOANS>5 THEN '>5'
    ELSE NULL
    END TOTAL_LOANS_BUCK,
    case WHEN TOTAL_ACTIV_LOANS=1 THEN '1'
         WHEN TOTAL_ACTIV_LOANS=2 THEN '2'
         WHEN TOTAL_ACTIV_LOANS=3 THEN '3'
         WHEN TOTAL_ACTIV_LOANS=4 THEN '4'
         WHEN TOTAL_ACTIV_LOANS=5 THEN '5'
         WHEN TOTAL_ACTIV_LOANS>5 THEN '>5'
    ELSE NULL
    END ACTIVE_LOANS_BUCK,
    CASE WHEN ((DPD=0 OR DPD IS NULL) AND DAS IN ('S04','S05','S20','S25','S18')) THEN '0'
         WHEN DPD BETWEEN 1 AND 30 THEN '1_30'
         WHEN DPD BETWEEN 31 AND 60 THEN '31_60'
         WHEN DPD BETWEEN 61 AND 90 THEN '61_90'
         WHEN DPD BETWEEN 91 AND 180 THEN '91_180'
         WHEN DPD BETWEEN 181 AND 360 THEN '181_360'
         WHEN DPD>360 THEN '>360'
         ELSE NULL
    END MAX_DPD_BUCK,
    COUNT(DISTINCT a.max_clst_id) TOTAL_CLUSTER,
    COUNT(DISTINCT(CASE WHEN (CLOSED_DT IS NULL OR CLOSED_DT> from_unixtime(unix_timestamp(LOAD_END_DT, 'dd-MMM-yy'), 'yyyy-MM-dd')) AND DAS IN ('S04','S05','S20','S25','S18') THEN A.max_clst_id ELSE NULL END)) ACTIVE_CLUSTER,
    count(distinct ACCOUNT_KEY) TOTAL_LOANS,
    COUNT(DISTINCT(CASE WHEN (CLOSED_DT IS NULL OR CLOSED_DT> from_unixtime(unix_timestamp(LOAD_END_DT, 'dd-MMM-yy'), 'yyyy-MM-dd')) AND DAS IN ('S04','S05','S20','S25','S18') THEN ACCOUNT_KEY ELSE NULL END)) TOTAL_ACTIV_LOANS,
    SUM(CASE WHEN (CLOSED_DT IS NULL OR CLOSED_DT> from_unixtime(unix_timestamp(LOAD_END_DT, 'dd-MMM-yy'), 'yyyy-MM-dd')) AND DAS IN ('S04','S05','S20','S25','S18') THEN CURRENT_BALANCE ELSE 0 END) TOTAL_ACT_OUTSTANDING_AMOUNT,
    SUM(ABS(COALESCE(HIGH_CREDIT,DISBURSED_AMOUNT,CREDIT_LIMIT))) TOTAL_DISB_AMOUNT,
    SUM(CASE WHEN (CLOSED_DT IS NULL OR CLOSED_DT> from_unixtime(unix_timestamp(LOAD_END_DT, 'dd-MMM-yy'), 'yyyy-MM-dd')) AND DAS IN ('S04','S05','S20','S25','S18') THEN ABS(COALESCE(HIGH_CREDIT,DISBURSED_AMOUNT,CREDIT_LIMIT)) ELSE 0 END) TOTAL_ACT_DISB_AMOUNT,
    SUM(MIN_AMT_DUE_FNL) MIN_AMOUNT_DUE,
    SUM(ANNUAL_INCOME_FNL) ANUAL_INCOME
FROM (
      SELECT 'JUL19' AS_OF,'30-JUN-19' LOAD_END_DT, A.*,CASE WHEN rw=1 THEN annual_income ELSE null END ANNUAL_INCOME_FNL,CASE WHEN rw=1 AND Min_amt_threshold=1 THEN min_amount_due ELSE null END MIN_AMT_DUE_FNL
        FROM hmanalytics.hm_hc_full_base_tbl_sep_19_fnl A
       WHERE threshold_flag_overall=1  and threshold_acnt_type=1 
      UNION ALL
    SELECT 'AUG19' AS_OF,'31-JUL-19' LOAD_END_DT, A.*,CASE WHEN rw=1 THEN annual_income ELSE null END ANNUAL_INCOME_FNL,CASE WHEN rw=1 AND Min_amt_threshold=1 THEN min_amount_due ELSE null END MIN_AMT_DUE_FNL
        FROM hmanalytics.hm_hc_full_base_tbl_oct_19_fnl A
       WHERE threshold_flag_overall=1  and threshold_acnt_type=1 
     UNION ALL
    SELECT 'SEP19' AS_OF,'30-AUG-19' LOAD_END_DT, A.*,CASE WHEN rw=1 THEN annual_income ELSE null END ANNUAL_INCOME_FNL,CASE WHEN rw=1 AND Min_amt_threshold=1 THEN min_amount_due ELSE null END MIN_AMT_DUE_FNL
        FROM hmanalytics.hm_hc_full_base_tbl_nov_19_fnl A
       WHERE threshold_flag_overall=1  and threshold_acnt_type=1 
     
      )  A,
   (
     SELECT 'JUL19' AS_OF,*
      FROM hmanalytics.hm_hc_full_aggr_tbl_sep_19_fnl
     UNION ALL
     SELECT 'AUG19' AS_OF,*
      FROM hmanalytics.hm_hc_full_aggr_tbl_oct_19_fnl
     UNION ALL
     SELECT 'SEP19' AS_OF,*
      FROM hmanalytics.hm_hc_full_aggr_tbl_nov_19_fnl
     
    
 ) b
where A.AS_OF=B.AS_OF
  AND a.max_clst_id=b.max_clst_id
Group By A.AS_OF,
         A.CLUST_IND,
        CASE WHEN ABS(COALESCE(HIGH_CREDIT,DISBURSED_AMOUNT,CREDIT_LIMIT)) BETWEEN 0 AND 5000 THEN '0-5K'
        WHEN ABS(COALESCE(HIGH_CREDIT,DISBURSED_AMOUNT,CREDIT_LIMIT)) BETWEEN 5001 AND 10000 THEN '5K-10K'
        WHEN ABS(COALESCE(HIGH_CREDIT,DISBURSED_AMOUNT,CREDIT_LIMIT)) BETWEEN 10001 AND 15000 THEN '10K-15K'
        WHEN ABS(COALESCE(HIGH_CREDIT,DISBURSED_AMOUNT,CREDIT_LIMIT)) BETWEEN 15001 AND 20000 THEN '15K-20K'
        WHEN ABS(COALESCE(HIGH_CREDIT,DISBURSED_AMOUNT,CREDIT_LIMIT)) BETWEEN 20001 AND 50000 THEN '20K-50K'
        WHEN ABS(COALESCE(HIGH_CREDIT,DISBURSED_AMOUNT,CREDIT_LIMIT)) BETWEEN 50001 AND 500000 THEN '50K-5LAC'
     ELSE 'OTHERS'
    END,
    CASE WHEN MFI_ID='NBF0000399' THEN 'Home Credit'
         WHEN (MFI_ID LIKE 'ARC%' OR MFI_ID LIKE 'CCC%' OR MFI_ID LIKE 'NBF%' OR MFI_ID LIKE 'HFC%')  THEN 'Non Banks'
         ELSE 'Banks'
    END,
    CASE WHEN ACCOUNT_TYPE IN ('A01','A04','A09','A08','A07','A24','A06','A25','A20','A13',
                               'A31','A67','A03','A41','A61','A05','A42','A11') THEN 'Secured'
         WHEN ACCOUNT_TYPE IN ('A15','A62','A19','A28','A12','A22','A15','A63','A29','A27') Then 'Unsecured'
         else 'Others'
    END,
    case WHEN TOTAL_LOANS=1 THEN '1'
         WHEN TOTAL_LOANS=2 THEN '2'
         WHEN TOTAL_LOANS=3 THEN '3'
         WHEN TOTAL_LOANS=4 THEN '4'
         WHEN TOTAL_LOANS=5 THEN '5'
         WHEN TOTAL_LOANS>5 THEN '>5'
    ELSE NULL
    END,
    case WHEN TOTAL_ACTIV_LOANS=1 THEN '1'
         WHEN TOTAL_ACTIV_LOANS=2 THEN '2'
         WHEN TOTAL_ACTIV_LOANS=3 THEN '3'
         WHEN TOTAL_ACTIV_LOANS=4 THEN '4'
         WHEN TOTAL_ACTIV_LOANS=5 THEN '5'
         WHEN TOTAL_ACTIV_LOANS>5 THEN '>5'
    ELSE NULL
    END,
    CASE WHEN ((DPD=0 OR DPD IS NULL) AND DAS IN ('S04','S05','S20','S25','S18')) THEN '0'
         WHEN DPD BETWEEN 1 AND 30 THEN '1_30'
         WHEN DPD BETWEEN 31 AND 60 THEN '31_60'
         WHEN DPD BETWEEN 61 AND 90 THEN '61_90'
         WHEN DPD BETWEEN 91 AND 180 THEN '91_180'
         WHEN DPD BETWEEN 181 AND 360 THEN '181_360'
         WHEN DPD>360 THEN '>360'
         ELSE NULL
   END
""")
ActDF.write.mode("overwrite").saveAsTable("hmanalytics.hm_hc_full_tbl_fnl2")

/*val aggrDataDF = spark.sql(s"""
SELECT A.*,ROUND(((CASE WHEN TOTAL_ACTIV_LOANS >0 THEN MIN_AMT_DUE_ANUAL else 0 end)/(CASE WHEN (CASE WHEN TOTAL_ACTIV_LOANS>0 then ANUAL_INCOME_NEW ELSE 0 END) =0 THEN 1 ELSE (CASE WHEN TOTAL_ACTIV_LOANS>0 then ANUAL_INCOME_NEW ELSE 0 END) END))*100,2)  dEBT_TO_INCOME_RATIO_FNL ,
       ROUND(((TOTAL_ACT_OUTSTANDING_AMOUNT)/(CASE WHEN(CASE WHEN TOTAL_ACTIV_LOANS>0 then ANUAL_INCOME_NEW ELSE 0 END)=0 THEN 1 ELSE (CASE WHEN TOTAL_ACTIV_LOANS>0 then ANUAL_INCOME_NEW ELSE 0 END) END))*100 ,2) PORTFOLIO_TO_INCM_RATIO,
       ROUND(((CASE WHEN TOTAL_ACTIV_LOANS >0 THEN MIN_AMT_DUE_ANUAL ELSE 0 END)/(CASE WHEN(CASE WHEN TOTAL_ACTIV_LOANS>0 THEN ANUAL_INCOME_NEW ELSE 0 END)=0 THEN 1 ELSE(CASE WHEN TOTAL_ACTIV_LOANS>0 THEN ANUAL_INCOME_NEW ELSE 0 END) END))*100,2)  dEBT_INCOME_RATIO_ACTV_FNL 
FROM 
(SELECT 'APR19' AS_OF,*,round(((MIN_AMOUNT_DUE*12)/( CASE WHEN (anual_income)=0 THEN 1 ELSE anual_income END)),2) debt_income_ration,
           case when round(((MIN_AMOUNT_DUE*12)/(CASE WHEN (anual_income)=0 THEN 1 ELSE anual_income END)),2)>1 then (anual_income*12) 
                else anual_income 
           end ANUAL_INCOME_NEW,
           (MIN_AMOUNT_DUE*12) MIN_AMT_DUE_ANUAL,                                                                                 
           round(((MIN_AMOUNT_DUE)/(CASE WHEN anual_income=0 THEN 1 ELSE anual_income END)),2) min_amt_due_ration                                                                                         
          FROM hmanalytics.hm_hc_full_aggr_tbl_jun_19_fnl
UNION ALL

SELECT 'MAY19' AS_OF,*,round(((MIN_AMOUNT_DUE*12)/( CASE WHEN (anual_income)=0 THEN 1 ELSE anual_income END)),2) debt_income_ration,
           case when round(((MIN_AMOUNT_DUE*12)/(CASE WHEN (anual_income)=0 THEN 1 ELSE anual_income END)),2)>1 then (anual_income*12) 
                else anual_income 
           end ANUAL_INCOME_NEW,
           (MIN_AMOUNT_DUE*12) MIN_AMT_DUE_ANUAL,                                                                                 
           round(((MIN_AMOUNT_DUE)/(CASE WHEN anual_income=0 THEN 1 ELSE anual_income END)),2) min_amt_due_ration                                                                                         
          FROM hmanalytics.hm_hc_full_aggr_tbl_jul_19_fnl
UNION ALL

SELECT 'JUN19' AS_OF,*,round(((MIN_AMOUNT_DUE*12)/( CASE WHEN (anual_income)=0 THEN 1 ELSE anual_income END)),2) debt_income_ration,
           case when round(((MIN_AMOUNT_DUE*12)/(CASE WHEN (anual_income)=0 THEN 1 ELSE anual_income END)),2)>1 then (anual_income*12) 
                else anual_income 
           end ANUAL_INCOME_NEW,
           (MIN_AMOUNT_DUE*12) MIN_AMT_DUE_ANUAL,                                                                                 
           round(((MIN_AMOUNT_DUE)/(CASE WHEN anual_income=0 THEN 1 ELSE anual_income END)),2) min_amt_due_ration                                                                                         
          FROM hmanalytics.hm_hc_full_aggr_tbl_aug_19_fnl)		  
A
""")*/

val maxanlincmdf = spark.sql("""
select max_clst_id,max(anual_income_latest) old_anual_income
from hmanalytics.hm_hc_full_aggr_tbl_aug_19_fnl1 
group by max_clst_id
""")
maxanlincmdf.createOrReplaceTempView("maxanlincm")

val aggrDataDF = spark.sql(s"""
SELECT A.*,ROUND(((CASE WHEN TOTAL_ACTIV_LOANS >0 THEN MIN_AMT_DUE_ANUAL else 0 end)/(CASE WHEN (CASE WHEN TOTAL_ACTIV_LOANS>0 then ANUAL_INCOME_NEW ELSE 0 END) =0 THEN 1 ELSE (CASE WHEN TOTAL_ACTIV_LOANS>0 then ANUAL_INCOME_NEW ELSE 0 END) END))*100,2)  dEBT_TO_INCOME_RATIO_FNL ,
       ROUND(((TOTAL_ACT_OUTSTANDING_AMOUNT)/(CASE WHEN(CASE WHEN TOTAL_ACTIV_LOANS>0 then ANUAL_INCOME_NEW ELSE 0 END)=0 THEN 1 ELSE (CASE WHEN TOTAL_ACTIV_LOANS>0 then ANUAL_INCOME_NEW ELSE 0 END) END))*100 ,2) PORTFOLIO_TO_INCM_RATIO,
       ROUND(((CASE WHEN TOTAL_ACTIV_LOANS >0 THEN MIN_AMT_DUE_ANUAL ELSE 0 END)/(CASE WHEN(CASE WHEN TOTAL_ACTIV_LOANS>0 THEN ANUAL_INCOME_NEW ELSE 0 END)=0 THEN 1 ELSE(CASE WHEN TOTAL_ACTIV_LOANS>0 THEN ANUAL_INCOME_NEW ELSE 0 END) END))*100,2)  dEBT_INCOME_RATIO_ACTV_FNL 
FROM 
(SELECT 'JUL19' AS_OF,*,round(((MIN_AMOUNT_DUE*12)/( CASE WHEN (anual_income_latest)=0 THEN 1 ELSE anual_income_latest END)),2) debt_income_ration,
           case when round(((MIN_AMOUNT_DUE*12)/(CASE WHEN (anual_income_latest)=0 THEN 1 ELSE anual_income_latest END)),2)>1 then (anual_income_latest*12) 
                else anual_income_latest 
           end ANUAL_INCOME_NEW,
           (MIN_AMOUNT_DUE*12) MIN_AMT_DUE_ANUAL,                                                                                 
           round(((MIN_AMOUNT_DUE)/(CASE WHEN anual_income_latest=0 THEN 1 ELSE anual_income_latest END)),2) min_amt_due_ration                                                                                         
          FROM (select 
A.*,
case when A.anual_income = 0 or A.anual_income is null THEN B.old_anual_income ELSE A.anual_income  END anual_income_latest
from
hmanalytics.hm_hc_full_aggr_tbl_sep_19_fnl A
JOIN
maxanlincm B
ON(A.max_clst_id = B.max_clst_id))
UNION ALL

SELECT 'AUG19' AS_OF,*,round(((MIN_AMOUNT_DUE*12)/( CASE WHEN (anual_income_latest)=0 THEN 1 ELSE anual_income_latest END)),2) debt_income_ration,
           case when round(((MIN_AMOUNT_DUE*12)/(CASE WHEN (anual_income_latest)=0 THEN 1 ELSE anual_income_latest END)),2)>1 then (anual_income_latest*12) 
                else anual_income_latest 
           end ANUAL_INCOME_NEW,
           (MIN_AMOUNT_DUE*12) MIN_AMT_DUE_ANUAL,                                                                                 
           round(((MIN_AMOUNT_DUE)/(CASE WHEN anual_income_latest=0 THEN 1 ELSE anual_income_latest END)),2) min_amt_due_ration                                                                                         
          FROM (select 
A.*,
case when A.anual_income = 0 or A.anual_income is null THEN B.old_anual_income ELSE A.anual_income  END anual_income_latest
from
hmanalytics.hm_hc_full_aggr_tbl_oct_19_fnl A
JOIN
maxanlincm B
ON(A.max_clst_id = B.max_clst_id))
UNION ALL

SELECT 'SEP19' AS_OF,*,round(((MIN_AMOUNT_DUE*12)/( CASE WHEN (anual_income_latest)=0 THEN 1 ELSE anual_income_latest END)),2) debt_income_ration,
           case when round(((MIN_AMOUNT_DUE*12)/(CASE WHEN (anual_income_latest)=0 THEN 1 ELSE anual_income_latest END)),2)>1 then (anual_income_latest*12) 
                else anual_income_latest 
           end ANUAL_INCOME_NEW,
           (MIN_AMOUNT_DUE*12) MIN_AMT_DUE_ANUAL,                                                                                 
           round(((MIN_AMOUNT_DUE)/(CASE WHEN anual_income_latest=0 THEN 1 ELSE anual_income_latest END)),2) min_amt_due_ration                                                                                         
          FROM (select 
A.*,
case when A.anual_income = 0 or A.anual_income is null THEN B.old_anual_income ELSE A.anual_income  END anual_income_latest
from
hmanalytics.hm_hc_full_aggr_tbl_nov_19_fnl A
JOIN
maxanlincm B
ON(A.max_clst_id = B.max_clst_id)))		  
A
""")

aggrDataDF.write.mode("overwrite").saveAsTable("hmanalytics.hm_hc_full_aggr_fnl_12")
	
	val dtiDPDDF = spark.sql(s"""
SELECT  AS_OF,
       CASE WHEN MAX_DPD=0 THEN '0'
            WHEN MAX_DPD BETWEEN 1 AND 30 THEN '1_30'
            WHEN MAX_DPD BETWEEN 31 AND 60 THEN '31_60'
            WHEN MAX_DPD BETWEEN 61 AND 90 THEN '61_90'
            WHEN MAX_DPD BETWEEN 91 AND 180 THEN '91_180'
            WHEN MAX_DPD BETWEEN 181 AND 360 THEN '181_360'
            WHEN MAX_DPD>360 THEN '>360'
            ELSE NULL
            END MAX_DPD_BUCK,
       CASE WHEN MAX_DPD_HC=0 THEN '0'
            WHEN MAX_DPD_HC BETWEEN 1 AND 30 THEN '1_30'
            WHEN MAX_DPD_HC BETWEEN 31 AND 60 THEN '31_60'
            WHEN MAX_DPD_HC BETWEEN 61 AND 90 THEN '61_90'
            WHEN MAX_DPD_HC BETWEEN 91 AND 180 THEN '91_180'
            WHEN MAX_DPD_HC BETWEEN 181 AND 360 THEN '181_360'
            WHEN MAX_DPD_HC>360 THEN '>360'
            ELSE NULL
            END  MAX_DPD_HC_BUCK,
        CASE WHEN MAX_DPD_HCWTHOTHERS=0 THEN '0'
            WHEN MAX_DPD_HCWTHOTHERS BETWEEN 1 AND 30 THEN '1_30'
            WHEN MAX_DPD_HCWTHOTHERS BETWEEN 31 AND 60 THEN '31_60'
            WHEN MAX_DPD_HCWTHOTHERS BETWEEN 61 AND 90 THEN '61_90'
            WHEN MAX_DPD_HCWTHOTHERS BETWEEN 91 AND 180 THEN '91_180'
            WHEN MAX_DPD_HCWTHOTHERS BETWEEN 181 AND 360 THEN '181_360'
            WHEN MAX_DPD_HCWTHOTHERS>360 THEN '>360'
            ELSE NULL
            END  MAX_DPD_HCWTHOTHERS_BUCK,
        CASE WHEN MAX_DPD_OTHERS=0 THEN '0'
            WHEN MAX_DPD_OTHERS BETWEEN 1 AND 30 THEN '1_30'
            WHEN MAX_DPD_OTHERS BETWEEN 31 AND 60 THEN '31_60'
            WHEN MAX_DPD_OTHERS BETWEEN 61 AND 90 THEN '61_90'
            WHEN MAX_DPD_OTHERS BETWEEN 91 AND 180 THEN '91_180'
            WHEN MAX_DPD_OTHERS BETWEEN 181 AND 360 THEN '181_360'
            WHEN MAX_DPD_OTHERS>360 THEN '>360'
            ELSE NULL
            END  MAX_DPD_OTHERS_BUCK,
        CASE WHEN MAX_DPD_MIXED=0 THEN '0'
            WHEN MAX_DPD_MIXED BETWEEN 1 AND 30 THEN '1_30'
            WHEN MAX_DPD_MIXED BETWEEN 31 AND 60 THEN '31_60'
            WHEN MAX_DPD_MIXED BETWEEN 61 AND 90 THEN '61_90'
            WHEN MAX_DPD_MIXED BETWEEN 91 AND 180 THEN '91_180'
            WHEN MAX_DPD_MIXED BETWEEN 181 AND 360 THEN '181_360'
            WHEN MAX_DPD_MIXED>360 THEN '>360'
            ELSE NULL
            END  MAX_DPD_MIXED_BUCK,
         CASE WHEN MAX_DPD_HC_MIXED=0 THEN '0'
            WHEN MAX_DPD_HC_MIXED BETWEEN 1 AND 30 THEN '1_30'
            WHEN MAX_DPD_HC_MIXED BETWEEN 31 AND 60 THEN '31_60'
            WHEN MAX_DPD_HC_MIXED BETWEEN 61 AND 90 THEN '61_90'
            WHEN MAX_DPD_HC_MIXED BETWEEN 91 AND 180 THEN '91_180'
            WHEN MAX_DPD_HC_MIXED BETWEEN 181 AND 360 THEN '181_360'
            WHEN MAX_DPD_HC_MIXED>360 THEN '>360'
            ELSE NULL
            END  MAX_DPD_HC_MIXED_BUCK,
         CASE WHEN MAX_DPD_OTHERS_MIXED=0 THEN '0'
            WHEN MAX_DPD_OTHERS_MIXED BETWEEN 1 AND 30 THEN '1_30'
            WHEN MAX_DPD_OTHERS_MIXED BETWEEN 31 AND 60 THEN '31_60'
            WHEN MAX_DPD_OTHERS_MIXED BETWEEN 61 AND 90 THEN '61_90'
            WHEN MAX_DPD_OTHERS_MIXED BETWEEN 91 AND 180 THEN '91_180'
            WHEN MAX_DPD_OTHERS_MIXED BETWEEN 181 AND 360 THEN '181_360'
            WHEN MAX_DPD_OTHERS_MIXED>360 THEN '>360'
            ELSE NULL
         END  MAX_DPD_OTHERS_MIXED_BUCK,
        CASE WHEN (debt_income_ration*100) BETWEEN 0 AND 10 THEN '0-10'
             WHEN (debt_income_ration*100) BETWEEN 11 AND 25 THEN '11-25'
             WHEN (debt_income_ration*100) BETWEEN 26 AND 50 THEN '26-50'  
             WHEN (debt_income_ration*100) BETWEEN 51 AND 75 THEN '51-75'
             WHEN (debt_income_ration*100) BETWEEN 76 AND 100 THEN '76-100'        
             WHEN (debt_income_ration*100) >100 THEN '>100'
        END debt_income_ration_BUCK,
        CASE WHEN dEBT_TO_INCOME_RATIO_FNL BETWEEN 0 AND 10 THEN '0-10'
             WHEN dEBT_TO_INCOME_RATIO_FNL BETWEEN 11 AND 25 THEN '11-25'
             WHEN dEBT_TO_INCOME_RATIO_FNL BETWEEN 26 AND 50 THEN '26-50'  
             WHEN dEBT_TO_INCOME_RATIO_FNL BETWEEN 51 AND 75 THEN '51-75'
             WHEN dEBT_TO_INCOME_RATIO_FNL BETWEEN 76 AND 100 THEN '76-100'      
             WHEN dEBT_TO_INCOME_RATIO_FNL >100 THEN '>100'
        END dEBT_TO_INCOME_RATIO_FNL_buck,
        CASE WHEN dEBT_INCOME_RATIO_ACTV_FNL BETWEEN 0 AND 10 THEN '0-10'
             WHEN dEBT_INCOME_RATIO_ACTV_FNL BETWEEN 11 AND 25 THEN '11-25'
             WHEN dEBT_INCOME_RATIO_ACTV_FNL BETWEEN 26 AND 50 THEN '26-50'  
             WHEN dEBT_INCOME_RATIO_ACTV_FNL BETWEEN 51 AND 75 THEN '51-75'
             WHEN dEBT_INCOME_RATIO_ACTV_FNL BETWEEN 76 AND 100 THEN '76-100' 
             WHEN dEBT_INCOME_RATIO_ACTV_FNL >100 THEN '>100'
        END dEBT_INCOME_ACTV_FNL_BUCK,
        CASE WHEN PORTFOLIO_TO_INCM_RATIO BETWEEN 0 AND 100 THEN '0-100'
             WHEN PORTFOLIO_TO_INCM_RATIO BETWEEN 101 AND 200 THEN '100-200'  
             WHEN PORTFOLIO_TO_INCM_RATIO BETWEEN 201 AND 500 THEN '200-500'
             WHEN PORTFOLIO_TO_INCM_RATIO BETWEEN 501 AND 1000 THEN '500-1000' 
             WHEN PORTFOLIO_TO_INCM_RATIO BETWEEN 1001 AND 2000 THEN '1000-2000'  
             WHEN PORTFOLIO_TO_INCM_RATIO >2000 THEN '>2000'
        END PORTFOLIO_TO_INCM_RATIO_BUCK,
        CASE WHEN min_amt_due_ration BETWEEN 0 AND 2 THEN '0-2'
             WHEN min_amt_due_ration BETWEEN 3 AND 5 THEN '3-5'
             WHEN min_amt_due_ration BETWEEN 5 AND 7 THEN '5-7'
             WHEN min_amt_due_ration BETWEEN 8 AND 10 THEN '8-10'
             WHEN min_amt_due_ration BETWEEN 11 AND 15 THEN '10-15'
             WHEN min_amt_due_ration BETWEEN 16 AND 20 THEN '15-20'
             WHEN min_amt_due_ration BETWEEN 21 AND 30 THEN '20-30'
             WHEN min_amt_due_ration BETWEEN 31 AND 50 THEN '31-50'
             WHEN min_amt_due_ration BETWEEN 51 AND 100 THEN '51-100'
             WHEN min_amt_due_ration >100 THEN '>100'
        END min_amt_due_ration_BUCK,                               
        CLUST_IND LENDER_TYPE,
        count(distinct MAX_CLST_ID) TOTAL_CLUSTER,
        COUNT(DISTINCT CASE WHEN TOTAL_ACTIV_LOANS<>0 THEN MAX_CLST_ID ELSE NULL END) ACTIVE_CLUSTER,
        COUNT(DISTINCT CASE WHEN CLUST_IND='ONLY HC'  THEN MAX_CLST_ID ELSE NULL END) TOTAL_CLUSTER_HC,
        COUNT(DISTINCT CASE WHEN CLUST_IND='HC_WITH_OTHERS' THEN MAX_CLST_ID ELSE NULL END) TOTAL_CLST_HCWTH_OTHR,
        COUNT(DISTINCT CASE WHEN CLUST_IND='OTHR CLST' THEN MAX_CLST_ID ELSE NULL END) TOTAL_CLUSTER_OTRS,
        count(distinct case when CLST_HC_MIXED is not null then MAX_CLST_ID else null end) TOTAL_CLUSTER_HX_MIXED,
        count(distinct case when CLST_OTHERS_MIXED is not null then MAX_CLST_ID else null end) TOTAL_CLUSTER_OTHR_MIXED,
        count(distinct case when TOTAL_CLST_BANKS is not null then MAX_CLST_ID else null end) TOTAL_CLUSTER_BANKS,
        count(distinct case when TOTAL_CLST_NONBANKS is not null then MAX_CLST_ID else null end) TOTAL_CLUSTER_NONBANKS,
        count(distinct case when TOTAL_CLST_SECURED is not null then MAX_CLST_ID else null end) TOTAL_CLUSTER_SECURED,
        count(distinct case when TOTAL_CLST_UNSECURED is not null then MAX_CLST_ID else null end) TOTAL_CLUSTER_UNSECURED,
        count(distinct case when TOTAL_CLST_Othr_Prodct is not null then MAX_CLST_ID else null end) TOTAL_CLST_Othr_Prodct,
        sum(TOTAL_LOANS) TOTAL_LOANS,
        sum(TOTAL_ACTIV_LOANS) TOTAL_ACTIV_LOANS,
        sum(TOTAL_ACT_OUTSTANDING_AMOUNT) TOTAL_ACT_OUTSTANDING_AMOUNT,
        sum(TOTAL_DISB_AMOUNT) TOTAL_DISB_AMOUNT,
        sum(TOTAL_ACT_DISB_AMOUNT) TOTAL_ACT_DISB_AMOUNT,
        sum(MIN_AMOUNT_DUE) MIN_AMOUNT_DUE,
        sum(MINAMT_DUE_HC) MINAMT_DUE_HC,
        SUM(MINAMT_HC_WTH_OTHR) MINAMT_HC_WTH_OTHR,
        sum(MIN_AMT_DUE_OTHERS) MIN_AMT_DUE_OTHERS,
        sum(ANUAL_INCOME) ANUAL_INCOME,
        sum(ANUAL_INCOME_HC) ANUAL_INCOME_HC,
        SUM(ANUAL_INCOME_HC_WTH_OTHR) ANUAL_INCOME_HC_WTH_OTHR,
        sum(ANUAL_INCOME_MIXED) ANUAL_INCOME_MIXED,
        sum(ANUAL_INCOME_OTHERS) ANUAL_INCOME_OTHERS
  FROM hmanalytics.hm_hc_full_aggr_fnl_12
group by  AS_OF,
         CASE WHEN MAX_DPD=0 THEN '0'
            WHEN MAX_DPD BETWEEN 1 AND 30 THEN '1_30'
            WHEN MAX_DPD BETWEEN 31 AND 60 THEN '31_60'
            WHEN MAX_DPD BETWEEN 61 AND 90 THEN '61_90'
            WHEN MAX_DPD BETWEEN 91 AND 180 THEN '91_180'
            WHEN MAX_DPD BETWEEN 181 AND 360 THEN '181_360'
            WHEN MAX_DPD>360 THEN '>360'
            ELSE NULL
            END,
        CASE WHEN MAX_DPD_HC=0 THEN '0'
            WHEN MAX_DPD_HC BETWEEN 1 AND 30 THEN '1_30'
            WHEN MAX_DPD_HC BETWEEN 31 AND 60 THEN '31_60'
            WHEN MAX_DPD_HC BETWEEN 61 AND 90 THEN '61_90'
            WHEN MAX_DPD_HC BETWEEN 91 AND 180 THEN '91_180'
            WHEN MAX_DPD_HC BETWEEN 181 AND 360 THEN '181_360'
            WHEN MAX_DPD_HC>360 THEN '>360'
            ELSE NULL
            END ,
        CASE WHEN MAX_DPD_HCWTHOTHERS=0 THEN '0'
            WHEN MAX_DPD_HCWTHOTHERS BETWEEN 1 AND 30 THEN '1_30'
            WHEN MAX_DPD_HCWTHOTHERS BETWEEN 31 AND 60 THEN '31_60'
            WHEN MAX_DPD_HCWTHOTHERS BETWEEN 61 AND 90 THEN '61_90'
            WHEN MAX_DPD_HCWTHOTHERS BETWEEN 91 AND 180 THEN '91_180'
            WHEN MAX_DPD_HCWTHOTHERS BETWEEN 181 AND 360 THEN '181_360'
            WHEN MAX_DPD_HCWTHOTHERS>360 THEN '>360'
            ELSE NULL
            END ,
        CASE WHEN MAX_DPD_OTHERS=0 THEN '0'
            WHEN MAX_DPD_OTHERS BETWEEN 1 AND 30 THEN '1_30'
            WHEN MAX_DPD_OTHERS BETWEEN 31 AND 60 THEN '31_60'
            WHEN MAX_DPD_OTHERS BETWEEN 61 AND 90 THEN '61_90'
            WHEN MAX_DPD_OTHERS BETWEEN 91 AND 180 THEN '91_180'
            WHEN MAX_DPD_OTHERS BETWEEN 181 AND 360 THEN '181_360'
            WHEN MAX_DPD_OTHERS>360 THEN '>360'
            ELSE NULL
            END ,
        CASE WHEN MAX_DPD_MIXED=0 THEN '0'
            WHEN MAX_DPD_MIXED BETWEEN 1 AND 30 THEN '1_30'
            WHEN MAX_DPD_MIXED BETWEEN 31 AND 60 THEN '31_60'
            WHEN MAX_DPD_MIXED BETWEEN 61 AND 90 THEN '61_90'
            WHEN MAX_DPD_MIXED BETWEEN 91 AND 180 THEN '91_180'
            WHEN MAX_DPD_MIXED BETWEEN 181 AND 360 THEN '181_360'
            WHEN MAX_DPD_MIXED>360 THEN '>360'
            ELSE NULL
            END ,
         CASE WHEN MAX_DPD_HC_MIXED=0 THEN '0'
            WHEN MAX_DPD_HC_MIXED BETWEEN 1 AND 30 THEN '1_30'
            WHEN MAX_DPD_HC_MIXED BETWEEN 31 AND 60 THEN '31_60'
            WHEN MAX_DPD_HC_MIXED BETWEEN 61 AND 90 THEN '61_90'
            WHEN MAX_DPD_HC_MIXED BETWEEN 91 AND 180 THEN '91_180'
            WHEN MAX_DPD_HC_MIXED BETWEEN 181 AND 360 THEN '181_360'
            WHEN MAX_DPD_HC_MIXED>360 THEN '>360'
            ELSE NULL
            END ,
         CASE WHEN MAX_DPD_OTHERS_MIXED=0 THEN '0'
            WHEN MAX_DPD_OTHERS_MIXED BETWEEN 1 AND 30 THEN '1_30'
            WHEN MAX_DPD_OTHERS_MIXED BETWEEN 31 AND 60 THEN '31_60'
            WHEN MAX_DPD_OTHERS_MIXED BETWEEN 61 AND 90 THEN '61_90'
            WHEN MAX_DPD_OTHERS_MIXED BETWEEN 91 AND 180 THEN '91_180'
            WHEN MAX_DPD_OTHERS_MIXED BETWEEN 181 AND 360 THEN '181_360'
            WHEN MAX_DPD_OTHERS_MIXED>360 THEN '>360'
            ELSE NULL
         END ,
        CASE WHEN (debt_income_ration*100) BETWEEN 0 AND 10 THEN '0-10'
             WHEN (debt_income_ration*100) BETWEEN 11 AND 25 THEN '11-25'
             WHEN (debt_income_ration*100) BETWEEN 26 AND 50 THEN '26-50'  
             WHEN (debt_income_ration*100) BETWEEN 51 AND 75 THEN '51-75'
             WHEN (debt_income_ration*100) BETWEEN 76 AND 100 THEN '76-100'        
             WHEN (debt_income_ration*100) >100 THEN '>100'
        END,
        CASE WHEN dEBT_TO_INCOME_RATIO_FNL BETWEEN 0 AND 10 THEN '0-10'
             WHEN dEBT_TO_INCOME_RATIO_FNL BETWEEN 11 AND 25 THEN '11-25'
             WHEN dEBT_TO_INCOME_RATIO_FNL BETWEEN 26 AND 50 THEN '26-50'  
             WHEN dEBT_TO_INCOME_RATIO_FNL BETWEEN 51 AND 75 THEN '51-75'
             WHEN dEBT_TO_INCOME_RATIO_FNL BETWEEN 76 AND 100 THEN '76-100'      
             WHEN dEBT_TO_INCOME_RATIO_FNL >100 THEN '>100'
        END,
        CASE WHEN dEBT_INCOME_RATIO_ACTV_FNL BETWEEN 0 AND 10 THEN '0-10'
             WHEN dEBT_INCOME_RATIO_ACTV_FNL BETWEEN 11 AND 25 THEN '11-25'
             WHEN dEBT_INCOME_RATIO_ACTV_FNL BETWEEN 26 AND 50 THEN '26-50'  
             WHEN dEBT_INCOME_RATIO_ACTV_FNL BETWEEN 51 AND 75 THEN '51-75'
             WHEN dEBT_INCOME_RATIO_ACTV_FNL BETWEEN 76 AND 100 THEN '76-100' 
             WHEN dEBT_INCOME_RATIO_ACTV_FNL >100 THEN '>100'
        END,
        CASE WHEN PORTFOLIO_TO_INCM_RATIO BETWEEN 0 AND 100 THEN '0-100'
             WHEN PORTFOLIO_TO_INCM_RATIO BETWEEN 101 AND 200 THEN '100-200'  
             WHEN PORTFOLIO_TO_INCM_RATIO BETWEEN 201 AND 500 THEN '200-500'
             WHEN PORTFOLIO_TO_INCM_RATIO BETWEEN 501 AND 1000 THEN '500-1000' 
             WHEN PORTFOLIO_TO_INCM_RATIO BETWEEN 1001 AND 2000 THEN '1000-2000'  
             WHEN PORTFOLIO_TO_INCM_RATIO >2000 THEN '>2000'
        END,
        CASE WHEN min_amt_due_ration BETWEEN 0 AND 2 THEN '0-2'
             WHEN min_amt_due_ration BETWEEN 3 AND 5 THEN '3-5'
             WHEN min_amt_due_ration BETWEEN 5 AND 7 THEN '5-7'
             WHEN min_amt_due_ration BETWEEN 8 AND 10 THEN '8-10'
             WHEN min_amt_due_ration BETWEEN 11 AND 15 THEN '10-15'
             WHEN min_amt_due_ration BETWEEN 16 AND 20 THEN '15-20'
             WHEN min_amt_due_ration BETWEEN 21 AND 30 THEN '20-30'
             WHEN min_amt_due_ration BETWEEN 31 AND 50 THEN '31-50'
             WHEN min_amt_due_ration BETWEEN 51 AND 100 THEN '51-100'
             WHEN min_amt_due_ration >100 THEN '>100'
        END,                            
        CLUST_IND
""")
dtiDPDDF.write.mode("overwrite").saveAsTable("hmanalytics.HM_DTI_DPD_full_FINAL_12")

}