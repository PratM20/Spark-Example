package com.crif.highmark.CrifSemantic.DataLoad
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.types._
import org.apache.spark.sql.SQLContext

object clusterMarketEntry extends App {
  val spark = SparkSession.builder().appName("homeCreditLoad")
											.config("hive.metastore.uris", "thrift://chmcisprbdmn01.chm.intra:9083")
											.config("hive.exec.dynamic.partition", "true")
                      .config("hive.exec.dynamic.partition.mode", "nonstrict")
                      .config("hive.enforce.bucketing", "true")
                      .config("hive.exec.max.dynamic.partitions", "20000")
											.enableHiveSupport().getOrCreate()
	import spark.implicits._

/*val ActDF = spark.sql(s"""
select 
  consumer_key,
  disbursed_dt,
  account_type
from 
  hmanalytics.hm_cns_macro_analysis_nov_18_parquet 
		""")	
		
val CloDF = spark.sql(s"""
select 
  consumer_key,
  disbursed_dt,
  account_type
from 
  hmanalytics.hm_hc_cns_macro_closed_parquet 
where 
  (
    closed_dt IS NULL 
    or closed_dt <= from_unixtime(
      unix_timestamp('31-Oct-2015', 'dd-MMM-yy'), 
      'yyyy-MM-dd'
    )
  ) 
  and REPORTED_DT <= from_unixtime(
    unix_timestamp('08112015', 'ddMMyyyy'), 
    'yyyy-MM-dd'
  ) 
  and ACT_INSERT_DT <= from_unixtime(
    unix_timestamp('08112015', 'ddMMyyyy'), 
    'yyyy-MM-dd'
  ) 
  
""")

val unionDF = ActDF.union(CloDF).distinct()
val clusterDF = spark.sql(s"select CANDIDATE_ID,MAX_CLST_ID from hmanalytics.hm_cns_clst_20dec18").distinct()

val joinDF = unionDF.join(clusterDF , unionDF("CONSUMER_KEY") === clusterDF("CANDIDATE_ID"))

val minDsbDt = joinDF.withColumn("min_disb_dt",min("disbursed_dt").over(Window.partitionBy(col("MAX_CLST_ID")))).where(col("disbursed_dt") === col("min_disb_dt")).drop("min_disb_dt","CANDIDATE_ID")

val fnLDF = minDsbDt.withColumn("Year", when( !(year(col("disbursed_dt")).isin("2016","2017","2018"))  ,"Others").otherwise(year(col("disbursed_dt")))).select(col("Year"))




minDsbDt.write.mode("append").saveAsTable("hmanalytics.hm_clst_mkt_entry_fnl")*/
/*val ActDF = spark.sql(s"""
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
FROM (SELECT 'JAN18' AS_OF,'28-FEB-18' LOAD_END_DT, A.*,CASE WHEN rw=1 THEN annual_income ELSE null END ANNUAL_INCOME_FNL,CASE WHEN rw=1 AND Min_amt_threshold=1 THEN min_amount_due ELSE null END MIN_AMT_DUE_FNL
        FROM hmanalytics.hm_hc_a13_base_tbl_mar_18_fnl A
       WHERE threshold_flag_overall=1  and threshold_acnt_type=1 
      UNION ALL 
      SELECT 'FEB18' AS_OF,'31-MAR-18' LOAD_END_DT, A.*,CASE WHEN rw=1 THEN annual_income ELSE null END ANNUAL_INCOME_FNL,CASE WHEN rw=1 AND Min_amt_threshold=1 THEN min_amount_due ELSE null END MIN_AMT_DUE_FNL
        FROM hmanalytics.hm_hc_a13_base_tbl_apr_18_fnl A
       WHERE threshold_flag_overall=1  and threshold_acnt_type=1 
     UNION ALL 
      SELECT 'MAR18' AS_OF,'30-APR-18' LOAD_END_DT, A.*,CASE WHEN rw=1 THEN annual_income ELSE null END ANNUAL_INCOME_FNL,CASE WHEN rw=1 AND Min_amt_threshold=1 THEN min_amount_due ELSE null END MIN_AMT_DUE_FNL
        FROM hmanalytics.hm_hc_a13_base_tbl_MAY_18_fnl A
       WHERE threshold_flag_overall=1  and threshold_acnt_type=1
      UNION ALL 
      SELECT 'APR18' AS_OF,'31-MAY-18' LOAD_END_DT, A.*,CASE WHEN rw=1 THEN annual_income ELSE null END ANNUAL_INCOME_FNL,CASE WHEN rw=1 AND Min_amt_threshold=1 THEN min_amount_due ELSE null END MIN_AMT_DUE_FNL
        FROM hmanalytics.hm_hc_a13_base_tbl_jun_18_fnl A
       WHERE threshold_flag_overall=1  and threshold_acnt_type=1
      UNION ALL 
      SELECT 'MAY18' AS_OF,TO_DATE('30-JUN-18') LOAD_END_DT, A.*,CASE WHEN rw=1 THEN annual_income ELSE null END ANNUAL_INCOME_FNL,CASE WHEN rw=1 AND Min_amt_threshold=1 THEN min_amount_due ELSE null END MIN_AMT_DUE_FNL
        FROM hmanalytics.hm_hc_a13_base_tbl_jul_18_fnl A
       WHERE threshold_flag_overall=1  and threshold_acnt_type=1 
      UNION ALL 
      SELECT 'JUN18' AS_OF,'31-JUL-18' LOAD_END_DT, A.*,CASE WHEN rw=1 THEN annual_income ELSE null END ANNUAL_INCOME_FNL,CASE WHEN rw=1 AND Min_amt_threshold=1 THEN min_amount_due ELSE null END MIN_AMT_DUE_FNL
        FROM hmanalytics.hm_hc_a13_base_tbl_aug_18_fnl A
       WHERE threshold_flag_overall=1  and threshold_acnt_type=1 
      UNION ALL 
      SELECT 'JUL18' AS_OF,'31-AUG-18' LOAD_END_DT, A.*,CASE WHEN rw=1 THEN annual_income ELSE null END ANNUAL_INCOME_FNL,CASE WHEN rw=1 AND Min_amt_threshold=1 THEN min_amount_due ELSE null END MIN_AMT_DUE_FNL
        FROM hmanalytics.hm_hc_a13_base_tbl_sep_18_fnl A
       WHERE threshold_flag_overall=1  and threshold_acnt_type=1 
      UNION ALL 
      SELECT 'AUG18' AS_OF,'30-SEP-18' LOAD_END_DT, A.*,CASE WHEN rw=1 THEN annual_income ELSE null END ANNUAL_INCOME_FNL,CASE WHEN rw=1 AND Min_amt_threshold=1 THEN min_amount_due ELSE null END MIN_AMT_DUE_FNL
        FROM hmanalytics.hm_hc_a13_base_tbl_oct_18_fnl A
       WHERE threshold_flag_overall=1  and threshold_acnt_type=1 
     UNION ALL 
      SELECT 'SEP18' AS_OF,'31-OCT-18' LOAD_END_DT, A.*,CASE WHEN rw=1 THEN annual_income ELSE null END ANNUAL_INCOME_FNL,CASE WHEN rw=1 AND Min_amt_threshold=1 THEN min_amount_due ELSE null END MIN_AMT_DUE_FNL
        FROM hmanalytics.hm_hc_a13_base_tbl_nov_18_fnl A
       WHERE threshold_flag_overall=1  and threshold_acnt_type=1     
      )  A,
   (SELECT 'JAN18' AS_OF, A.*
      FROM hmanalytics.hm_hc_a13_aggr_tbl_mar_18_fnl A
     UNION ALL 
    SELECT 'FEB18' AS_OF,A.*
      FROM hmanalytics.hm_hc_a13_aggr_tbl_apr_18_fnl A
     UNION ALL 
    SELECT 'MAR18' AS_OF,A.*
      FROM hmanalytics.hm_hc_a13_aggr_tbl_may_18_fnl A
     UNION ALL 
    SELECT 'APR18' AS_OF,A.*
      FROM hmanalytics.hm_hc_a13_aggr_tbl_jun_18_fnl A
     UNION ALL 
    SELECT 'MAY18' AS_OF,A.*
      FROM hmanalytics.hm_hc_a13_aggr_tbl_jul_18_fnl A
     UNION ALL 
    SELECT 'JUN18' AS_OF,A.*
      FROM hmanalytics.hm_hc_a13_aggr_tbl_aug_18_fnl A
     UNION ALL 
    SELECT 'JUL18' AS_OF, A.*
      FROM hmanalytics.hm_hc_a13_aggr_tbl_sep_18_fnl A
     UNION ALL 
    SELECT 'AUG18' AS_OF, A.*
      FROM hmanalytics.hm_hc_a13_aggr_tbl_OCT_18_fnl A
     UNION ALL 
    SELECT 'SEP18' AS_OF,A.*
      FROM hmanalytics.hm_hc_a13_aggr_tbl_NOV_18_fnl A
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
ActDF.write.mode("append").saveAsTable("hmanalytics.hm_hc_a13_tbl_fnl")*/
	
	/*val ActDF = spark.sql(s"""
select 
  consumer_key,
  account_key,
  account_type,
  das,
  closed_dt
  
from 
  hmanalytics.hm_cns_macro_analysis_nov_18_parquet

		""")	
		
val CloDF = spark.sql(s"""
select 
  consumer_key,
  account_key,
  account_type,
  das,
  closed_dt
from 
  hmanalytics.hm_hc_cns_macro_closed_parquet 
where 
  (
    closed_dt IS NULL 
    or closed_dt <= from_unixtime(
      unix_timestamp('31-OCT-18', 'dd-MMM-yy'), 
      'yyyy-MM-dd'
    )
  ) 
  and REPORTED_DT <= from_unixtime(
    unix_timestamp('08112018', 'ddMMyyyy'), 
    'yyyy-MM-dd'
  ) 
  and ACT_INSERT_DT <= from_unixtime(
    unix_timestamp('08112018', 'ddMMyyyy'), 
    'yyyy-MM-dd'
  ) 
  
""")
val unionDF = ActDF.union(CloDF)
val clusterDF = spark.sql(s"select CANDIDATE_ID,MAX_CLST_ID from hmanalytics.hm_cns_clst_20dec18").distinct()

val joinDF = unionDF.join(clusterDF , unionDF("CONSUMER_KEY") === clusterDF("CANDIDATE_ID"))

val a13AggrDf = joinDF.groupBy(col("ACCOUNT_TYPE")).agg(
size(collect_set(col("MAX_CLST_ID"))).as("TOTAL_CLUSTER"),
size(collect_set(when((col("CLOSED_DT").isNull || col("CLOSED_DT") > from_unixtime(unix_timestamp(lit("31-OCT-18"), "dd-MMM-yy"), "yyyy-MM-dd")) && col("DAS").isin("S04", "S05", "S20", "S25", "S18"), col("MAX_CLST_ID")).otherwise(lit(null)))).as("TOTAL_ACTIV_CLUSTERS"),
size(collect_set(col("ACCOUNT_KEY"))).as("TOTAL_LOANS"),
size(collect_set(when((col("CLOSED_DT").isNull || col("CLOSED_DT") > from_unixtime(unix_timestamp(lit("31-OCT-18"), "dd-MMM-yy"), "yyyy-MM-dd")) && col("DAS").isin("S04", "S05", "S20", "S25", "S18"), col("ACCOUNT_KEY")).otherwise(lit(null)))).as("TOTAL_ACTIV_LOANS")
)

val a13AggrDf = joinDF.groupBy(col("ACCOUNT_TYPE")).agg(
countDistinct(col("MAX_CLST_ID")).as("TOTAL_CLUSTER"),
countDistinct(when((col("CLOSED_DT").isNull || col("CLOSED_DT") > from_unixtime(unix_timestamp(lit("31-OCT-18"), "dd-MMM-yy"), "yyyy-MM-dd")) && col("DAS").isin("S04", "S05", "S20", "S25", "S18"), col("MAX_CLST_ID")).otherwise(lit(null))).as("TOTAL_ACTIV_CLUSTERS"),
countDistinct(col("ACCOUNT_KEY")).as("TOTAL_LOANS"),
countDistinct(when((col("CLOSED_DT").isNull || col("CLOSED_DT") > from_unixtime(unix_timestamp(lit("31-OCT-18"), "dd-MMM-yy"), "yyyy-MM-dd")) && col("DAS").isin("S04", "S05", "S20", "S25", "S18"), col("ACCOUNT_KEY")).otherwise(lit(null))).as("TOTAL_ACTIV_LOANS")
)

//a13AggrDf.write.mode("append").saveAsTable("hmanalytics.account_type_cluster_count_fnl")
a13AggrDf.repartition(1).write.mode("append").option("header", "true").csv("/tmp/prateek/account_type_cluster_count")
*/
	
val cnsClustDF = spark.read.format("jdbc") .option("url", "jdbc:oracle:thin:@172.16.5.2:1521:CHM01")  .option("dbtable", "(SELECT  CLST_ID,CANDIDATE_ID,MOD(CANDIDATE_ID,10) as part FROM HMCORE.HM_INQ_MATCH_INVOL_P1_R02@X4_TO_X7) a")  .option("user", "HMANALYTICS") .option("password", "HM#2019")  .option("driver", "oracle.jdbc.driver.OracleDriver").option("numPartitions", 10) .option("partitionColumn", "part").option("lowerBound", 0).option("upperBound", 9).option("fetchSize","10000").load()
val maxCLstDF = cnsClustDF.distinct.groupBy(col("CANDIDATE_ID")).agg(max(col("CLST_ID")).as("MAX_CLST_ID"))
maxCLstDF.drop(col("PART")).repartition(500).write.mode("overwrite").saveAsTable("hmanalytics.hm_cns_clst_Sep19")


val mfiClustDF = spark.read.format("jdbc") .option("url", "jdbc:oracle:thin:@172.16.5.2:1521:CHM01")  .option("dbtable", "(SELECT  CLST_ID,CANDIDATE_ID,MOD(CANDIDATE_ID,10) as part FROM HMCORE.HM_INQ_MATCH_INVOL_P1_R01@X4_TO_X7) a")  .option("user", "HMANALYTICS") .option("password", "HM#2019")  .option("driver", "oracle.jdbc.driver.OracleDriver").option("numPartitions", 10) .option("partitionColumn", "part").option("lowerBound", 0).option("upperBound", 9).option("fetchSize","10000").load()
val maxmfiCLstDF = mfiClustDF.distinct.groupBy(col("CANDIDATE_ID")).agg(max(col("CLST_ID")).as("MAX_CLST_ID"))
maxmfiCLstDF.drop(col("PART")).repartition(500).write.mode("overwrite").saveAsTable("hmanalytics.hm_mfi_clst_Sep19")

val cmlClustDF = spark.read.format("jdbc") .option("url", "jdbc:oracle:thin:@172.16.5.2:1521:CHM01")  .option("dbtable", "(SELECT  CLST_ID,CANDIDATE_ID,MOD(CANDIDATE_ID,10) as part FROM HMCORE.HM_INQ_MATCH_INVOL_P1_R10@X4_TO_X7) a")  .option("user", "HMANALYTICS") .option("password", "HM#2019")  .option("driver", "oracle.jdbc.driver.OracleDriver").option("numPartitions", 10) .option("partitionColumn", "part").option("lowerBound", 0).option("upperBound", 9).option("fetchSize","10000").load()
val maxcmlCLstDF = cmlClustDF.distinct.groupBy(col("CANDIDATE_ID")).agg(max(col("CLST_ID")).as("MAX_CLST_ID"))
maxcmlCLstDF.drop(col("PART")).repartition(500).write.mode("overwrite").saveAsTable("hmanalytics.hm_cml_clst_Sep19")

	
	val trendDF = spark.read.format("jdbc") .option("url", "jdbc:oracle:thin:@172.16.5.2:1521:CHM01")
 .option("dbtable", "(select cast(ACCOUNT_KEY as number(38,0)) ACCOUNT_KEY ,MFI_ID ,REPORTED_DT ,ACT_REPORTED_DT ,MIN_AMOUNT_DUE ,CURRENT_BALANCE ,AMOUNT_OVERDUE_TOTAL ,DAYS_PAST_DUE ,CHARGEOFF_AMT ,ASSET_CLASS ,SUIT_FILED ,WRITTEN_OFF_STATUS ,SETLLEMENT_AMT ,SPECIAL_REMARKS_1 ,MANUAL_UPDATE_DT ,MANUAL_UPDATE_IND ,DAS ,UPDATE_DT ,CREDIT_FACILITY_STATUS ,ACCOUNT_TYPE ,CHARGEOFF_DT ,LAST_PAYMENT_DT ,to_char(INTEREST_RATE) INTEREST_RATE  ,CHARGEOFF_AMT_PRINCIPAL ,to_char(ACTUAL_PAYMT_AMT) ACTUAL_PAYMT_AMT ,HIGH_CREDIT,MOD(ACCOUNT_KEY,10) as part from CHMCNSTREND.HM_MFI_ACCOUNT where ACT_REPORTED_DT >= to_date('01-06-2019','DD-MM-YYYY') AND ACT_REPORTED_DT <= to_date('31-07-2019','DD-MM-YYYY') ) a") 
 .option("user", "HMANALYTICS") .option("password", "HM#2019")
 .option("driver", "oracle.jdbc.driver.OracleDriver") 
 .option("numPartitions", 10) .option("partitionColumn", "part") 
 .option("lowerBound", 0).option("upperBound", 9) .option("fetchSize","10000").load()

trendDF.drop(col("PART")).repartition(2000).write.mode("overwrite").insertInto("hmanalytics.hm_mfi_account_trend_tempjul19")

}