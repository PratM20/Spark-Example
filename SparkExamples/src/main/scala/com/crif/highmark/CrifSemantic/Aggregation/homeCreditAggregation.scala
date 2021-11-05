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

object homeCreditAggregation {

	def DataAggregation(spark: SparkSession, unMnlDF: DataFrame,argument : Array[String]): Unit = {
		
		//val a13InpDf = spark.sql("Select * from hmanalytics.hm_hc_work_4")
		val mon_rn=argument(0)
		
		val end_dt=argument(1)

		val a13FnlDf = unMnlDF.where(col("THRESHOLD_FLAG_OVERALL") === 1 && col("THRESHOLD_ACNT_TYPE") === 1).withColumn("ANNUAL_INCOME_FNL", when(col("RW") === 1, col("ANNUAL_INCOME")).otherwise(lit(null)))
			.withColumn("MONTHLY_INCOME_FNL", when(col("RW_mnth") === 1, col("MONTHLY_INCOME") * 12).otherwise(lit(null)))
			.withColumn("MIN_AMT_DUE_FNL", when(col("Min_amt_threshold") === 1 && col("RW") === 1, col("MIN_AMOUNT_DUE")).otherwise(lit(null)))

		val a13AggrDf = a13FnlDf.groupBy(col("MAX_CLST_ID"), col("CLUST_IND"))
			.agg(
				min(col("DISBURSED_DT")).as("MIN_DISB_DT"),
				min(when(col("MFI_ID") === "NBF0000399" && col("CLUST_IND") === "ONLY HC", col("DISBURSED_DT")).otherwise(lit(null))).as("MIN_DISB_HC"),
				min(when(col("CLUST_IND") === "HC_WITH_OTHERS", col("DISBURSED_DT")).otherwise(lit(null))).as("MIN_DISB_HCWTH_OTHR"),
				min(when((col("CLUST_IND") === "HC_WITH_OTHERS" || col("CLUST_IND") === "ONLY HC"), col("DISBURSED_DT")).otherwise(lit(null))).as("MIN_DISB_MIXED"),
				min(when((col("MFI_ID") =!= "NBF0000399" && col("CLUST_IND") === "OTHR CLST"), col("DISBURSED_DT")).otherwise(lit(null))).as("MIN_DISB_OTHERS"),
				min(when(col("MFI_ID") === "NBF0000399", col("DISBURSED_DT")).otherwise(lit(null))).as("MIN_DISB_HC_MIXED"),
				min(when(col("MFI_ID") =!= "NBF0000399", col("DISBURSED_DT")).otherwise(lit(null))).as("MIN_DISB_OTHERS_MIXED"),
				max(col("DPD")).as("MAX_DPD"),
				max(when(col("MFI_ID") === "NBF0000399" && col("CLUST_IND") === "ONLY HC", col("DPD")).otherwise(lit(null))).as("MAX_DPD_HC"),
				max(when(col("CLUST_IND") === "HC_WITH_OTHERS", col("DPD")).otherwise(lit(null))).as("MAX_DPD_HCWTHOTHERS"),
				max(when((col("CLUST_IND") === "HC_WITH_OTHERS" || col("CLUST_IND") === "ONLY HC"), col("DPD")).otherwise(lit(null))).as("MAX_DPD_MIXED"),
				max(when((col("MFI_ID") =!= "NBF0000399" && col("CLUST_IND") === "OTHR CLST"), col("DPD")).otherwise(lit(null))).as("MAX_DPD_OTHERS"),
				max(when(col("MFI_ID") === "NBF0000399", col("DPD")).otherwise(lit(null))).as("MAX_DPD_HC_MIXED"),
				max(when(col("MFI_ID") =!= "NBF0000399", col("DPD")).otherwise(lit(null))).as("MAX_DPD_OTHERS_MIXED"),
				size(collect_set(col("MAX_CLST_ID"))).as("TOTAL_CLUSTER"),
				size(collect_set(when(col("MFI_ID") === "NBF0000399" && col("CLUST_IND") === "ONLY HC", col("MAX_CLST_ID")).otherwise(lit(null)))).as("ONLY_HC_CLUSTER"),
				size(collect_set(when(col("CLUST_IND") === "HC_WITH_OTHERS", col("MAX_CLST_ID")).otherwise(lit(null)))).as("HC_WITH_OTHRS"),
				size(collect_set(when((col("CLUST_IND") === "HC_WITH_OTHERS" || col("CLUST_IND") === "ONLY HC"), col("MAX_CLST_ID")).otherwise(lit(null)))).as("CLUST_MIXED"),
				size(collect_set(when((col("MFI_ID") =!= "NBF0000399" && col("CLUST_IND") === "OTHR CLST"), col("MAX_CLST_ID")).otherwise(lit(null)))).as("TOTAL_CLUSTER_OTRS"),
				size(collect_set(when(col("LENDER_TYPE") === "Banks", col("MAX_CLST_ID")).otherwise(lit(null)))).as("TOTAL_clst_BANKS"),
				size(collect_set(when(col("LENDER_TYPE") === "Non Banks", col("MAX_CLST_ID")).otherwise(lit(null)))).as("TOTAL_clst_NONBANKS"),
				size(collect_set(when(col("PRODUCT") === "Secured", col("MAX_CLST_ID")).otherwise(lit(null)))).as("TOTAL_clst_SECURED"),
				size(collect_set(when(col("PRODUCT") === "Unsecured", col("MAX_CLST_ID")).otherwise(lit(null)))).as("TOTAL_clst_UNSECURED"),
				size(collect_set(when(col("PRODUCT").isin("Both", "Others"), col("MAX_CLST_ID")).otherwise(lit(null)))).as("TOTAL_CLST_Othr_Prodct"),
				size(collect_set(when(col("MFI_ID") === "NBF0000399", col("MAX_CLST_ID")).otherwise(lit(null)))).as("CLST_HC_MIXED"),
				size(collect_set(when(col("MFI_ID") =!= "NBF0000399", col("MAX_CLST_ID")).otherwise(lit(null)))).as("CLST_OTHERS_MIXED"),
				size(collect_set(col("ACCOUNT_KEY"))).as("TOTAL_LOANS"),
				size(collect_set(when(col("ACCOUNT_TYPE") === "A13", col("ACCOUNT_KEY")).otherwise(lit(null)))).as("TOTAL_LOANS_CL"),
				size(collect_set(when(col("ACCOUNT_TYPE") =!= "A13", col("ACCOUNT_KEY")).otherwise(lit(null)))).as("TOTAL_LOANS_NONCL"),
				size(collect_set(when(col("MFI_ID") === "NBF0000399" && col("CLUST_IND") === "ONLY HC", col("ACCOUNT_KEY")).otherwise(lit(null)))).as("TOTAL_LOANS_HC"),
				size(collect_set(when(col("CLUST_IND") === "HC_WITH_OTHERS", col("ACCOUNT_KEY")).otherwise(lit(null)))).as("TOTAL_LNS_HC_WTH_OTHR"),
				size(collect_set(when((col("CLUST_IND") === "HC_WITH_OTHERS" || col("CLUST_IND") === "ONLY HC"), col("ACCOUNT_KEY")).otherwise(lit(null)))).as("TOTAL_LOANS_MIXED"),
				size(collect_set(when(col("LENDER_TYPE") === "Banks", col("ACCOUNT_KEY")).otherwise(lit(null)))).as("TOTAL_LOANS_BANKS"),
				size(collect_set(when(col("LENDER_TYPE") === "Non Banks", col("ACCOUNT_KEY")).otherwise(lit(null)))).as("TOTAL_LOANS_NONBANKS"),
				size(collect_set(when(col("PRODUCT") === "Secured", col("ACCOUNT_KEY")).otherwise(lit(null)))).as("TOTAL_LNS_SECURED"),
				size(collect_set(when(col("PRODUCT") === "Unsecured", col("ACCOUNT_KEY")).otherwise(lit(null)))).as("TOTAL_LNS_UNSECURED"),
				size(collect_set(when(col("PRODUCT").isin("Both", "Others"), col("ACCOUNT_KEY")).otherwise(lit(null)))).as("TOTAL_LNS_Othr_Prodct"),
				size(collect_set(when((col("MFI_ID") =!= "NBF0000399" && col("CLUST_IND") === "OTHR CLST"), col("ACCOUNT_KEY")).otherwise(lit(null)))).as("TOTAL_LOANS_OTHERS"),
				size(collect_set(when(col("MFI_ID") === "NBF0000399", col("ACCOUNT_KEY")).otherwise(lit(null)))).as("TOTAL_HC_MIXED"),
				size(collect_set(when(col("MFI_ID") =!= "NBF0000399", col("ACCOUNT_KEY")).otherwise(lit(null)))).as("TOTAL_OTHERS_MIXED"),
				size(collect_set(when((col("CLOSED_DT").isNull || col("CLOSED_DT") > from_unixtime(unix_timestamp(lit(end_dt), "dd-MMM-yy"), "yyyy-MM-dd")) && col("DAS").isin("S04", "S05", "S20", "S25", "S18"), col("ACCOUNT_KEY")).otherwise(lit(null)))).as("TOTAL_ACTIV_LOANS"),
				size(collect_set(when((col("CLOSED_DT").isNull || col("CLOSED_DT") > from_unixtime(unix_timestamp(lit(end_dt), "dd-MMM-yy"), "yyyy-MM-dd")) && col("DAS").isin("S04", "S05", "S20", "S25", "S18") && col("ACCOUNT_TYPE") === "A13", col("ACCOUNT_KEY")).otherwise(lit(null)))).as("ACTIV_LOANS_CL"),
				size(collect_set(when((col("CLOSED_DT").isNull || col("CLOSED_DT") > from_unixtime(unix_timestamp(lit(end_dt), "dd-MMM-yy"), "yyyy-MM-dd")) && col("DAS").isin("S04", "S05", "S20", "S25", "S18") && col("ACCOUNT_TYPE") =!= "A13", col("ACCOUNT_KEY")).otherwise(lit(null)))).as("ACTIV_LOANS_NONCL"),
				size(collect_set(when((col("CLOSED_DT").isNull || col("CLOSED_DT") > from_unixtime(unix_timestamp(lit(end_dt), "dd-MMM-yy"), "yyyy-MM-dd")) && col("DAS").isin("S04", "S05", "S20", "S25", "S18") && col("MFI_ID") === "NBF0000399" && col("CLUST_IND") === "ONLY HC", col("ACCOUNT_KEY")).otherwise(lit(null)))).as("ACTIV_LOANS_HC"),
				size(collect_set(when((col("CLOSED_DT").isNull || col("CLOSED_DT") > from_unixtime(unix_timestamp(lit(end_dt), "dd-MMM-yy"), "yyyy-MM-dd")) && col("DAS").isin("S04", "S05", "S20", "S25", "S18") && col("CLUST_IND") === "HC_WITH_OTHERS", col("ACCOUNT_KEY")).otherwise(lit(null)))).as("ACTIV_LN_HCWTHOTHERS"),
				size(collect_set(when((col("CLOSED_DT").isNull || col("CLOSED_DT") > from_unixtime(unix_timestamp(lit(end_dt), "dd-MMM-yy"), "yyyy-MM-dd")) && col("DAS").isin("S04", "S05", "S20", "S25", "S18") && (col("CLUST_IND") === "HC_WITH_OTHERS" || col("CLUST_IND") === "ONLY HC"), col("ACCOUNT_KEY")).otherwise(lit(null)))).as("ACTIV_LOANS_MIXED"),
				size(collect_set(when((col("CLOSED_DT").isNull || col("CLOSED_DT") > from_unixtime(unix_timestamp(lit(end_dt), "dd-MMM-yy"), "yyyy-MM-dd")) && col("DAS").isin("S04", "S05", "S20", "S25", "S18") && col("MFI_ID") =!= "NBF0000399" && col("CLUST_IND") === "OTHR CLST", col("ACCOUNT_KEY")).otherwise(lit(null)))).as("ACTIV_LOANS_OTHERS"),
				size(collect_set(when((col("CLOSED_DT").isNull || col("CLOSED_DT") > from_unixtime(unix_timestamp(lit(end_dt), "dd-MMM-yy"), "yyyy-MM-dd")) && col("DAS").isin("S04", "S05", "S20", "S25", "S18") && col("LENDER_TYPE") === "Banks", col("ACCOUNT_KEY")).otherwise(lit(null)))).as("ACTIV_LOANS_BANKS"),
				size(collect_set(when((col("CLOSED_DT").isNull || col("CLOSED_DT") > from_unixtime(unix_timestamp(lit(end_dt), "dd-MMM-yy"), "yyyy-MM-dd")) && col("DAS").isin("S04", "S05", "S20", "S25", "S18") && col("LENDER_TYPE") === "Non Banks", col("ACCOUNT_KEY")).otherwise(lit(null)))).as("ACTIV_LOANS_NONBANKS"),
				size(collect_set(when((col("CLOSED_DT").isNull || col("CLOSED_DT") > from_unixtime(unix_timestamp(lit(end_dt), "dd-MMM-yy"), "yyyy-MM-dd")) && col("DAS").isin("S04", "S05", "S20", "S25", "S18") && col("PRODUCT") === "Secured", col("ACCOUNT_KEY")).otherwise(lit(null)))).as("ACTIV_LOANS_SECURED"),
				size(collect_set(when((col("CLOSED_DT").isNull || col("CLOSED_DT") > from_unixtime(unix_timestamp(lit(end_dt), "dd-MMM-yy"), "yyyy-MM-dd")) && col("DAS").isin("S04", "S05", "S20", "S25", "S18") && col("PRODUCT") === "Unsecured", col("ACCOUNT_KEY")).otherwise(lit(null)))).as("ACTIV_LOANS_UNSECURED"),
				size(collect_set(when((col("CLOSED_DT").isNull || col("CLOSED_DT") > from_unixtime(unix_timestamp(lit(end_dt), "dd-MMM-yy"), "yyyy-MM-dd")) && col("DAS").isin("S04", "S05", "S20", "S25", "S18") && col("PRODUCT").isin("Both", "Others"), col("ACCOUNT_KEY")).otherwise(lit(null)))).as("ACTIV_LOANS_Othr_Prodct"),
				size(collect_set(when((col("CLOSED_DT").isNull || col("CLOSED_DT") > from_unixtime(unix_timestamp(lit(end_dt), "dd-MMM-yy"), "yyyy-MM-dd")) && col("DAS").isin("S04", "S05", "S20", "S25", "S18") && col("MFI_ID") === "NBF0000399", col("ACCOUNT_KEY")).otherwise(lit(null)))).as("ACTIV_LNS_HC_MIXED"),
				size(collect_set(when((col("CLOSED_DT").isNull || col("CLOSED_DT") > from_unixtime(unix_timestamp(lit(end_dt), "dd-MMM-yy"), "yyyy-MM-dd")) && col("DAS").isin("S04", "S05", "S20", "S25", "S18") && col("MFI_ID") =!= "NBF0000399", col("ACCOUNT_KEY")).otherwise(lit(null)))).as("ACTIV_LNS_OTHR_MIXED"),
				sum(when((col("CLOSED_DT").isNull || col("CLOSED_DT") > from_unixtime(unix_timestamp(lit(end_dt), "dd-MMM-yy"), "yyyy-MM-dd")) && col("DAS").isin("S04", "S05", "S20", "S25", "S18"), col("CURRENT_BALANCE")).otherwise(0)).as("TOTAL_ACT_OUTSTANDING_AMOUNT"),
				sum(when((col("CLOSED_DT").isNull || col("CLOSED_DT") > from_unixtime(unix_timestamp(lit(end_dt), "dd-MMM-yy"), "yyyy-MM-dd")) && col("DAS").isin("S04", "S05", "S20", "S25", "S18") && col("ACCOUNT_TYPE") === "A13", col("CURRENT_BALANCE")).otherwise(0)).as("PORTFOLIO_CL"),
				sum(when((col("CLOSED_DT").isNull || col("CLOSED_DT") > from_unixtime(unix_timestamp(lit(end_dt), "dd-MMM-yy"), "yyyy-MM-dd")) && col("DAS").isin("S04", "S05", "S20", "S25", "S18") && col("ACCOUNT_TYPE") =!= "A13", col("CURRENT_BALANCE")).otherwise(0)).as("PORTFOLIO_NONCL"),
				sum(when((col("CLOSED_DT").isNull || col("CLOSED_DT") > from_unixtime(unix_timestamp(lit(end_dt), "dd-MMM-yy"), "yyyy-MM-dd")) && col("DAS").isin("S04", "S05", "S20", "S25", "S18") && col("MFI_ID") === "NBF0000399" && col("CLUST_IND") === "ONLY HC", col("CURRENT_BALANCE")).otherwise(0)).as("OUTSTANDING_AMOUNT_HC"),
				sum(when((col("CLOSED_DT").isNull || col("CLOSED_DT") > from_unixtime(unix_timestamp(lit(end_dt), "dd-MMM-yy"), "yyyy-MM-dd")) && col("DAS").isin("S04", "S05", "S20", "S25", "S18") && col("CLUST_IND") === "HC_WITH_OTHERS", col("CURRENT_BALANCE")).otherwise(0)).as("OUTSTANDING_AMT_HCWTH_OTRS"),
				sum(when((col("CLOSED_DT").isNull || col("CLOSED_DT") > from_unixtime(unix_timestamp(lit(end_dt), "dd-MMM-yy"), "yyyy-MM-dd")) && col("DAS").isin("S04", "S05", "S20", "S25", "S18") && (col("CLUST_IND") === "HC_WITH_OTHERS" || col("CLUST_IND") === "ONLY HC"), col("CURRENT_BALANCE")).otherwise(0)).as("OUTSTANDING_AMOUNT_MIXED"),
				sum(when((col("CLOSED_DT").isNull || col("CLOSED_DT") > from_unixtime(unix_timestamp(lit(end_dt), "dd-MMM-yy"), "yyyy-MM-dd")) && col("DAS").isin("S04", "S05", "S20", "S25", "S18") && col("MFI_ID") =!= "NBF0000399" && col("CLUST_IND") === "OTHR CLST", col("CURRENT_BALANCE")).otherwise(0)).as("OUTSTANDING_AMOUNT_OTHERS"),
				sum(when((col("CLOSED_DT").isNull || col("CLOSED_DT") > from_unixtime(unix_timestamp(lit(end_dt), "dd-MMM-yy"), "yyyy-MM-dd")) && col("DAS").isin("S04", "S05", "S20", "S25", "S18") && col("LENDER_TYPE") === "Banks", col("CURRENT_BALANCE")).otherwise(0)).as("OUTSTANDING_AMOUNT_BANKS"),
				sum(when((col("CLOSED_DT").isNull || col("CLOSED_DT") > from_unixtime(unix_timestamp(lit(end_dt), "dd-MMM-yy"), "yyyy-MM-dd")) && col("DAS").isin("S04", "S05", "S20", "S25", "S18") && col("LENDER_TYPE") === "Non Banks", col("CURRENT_BALANCE")).otherwise(0)).as("OUTSTANDING_AMOUNT_NONBANKS"),
				sum(when((col("CLOSED_DT").isNull || col("CLOSED_DT") > from_unixtime(unix_timestamp(lit(end_dt), "dd-MMM-yy"), "yyyy-MM-dd")) && col("DAS").isin("S04", "S05", "S20", "S25", "S18") && col("PRODUCT") === "Secured", col("CURRENT_BALANCE")).otherwise(0)).as("OUTSTANDING_AMOUNT_SECURED"),
				sum(when((col("CLOSED_DT").isNull || col("CLOSED_DT") > from_unixtime(unix_timestamp(lit(end_dt), "dd-MMM-yy"), "yyyy-MM-dd")) && col("DAS").isin("S04", "S05", "S20", "S25", "S18") && col("PRODUCT") === "Unsecured", col("CURRENT_BALANCE")).otherwise(0)).as("OUTSTANDING_AMOUNT_UNSECURED"),
				sum(when((col("CLOSED_DT").isNull || col("CLOSED_DT") > from_unixtime(unix_timestamp(lit(end_dt), "dd-MMM-yy"), "yyyy-MM-dd")) && col("DAS").isin("S04", "S05", "S20", "S25", "S18") && col("PRODUCT").isin("Both", "Others"), col("CURRENT_BALANCE")).otherwise(0)).as("OUTSTANDING_AMOUNT_Othr_Prodct"),
				sum(when((col("CLOSED_DT").isNull || col("CLOSED_DT") > from_unixtime(unix_timestamp(lit(end_dt), "dd-MMM-yy"), "yyyy-MM-dd")) && col("DAS").isin("S04", "S05", "S20", "S25", "S18") && col("MFI_ID") === "NBF0000399", col("CURRENT_BALANCE")).otherwise(0)).as("OUTSTANDING_HC_MIXED"),
				sum(when((col("CLOSED_DT").isNull || col("CLOSED_DT") > from_unixtime(unix_timestamp(lit(end_dt), "dd-MMM-yy"), "yyyy-MM-dd")) && col("DAS").isin("S04", "S05", "S20", "S25", "S18") && col("MFI_ID") =!= "NBF0000399", col("CURRENT_BALANCE")).otherwise(0)).as("OUTSTANDING_OTHERS_MIXED"),
				sum(abs(coalesce(col("HIGH_CREDIT"), col("DISBURSED_AMOUNT"), col("CREDIT_LIMIT")))).as("TOTAL_DISB_AMOUNT"),
				sum(when(col("MFI_ID") === "NBF0000399" && col("CLUST_IND") === "ONLY HC", abs(coalesce(col("HIGH_CREDIT"), col("DISBURSED_AMOUNT"), col("CREDIT_LIMIT")))).otherwise(0)).as("DISB_AMOUNT_HC"),
				sum(when(col("CLUST_IND") === "HC_WITH_OTHERS", abs(coalesce(col("HIGH_CREDIT"), col("DISBURSED_AMOUNT"), col("CREDIT_LIMIT")))).otherwise(0)).as("DISB_AMNT_HCWTH_OTHR"),
				sum(when((col("CLUST_IND") === "HC_WITH_OTHERS" || col("CLUST_IND") === "ONLY HC"), abs(coalesce(col("HIGH_CREDIT"), col("DISBURSED_AMOUNT"), col("CREDIT_LIMIT")))).otherwise(0)).as("DISB_AMOUNT_MIXED"),
				sum(when((col("MFI_ID") =!= "NBF0000399" && col("CLUST_IND") === "OTHR CLST"), abs(coalesce(col("HIGH_CREDIT"), col("DISBURSED_AMOUNT"), col("CREDIT_LIMIT")))).otherwise(0)).as("DISB_AMOUNT_OTHERS"),
				sum(when(col("LENDER_TYPE") === "Banks", abs(coalesce(col("HIGH_CREDIT"), col("DISBURSED_AMOUNT"), col("CREDIT_LIMIT")))).otherwise(0)).as("DISB_AMOUNT_BANK"),
				sum(when(col("LENDER_TYPE") === "Non Banks", abs(coalesce(col("HIGH_CREDIT"), col("DISBURSED_AMOUNT"), col("CREDIT_LIMIT")))).otherwise(0)).as("DISB_AMOUNT_NONBANK"),
				sum(when(col("PRODUCT") === "Secured", abs(coalesce(col("HIGH_CREDIT"), col("DISBURSED_AMOUNT"), col("CREDIT_LIMIT")))).otherwise(0)).as("DISB_AMOUNT_SECURED"),
				sum(when(col("PRODUCT") === "Unsecured", abs(coalesce(col("HIGH_CREDIT"), col("DISBURSED_AMOUNT"), col("CREDIT_LIMIT")))).otherwise(0)).as("DISB_AMOUNT_UNSECURED"),
				sum(when(col("PRODUCT").isin("Both", "Others"), abs(coalesce(col("HIGH_CREDIT"), col("DISBURSED_AMOUNT"), col("CREDIT_LIMIT")))).otherwise(0)).as("DISB_AMOUNT_Othr_Prodct"),
				sum(when(col("MFI_ID") === "NBF0000399", abs(coalesce(col("HIGH_CREDIT"), col("DISBURSED_AMOUNT"), col("CREDIT_LIMIT")))).otherwise(0)).as("DISB_AMOUNT_HC_MIXED"),
				sum(when(col("MFI_ID") =!= "NBF0000399", abs(coalesce(col("HIGH_CREDIT"), col("DISBURSED_AMOUNT"), col("CREDIT_LIMIT")))).otherwise(0)).as("DISB_AMOUNT_OTHERS_MIXED"),
				sum(when((col("CLOSED_DT").isNull || col("CLOSED_DT") > from_unixtime(unix_timestamp(lit(end_dt), "dd-MMM-yy"), "yyyy-MM-dd")) && col("DAS").isin("S04", "S05", "S20", "S25", "S18"), abs(coalesce(col("HIGH_CREDIT"), col("DISBURSED_AMOUNT"), col("CREDIT_LIMIT")))).otherwise(0)).as("TOTAL_ACT_DISB_AMOUNT"),
				sum(when((col("CLOSED_DT").isNull || col("CLOSED_DT") > from_unixtime(unix_timestamp(lit(end_dt), "dd-MMM-yy"), "yyyy-MM-dd")) && col("DAS").isin("S04", "S05", "S20", "S25", "S18") && col("MFI_ID") === "NBF0000399" && col("CLUST_IND") === "ONLY HC", abs(coalesce(col("HIGH_CREDIT"), col("DISBURSED_AMOUNT"), col("CREDIT_LIMIT")))).otherwise(0)).as("ACT_DISB_AMOUNT_HC"),
				sum(when((col("CLOSED_DT").isNull || col("CLOSED_DT") > from_unixtime(unix_timestamp(lit(end_dt), "dd-MMM-yy"), "yyyy-MM-dd")) && col("DAS").isin("S04", "S05", "S20", "S25", "S18") && col("CLUST_IND") === "HC_WITH_OTHERS", abs(coalesce(col("HIGH_CREDIT"), col("DISBURSED_AMOUNT"), col("CREDIT_LIMIT")))).otherwise(0)).as("ACT_DISB_AMT_HC_WTH_OTH"),
				sum(when((col("CLOSED_DT").isNull || col("CLOSED_DT") > from_unixtime(unix_timestamp(lit(end_dt), "dd-MMM-yy"), "yyyy-MM-dd")) && col("DAS").isin("S04", "S05", "S20", "S25", "S18") && (col("CLUST_IND") === "HC_WITH_OTHERS" || col("CLUST_IND") === "ONLY HC"), abs(coalesce(col("HIGH_CREDIT"), col("DISBURSED_AMOUNT"), col("CREDIT_LIMIT")))).otherwise(0)).as("ACT_DISB_AMOUNT_MIXED"),
				sum(when((col("CLOSED_DT").isNull || col("CLOSED_DT") > from_unixtime(unix_timestamp(lit(end_dt), "dd-MMM-yy"), "yyyy-MM-dd")) && col("DAS").isin("S04", "S05", "S20", "S25", "S18") && col("MFI_ID") =!= "NBF0000399" && col("CLUST_IND") === "OTHR CLST", abs(coalesce(col("HIGH_CREDIT"), col("DISBURSED_AMOUNT"), col("CREDIT_LIMIT")))).otherwise(0)).as("ACT_DISB_AMOUNT_OTHERS"),
				sum(when((col("CLOSED_DT").isNull || col("CLOSED_DT") > from_unixtime(unix_timestamp(lit(end_dt), "dd-MMM-yy"), "yyyy-MM-dd")) && col("DAS").isin("S04", "S05", "S20", "S25", "S18") && col("LENDER_TYPE") === "Banks", abs(coalesce(col("HIGH_CREDIT"), col("DISBURSED_AMOUNT"), col("CREDIT_LIMIT")))).otherwise(0)).as("ACT_DISB_AMOUNT_BANKS"),
				sum(when((col("CLOSED_DT").isNull || col("CLOSED_DT") > from_unixtime(unix_timestamp(lit(end_dt), "dd-MMM-yy"), "yyyy-MM-dd")) && col("DAS").isin("S04", "S05", "S20", "S25", "S18") && col("LENDER_TYPE") === "Non Banks", abs(coalesce(col("HIGH_CREDIT"), col("DISBURSED_AMOUNT"), col("CREDIT_LIMIT")))).otherwise(0)).as("ACT_DISB_AMOUNT_NONBANKS"),
				sum(when((col("CLOSED_DT").isNull || col("CLOSED_DT") > from_unixtime(unix_timestamp(lit(end_dt), "dd-MMM-yy"), "yyyy-MM-dd")) && col("DAS").isin("S04", "S05", "S20", "S25", "S18") && col("PRODUCT") === "Secured", abs(coalesce(col("HIGH_CREDIT"), col("DISBURSED_AMOUNT"), col("CREDIT_LIMIT")))).otherwise(0)).as("ACT_DISB_AMOUNT_SECURED"),
				sum(when((col("CLOSED_DT").isNull || col("CLOSED_DT") > from_unixtime(unix_timestamp(lit(end_dt), "dd-MMM-yy"), "yyyy-MM-dd")) && col("DAS").isin("S04", "S05", "S20", "S25", "S18") && col("PRODUCT") === "Unsecured", abs(coalesce(col("HIGH_CREDIT"), col("DISBURSED_AMOUNT"), col("CREDIT_LIMIT")))).otherwise(0)).as("ACT_DISB_AMOUNT_UNSECURED"),
				sum(when((col("CLOSED_DT").isNull || col("CLOSED_DT") > from_unixtime(unix_timestamp(lit(end_dt), "dd-MMM-yy"), "yyyy-MM-dd")) && col("DAS").isin("S04", "S05", "S20", "S25", "S18") && col("PRODUCT").isin("Both", "Others"), abs(coalesce(col("HIGH_CREDIT"), col("DISBURSED_AMOUNT"), col("CREDIT_LIMIT")))).otherwise(0)).as("ACT_DISB_AMOUNT_Othr_Prodct"),
				sum(when((col("CLOSED_DT").isNull || col("CLOSED_DT") > from_unixtime(unix_timestamp(lit(end_dt), "dd-MMM-yy"), "yyyy-MM-dd")) && col("DAS").isin("S04", "S05", "S20", "S25", "S18") && col("MFI_ID") === "NBF0000399", abs(coalesce(col("HIGH_CREDIT"), col("DISBURSED_AMOUNT"), col("CREDIT_LIMIT")))).otherwise(0)).as("ACT_DISB_HC_MIXED"),
				sum(when((col("CLOSED_DT").isNull || col("CLOSED_DT") > from_unixtime(unix_timestamp(lit(end_dt), "dd-MMM-yy"), "yyyy-MM-dd")) && col("DAS").isin("S04", "S05", "S20", "S25", "S18") && col("MFI_ID") =!= "NBF0000399", abs(coalesce(col("HIGH_CREDIT"), col("DISBURSED_AMOUNT"), col("CREDIT_LIMIT")))).otherwise(0)).as("ACT_DISB_OTHR_MIXED"),
				sum(col("MIN_AMT_DUE_FNL")).as("MIN_AMOUNT_DUE"),
				sum(when(col("MFI_ID") === "NBF0000399" && col("CLUST_IND") === "ONLY HC", col("MIN_AMT_DUE_FNL")).otherwise(0)).as("MINAMT_DUE_HC"),
				sum(when(col("CLUST_IND") === "HC_WITH_OTHERS", col("MIN_AMT_DUE_FNL")).otherwise(0)).as("MINAMT_HC_WTH_OTHR"),
				sum(when((col("CLUST_IND") === "HC_WITH_OTHERS" || col("CLUST_IND") === "ONLY HC"), col("MIN_AMT_DUE_FNL")).otherwise(0)).as("MINAMT_DUE_MIXED"),
				sum(when((col("MFI_ID") =!= "NBF0000399" && col("CLUST_IND") === "OTHR CLST"), col("MIN_AMT_DUE_FNL")).otherwise(0)).as("MIN_AMT_DUE_OTHERS"),
				sum(when(col("MFI_ID") === "NBF0000399", col("MIN_AMT_DUE_FNL")).otherwise(0)).as("MIN_AMT_DUE_HC_MIXED"),
				sum(when(col("MFI_ID") =!= "NBF0000399", col("MIN_AMT_DUE_FNL")).otherwise(0)).as("MIN_AMT_DUE_OTHR_MIXED"),
				max(coalesce(col("ANNUAL_INCOME_FNL"), col("MONTHLY_INCOME_FNL"))).as("ANUAL_INCOME"),
				max(when(col("MFI_ID") === "NBF0000399" && col("CLUST_IND") === "ONLY HC", coalesce(col("ANNUAL_INCOME_FNL"), col("MONTHLY_INCOME_FNL"))).otherwise(lit(null))).as("ANUAL_INCOME_HC"),
				max(when(col("CLUST_IND") === "HC_WITH_OTHERS", coalesce(col("ANNUAL_INCOME_FNL"), col("MONTHLY_INCOME_FNL"))).otherwise(lit(null))).as("ANUAL_INCOME_HC_WTH_OTHR"),
				max(when((col("CLUST_IND") === "HC_WITH_OTHERS" || col("CLUST_IND") === "ONLY HC"), coalesce(col("ANNUAL_INCOME_FNL"), col("MONTHLY_INCOME_FNL"))).otherwise(lit(null))).as("ANUAL_INCOME_MIXED"),
				max(when((col("MFI_ID") =!= "NBF0000399" && col("CLUST_IND") === "OTHR CLST"), coalesce(col("ANNUAL_INCOME_FNL"), col("MONTHLY_INCOME_FNL"))).otherwise(lit(null))).as("ANUAL_INCOME_OTHERS"),
				max(when(col("MFI_ID") === "NBF0000399", coalesce(col("ANNUAL_INCOME_FNL"), col("MONTHLY_INCOME_FNL"))).otherwise(lit(null))).as("ANUAL_INCOME_HC_MIXED"),
				max(when(col("MFI_ID") =!= "NBF0000399", coalesce(col("ANNUAL_INCOME_FNL"), col("MONTHLY_INCOME_FNL"))).otherwise(lit(null))).as("ANUAL_INCOME_OTHERS_MIXED"))
    

				a13AggrDf.repartition(2000).write.mode("overwrite").saveAsTable(s"""hmanalytics.hm_hc_full_aggr_tbl_"""+mon_rn+"""_fnl""")

/*val f1 = Future{
		unMnlDF.repartition(2000).write.mode("overwrite").saveAsTable(s"""hmanalytics.hm_hc_full_base_tbl_"""+mon_rn+"""_fnl""")
	}
	
	val f2 = Future{
		a13AggrDf.repartition(2000).write.mode("overwrite").saveAsTable(s"""hmanalytics.hm_hc_full_aggr_tbl_"""+mon_rn+"""_fnl""")
	}
	
	Await.ready(f1, Duration.Inf)
	Await.ready(f2, Duration.Inf)*/
	}
}