package com.crif.highmark.CrifSemantic.Service
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.types._
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.DataFrame
import com.crif.highmark.CrifSemantic.Aggregation.homeCreditAggregation


object homeCreditDataBlend {
  def DataBlend(spark : SparkSession , extraJoinDF : DataFrame ,argument : Array[String]): Unit = {
  	val mon_rn=argument(0)
  	val end_dt=argument(1)
  	
 val clstIndDataDF = extraJoinDF.where(col("MFI_ID") === "NBF0000399").select(col("MAX_CLST_ID").as("CLUSTER_ID")).distinct()


val clstIndData1DF = extraJoinDF.groupBy("MAX_CLST_ID")
																.agg(size(collect_set(col("ACCOUNT_KEY"))).as("TOTAL_LOANS"),
																		size(collect_set(when(col("MFI_ID") ==="NBF0000399", col("ACCOUNT_KEY")).otherwise(lit(null)))).as("TOTAL_LOANS_HC"),
																		size(collect_set(when((col("CLOSED_DT").isNull  || col("CLOSED_DT") > from_unixtime(unix_timestamp(lit(end_dt),"dd-MMM-yy") ,"yyyy-MM-dd")) && col("DAS").isin("S04","S05","S20","S25","S18") , col("ACCOUNT_KEY")).otherwise(lit(null)))).as("TOTAL_ACTIV_LOANS"),
																		sum(when((col("CLOSED_DT").isNull  || col("CLOSED_DT") > from_unixtime(unix_timestamp(lit(end_dt),"dd-MMM-yy") ,"yyyy-MM-dd")) && col("DAS").isin("S04","S05","S20","S25","S18") ,col("CURRENT_BALANCE")).otherwise(0)).as("TOTAL_ACT_OUTSTANDING_AMOUNT")).persist()

val nbfInDF = clstIndData1DF.join(clstIndDataDF,clstIndDataDF("CLUSTER_ID") === clstIndData1DF("MAX_CLST_ID"))
														.drop(clstIndDataDF("CLUSTER_ID"))

val nbfNotInDF = clstIndData1DF.join(clstIndDataDF,clstIndDataDF("CLUSTER_ID") === clstIndData1DF("MAX_CLST_ID"),"left")
															 .where(clstIndDataDF("CLUSTER_ID").isNull).drop(clstIndDataDF("CLUSTER_ID"))
clstIndData1DF.unpersist()
val clstIndFnl1DF = nbfInDF.withColumn("CLUST_IND", when(col("TOTAL_LOANS") === col("TOTAL_LOANS_HC"),"ONLY HC")
													 .when(col("TOTAL_LOANS") =!= col("TOTAL_LOANS_HC") , "HC_WITH_OTHERS"))

val clstIndFnl2DF = nbfNotInDF.withColumn("CLUST_IND" , lit("OTHR CLST"))

val clstIndFnlDF = clstIndFnl1DF.union(clstIndFnl2DF)

//clstIndFnlDF.write.mode("append").saveAsTable("hmanalytics.hm_hc_work_2")


val clstDataTblDF = extraJoinDF.withColumn("Lender_Type", when(col("MFI_ID").like("ARC%")  || col("MFI_ID").like("CCC%") || col("MFI_ID").like("NBF%") || col("MFI_ID").like("HFC%"),"Non Banks").otherwise("Banks"))
															 .withColumn("PRODUCT", when(col("ACCOUNT_TYPE").isin("A01","A04","A09","A08","A07","A01","A06","A20","A07","A09","A04","A31","A67","A03","A41","A10","A08","A61","A05","A42","A11"),"Secured")
															 .when(col("ACCOUNT_TYPE").isin("A62","A19","A24","A25","A28","A12","A22","A26","A14","A15","A13","A63","A29","A27","A40","A37","A40"), "Unsecured").otherwise("Others"))
	
																	 
val JoinclstDataTblDF =  clstDataTblDF.join(clstIndFnlDF , clstDataTblDF("MAX_CLST_ID") === clstIndFnlDF("MAX_CLST_ID"),"left")
																			.select(clstDataTblDF("*"),col("CLUST_IND"))

//val minDueValDF = JoinclstDataTblDF.crossJoin(spark.sql(s"""select * from hmanalytics.hm_overall_threshold_tbl where mon_yr='"""+mon_rn+"""'"""))
val minDueValDF = JoinclstDataTblDF.crossJoin(spark.sql(s"""select * from hmanalytics.hm_overall_threshold_tbl where mon_yr='NOV_18'"""))
																	.withColumn("Min_amt_threshold", when((col("MIN_DUE_99PT96_PERCENTILE").isNull) || (col("MIN_DUE_99PT996_PERCENTILE").isNotNull && coalesce(abs(col("MIN_AMOUNT_DUE")),lit(0)) <= col("MIN_DUE_99PT996_PERCENTILE")),1).otherwise(0))
																	.withColumn("THRESHOLD_FLAG_OVERALL", when((col("CB_99PT996_PERCENTILE").isNull) || (col("CB_99PT996_PERCENTILE").isNotNull && coalesce(abs(col("CURRENT_BALANCE")),lit(0))<= col("CB_99PT996_PERCENTILE") && abs(coalesce(col("HIGH_CREDIT"),col("DISBURSED_AMOUNT"),col("CREDIT_LIMIT"),lit(0))) <= col("DISBAMT_99PT996_PERCENTILE")),1).otherwise(0))
																	.select(JoinclstDataTblDF("*"),col("Min_amt_threshold"),col("THRESHOLD_FLAG_OVERALL"))
																	
//val thresDF = spark.sql(s"""select * from hmanalytics.hm_threshold_tbl_hc where mon_yr='"""+mon_rn+"""'""")										
val thresDF = spark.sql(s"""select * from hmanalytics.hm_threshold_tbl_hc where mon_yr='MAY_19'""")										
																	
val minThresValDF = minDueValDF.join(thresDF, minDueValDF("ACCOUNT_TYPE") === thresDF("ACCOUNT_TYPE") && thresDF("ACTIVE")===1,"left")																
															 .withColumn("THRESHOLD_ACNT_TYPE", when((col("CUR_BAL_99PT996").isNull) || (col("CUR_BAL_99PT996").isNotNull && coalesce(abs(col("CURRENT_BALANCE")),lit(0))<= col("CUR_BAL_99PT996") && abs(coalesce(col("HIGH_CREDIT"),col("DISBURSED_AMOUNT"),col("CREDIT_LIMIT"),lit(0)))<= col("DISB_AMT_99PT996")),1).otherwise(0))
															 .select(minDueValDF("*"), col("THRESHOLD_ACNT_TYPE"))

//minThresValDF.write.mode("append").saveAsTable("hmanalytics.hm_hc_work_3")

val anlIncNnDF = minThresValDF.where(col("ANNUAL_INCOME").isNotNull)
															.withColumn("RW", row_number().over(Window.partitionBy(col("MAX_CLST_ID")).orderBy(desc("REPORTED_DT"),desc("DISBURSED_DT"),desc("ANNUAL_INCOME")))) 
val anlIncNDF = minThresValDF.where(col("ANNUAL_INCOME").isNull).withColumn("RW", lit(null)) 

val unAnlDF = anlIncNnDF.union(anlIncNDF)

val mnlIncNnDF = unAnlDF.where(col("MONTHLY_INCOME").isNotNull)
												.withColumn("RW_mnth", row_number().over(Window.partitionBy(col("MAX_CLST_ID")).orderBy(desc("REPORTED_DT"),desc("DISBURSED_DT"),desc_nulls_last("MONTHLY_INCOME")))) 
val mnlIncNDF = unAnlDF.where(col("MONTHLY_INCOME").isNull).withColumn("RW_mnth", lit(null)) 

val unMnlDF1 = mnlIncNnDF.union(mnlIncNDF)

//unMnlDF.write.mode("append").saveAsTable("hmanalytics.hm_hc_work_4")

unMnlDF1.repartition(2000).write.mode("overwrite").saveAsTable(s"""hmanalytics.hm_hc_full_base_tbl_"""+mon_rn+"""_fnl""")

val unMnlDF = spark.sql(s"""select * from hmanalytics.hm_hc_full_base_tbl_"""+mon_rn+"""_fnl""")


homeCreditAggregation.DataAggregation(spark , unMnlDF ,argument)
  	
  }
}