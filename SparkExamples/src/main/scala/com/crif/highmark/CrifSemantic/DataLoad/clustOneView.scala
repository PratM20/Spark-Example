package com.crif.highmark.CrifSemantic.DataLoad
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.expressions.Window


object clustOneView {
  def oneView(spark: SparkSession,mon_rn : String): Unit = {

  	
val af = spark.sql("""
select 
A.*,
C.disbursed_dt,
C.reported_dt
from
(select 
A.consumer_key,
A.account_key,
A.mfi_id,
A.pin_code,
A.std_state_code,
COALESCE(A.std_locality1_value,A.std_locality1a_value) locality,
COALESCE(A.std_derived_city,A.std_city_1,A.std_city_2) city,
A.district,
A.monthly_income,
A.annual_income,
A.birth_dt,
A.gender,
A.is_commercial,
A.occupation,
COALESCE(B.max_clst_id,A.consumer_key) clst_id
from
hmanalytics.hm_cns_analytics A
Left Outer Join 
hmanalytics.hm_cns_clst_"""+mon_rn+"""  B
ON(
A.consumer_key = B.candidate_id
))A
Left Outer Join
(select account_key,disbursed_dt,reported_dt from hmanalytics.hm_cns_account_prod) C
ON(A.account_key = C.account_key)
""")

af.createOrReplaceTempView("temp")
af.repartition(100).write.mode("overwrite").saveAsTable("hmanalytics.hm_cns_cluster_wrk1")


val DF = spark.sql("""
select *,
CASE WHEN STD_STATE_CODE IS NOT NULL AND DISTRICT IS NOT NULL AND PIN_CODE IS NOT NULL AND CITY IS NOT NULL AND LOCALITY IS NOT NULL THEN 1
     WHEN STD_STATE_CODE IS NOT NULL AND DISTRICT IS NOT NULL AND PIN_CODE IS NOT NULL AND CITY IS NOT NULL AND LOCALITY IS NULL THEN 2
     WHEN STD_STATE_CODE IS NOT NULL AND DISTRICT IS NOT NULL AND PIN_CODE IS NOT NULL AND CITY IS NULL AND LOCALITY IS NOT NULL THEN 3
     WHEN STD_STATE_CODE IS NOT NULL AND DISTRICT IS NOT NULL AND PIN_CODE IS NULL AND CITY IS NOT NULL AND LOCALITY IS NOT NULL THEN 4
     WHEN STD_STATE_CODE IS NOT NULL AND DISTRICT IS NULL AND PIN_CODE IS NOT NULL AND CITY IS NOT NULL AND LOCALITY IS NOT NULL THEN 5
     WHEN STD_STATE_CODE IS NULL AND DISTRICT IS NOT NULL AND PIN_CODE IS NOT NULL AND CITY IS NOT NULL AND LOCALITY IS NOT NULL THEN 6
	 ELSE 7
END SDPCL,
COUNT(CASE WHEN is_commercial  THEN 1  else null END) OVER (PARTITION BY clst_id)  is_commercial_cnt
FROM
hmanalytics.hm_cns_cluster_wrk1
""")
DF.createOrReplaceTempView("temp1")



 
val rowDF  = spark.sql("""
select consumer_key,account_key,mfi_id,clst_id,disbursed_dt,reported_dt,pin_code,std_state_code,locality,city,district
from (
select consumer_key,account_key,mfi_id,clst_id,disbursed_dt,reported_dt,pin_code,std_state_code,locality,city,district,
ROW_NUMBER () OVER (PARTITION BY clst_id ORDER BY SDPCL,reported_dt desc,disbursed_dt desc) SDPCL_RW
FROM temp1) A
WHERE SDPCL_RW =1
""")

rowDF.createOrReplaceTempView("tempSDPCLRowNum")

val sdpclDF  = spark.sql("""
select A.consumer_key,A.account_key,A.mfi_id,A.clst_id,A.disbursed_dt,A.reported_dt,B.pin_code,B.std_state_code,B.locality,B.city,B.district,
CASE WHEN A.is_commercial_cnt > 0 THEN  true ELSE false END is_commercial
FROM temp1 A
Left Outer Join
tempSDPCLRowNum B
ON(A.clst_id = B.clst_id)
""")

sdpclDF.createOrReplaceTempView("tempSDPCL")



val mntIncDF  = spark.sql("""
select A.*,B.monthly_income
FROM tempSDPCL A
Left Outer Join
(select clst_id,monthly_income from (select clst_id,monthly_income,ROW_NUMBER () OVER (PARTITION BY clst_id ORDER BY reported_dt  desc,disbursed_dt desc ,monthly_income desc  ) monthly_income_rw from hmanalytics.hm_cns_cluster_wrk1 where monthly_income is not null) where monthly_income_rw =1) B
ON(A.clst_id = B.clst_id)
""")
mntIncDF.createOrReplaceTempView("tempMnthl")

val anlIncDF  = spark.sql("""
select A.*,B.annual_income
FROM tempMnthl A
Left Outer Join
(select clst_id,annual_income from (select clst_id,annual_income,ROW_NUMBER () OVER (PARTITION BY clst_id ORDER BY reported_dt  desc,disbursed_dt desc ,annual_income desc  ) annual_income_rw from hmanalytics.hm_cns_cluster_wrk1 where annual_income is not null) where annual_income_rw =1) B
ON(A.clst_id = B.clst_id)
""")
anlIncDF.createOrReplaceTempView("tempAnl")

val dobDF  = spark.sql("""
select A.*,B.birth_dt
FROM tempAnl A
Left Outer Join
(select clst_id,birth_dt from (select clst_id,birth_dt,ROW_NUMBER () OVER (PARTITION BY clst_id ORDER BY reported_dt  desc,disbursed_dt desc) dob_rw from hmanalytics.hm_cns_cluster_wrk1 where birth_dt is not null) where dob_rw =1) B
ON(A.clst_id = B.clst_id)
""")
dobDF.createOrReplaceTempView("tempBirthDt")
val genderDF  = spark.sql("""
select A.*,B.gender
FROM tempBirthDt A
Left Outer Join
(select clst_id,gender from (select clst_id,gender,ROW_NUMBER () OVER (PARTITION BY clst_id ORDER BY reported_dt  desc,disbursed_dt desc) gender_rw from hmanalytics.hm_cns_cluster_wrk1 where gender is not null) where gender_rw =1) B
ON(A.clst_id = B.clst_id)
""")
genderDF.createOrReplaceTempView("tempGender")
val occDF  = spark.sql("""
select A.*,B.occupation
FROM tempGender A
Left Outer Join
(select clst_id,occupation from (select clst_id,occupation,ROW_NUMBER () OVER (PARTITION BY clst_id ORDER BY reported_dt  desc,disbursed_dt desc) occupation_rw from hmanalytics.hm_cns_cluster_wrk1 where occupation is not null) where occupation_rw =1) B
ON(A.clst_id = B.clst_id)
""")

occDF.repartition(500).write.mode("overwrite").saveAsTable("hmanalytics.hm_cns_cluster_unique_row")
  }
}