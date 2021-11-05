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


object creditCardTab {
  def DataAggregation(spark: SparkSession,argument : Array[String]): Unit = {

val argument = spark.sparkContext.getConf.get("spark.driver.args").split("\\s+")
import scala.collection.JavaConverters._
val prop = new Properties()
prop.load(new FileInputStream("CHMprop.txt"))
val dateFormat = new SimpleDateFormat("yyyy-MM-dd")

val self=argument(0)
val end_dt=argument(1)
val mon_rn=argument(2)
val peer_group=prop.get("PEER_GROUP").toString().replaceAll("self", s"'$self'")

val endtemp = end_dt
val end = dateFormat.format(dateFormat.parse(endtemp))


val starttemp = DateUtils.addMonths(dateFormat.parse(endtemp), -24)	
val start = dateFormat.format(starttemp)

/* Projection Data for tabs Prod + Trend */

/*val trenddf = spark.sql("""
select
last_day(act_reported_dt) REPORTED_DT,
mfi_id ,
'T' data_ind,
mob,
city,
COUNT(CASE WHEN  derived_dpd <= 180 THEN ACCOUNT_KEY END ) ACTIVE_CARDS_DPD180, 
COUNT(CASE WHEN  DERIVED_DPD > 30 AND derived_dpd <= 180 THEN ACCOUNT_KEY END) LOANS_DPD_GTR_30,
COUNT(CASE WHEN  DERIVED_DPD > 60 AND derived_dpd <= 180 THEN ACCOUNT_KEY END) LOANS_DPD_GTR_60, 
COUNT(CASE WHEN  DERIVED_DPD > 90 AND derived_dpd <= 180 THEN ACCOUNT_KEY END) LOANS_DPD_GTR_90, 
SUM( CASE WHEN   derived_dpd <= 180 THEN CURRENT_BALANCE ELSE 0 END ) TOTAL_ACTIVE_POS_DPD180,
SUM( CASE WHEN  DERIVED_DPD > 30 AND derived_dpd <= 180 THEN CURRENT_BALANCE ELSE 0 END ) POS_DPD_GTR_30,
SUM( CASE WHEN  DERIVED_DPD > 60 AND derived_dpd <= 180 THEN CURRENT_BALANCE ELSE 0 END ) POS_DPD_GTR_60,
SUM( CASE WHEN  DERIVED_DPD > 90 AND derived_dpd <= 180 THEN CURRENT_BALANCE ELSE 0 END ) POS_DPD_GTR_90,
COUNT(ACCOUNT_KEY) ACTIVE_CARDS, 
SUM(CURRENT_BALANCE) TOTAL_ACTIVE_POS,
AVG(CASE WHEN (SANCTIONED_AMOUNT = 0 OR SANCTIONED_AMOUNT is null) THEN 0 ELSE  ABS(CURRENT_BALANCE/SANCTIONED_AMOUNT) * 100 END) AVERAGE_UTILIZATION_PERCENT,
AVG(CASE WHEN (SANCTIONED_AMOUNT = 0 OR SANCTIONED_AMOUNT is null) THEN 0 ELSE SANCTIONED_AMOUNT END) AVERAGE_CREDIT_LIMIT,
AVG(CASE WHEN (SANCTIONED_AMOUNT = 0 OR SANCTIONED_AMOUNT is null) AND CURRENT_BALANCE < 1000 THEN 0 WHEN  CURRENT_BALANCE < 1000  THEN  ABS(CURRENT_BALANCE/SANCTIONED_AMOUNT) * 100 END) LOW_UTILIZATION_PF_PERCENT,
COUNT(CASE WHEN CURRENT_BALANCE < 1000 THEN ACCOUNT_KEY  END) LOW_UTILIZATION_LOAN,
AVG(CASE WHEN (SANCTIONED_AMOUNT = 0 OR SANCTIONED_AMOUNT is null) AND CURRENT_BALANCE < 1000 THEN 0 WHEN  CURRENT_BALANCE < 1000  THEN  SANCTIONED_AMOUNT END) AVERAGE_LOW_CREDIT_LIMIT

 
FROM
hmanalytics.hm_cc_cns_trend_data
where
act_reported_dt between add_months('"""+end_dt+"""',-24) and '"""+end_dt+"""'
and month(act_reported_dt) IN (3,6,9,12)
AND (NOT_CLOSED_IND = 'Y') AND DAS IN( 'S04', 'S05', 'S11', 'S12', 'S16', 'S18', 'S20' )
GROUP BY 
last_day(act_reported_dt) ,
mfi_id,
mob,
city
""")

trenddf.write.mode("overwrite").saveAsTable("hmanalytics.hm_cc_data1")

*/

spark.sql("""truncate table hmanalytics.hm_cc_data1""")
var start1 = start
do{
	val tempdf = spark.sql("""select  
account_key , 
max(act_reported_dt) max_rpt_dt  
from hmanalytics.hm_cc_cns_trend_data
where  act_reported_dt  <=  last_day('"""+start1+"""')
group by
account_key """).distinct()

val prodTempDF =spark.sql(""" select distinct account_key 
 from hmanalytics.hm_cc_cns_prd_data where  
act_reported_dt  <= last_day('"""+start1+"""')
""")
val tempProdDf = tempdf.join(prodTempDF, tempdf("ACCOUNT_KEY") === prodTempDF("ACCOUNT_KEY") ,"left_anti").distinct()
tempProdDf.createOrReplaceTempView("tempTrend")


val proddf = spark.sql(s"""
select
last_day('"""+start1+"""') REPORTED_DT,
mfi_id ,
'T' data_ind,
mob,
city,
COUNT(CASE WHEN  derived_dpd <= 180 THEN A.ACCOUNT_KEY END ) ACTIVE_CARDS_DPD180, 
COUNT(CASE WHEN  DERIVED_DPD > 30 AND derived_dpd <= 180 THEN A.ACCOUNT_KEY END) LOANS_DPD_GTR_30,
COUNT(CASE WHEN  DERIVED_DPD > 60 AND derived_dpd <= 180 THEN A.ACCOUNT_KEY END) LOANS_DPD_GTR_60, 
COUNT(CASE WHEN  DERIVED_DPD > 90 AND derived_dpd <= 180 THEN A.ACCOUNT_KEY END) LOANS_DPD_GTR_90, 
SUM( CASE WHEN   derived_dpd <= 180 THEN CURRENT_BALANCE ELSE 0 END ) TOTAL_ACTIVE_POS_DPD180,
SUM( CASE WHEN  DERIVED_DPD > 30 AND derived_dpd <= 180 THEN CURRENT_BALANCE ELSE 0 END ) POS_DPD_GTR_30,
SUM( CASE WHEN  DERIVED_DPD > 60 AND derived_dpd <= 180 THEN CURRENT_BALANCE ELSE 0 END ) POS_DPD_GTR_60,
SUM( CASE WHEN  DERIVED_DPD > 90 AND derived_dpd <= 180 THEN CURRENT_BALANCE ELSE 0 END ) POS_DPD_GTR_90,
COUNT(A.ACCOUNT_KEY) ACTIVE_CARDS, 
SUM(CURRENT_BALANCE) TOTAL_ACTIVE_POS,
AVG(CASE WHEN (SANCTIONED_AMOUNT = 0 OR SANCTIONED_AMOUNT is null) THEN 0 ELSE  ABS(CURRENT_BALANCE/SANCTIONED_AMOUNT) * 100 END) AVERAGE_UTILIZATION_PERCENT,
AVG(CASE WHEN (SANCTIONED_AMOUNT = 0 OR SANCTIONED_AMOUNT is null) THEN 0 ELSE SANCTIONED_AMOUNT END) AVERAGE_CREDIT_LIMIT,
AVG(CASE WHEN (SANCTIONED_AMOUNT = 0 OR SANCTIONED_AMOUNT is null) AND CURRENT_BALANCE < 1000 THEN 0 WHEN  CURRENT_BALANCE < 1000  THEN  ABS(CURRENT_BALANCE/SANCTIONED_AMOUNT) * 100 END) LOW_UTILIZATION_PF_PERCENT,
COUNT(CASE WHEN CURRENT_BALANCE < 1000 THEN A.ACCOUNT_KEY  END) LOW_UTILIZATION_LOAN,
AVG(CASE WHEN (SANCTIONED_AMOUNT = 0 OR SANCTIONED_AMOUNT is null) AND CURRENT_BALANCE < 1000 THEN 0 WHEN  CURRENT_BALANCE < 1000  THEN  SANCTIONED_AMOUNT END) AVERAGE_LOW_CREDIT_LIMIT

 
FROM
hmanalytics.hm_cc_cns_trend_data A 
JOIN
  tempTrend B
  ON(A.account_key = B.account_key
  AND A.act_reported_dt = B.max_rpt_dt)

where
(NOT_CLOSED_IND = 'Y') AND DAS IN( 'S04', 'S05', 'S11', 'S12', 'S16', 'S18', 'S20' )
GROUP BY 
last_day(act_reported_dt) ,
mfi_id,
mob,
city

UNION ALL

select
last_day('"""+start1+"""') REPORTED_DT,
mfi_id ,
'P' data_ind,
mob,
city,
COUNT(CASE WHEN  derived_dpd <= 180 THEN ACCOUNT_KEY END ) ACTIVE_CARDS_DPD180, 
COUNT(CASE WHEN  DERIVED_DPD > 30 AND derived_dpd <= 180 THEN ACCOUNT_KEY END) LOANS_DPD_GTR_30,
COUNT(CASE WHEN  DERIVED_DPD > 60 AND derived_dpd <= 180 THEN ACCOUNT_KEY END) LOANS_DPD_GTR_60, 
COUNT(CASE WHEN  DERIVED_DPD > 90 AND derived_dpd <= 180 THEN ACCOUNT_KEY END) LOANS_DPD_GTR_90, 
SUM( CASE WHEN   derived_dpd <= 180 THEN CURRENT_BALANCE ELSE 0 END ) TOTAL_ACTIVE_POS_DPD180,
SUM( CASE WHEN  DERIVED_DPD > 30 AND derived_dpd <= 180 THEN CURRENT_BALANCE ELSE 0 END ) POS_DPD_GTR_30,
SUM( CASE WHEN  DERIVED_DPD > 60 AND derived_dpd <= 180 THEN CURRENT_BALANCE ELSE 0 END ) POS_DPD_GTR_60,
SUM( CASE WHEN  DERIVED_DPD > 90 AND derived_dpd <= 180 THEN CURRENT_BALANCE ELSE 0 END ) POS_DPD_GTR_90,
COUNT(ACCOUNT_KEY) ACTIVE_CARDS, 
SUM(CURRENT_BALANCE) TOTAL_ACTIVE_POS,
AVG(CASE WHEN (SANCTIONED_AMOUNT = 0 OR SANCTIONED_AMOUNT is null) THEN 0 ELSE  ABS(CURRENT_BALANCE/SANCTIONED_AMOUNT) * 100 END) AVERAGE_UTILIZATION_PERCENT,
AVG(CASE WHEN (SANCTIONED_AMOUNT = 0 OR SANCTIONED_AMOUNT is null) THEN 0 ELSE SANCTIONED_AMOUNT END) AVERAGE_CREDIT_LIMIT,
AVG(CASE WHEN (SANCTIONED_AMOUNT = 0 OR SANCTIONED_AMOUNT is null) AND CURRENT_BALANCE < 1000 THEN 0 WHEN  CURRENT_BALANCE < 1000  THEN  ABS(CURRENT_BALANCE/SANCTIONED_AMOUNT) * 100 END) LOW_UTILIZATION_PF_PERCENT,
COUNT(CASE WHEN CURRENT_BALANCE < 1000 THEN ACCOUNT_KEY  END) LOW_UTILIZATION_LOAN,
AVG(CASE WHEN (SANCTIONED_AMOUNT = 0 OR SANCTIONED_AMOUNT is null) AND CURRENT_BALANCE < 1000 THEN 0 WHEN  CURRENT_BALANCE < 1000  THEN  SANCTIONED_AMOUNT END) AVERAGE_LOW_CREDIT_LIMIT
 
FROM
hmanalytics.hm_cc_cns_prd_data
where
act_reported_dt <= last_day('"""+start1+"""')
AND (NOT_CLOSED_IND = 'Y') AND DAS IN( 'S04', 'S05', 'S11', 'S12', 'S16', 'S18', 'S20' )
GROUP BY 
last_day(act_reported_dt) ,
mfi_id,
mob,
city
""")

var temp = DateUtils.addMonths(dateFormat.parse(start1), 3)
start1 = dateFormat.format(temp)

val f1 = Future{
proddf.write.mode("append").saveAsTable("hmanalytics.hm_cc_data1")
}
      Await.ready(f1, Duration.Inf)

}while (start1 <= end )
	

	/* Tab 1 to Tab 7 Data Load  */

	val tab1DF = spark.sql("""
select
'"""+mon_rn+"""' mon_rn,
 A.*,
NVL(B.Rank,0) Rank
from
(select
size(collect_set(ACCOUNT_KEY)) TOTAL_CC_ACQUISTIONS,
last_day(disbursed_dt) DISBURSED_DT,
"""+peer_group+""" PEER_GROUP
FROM
hmanalytics.hm_cc_cns_prd_data
where
DISBURSED_DT between add_months('"""+end_dt+"""',-24) and '"""+end_dt+"""'
AND derived_dpd <= 180
AND DAS IN( 'S04', 'S05', 'S11', 'S12', 'S16', 'S18', 'S20' )
GROUP BY 
last_day(disbursed_dt) ,
"""+peer_group+"""
ORDER BY 
last_day(disbursed_dt) desc)A

Left Outer Join
(
select 
DISBURSED_DT,
Lender_type,
Rank
from(
select 
DISBURSED_DT,
'SELF' Lender_type,
mfi_id,
RANK() OVER (PARTITION BY disbursed_dt ORDER BY TOTAL_CC_ACQUISTIONS desc ) AS Rank
from
(select
last_day(disbursed_dt) DISBURSED_DT,
mfi_id,
size(collect_set(ACCOUNT_KEY)) TOTAL_CC_ACQUISTIONS
FROM
hmanalytics.hm_cc_cns_prd_data
where
DISBURSED_DT between add_months('"""+end_dt+"""',-24) and '"""+end_dt+"""'
AND derived_dpd <= 180
AND DAS IN( 'S04', 'S05', 'S11', 'S12', 'S16', 'S18', 'S20' )
AND MFI_ID IN('CCC0000001','FRB0000013' ,'FRB0000004' ,'FRB0000006' ,'FRB0000010','PRB0000001' ,'PRB0000003' ,'PRB0000009' ,'PRB0000011' ,'PRB0000016' ,'PRB0000027' ,'PRB0000011')
GROUP BY 
last_day(disbursed_dt),
mfi_id
) A)B where MFI_ID='"""+self+"""'
ORDER BY
DISBURSED_DT desc)B
ON(A.DISBURSED_DT = B.DISBURSED_DT
AND A.PEER_GROUP = B.Lender_type)
""")

val tab2DF = spark.sql(s""" 
select 
'"""+mon_rn+"""' mon_rn,
b.fyq Financial_Qtr,
"""+peer_group+""" PEER_GROUP,
mob+1 mob,
sum(ACTIVE_CARDS_DPD180) ACTIVE_CARDS,
sum(loans_dpd_gtr_30)loans_dpd_gtr_30,
sum(loans_dpd_gtr_90) loans_dpd_gtr_90,
sum(total_active_pos) total_active_pos,
sum(pos_dpd_gtr_30) pos_dpd_gtr_30,
sum(pos_dpd_gtr_90) pos_dpd_gtr_90

from hmanalytics.hm_cc_data1 A
JOIN hmanalytics.hm_date_dim2 B
ON(cast(From_unixtime(Unix_timestamp(A.REPORTED_DT, 'yyyy-MM-dd'), 'yyyyMM') as int)  = B.yyyymm)
WHERE  cast(mob as int) IN (2,5,11)
GROUP BY
b.fyq ,
"""+peer_group+""",
mob
""")

tab2DF.write.mode("overwrite").saveAsTable("hmanalytics.hm_cc_tab2")


val tab3DF = spark.sql(s""" 
select
'"""+mon_rn+"""' mon_rn, 
reported_dt,
PEER_GROUP,
city,
TOTAL_ACTIVE_CARDS,
TOTAL_ACTIVE_POS,
AVERAGE_UTILIZATION_PERCENT,
AVERAGE_CREDIT_LIMIT
FROM
(
select 
reported_dt,
PEER_GROUP,
city,
TOTAL_ACTIVE_CARDS,
TOTAL_ACTIVE_POS,
AVERAGE_UTILIZATION_PERCENT,
AVERAGE_CREDIT_LIMIT,
ROW_NUMBER () OVER (PARTITION BY PEER_GROUP ORDER BY TOTAL_ACTIVE_CARDS desc) RW

FROM
(
select 
reported_dt,
"""+peer_group+""" PEER_GROUP,
city,
sum(ACTIVE_CARDS) TOTAL_ACTIVE_CARDS,
sum(TOTAL_ACTIVE_POS) TOTAL_ACTIVE_POS,
AVG(AVERAGE_UTILIZATION_PERCENT) AVERAGE_UTILIZATION_PERCENT,
AVG(AVERAGE_CREDIT_LIMIT) AVERAGE_CREDIT_LIMIT
FROM
hmanalytics.hm_cc_data1
 where city is not null
 AND reported_dt='"""+end_dt+"""'
GROUP BY
reported_dt,
"""+peer_group+""" ,
city
) B)C
WHERE RW <=50
""")

tab3DF.write.mode("overwrite").saveAsTable("hmanalytics.hm_cc_tab3")

val tab3_1DF = spark.sql(s""" 
select
'"""+mon_rn+"""' mon_rn,
reported_dt,
PEER_GROUP,
city,
TOTAL_ACTIVE_CARDS,
TOTAL_ACTIVE_POS,
AVERAGE_UTILIZATION_PERCENT,
AVERAGE_CREDIT_LIMIT
FROM
(
select 
reported_dt,
B.PEER_GROUP,
B.city,
TOTAL_ACTIVE_CARDS,
TOTAL_ACTIVE_POS,
AVERAGE_UTILIZATION_PERCENT,
AVERAGE_CREDIT_LIMIT,
ROW_NUMBER () OVER (PARTITION BY B.PEER_GROUP ORDER BY TOTAL_ACTIVE_CARDS desc) RW

FROM
(
select 
reported_dt,
"""+peer_group+""" PEER_GROUP,
city,
sum(ACTIVE_CARDS) TOTAL_ACTIVE_CARDS,
sum(TOTAL_ACTIVE_POS) TOTAL_ACTIVE_POS,
AVG(AVERAGE_UTILIZATION_PERCENT) AVERAGE_UTILIZATION_PERCENT,
AVG(AVERAGE_CREDIT_LIMIT) AVERAGE_CREDIT_LIMIT
FROM
hmanalytics.hm_cc_data1
 where city is not null
 AND reported_dt= add_months('"""+end_dt+"""',-12)
GROUP BY
reported_dt,
"""+peer_group+""",
city
) B JOIN (select PEER_GROUP,city from hmanalytics.hm_cc_tab3) D ON(B.PEER_GROUP = D.PEER_GROUP AND B.city = D.city) )C
WHERE RW <=50
""")

val tab4DF = spark.sql("""
select
'"""+mon_rn+"""' mon_rn,
REPORTED_DT,
"""+peer_group+""" PEER_GROUP,
sum(active_cards) active_cards,
sum(total_active_pos) total_active_pos

from hmanalytics.hm_cc_data1
GROUP BY
REPORTED_DT ,
"""+peer_group+"""
""")

val tab5DF = spark.sql("""
select '"""+mon_rn+"""' mon_rn,*
from(
select 
REPORTED_DT,
MFI_ID,
RANK() OVER (PARTITION BY REPORTED_DT ORDER BY TOTAL_CC_ACQUISTIONS desc ) AS Rank
from
(select
REPORTED_DT,
MFI_ID,
sum(active_cards) TOTAL_CC_ACQUISTIONS
FROM
hmanalytics.hm_cc_data1
where
MFI_ID IN('CCC0000001','FRB0000013' ,'FRB0000004' ,'FRB0000006' ,'FRB0000010','PRB0000001' ,'PRB0000003' ,'PRB0000009' ,'PRB0000011' ,'PRB0000016' ,'PRB0000027' ,'PRB0000011')
GROUP BY 
REPORTED_DT,
MFI_ID) A)B where MFI_ID='"""+self+"""'
ORDER BY
REPORTED_DT desc
""")

val tab6DF = spark.sql("""
select
'"""+mon_rn+"""' mon_rn, 
REPORTED_DT,
"""+peer_group+""" PEER_GROUP,
(SUM(LOW_UTILIZATION_LOAN)/SUM(ACTIVE_CARDS))*100 LOW_UTILIZATION_PERCENT,
avg(LOW_UTILIZATION_PF_PERCENT) LOW_UTILIZATION_PF_PERCENT,
avg(AVERAGE_LOW_CREDIT_LIMIT) AVERAGE_LOW_CREDIT_LIMIT
from
hmanalytics.hm_cc_data1
GROUP BY
REPORTED_DT ,
"""+peer_group+"""
""")

val tab7DF = spark.sql("""
select
'"""+mon_rn+"""' mon_rn, 
REPORTED_DT,
"""+peer_group+""" PEER_GROUP,
avg(ABS(A.loans_dpd_gtr_30/A.active_cards_dpd180)*100) loan_30_plus_percent,
avg(ABS(A.loans_dpd_gtr_60/A.active_cards_dpd180)*100)loan_60_plus_percent,
avg(ABS(A.loans_dpd_gtr_90/A.active_cards_dpd180)*100)loan_90_plus_percent,
avg(ABS(A.pos_dpd_gtr_30/A.total_active_pos_dpd180)*100)portfolio_30_plus_percent,
avg(ABS(A.pos_dpd_gtr_60/A.total_active_pos_dpd180)*100)portfolio_60_plus_percent,
avg(ABS(A.pos_dpd_gtr_90/A.total_active_pos_dpd180)*100)portfolio_90_plus_percent

from
hmanalytics.hm_cc_data1 A
GROUP BY
REPORTED_DT ,
"""+peer_group+"""
""")


val f1 = Future{
tab1DF.write.mode("overwrite").saveAsTable("hmanalytics.hm_cc_tab1")
   }

val f3 = Future{
tab3_1DF.write.mode("append").saveAsTable("hmanalytics.hm_cc_tab3")
  }
val f4 = Future{
tab4DF.write.mode("overwrite").saveAsTable("hmanalytics.hm_cc_tab4")
  }
val f5 = Future{
tab5DF.write.mode("overwrite").saveAsTable("hmanalytics.hm_cc_tab5")
  }
val f6 = Future{
tab6DF.write.mode("overwrite").saveAsTable("hmanalytics.hm_cc_tab6")
  }
val f7 = Future{
tab7DF.write.mode("overwrite").saveAsTable("hmanalytics.hm_cc_tab7")
  }
   Await.ready(f1, Duration.Inf)
   Await.ready(f3, Duration.Inf)
   Await.ready(f4, Duration.Inf)
   Await.ready(f5, Duration.Inf)
   Await.ready(f6, Duration.Inf)
   Await.ready(f7, Duration.Inf)
   
	/* Flow Rate */

val tab8step1DF = spark.sql("""
select
last_day(A.act_reported_dt) REPORTED_DT_FROM,
last_day(B.act_reported_dt) REPORTED_DT_TO,
A.ACCOUNT_KEY ACCOUNT_KEY,
A.MFI_ID MFI_ID,
CASE WHEN ( A.DERIVED_DPD = 0 OR A.DERIVED_DPD IS NULL ) THEN 'No Due'
WHEN  A.DERIVED_DPD BETWEEN  1 AND 30 THEN '1_30'
WHEN  A.DERIVED_DPD BETWEEN 31 AND 60 THEN '30_60'
WHEN  A.DERIVED_DPD BETWEEN 61 AND 90 THEN '60_90' 
WHEN  A.DERIVED_DPD BETWEEN 91 AND 180 THEN '90_180'  END  DPD_FROM,
CASE WHEN ( B.DERIVED_DPD = 0 OR B.DERIVED_DPD IS NULL ) THEN 'No Due'
WHEN  B.DERIVED_DPD BETWEEN 1 AND 30 THEN '1_30'
WHEN  B.DERIVED_DPD BETWEEN 31 AND 60 THEN '30_60'
WHEN  B.DERIVED_DPD BETWEEN 61 AND 90 THEN '60_90' 
WHEN  B.DERIVED_DPD BETWEEN 91 AND 180 THEN '90_180'  END DPD_TO,
A.current_balance current_balance_from,
B.current_balance current_balance_to,
A.sanctioned_amount sanctioned_amount_from,
B.sanctioned_amount sanctioned_amount_to
  FROM
hmanalytics.hm_cc_cns_trend_data A
LEFT OUTER JOIN hmanalytics.hm_cc_cns_trend_data B
ON(A.ACCOUNT_KEY = B.ACCOUNT_KEY
AND add_months(last_day(A.act_reported_dt),12) = last_day(B.act_reported_dt))
WHERE last_day(A.act_reported_dt) between add_months('"""+end_dt+"""',-12) and '"""+end_dt+"""'
AND  month(A.act_reported_dt) IN (3,6,9,12)
AND (B.NOT_CLOSED_IND = 'Y') AND B.DAS IN('S04', 'S05', 'S11', 'S12', 'S16', 'S18','S20')
AND (A.NOT_CLOSED_IND = 'Y') AND A.DAS IN('S04', 'S05', 'S11', 'S12', 'S16', 'S18','S20')
AND A.DERIVED_DPD <= 180
AND B.DERIVED_DPD <= 180
""")

tab8step1DF.write.mode("overwrite").saveAsTable("hmanalytics.hm_cc_flow_rate_base")

val tab8step2DF = spark.sql("""
Select * from
(select *, 
'No Due' NEW_DPD_FROM
from
hmanalytics.hm_cc_flow_rate_base
where DPD_FROM='No Due'

UNION All

select *,
'1+' NEW_DPD_FROM
from
hmanalytics.hm_cc_flow_rate_base
where DPD_FROM IN('1_30','30_60','60_90','90_180')

UNION All

select *,
'30+' NEW_DPD_FROM
from
hmanalytics.hm_cc_flow_rate_base
where DPD_FROM IN('30_60','60_90','90_180')

UNION All

select *,
'60+' NEW_DPD_FROM
from
hmanalytics.hm_cc_flow_rate_base
where DPD_FROM IN('60_90','90_180')

UNION All

select *,
'90+' NEW_DPD_FROM
from
hmanalytics.hm_cc_flow_rate_base
where DPD_FROM ='90_180') A
""")
tab8step2DF.createOrReplaceTempView("flowRateTemp")


val tab8step3DF = spark.sql("""
Select * from
(select *, 
'No Due' NEW_DPD_TO
from
flowRateTemp
where DPD_TO='No Due'

UNION All

select *,
'1+' NEW_DPD_TO
from
flowRateTemp
where DPD_TO IN('1_30','30_60','60_90','90_180')

UNION All

select *,
'30+' NEW_DPD_TO
from
flowRateTemp
where DPD_TO IN('30_60','60_90','90_180')

UNION All

select *,
'60+' NEW_DPD_TO
from
flowRateTemp
where DPD_TO IN('60_90','90_180')

UNION All

select *,
'90+' NEW_DPD_TO
from
flowRateTemp
where DPD_TO ='90_180') A
""")

tab8step3DF.createOrReplaceTempView("flowRateBasePlus")

val tab8step4DF = spark.sql("""
Select A.*
,count(A.account_key) over (partition by A.new_dpd_from,A.reported_dt_from,PEER_GROUP) total_dpd
,sum(A.current_balance_from) over (partition by A.new_dpd_from,A.reported_dt_from,PEER_GROUP) total_cb
from
(select 
reported_dt_from,
new_dpd_from,new_dpd_to,account_key,current_balance_from,current_balance_to
,"""+peer_group+""" PEER_GROUP
from flowRateBasePlus)A 
""")

tab8step4DF.write.mode("overwrite").saveAsTable("hmanalytics.hm_cc_flow_rate_base_sum")


val tab8DF = spark.sql("""
select '"""+mon_rn+"""' mon_rn,reported_dt_from,
PEER_GROUP,
new_dpd_from,total_dpd,total_cb ,new_dpd_to, count(account_key) ACTIVE_CARDS,sum(current_balance_to) current_balance_to,
round((count(account_key)/total_dpd)*100,2) loans_flow_rate_percent,
round((sum(current_balance_to)/total_cb)*100,2) portfolio_flow_rate_percent
from
hmanalytics.hm_cc_flow_rate_base_sum
group by 
reported_dt_from,PEER_GROUP,
new_dpd_from,total_dpd,total_cb,new_dpd_to
""")

tab8DF.write.mode("overwrite").saveAsTable("hmanalytics.hm_cc_tab8")
/* Share Of Wallet */

val tab9Step1DF = spark.sql("""
select
last_day(act_reported_dt) REPORTED_DT,
mfi_id ,
'T' data_ind,
max_clst_id clst_id,
account_key,
CURRENT_BALANCE
 
FROM
hmanalytics.hm_cc_cns_trend_data
where
last_day(act_reported_dt) IN (add_months('"""+end_dt+"""',-12),'"""+end_dt+"""')
AND (NOT_CLOSED_IND = 'Y') AND DAS IN( 'S04', 'S05', 'S11', 'S12', 'S16', 'S18', 'S20' )
""")

tab9Step1DF.write.mode("overwrite").saveAsTable("hmanalytics.hm_cc_sow_data")


val tab9Step2_1DF = spark.sql(s"""
select
cast(add_months('"""+end_dt+"""',-12) as date) REPORTED_DT,
mfi_id ,
'P' data_ind,
max_clst_id clst_id,
account_key,
CURRENT_BALANCE
FROM
hmanalytics.hm_cc_cns_prd_data
where
act_reported_dt <= add_months('"""+end_dt+"""',-12)
AND (NOT_CLOSED_IND = 'Y') AND DAS IN( 'S04', 'S05', 'S11', 'S12', 'S16', 'S18', 'S20' )
""")
tab9Step2_1DF.write.mode("append").saveAsTable("hmanalytics.hm_cc_sow_data")

val tab9Step2_2DF = spark.sql(s"""
select
cast('"""+end_dt+"""' as date) REPORTED_DT,
mfi_id ,
'P' data_ind,
max_clst_id clst_id,
account_key,
CURRENT_BALANCE
FROM
hmanalytics.hm_cc_cns_prd_data
where
act_reported_dt <= '"""+end_dt+"""'
AND (NOT_CLOSED_IND = 'Y') AND DAS IN( 'S04', 'S05', 'S11', 'S12', 'S16', 'S18', 'S20' )
""")
tab9Step2_2DF.write.mode("append").saveAsTable("hmanalytics.hm_cc_sow_data")

spark.sql("""truncate table hmanalytics.hm_cc_tab9""")
var stmt = "CCC0000001,FRB0000013,FRB0000004,FRB0000006,FRB0000010,PRB0000001,PRB0000003,PRB0000009,PRB0000011,PRB0000016,PRB0000027,PRB0000006"
var words=stmt.split(",")
for(word <- words){
val tab9DF = spark.sql(s"""
select 
'"""+mon_rn+"""' mon_rn
 ,A.REPORTED_DT as REPORTED_DT
 ,'"""+word+"""' mfi_id
 ,SUM(CURRENT_BALANCE) clst_base_cb
 ,SUM(CASE WHEN   mfi_id='"""+word+"""' THEN CURRENT_BALANCE ELSE 0 END ) bank_cb
 ,SUM(CASE WHEN   CURRENT_BALANCE < 1000 THEN CURRENT_BALANCE ELSE 0 END ) low_clst_base_cb
 ,SUM(CASE WHEN   CURRENT_BALANCE < 1000 and mfi_id='"""+word+"""' THEN CURRENT_BALANCE ELSE 0 END ) low_bank_cb
FROM
 hmanalytics.hm_cc_sow_data A
 JOIN ( select distinct clst_id,REPORTED_DT from hmanalytics.hm_cc_sow_data  where mfi_id='"""+word+"""') B
 ON(A.clst_id = B.clst_id
 AND A.REPORTED_DT = B.REPORTED_DT)
 GROUP BY
 A.REPORTED_DT
""")

tab9DF.write.mode("append").saveAsTable("hmanalytics.hm_cc_tab9")

}
   

   /* NTP,KTP .... */


spark.sql("""truncate table hmanalytics.hm_cc_all_base_data""")
start1 = start
do{
var tab10Step2DF = spark.sql(s"""
SELECT 'T' data_ind,
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
  A.MFI_ID, 
  LAST_DAY(cast(A.ACT_REPORTED_DT as date)) REPORTED_DT, 
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
    MONTHS_BETWEEN(LAST_DAY(A.ACT_REPORTED_DT),LAST_DAY(B.DISBURSED_DT)) MOB,
    B.DISBURSED_DT DISBURSED_DT
FROM 
  hmanalytics.hm_mfi_account_trend A 
  JOIN hmanalytics.hm_cns_account_prod  B ON(
    A.ACCOUNT_KEY = B.ACCOUNT_KEY 
    AND B.ACTIVE = 1 
    AND B.SUPPRESS_INDICATOR is null)
	WHERE A.act_reported_dt = last_day('"""+start1+"""')

UNION ALL

SELECT 'P' data_ind,
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
      MFI_ID, 
      last_day('"""+start1+"""') REPORTED_DT,
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
    MONTHS_BETWEEN(LAST_DAY(ACT_REPORTED_DT),LAST_DAY(DISBURSED_DT)) MOB,
      DISBURSED_DT DISBURSED_DT
    FROM 
      hmanalytics.hm_cns_account_prod 
 
    WHERE 
       ACTIVE = 1 
      AND SUPPRESS_INDICATOR is null
	  AND act_reported_dt <= last_day('"""+start1+"""')
""")

var temp = DateUtils.addMonths(dateFormat.parse(start1), 3)
start1 = dateFormat.format(temp)

val f1 = Future{
tab10Step2DF.repartition(500).write.mode("append").saveAsTable("hmanalytics.hm_cc_all_base_data")
}
      Await.ready(f1, Duration.Inf)

}while (start1 <= end )



val tab10Step3DF = spark.sql(s"""
select distinct A.*,clst_id from hmanalytics.hm_cc_all_base_data A
JOIN (select distinct account_key, clst_id from hmanalytics.hm_cns_cluster_unique_row) B
ON(A.ACCOUNT_KEY = B.ACCOUNT_KEY)
""")
tab10Step3DF.repartition(1000).write.mode("overwrite").saveAsTable("hmanalytics.hm_cc_all_base_clst_data")


val tab10Step4DF = spark.sql("""
select
last_day(act_reported_dt) REPORTED_DT,
'T' data_ind,
max_clst_id clst_id,
account_key
FROM
hmanalytics.hm_cc_cns_trend_data
where
     act_reported_dt between add_months('"""+end_dt+"""',-24) and '"""+end_dt+"""'
    AND month(act_reported_dt) IN (3,6,9,12) 
""")
tab10Step4DF.write.mode("overwrite").saveAsTable("hmanalytics.hm_cc_cust_prof_data")

start1 = start
do{
val tab10Step5DF = spark.sql(s"""
select
last_day('"""+start1+"""') REPORTED_DT,
'P' data_ind,
max_clst_id clst_id,
account_key
 FROM
hmanalytics.hm_cc_cns_prd_data
where
act_reported_dt <= last_day('"""+start1+"""')
""")
var temp = DateUtils.addMonths(dateFormat.parse(start1), 3)
start1 = dateFormat.format(temp)
val f1 = Future{
tab10Step5DF.write.mode("append").saveAsTable("hmanalytics.hm_cc_cust_prof_data")
}
      Await.ready(f1, Duration.Inf)

}while (start1 <= end )


val tab10Step6DF = spark.sql(s"""
select distinct A.* from hmanalytics.hm_cc_all_base_clst_data A
JOIN (select distinct clst_id from hmanalytics.hm_cc_cust_prof_data) B
ON(A.clst_id = B.clst_id)
""")
tab10Step6DF.repartition(1000).write.mode("overwrite").saveAsTable("hmanalytics.hm_cc_all_products_base")


val tab10Step7DF = spark.sql(s"""
select 
A.*
,count(*) OVER (PARTITION BY clst_id,REPORTED_DT ) RW
,count(*) OVER (PARTITION BY clst_id,REPORTED_DT,PEER_GROUP ) RW_PEER
,count(*) OVER (PARTITION BY clst_id,REPORTED_DT,ACCOUNT_TYPE ) RW_PRODUCT
FROM (
select REPORTED_DT,
"""+peer_group+""" PEER_GROUP ,
 clst_id,
account_key,
CURRENT_BALANCE,
disbursed_dt,
DAS,
ACCOUNT_TYPE,
NOT_CLOSED_IND
from 
hmanalytics.hm_cc_all_products_base) A
""")
tab10Step7DF.write.mode("overwrite").saveAsTable("hmanalytics.hm_cc_all_products_sbi_base")


val tab10Step8DF = spark.sql("""
select 
REPORTED_DT
,PEER_GROUP
,COUNT(CASE WHEN RW=1 THEN ACCOUNT_KEY END) NTC
,COUNT(CASE WHEN RW_PEER=1 THEN ACCOUNT_KEY END) NTB
,COUNT(CASE WHEN RW_PEER > 1 THEN ACCOUNT_KEY END) ETB
,COUNT(CASE WHEN ACCOUNT_TYPE='A15' AND RW_PRODUCT = 1 THEN ACCOUNT_KEY END) NTP
,COUNT(CASE WHEN ACCOUNT_TYPE='A15' AND RW_PRODUCT > 1 THEN ACCOUNT_KEY END) KTP

from
hmanalytics.hm_cc_all_products_sbi_base
GROUP BY 
REPORTED_DT
,PEER_GROUP
""")

tab10Step8DF.write.mode("overwrite").saveAsTable("hmanalytics.hm_cc_tab10")
   



  }
}