package com.crif.highmark.CrifSemantic.DataLoad
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object clusterTableIngestion extends App{
  val spark = SparkSession.builder().appName("MfiDataIngestion")
.config("hive.metastore.uris", "thrift://chmcisprbdmn01.chm.intra:9083")
.config("hive.exec.dynamic.partition", "true")
.config("hive.exec.dynamic.partition.mode", "nonstrict")
.config("hive.enforce.bucketing", "true")
.config("hive.exec.max.dynamic.partitions", "20000")
.enableHiveSupport().getOrCreate()

val mon_rn = spark.sparkContext.getConf.get("spark.driver.args")


val cnsClustDF = spark.read.format("jdbc") .option("url", "jdbc:oracle:thin:@172.16.5.2:1521:CHM01")  .option("dbtable", "(SELECT  CLST_ID,CANDIDATE_ID,MOD(CANDIDATE_ID,10) as part FROM HMCORE.HM_INQ_MATCH_INVOL_P1_R02) a")  .option("user", "HMANALYTICS") .option("password", "HM#2019")  .option("driver", "oracle.jdbc.driver.OracleDriver").option("numPartitions", 10) .option("partitionColumn", "part").option("lowerBound", 0).option("upperBound", 9).option("fetchSize","10000").load()
val maxCLstDF = cnsClustDF.distinct.groupBy(col("CANDIDATE_ID")).agg(max(col("CLST_ID")).as("MAX_CLST_ID"))
maxCLstDF.drop(col("PART")).repartition(500).write.mode("overwrite").saveAsTable("hmanalytics.hm_cns_clst_"+mon_rn+"")




val cmlClustDF = spark.read.format("jdbc") .option("url", "jdbc:oracle:thin:@172.16.5.2:1521:CHM01")  .option("dbtable", "(SELECT  CLST_ID,CANDIDATE_ID,MOD(CANDIDATE_ID,10) as part FROM HMCORE.HM_INQ_MATCH_INVOL_P1_R10) a")  .option("user", "HMANALYTICS") .option("password", "HM#2019")  .option("driver", "oracle.jdbc.driver.OracleDriver").option("numPartitions", 10) .option("partitionColumn", "part").option("lowerBound", 0).option("upperBound", 9).option("fetchSize","10000").load()
val maxcmlCLstDF = cmlClustDF.distinct.groupBy(col("CANDIDATE_ID")).agg(max(col("CLST_ID")).as("MAX_CLST_ID"))
maxcmlCLstDF.drop(col("PART")).repartition(500).write.mode("overwrite").saveAsTable("hmanalytics.hm_cml_clst_"+mon_rn+"")

}