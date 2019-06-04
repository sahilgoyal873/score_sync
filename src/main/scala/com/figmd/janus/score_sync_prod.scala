package com.figmd.janus

import java.util.Calendar
import java.util.logging.{Level, Logger}

import com.datastax.driver.core._
import com.figmd.janus.util.{GCSUtility, PostgreUtility, SparkUtility}
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}
import org.apache.spark.storage.StorageLevel

object score_sync_prod {

  var cassHostName = ""
  var inDir = ""
  var keyspace_prod = ""
  var keyspace_clean = ""
  var bucket_name = ""
  var repartition = 200

  def main(args: Array[String]) {
    try {

      val start_date = Calendar.getInstance.getTime

      Logger.getLogger("org").setLevel(Level.OFF)
      Logger.getLogger("akka").setLevel(Level.OFF)
      Logger.getLogger("spark-log4j").setLevel(Level.OFF)

      println("[" + Calendar.getInstance().getTime() + "] " + "JOB STARTED")

      GCSUtility.readS3ConfigFile(args)

      val reg_name = GCSUtility.prop.getProperty("registry_name")
      inDir = GCSUtility.prop.getProperty("backupdir_name")
      cassHostName = GCSUtility.prop.getProperty("cassandra_host")
      keyspace_prod = GCSUtility.prop.getProperty("dashboard_keyspace")
      keyspace_clean = GCSUtility.prop.getProperty("execution_keyspace")
      bucket_name = GCSUtility.prop.getProperty("bucket_name")
      repartition = GCSUtility.prop.getProperty("repartition").toInt

      val spark = SparkUtility.getSparkSession(cassHostName)

      var measures = args(1)
      var practiceid = args(2)

      import spark.implicits._;

      if(practiceid == "NA") {
        println("\ngetting all practices from "+keyspace_clean)

        practiceid = spark.read.format("org.apache.spark.sql.cassandra").options(Map("table" -> "summary_details_by_practice", "keyspace" -> keyspace_clean)).load()
          .select("practice_id").distinct().map(x=> x.toString()).collect().toList.mkString(",")
          .replace("[","")
          .replace("]","")
          .replace(" ","")


      }

      if(measures == "NA") {
        println("\ngetting all measures from "+keyspace_clean)

        measures = spark.read.format("org.apache.spark.sql.cassandra").options(Map("table" -> "summary_details_by_measure", "keyspace" -> keyspace_clean)).load()
          .select("measure_id").distinct().map(x=> x.toString()).collect().toList.mkString(",")
          .replace("[","")
          .replace("]","")
          .replace(" ","")


      }


      println("syncing data for practices : "+practiceid)
      println("syncing data for measures  : "+measures)

      val measure_list = "'" + measures.replace(",", "','") + "'"


      val tablenames = "summary_details_by_practice,summary_details_by_provider,summary_details_by_location,summary_details_by_measure".split(",")
      //val tablenames = "summary_details_by_provider,summary_details_by_location,summary_details_by_measure".split(",")

      println("Registry : " + reg_name)

      tablenames.foreach(table => {
        cleanScoreCard(spark, reg_name, table, measure_list, practiceid)

      })

      println("Start Time  : " + start_date)
      println("End Time    : " + Calendar.getInstance.getTime)

    }
    catch {
      case e: Exception => {
        println("[" + Calendar.getInstance().getTime() + "] " + e.printStackTrace())
        System.exit(-1)
      }
    }
  }

  def cleanScoreCard(spark: SparkSession, registry: String, tableName: String, measureList: String, practiceList: String): Unit = {

    println("\n\n---------------------------------  Starting table : " + tableName + "----------------------------------------\n\n")

    val prod_bck_save_path = s"${bucket_name}/cassandra/backup/clean_scorecard/" + inDir
    val prod_clean_save_path = s"${bucket_name}/cassandra/prod_sync/clean_scorecard/" + inDir

    alterScoreCardCassandraTable(registry, keyspace_prod, tableName)

    val df = spark.read.format("org.apache.spark.sql.cassandra").options(Map("table" -> tableName, "keyspace" -> keyspace_prod)).load().filter(col("midname").isNull || !col("midname").contains("\u0017"))
      .select("practice_id", "measure_id", "visit_dt", "unit", "visit_uid", "created_dt", "datasource", "denom", "denom_exception", "denom_exclusion", "dob", "firstname", "gender", "initial_population", "lastname", "location_id", "midname", "mrn", "notmet", "num", "num_exception", "num_exclusion", "patient_uid", "provider_id", "result", "ssn", "updated_dt")
    df.persist(StorageLevel.MEMORY_AND_DISK_SER)
    println("prod full count                       :: " + df.count)
    df.createOrReplaceTempView(tableName)

    df.write.mode(SaveMode.Overwrite).option("header", "true").option("delimiter", "~").csv(s"gs://$prod_bck_save_path/$tableName/")

    val df1 = spark.read.format("org.apache.spark.sql.cassandra").options(Map("table" -> tableName, "keyspace" -> keyspace_clean)).load().filter(col("midname").isNull || !col("midname").contains("\u0017"))
      .select("practice_id", "measure_id", "visit_dt", "unit", "visit_uid", "created_dt", "datasource", "denom", "denom_exception", "denom_exclusion", "dob", "firstname", "gender", "initial_population", "lastname", "location_id", "midname", "mrn", "notmet", "num", "num_exception", "num_exclusion", "patient_uid", "provider_id", "result", "ssn", "updated_dt")
    df1.persist(StorageLevel.MEMORY_AND_DISK_SER)
    df1.createOrReplaceTempView(tableName + "_etl4")


    println(s"select count(1) from ${tableName}_etl4 where practice_id in ($practiceList) and measure_id in ($measureList)")
    println(s"select count(1) from $tableName where  practice_id in ($practiceList) and measure_id in ($measureList)")

    val addVisits = spark.sql(s"select 1 from ${tableName}_etl4 where practice_id in ($practiceList) and measure_id in ($measureList)").count
    val deleteVisits = spark.sql(s"select 1 from $tableName where  practice_id in ($practiceList) and measure_id in ($measureList)").count
    val delete = spark.sql(s"select * from $tableName where  practice_id in ($practiceList) and measure_id in ($measureList)")

    var clean = spark.sqlContext.emptyDataFrame

    if (deleteVisits > 0) {
      clean = df.except(delete)
    } else
      clean = df

    println("visits to be removed from  prod       :: " + deleteVisits)
    println("visits to be added in  prod           :: " + addVisits)
    println("visits after removal from prod        :: " + clean.count)

    import org.apache.spark.sql.functions._

    val add = spark.sql(s"select * from ${tableName}_etl4 where  practice_id in ($practiceList) and measure_id in ($measureList)")
    val finalDF = clean.union(add).filter(col("midname").isNull || !col("midname").contains("\u0017"))

    finalDF.persist(StorageLevel.MEMORY_AND_DISK_SER)

    println("visits after add of clean records prod :: " + finalDF.count)

    finalDF.write.mode(SaveMode.Overwrite).option("header", "true").option("delimiter", "~").csv(s"gs://$prod_clean_save_path/$tableName/")

    loadCleanData(spark, keyspace_prod, tableName, finalDF)

    if (tableName == "summary_details_by_practice") PostgreUtility.updatePostgresdates(spark, finalDF.where("practice_id in ("+practiceList+")"))

    println("\n\n---------------------------------  End table : " + tableName + "----------------------------------------\n\n")
  }

  def loadCleanData(spark: SparkSession, registry: String, tableName: String, cleanDF: DataFrame): Unit = {

    cleanDF.write.mode(SaveMode.Overwrite)
      .option("confirm.truncate", "true")
      .format("org.apache.spark.sql.cassandra")
      .options(Map("table" -> tableName, "keyspace" -> registry))
      .save()


  }

  def alterScoreCardCassandraTable(registry: String, keyspace: String, tableName: String): Unit = {

    executeCassandraQuery(registry + "_dm", s"CREATE KEYSPACE IF NOT EXISTS $keyspace WITH REPLICATION = { 'class' : 'SimpleStrategy', 'replication_factor' : 1 };")

    /*var schema = s"create table IF NOT EXISTS $keyspace.$tableName (" +
      "practice_id int, location_id int, provider_id text, patient_uid text, visit_uid text, " +
      "visit_dt timestamp, measure_id text, dob date, firstname text, midname text, lastname text, " +
      "gender text, mrn text, ssn text, initial_population int, num int, num_exception int, num_exclusion int, denom int," +
      " denom_exception int, denom_exclusion int, notmet int, created_dt timestamp,updated_dt timestamp, result float,unit text, "

    if (tableName.equalsIgnoreCase("summary_details_by_practice"))
      schema = schema + "PRIMARY KEY (practice_id,measure_id,visit_dt,unit,visit_uid))"
    else if (tableName.equalsIgnoreCase("summary_details_by_provider"))
      schema = schema + "PRIMARY KEY (provider_id,practice_id,measure_id, visit_dt, unit, visit_uid))"
    else if (tableName.equalsIgnoreCase("summary_details_by_location"))
      schema = schema + "PRIMARY KEY (location_id,practice_id, measure_id, visit_dt,unit,visit_uid))"
    else if (tableName.equalsIgnoreCase("summary_details_by_measure"))
      schema = schema + "PRIMARY KEY (measure_id,visit_dt,practice_id,unit,visit_uid))"*/

    var schema = s"alter table  $keyspace.$tableName add (datasource int);"


    executeCassandraQuery(keyspace, schema)
  }

  def executeCassandraQuery(keyspace: String, query: String): Unit = {
    try {
      val cluster = Cluster.builder
        .addContactPoint(cassHostName)
        .withSocketOptions(new SocketOptions().setConnectTimeoutMillis(60000000))
        .withQueryOptions(new QueryOptions().setConsistencyLevel(ConsistencyLevel.LOCAL_ONE))
        .build
      val session = cluster.connect(keyspace)

      System.out.println("Cassandra Query Executed:::" + query)

      if (query != "")
        session.execute(query)
      else
        null

      session.close()
      cluster.close()
    }
    catch {
      case e: Exception => {
        println("[" + Calendar.getInstance().getTime() + "] " + e.printStackTrace())
      }
    }
  }

  def cleanWTScoreCard(spark: SparkSession, registry: String, tableName: String, measureList: String, practiceList: String): Unit = {

    println("\n\n---------------------------------  Starting table : " + tableName + "----------------------------------------\n\n")


    val keyspace_clean = "wt_scorecard_" + registry.toLowerCase
    val prod_clean_save_path = s"prod_cass_backup/${registry}/WT_clean_scorecard/" + inDir
    val prod_clean_table_path = s"gs://prod_cass_backup/${registry}/WT_clean_scorecard/" + inDir + "/" + tableName + "/"

    val structure = score_sync.selectSchema(tableName)

    val df = spark.read.option("delimiter", "~").schema(structure).csv(prod_clean_table_path).filter(col("midname").isNull || !col("midname").contains("\u0017")).repartition(repartition)
    println("prod full count                       :: " + df.count)
    df.createOrReplaceTempView(tableName)

    val df1 = spark.read.format("org.apache.spark.sql.cassandra").options(Map("table" -> tableName, "keyspace" -> keyspace_clean)).load().filter(col("midname").isNull || !col("midname").contains("\u0017")).repartition(repartition)
    df1.createOrReplaceTempView(tableName + "_etl3")


    println(s"select count(1) from ${tableName}_etl3 where practice_id in ($practiceList) and measure_id in ($measureList)")
    println(s"select count(1) from $tableName where  practice_id in ($practiceList) and measure_id in ($measureList)")

    val addVisits = spark.sql(s"select 1 from ${tableName}_etl3 where practice_id in ($practiceList) and measure_id in ($measureList)").count
    val deleteVisits = spark.sql(s"select 1 from $tableName where  practice_id in ($practiceList) and measure_id in ($measureList)").count
    val delete = spark.sql(s"select * from $tableName where  practice_id in ($practiceList) and measure_id in ($measureList)")

    var clean = spark.sqlContext.emptyDataFrame

    if (deleteVisits > 0) {
      clean = df.except(delete)
    } else
      clean = df

    println("visits to be removed from  prod       :: " + deleteVisits)
    println("visits to be added in  prod           :: " + addVisits)
    println("visits after removal from prod        :: " + clean.count)

    import org.apache.spark.sql.functions._

    val add = spark.sql(s"select * from ${tableName}_etl3 where  practice_id in ($practiceList) and measure_id in ($measureList)")
    val finalDF = clean.union(add).filter(col("midname").isNull || !col("midname").contains("\u0017"))

    finalDF.persist(StorageLevel.MEMORY_AND_DISK_SER)

    println("visits after add of clean records prod :: " + finalDF.count)

    //finalDF.coalesce(1).write.mode(SaveMode.Overwrite).option("delimiter", "~").csv(s"gs://$prod_clean_save_path/$tableName/")

    loadCleanData(spark, registry, tableName, finalDF)

    println("\n\n---------------------------------  End table : " + tableName + "----------------------------------------\n\n")
  }

}
