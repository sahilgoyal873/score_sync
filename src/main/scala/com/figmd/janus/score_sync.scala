package com.figmd.janus

import java.sql.{Connection, DriverManager}
import java.util.Calendar
import java.util.logging.{Level, Logger}

import com.datastax.driver.core._
import com.figmd.janus.util.SparkUtility
import org.apache.spark.sql.functions.{col, _}
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}
import org.apache.spark.storage.StorageLevel

object score_sync {

  var cassHostName = ""
  var inDir = ""

  def main(args: Array[String]) {
    try {

      val start_date = Calendar.getInstance.getTime

      Logger.getLogger("org").setLevel(Level.OFF)
      Logger.getLogger("akka").setLevel(Level.OFF)
      Logger.getLogger("spark-log4j").setLevel(Level.OFF)

      println("[" + Calendar.getInstance().getTime() + "] " + "JOB STARTED")

      val reg_name = args(0)
      inDir = args(1)
      cassHostName = args(2)
      val measure_list = args(3)
      val practiceid = args(4)

      val spark = SparkUtility.getSparkSession(cassHostName)
      //val measure_list = "'QPP238_2','QPP130','QPP402','QPP226_3','QPP431','QPP226_1','QPP226_2','QPP134','QPP238_1','ECQM128V7_1','QPP128','ECQM161V7','QPP383'"
      //val practiceid = "91,551,649,476,439,373,332,423,105,538,88,465,416,370,642,650,72,204,43,552,283,397,2,322,218,177,84,411,74,215,432,69,22,484,75,186,59,289,190,636,150,295,454,611,156,615,494,442,388,424,276,589,48,65,576,285,66,34"
      val tablenames = "summary_details_by_practice,summary_details_by_provider,summary_details_by_location,summary_details_by_measure".split(",")
      //val tablenames = "summary_details_by_practice,summary_details_by_location".split(",")

      println("Registry : " + reg_name)

      tablenames.foreach(table => {
        cleanScoreCard(spark, reg_name, table, measure_list, practiceid)

      })

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

    val prod_keyspace = registry.toLowerCase + "_prod_sync"
    // change
    val keyspace_clean = registry.toLowerCase + "_scorecard_etl3"
    val prod_clean_save_path = s"prod_cass_backup/${registry}/clean_scorecard/" + inDir
    val prod_clean_table_path = s"gs://prod_cass_backup/${registry}/clean_scorecard/" + inDir + "/" + tableName + "/"

    val structure = selectSchema(tableName)

    val df = spark.read.option("delimiter", "~").schema(structure).csv(prod_clean_table_path).filter(col("midname").isNull || !col("midname").contains("\u0017")).repartition(20005)
    println("prod full count                       :: " + df.count)
    df.createOrReplaceTempView(tableName)

    val df1 = spark.read.format("org.apache.spark.sql.cassandra").options(Map("table" -> tableName, "keyspace" -> keyspace_clean)).load().filter(col("midname").isNull || !col("midname").contains("\u0017")).repartition(20005)
    //println("et3 full count                        :: " + df1.count)
    df1.createOrReplaceTempView(tableName + "_etl3")


    println(s"select count(1) from ${tableName}_etl3 where practice_id in ($practiceList) and measure_id in ($measureList)")
    println(s"select count(1) from $tableName where  practice_id in ($practiceList) and measure_id in ($measureList)")

    var clean = spark.sqlContext.emptyDataFrame

    val deleteVisits = spark.sql(s"select 1 from $tableName where  practice_id in ($practiceList) and measure_id in ($measureList)").count
    val delete = spark.sql(s"select * from $tableName where  practice_id in ($practiceList) and measure_id in ($measureList)")


    println("visits to be removed from  prod       :: " + deleteVisits)

    if (deleteVisits > 0) {
      clean = df.except(delete)
    } else
      clean = df

/*
    val addVisits = spark.sql(s"select 1 from ${tableName}_etl3 where practice_id in ($practiceList) and measure_id in ($measureList)").count



    println("visits to be added in  prod           :: " + addVisits)
    println("visits after removal from prod        :: " + clean.count)*/

    import org.apache.spark.sql.functions._

    val add = spark.sql(s"select * from ${tableName}_etl3 where  practice_id in ($practiceList) and measure_id in ($measureList)")
    val finalDF = clean.union(add).filter(col("midname").isNull || !col("midname").contains("\u0017"))

    finalDF.persist(StorageLevel.MEMORY_AND_DISK_SER)

    println("visits after add of clean records prod :: " + finalDF.count)

    finalDF.coalesce(1).write.mode(SaveMode.Overwrite).option("delimiter", "~").csv(s"gs://$prod_clean_save_path/$tableName/")

    loadCleanData(spark, registry, tableName, finalDF)

    println("\n\n---------------------------------  End table : " + tableName + "----------------------------------------\n\n")
  }



  def cleanWTScoreCard(spark: SparkSession, registry: String, tableName: String, measureList: String, practiceList: String): Unit = {

    println("\n\n---------------------------------  Starting table : " + tableName + "----------------------------------------\n\n")

    val prod_keyspace = registry.toLowerCase + "_prod_sync"
    // change
    val keyspace_clean = "wt_scorecard_" + registry.toLowerCase
    val prod_clean_save_path = s"prod_cass_backup/${registry}/WT_clean_scorecard/" + inDir
    val prod_clean_table_path = s"gs://prod_cass_backup/${registry}/WT_clean_scorecard/" + inDir + "/" + tableName + "/"

    val structure = selectSchema(tableName)

    val df = spark.read.option("delimiter", "~").schema(structure).csv(prod_clean_table_path).filter(col("midname").isNull || !col("midname").contains("\u0017"))
    println("prod full count                       :: " + df.count)
    df.createOrReplaceTempView(tableName)

    val df1 = spark.read.format("org.apache.spark.sql.cassandra").options(Map("table" -> tableName, "keyspace" -> keyspace_clean)).load().filter(col("midname").isNull || !col("midname").contains("\u0017"))
    //println("et3 full count                        :: " + df1.count)
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

    finalDF.coalesce(1).write.mode(SaveMode.Overwrite).option("delimiter", "~").csv(s"gs://$prod_clean_save_path/$tableName/")

    loadCleanData(spark, registry, tableName, finalDF)

    println("\n\n---------------------------------  End table : " + tableName + "----------------------------------------\n\n")
  }

  def selectSchema(tableName: String): StructType = {

    tableName match {
      case "summary_details_by_practice" => StructType(
        List(
          StructField("practice_id", IntegerType, true),
          StructField("measure_id", StringType, true),
          StructField("visit_dt", StringType, true),
          StructField("unit", StringType, true),
          StructField("visit_uid", StringType, true),
          StructField("created_dt", StringType, true),
          StructField("denom", StringType, true),
          StructField("denom_exception", StringType, true),
          StructField("denom_exclusion", StringType, true),
          StructField("dob", StringType, true),
          StructField("firstname", StringType, true),
          StructField("gender", StringType, true),
          StructField("initial_population", StringType, true),
          StructField("lastname", StringType, true),
          StructField("location_id", StringType, true),
          StructField("midname", StringType, true),
          StructField("mrn", StringType, true),
          StructField("notmet", StringType, true),
          StructField("num", StringType, true),
          StructField("num_exception", StringType, true),
          StructField("num_exclusion", StringType, true),
          StructField("patient_uid", StringType, true),
          StructField("provider_id", StringType, true),
          StructField("result", StringType, true),
          StructField("ssn", StringType, true),
          StructField("updated_dt", StringType, true)
        ))

      case "summary_details_by_location" => StructType(
        List(
          StructField("location_id", StringType, true),
          StructField("practice_id", IntegerType, true),
          StructField("measure_id", StringType, true),
          StructField("visit_dt", StringType, true),
          StructField("unit", StringType, true),
          StructField("visit_uid", StringType, true),
          StructField("created_dt", StringType, true),
          StructField("denom", StringType, true),
          StructField("denom_exception", StringType, true),
          StructField("denom_exclusion", StringType, true),
          StructField("dob", StringType, true),
          StructField("firstname", StringType, true),
          StructField("gender", StringType, true),
          StructField("initial_population", StringType, true),
          StructField("lastname", StringType, true),
          StructField("midname", StringType, true),
          StructField("mrn", StringType, true),
          StructField("notmet", StringType, true),
          StructField("num", StringType, true),
          StructField("num_exception", StringType, true),
          StructField("num_exclusion", StringType, true),
          StructField("patient_uid", StringType, true),
          StructField("provider_id", StringType, true),
          StructField("result", StringType, true),
          StructField("ssn", StringType, true),
          StructField("updated_dt", StringType, true)
        ))

      case "summary_details_by_measure" => StructType(
        List(
          StructField("measure_id", StringType, true),
          StructField("visit_dt", StringType, true),
          StructField("practice_id", IntegerType, true),
          StructField("unit", StringType, true),
          StructField("visit_uid", StringType, true),
          StructField("created_dt", StringType, true),
          StructField("denom", StringType, true),
          StructField("denom_exception", StringType, true),
          StructField("denom_exclusion", StringType, true),
          StructField("dob", StringType, true),
          StructField("firstname", StringType, true),
          StructField("gender", StringType, true),
          StructField("initial_population", StringType, true),
          StructField("lastname", StringType, true),
          StructField("location_id", StringType, true),
          StructField("midname", StringType, true),
          StructField("mrn", StringType, true),
          StructField("notmet", StringType, true),
          StructField("num", StringType, true),
          StructField("num_exception", StringType, true),
          StructField("num_exclusion", StringType, true),
          StructField("patient_uid", StringType, true),
          StructField("provider_id", StringType, true),
          StructField("result", StringType, true),
          StructField("ssn", StringType, true),
          StructField("updated_dt", StringType, true)
        ))

      case "summary_details_by_provider" => StructType(
        List(
          StructField("provider_id", StringType, true),
          StructField("practice_id", IntegerType, true),
          StructField("measure_id", StringType, true),
          StructField("visit_dt", StringType, true),
          StructField("unit", StringType, true),
          StructField("visit_uid", StringType, true),
          StructField("created_dt", StringType, true),
          StructField("denom", StringType, true),
          StructField("denom_exception", StringType, true),
          StructField("denom_exclusion", StringType, true),
          StructField("dob", StringType, true),
          StructField("firstname", StringType, true),
          StructField("gender", StringType, true),
          StructField("initial_population", StringType, true),
          StructField("lastname", StringType, true),
          StructField("location_id", StringType, true),
          StructField("midname", StringType, true),
          StructField("mrn", StringType, true),
          StructField("notmet", StringType, true),
          StructField("num", StringType, true),
          StructField("num_exception", StringType, true),
          StructField("num_exclusion", StringType, true),
          StructField("patient_uid", StringType, true),
          StructField("result", StringType, true),
          StructField("ssn", StringType, true),
          StructField("updated_dt", StringType, true)
        ))
    }

  }

  def loadCleanData(spark: SparkSession, registry: String, tableName: String, cleanDF: DataFrame): Unit = {

    val cleanKeyspace = registry.toLowerCase + "_prod_clean"

    createScoreCardCassandraTable(registry, cleanKeyspace, tableName)
    val structure = selectSchema(tableName)

    /*val cleanDF = spark.read.option("delimiter", "|").schema(structure).csv(read_clean_path)
        .filter($"practice_id".isNotNull)*/

    //cleanDF.show()
    //cleanDF.printSchema()

    cleanDF.write.mode(SaveMode.Overwrite)
      .option("confirm.truncate", "true")
      .format("org.apache.spark.sql.cassandra")
      .options(Map("table" -> tableName, "keyspace" -> cleanKeyspace))
      .save()


  }

  def createScoreCardCassandraTable(registry: String, keyspace: String, tableName: String): Unit = {

    executeCassandraQuery(registry + "_dm", s"CREATE KEYSPACE IF NOT EXISTS $keyspace WITH REPLICATION = { 'class' : 'SimpleStrategy', 'replication_factor' : 1 };")

    var schema = s"create table IF NOT EXISTS $keyspace.$tableName (" +
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
      schema = schema + "PRIMARY KEY (measure_id,visit_dt,practice_id,unit,visit_uid))"
    executeCassandraQuery(keyspace, schema)
  }

  def executeCassandraQuery(keyspace: String, query: String): Unit = {
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


}
