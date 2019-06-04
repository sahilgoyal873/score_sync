package com.figmd.janus.util

import java.sql.{Connection, DriverManager, PreparedStatement, Timestamp}

import org.apache.commons.net.ntp.TimeStamp
import org.apache.spark.sql.functions.max
import org.apache.spark.sql.{DataFrame, SparkSession}

object PostgreUtility extends Serializable {

  var wf_id=GCSUtility.prop.getProperty("wf_id")

  def postgresConnect(): Connection ={

    val url = "jdbc:postgresql://"+GCSUtility.prop.getProperty("postgresHostName")+":"+GCSUtility.prop.getProperty("postgresHostPort")+"/"+GCSUtility.prop.getProperty("postgresMgmtDatabaseName") +"?tcpKeepAlive=true&socketTimeout=0&connectTimeout=0"
    val driver = "org.postgresql.Driver"
    val username = GCSUtility.prop.getProperty("postgresHostUserName")
    val password = GCSUtility.prop.getProperty("postgresUserPass")

    Class.forName(driver)
    return DriverManager.getConnection(url, username, password)
 }

  def insertIntoProcessDetails(con: Connection, practice_id: Int, end_date: Timestamp): Unit = {
    try {
      val pstatement = con.prepareStatement("update practicerefreshstatus set enddate=? , lastrefreshtime=now()  where practiceid=?")
      pstatement.setTimestamp(1, end_date)
      pstatement.setInt(2, practice_id)

      println(pstatement.toString)

      pstatement.executeUpdate()
    }
    catch {
      case e: Exception => println("\npostgres practicerefreshstatus connection ERROR..." + e.printStackTrace())
    }
  }

  def updatePostgresdates(spark: SparkSession, cleanDF: DataFrame): Unit = {

    try {

      val connection: Connection = postgresConnect()

      val out = cleanDF.select("practice_id", "visit_dt").distinct().groupBy("practice_id").agg(max("visit_dt")).collect()

      out.foreach(r => {

        println(r.toString())

        insertIntoProcessDetails(connection, r.getInt(0), r.getTimestamp(1))
      }
      )


      connection.close()
    }
    catch {
      case e: Exception => println("\npostgres process_details_stg connection ERROR..." + e.printStackTrace())
    }
  }


}
