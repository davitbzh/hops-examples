package com.databricks.spark.sql.perf

import org.apache.hadoop.fs.FileSystem
import org.apache.hadoop.fs.Path
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

import scala.util.control.NonFatal


object Main {

  def main(args: Array[String]): Unit = {


    // Setup Spark
    val conf: SparkConf = new SparkConf()
    val spark = SparkSession.builder.config(conf).getOrCreate()

//    // Database to be used:
//    // TPCDS Scale factor
    val scaleFactor = "1"
//    // If false, float type will be used instead of decimal.
//    val useDecimal = true
//    // If false, string type will be used instead of date.
//    val useDate = true
//
//    val filterNull = false
//    // name of database to be used.
//    val databaseName = s"tpcds_sf${scaleFactor}" +
//      s"""_${if (useDecimal) "with" else "no"}decimal""" +
//      s"""_${if (useDate) "with" else "no"}date""" +
//      s"""_${if (filterNull) "no" else "with"}nulls"""
//

    val dataLocation = args(0)
    val resultLocation = args(1)
    val iterations = args(2).toInt // how many times to run the whole set of queries.



    val timeout = 60 // timeout in hours

    val query_filter = Seq("q72-v1.4", "q64-v1.4", "q80-v1.4", "q95-v1.4", "q14b-v1.4") // Seq() == all queries
    //val query_filter = Seq("q6-v2.4", "q5-v2.4") // run subset of queries
    val randomizeQueries = false // run queries in a random order. Recommended for parallel runs.

    // Spark configuration
    spark.conf.set("spark.sql.broadcastTimeout", "10000") // good idea for Q14, Q88.

    // ... + any other configuration tuning

//    spark.sql(s"use $databaseName")

    //------------------------------------------------------------------------------------------------------
    val tables = Seq("catalog_page", "catalog_returns", "customer", "customer_address",
      "customer_demographics", "date_dim", "household_demographics", "inventory", "item",
      "promotion", "store", "store_returns", "catalog_sales", "web_sales", "store_sales",
      "web_returns", "web_site", "reason", "call_center", "warehouse", "ship_mode", "income_band",
      "time_dim", "web_page")
//    val tables = Seq(   "store_sales",
//                        "item",
//                        "date_dim",
//                        "catalog_sales",
//                        "inventory",
//                        "warehouse",
//                        "customer_demographics",
//                        "household_demographics",
//                        "promotion",
//                        "catalog_returns",
//                        "web_sales",
//                        "customer_address",
//                        "web_site"
//    )

    def setupTables(dataLocation: String): Map[String, Long] = {
      tables.map { tableName =>
        spark.read.parquet(s"$dataLocation/$tableName").createOrReplaceTempView(tableName)
        tableName -> spark.table(tableName).count()
      }.toMap
    }

    setupTables(dataLocation)


    //------------------------------------------------------------------------------------------------------

    import com.databricks.spark.sql.perf.tpcds.TPCDS

    val tpcds = new TPCDS (sqlContext = spark.sqlContext)
    def queries = {
      val filtered_queries = query_filter match {
        case Seq() => tpcds.tpcds1_4Queries //tpcds2_4Queries # FIXME (davit)
        case _ => tpcds.tpcds1_4Queries.filter(q => query_filter.contains(q.name)) //tpcds2_4Queries FIXME (davit)
      }
      if (randomizeQueries) scala.util.Random.shuffle(filtered_queries) else filtered_queries
    }
    val experiment = tpcds.runExperiment(
      queries,
      iterations = iterations,
      resultLocation = resultLocation,
      tags = Map("runtype" -> "benchmark", "database" -> "databaseName_change_this", "scale_factor" -> scaleFactor)
    )

    println(experiment.toString)
    experiment.waitForFinish(timeout*60*60)

    import org.apache.spark.sql.functions.{col, lit, substring}
    val summary = experiment.getCurrentResults
      .withColumn("Name", substring(col("name"), 2, 100))
      .withColumn("Runtime", (col("parsingTime") + col("analysisTime") + col("optimizationTime") + col("planningTime") + col("executionTime")) / 1000.0)
//      .select("Name", "Runtime")

    val timestamp = System.currentTimeMillis()
    val resultPath = s"$resultLocation/timestamp=$timestamp.parquet"

    try {
      experiment.logMessage(s"Results written to table: 'sqlPerformance' at $resultPath")
      summary
        .coalesce(1)
        .write
        .format("parquet")
        .save(resultPath)
    } catch {
      case NonFatal(e) =>
        experiment.logMessage(s"Failed to write data: $e")
        throw e
    }

  }
}

