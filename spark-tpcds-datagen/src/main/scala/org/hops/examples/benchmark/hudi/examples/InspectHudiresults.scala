package org.hops.examples.benchmark.hudi.examples


import org.apache.hudi.QuickstartUtils._

import scala.collection.JavaConversions._
import org.apache.spark.sql.SaveMode._
import org.apache.hudi.DataSourceReadOptions._
import org.apache.hudi.DataSourceWriteOptions._
import org.apache.hudi.config.HoodieWriteConfig._
import org.apache.hudi.DataSourceReadOptions
import org.apache.hudi.DataSourceWriteOptions
import org.apache.hudi.HoodieDataSourceHelpers
import org.apache.hudi.common.model.HoodieTableType
import org.apache.hudi.config.HoodieWriteConfig
import org.apache.hudi.hive.MultiPartKeysValueExtractor
import org.apache.hudi.hive.NonPartitionedExtractor
import org.apache.log4j.LogManager
import org.apache.log4j.Logger
import org.apache.spark.api.java.JavaSparkContext
import org.apache.spark.sql.DataFrameWriter
import org.apache.spark.sql.Dataset
import org.apache.spark.sql.Row
import org.apache.spark.sql.SaveMode
import org.apache.spark.sql.SparkSession
import io.hops.util.Hops
import org.apache.spark.sql._
import org.apache.spark.sql.types._
import java.sql.Date
import java.sql.Timestamp

import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.hudi.common.table.timeline.HoodieInstant


object InspectHudiresults {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder.getOrCreate()
    val sc = spark.sparkContext
    import spark.implicits._

    val tableName = "hudi_trips_cow"
    val basePath = "file:///tmp/hudi_trips_cow" //s"hdfs:///Projects/${Hops.getProjectName}/Resources/hello_hudi_1"

    val beginTime = "000" // Represents all commits > this time.
    val endTime = ""//commits(commits.length - 2) // commit time we are interested in

//    val dataGen = new DataGenerator

    //Hudi Commits
    (FileSystem.get(sc.hadoopConfiguration)
      .listStatus(new Path(basePath))
      .map(_.getPath).foreach(println)
      )

    val tripsSnapshotDF = spark.
      read.
      format("hudi").
      load(basePath + "/*/*/*/*")
    //load(basePath) use "/partitionKey=partitionValue" folder structure for Spark auto partition discovery
    tripsSnapshotDF.createOrReplaceTempView("hudi_trips_snapshot")

    spark.sql("select fare, begin_lon, begin_lat, ts from  hudi_trips_snapshot where fare > 20.0").show()
    spark.sql("select _hoodie_commit_time, _hoodie_record_key, _hoodie_partition_path, rider, driver, fare from  hudi_trips_snapshot").show()


    spark.
      read.
      format("hudi").
      load(basePath + "/*/*/*/*").
      createOrReplaceTempView("hudi_trips_snapshot")


    //----------------------------------------------------------------------------------------------
    //Query the Hudi Dataset



    val hello_hudi_df = (spark.read.format("org.apache.hudi")
      .load(s"$basePath/*/*"))
    hello_hudi_df.registerTempTable("hello_hudi_df")
    spark.sql("describe hello_hudi_df").show()

    // Inspect the updated commit timeline
    HoodieDataSourceHelpers.latestCommit(FileSystem.get(sc.hadoopConfiguration), basePath)


    val timeline = HoodieDataSourceHelpers.allCompletedCommitsCompactions(FileSystem.get(sc.hadoopConfiguration),
      basePath)

    val firstTimestamp = timeline.firstInstant.get.getTimestamp
    val secondTimestamp = timeline.nthInstant(1).get.getTimestamp

    HoodieDataSourceHelpers.listCommitsSince(FileSystem.get(sc.hadoopConfiguration),
      basePath, firstTimestamp)

    HoodieDataSourceHelpers.hasNewCommits(FileSystem.get(sc.hadoopConfiguration),
      basePath, firstTimestamp)

    val latestTimestamp = HoodieDataSourceHelpers.latestCommit(FileSystem.get(sc.hadoopConfiguration),
      basePath)

    HoodieDataSourceHelpers.hasNewCommits(FileSystem.get(sc.hadoopConfiguration),
      basePath, latestTimestamp)
    //----------------------------------------------------------------------------------------------

    // Time Travel
    //Incremental query

//    // Pull changes that include both commits (from 2017):
//    val incrementalDf_b = spark.read.format("org.apache.hudi")
//      .option("hoodie.datasource.view.type", "incremental")
//      .option("hoodie.datasource.read.begin.instanttime", "20170830115554") // or firstTimestamp
//      .load(basePath)
//    incrementalDf_b.registerTempTable("incremental_df_b")

    // Pull changes that happened *after* the the particilar timestamp commit
    val incrementalDF = spark.read.format("hudi").
      option(QUERY_TYPE_OPT_KEY, QUERY_TYPE_INCREMENTAL_OPT_VAL).
      option(BEGIN_INSTANTTIME_OPT_KEY, beginTime).
      load(basePath)
    incrementalDF.createOrReplaceTempView("hudi_incrementall_df")

    //    val commits = spark.sql("select distinct(_hoodie_commit_time) as commitTime from  hudi_trips_snapshot order by commitTime").map(k => k.getString(0)).collect()//.take(50)
    //    val beginTime = commits(0) // commit time we are interested in
    spark.sql("select `_hoodie_commit_time`, fare, begin_lon, begin_lat, ts from  hudi_incrementall_df where fare > 20.0").show()
    spark.sql(s"select `_hoodie_commit_time`, fare, begin_lon, begin_lat, ts from  hudi_incrementall_df _hoodie_commit_time=$firstTimestamp").show()
    spark.sql(s"select `_hoodie_commit_time`, fare, begin_lon, begin_lat, ts from  hudi_incrementall_df _hoodie_commit_time=$secondTimestamp").show()
    //------------------------------------------------------------------------------------------

    //Point in time query
    //incrementally query data
    val hudiPointInTimeDF = spark.read.format("hudi").
      option(QUERY_TYPE_OPT_KEY, QUERY_TYPE_INCREMENTAL_OPT_VAL).
      option(BEGIN_INSTANTTIME_OPT_KEY, beginTime).
      option(END_INSTANTTIME_OPT_KEY, endTime).
      load(basePath)
    hudiPointInTimeDF.createOrReplaceTempView("hudi_point_in_time_df")
    spark.sql("select `_hoodie_commit_time`, fare, begin_lon, begin_lat, ts from hudi_point_in_time_df where fare > 20.0").show()

//    //Delete data
//    // fetch total records count
//    spark.sql("select uuid, partitionpath from hudi_trips_snapshot").count()
//    // fetch two records to be deleted
//    val ds = spark.sql("select uuid, partitionpath from hudi_trips_snapshot").limit(2)
//
//    // issue deletes
//    val deletes = dataGen.generateDeletes(ds.collectAsList())
//    val df = spark
//      .read
//      .json(spark.sparkContext.parallelize(deletes, 2))
//
//    df
//      .write
//      .format("hudi")
//      .options(getQuickstartWriteConfigs)
//      .option(OPERATION_OPT_KEY,"delete")
//      .option(PRECOMBINE_FIELD_OPT_KEY, "ts")
//      .option(RECORDKEY_FIELD_OPT_KEY, "uuid")
//      .option(PARTITIONPATH_FIELD_OPT_KEY, "partitionpath")
//      .option(TABLE_NAME, tableName)
//      .mode(Append)
//      .save(basePath)
//
//    // run the same read query as above.
//    val roAfterDeleteViewDF = spark
//      .read
//      .format("hudi")
//      .load(basePath + "/*/*/*/*")
//
//    roAfterDeleteViewDF.registerTempTable("hudi_trips_snapshot")
//    // fetch should return (total - 2) records
//    spark.sql("select uuid, partitionpath from hudi_trips_snapshot").count()

  }

}
