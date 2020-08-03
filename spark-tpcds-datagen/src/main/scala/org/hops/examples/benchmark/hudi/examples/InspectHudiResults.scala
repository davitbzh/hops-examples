package org.hops.examples.benchmark.hudi.examples

import io.hops.util.Hops
import org.apache.hudi.DataSourceReadOptions._
import org.apache.hudi.HoodieDataSourceHelpers
import org.apache.spark.sql.SparkSession
import org.apache.hadoop.fs.{FileSystem, Path}


object InspectHudiResults {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder.getOrCreate()
    val sc = spark.sparkContext
    import spark.implicits._

    val whatToDo = args(0)
    val beginTime = args(1).toString   //"000" // Represents all commits > this time.
    val endTime = args(2)     //commits(commits.length - 2) // commit time we are interested in
    val basePath = args(3)    //s"hdfs:///Projects/${Hops.getProjectName}/Resources/hello_hudi_1"


//    val tableName = args(0)

    if (whatToDo.equals("list_files")) {
      //Explore Hudi Table
      println("*" * 80)
      (FileSystem.get(sc.hadoopConfiguration)
        .listStatus(new Path(basePath))
        .map(_.getPath).foreach(println)
        )
      println("*" * 80)

    } else if (whatToDo.equals("show_table")) {
      var hudiSnapshotDF = spark.sqlContext.emptyDataFrame

      try {
        hudiSnapshotDF = spark.
          read.
          format("org.apache.hudi").
          load(basePath + "/*/*/")
      } catch {
        case e: org.apache.spark.sql.AnalysisException => hudiSnapshotDF = spark.
          read.
          format("org.apache.hudi").
          load(basePath + "/*")
      }

      //load(basePath) use "/partitionKey=partitionValue" folder structure for Spark auto partition discovery
      hudiSnapshotDF.createOrReplaceTempView("hudi_snapshot_df")

//      spark.sql("SELECT * FROM hudi_snapshot_df").show()
      println("*" * 80)
      hudiSnapshotDF.show()
      println("*" * 80)
    }

//    spark.sql("select fare, begin_lon, begin_lat, ts from  hudi_trips_snapshot where fare > 20.0").show()
//    spark.sql("select _hoodie_commit_time, _hoodie_record_key, _hoodie_partition_path, rider, driver, fare from  hudi_trips_snapshot").show()
//
//    spark.
//      read.
//      format("hudi").
//      load(basePath + "/*/*/*/*").
//      createOrReplaceTempView("hudi_trips_snapshot")

    else if (whatToDo.equals("commit_timeline")) {
      //----------------------------------------------------------------------------------------------
      //Query the Hudi Dataset
      // Inspect the updated commit timeline
      val timeline = HoodieDataSourceHelpers.allCompletedCommitsCompactions(FileSystem.get(sc.hadoopConfiguration),
        basePath)

      var commit_timestampes = scala.collection.mutable.Map[String, String]()
      for (i <- 0 until timeline.countInstants()) {
        commit_timestampes += (i.toString -> timeline.nthInstant(i).get.getTimestamp)
      }

      println("*" * 80)
      println(commit_timestampes)
      println("*" * 80)
    }

//    val firstTimestamp = timeline.firstInstant.get.getTimestamp
//    val secondTimestamp = timeline.nthInstant(1).get.getTimestamp
//
//    HoodieDataSourceHelpers.listCommitsSince(FileSystem.get(sc.hadoopConfiguration),
//      basePath, firstTimestamp)
//
//    HoodieDataSourceHelpers.hasNewCommits(FileSystem.get(sc.hadoopConfiguration),
//      basePath, firstTimestamp)
//
//    val latestTimestamp = HoodieDataSourceHelpers.latestCommit(FileSystem.get(sc.hadoopConfiguration),
//      basePath)
//
//    HoodieDataSourceHelpers.hasNewCommits(FileSystem.get(sc.hadoopConfiguration),
//      basePath, latestTimestamp)
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
    else if (whatToDo.equals("incremental_query_after")) {
      val incrementalDF = spark.read.format("org.apache.hudi").
        option(QUERY_TYPE_OPT_KEY, QUERY_TYPE_INCREMENTAL_OPT_VAL).
        option(BEGIN_INSTANTTIME_OPT_KEY, beginTime).
        load(basePath)
      incrementalDF.createOrReplaceTempView("hudi_incrementall_df")
      println("*" * 80)
      spark.sql("hudi_snapshot_df").show()
      println("*" * 80)
    }
//    val commits = spark.sql("select distinct(_hoodie_commit_time) as commitTime from  hudi_trips_snapshot order by commitTime").map(k => k.getString(0)).collect()//.take(50)
//    val beginTime = commits(0) // commit time we are interested in

//    spark.sql("select `_hoodie_commit_time`, fare, begin_lon, begin_lat, ts from  hudi_incrementall_df where fare > 20.0").show()
//    spark.sql(s"select `_hoodie_commit_time`, fare, begin_lon, begin_lat, ts from  hudi_incrementall_df _hoodie_commit_time=$firstTimestamp").show()
//    spark.sql(s"select `_hoodie_commit_time`, fare, begin_lon, begin_lat, ts from  hudi_incrementall_df _hoodie_commit_time=$secondTimestamp").show()
    //------------------------------------------------------------------------------------------

    //Point in time query
    //incrementally query data
    else if (whatToDo.equals("incremental_query_after")) {

      val hudiPointInTimeDF = spark.read.format("org.apache.hudi").
        option(QUERY_TYPE_OPT_KEY, QUERY_TYPE_INCREMENTAL_OPT_VAL).
        option(BEGIN_INSTANTTIME_OPT_KEY, beginTime).
        option(END_INSTANTTIME_OPT_KEY, endTime).
        load(basePath)

      hudiPointInTimeDF.createOrReplaceTempView("hudi_point_in_time_df")

      println("*" * 80)
      hudiPointInTimeDF.show()
      println("*" * 80)
    }

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

    else if (whatToDo.equals("count_rows_default")) {


      var hudi_table_count = scala.collection.mutable.Map[String, Long]()

      val hudi_tables = FileSystem.get(sc.hadoopConfiguration)
        .listStatus(new Path(s"hdfs:///Projects/${Hops.getProjectName}/hoodie/"))

      for (table_dict <- hudi_tables){
        val table_path = table_dict.getPath

        var hudiSnapshotDF = spark.sqlContext.emptyDataFrame

        try {
          hudiSnapshotDF = spark.
            read.
            format("org.apache.hudi").
            load(basePath + "/*/*/")
        } catch {
          case e: org.apache.spark.sql.AnalysisException => hudiSnapshotDF = spark.
            read.
            format("org.apache.hudi").
            load(basePath + "/*")
        }

        hudi_table_count += ( table_path.toString -> hudiSnapshotDF.count())

      }

      val df = hudi_table_count.toSeq.toDF("name", "count")
      df
        .coalesce(1)
        .write.format("com.databricks.spark.csv")
        .option("header", "true")
        .save(s"hdfs:///Projects/${Hops.getProjectName}/Logs/hudi/hudi_counts.csv")

    }
  }
}
