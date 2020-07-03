package org.hops.examples.benchmark.hudi.examples

import org.apache.hadoop.fs.FileSystem;
import org.apache.hudi.DataSourceReadOptions;
import org.apache.hudi.DataSourceWriteOptions;
import org.apache.hudi.HoodieDataSourceHelpers;
import org.apache.hudi.common.model.HoodieTableType;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.hive.MultiPartKeysValueExtractor;
import org.apache.hudi.hive.NonPartitionedExtractor;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.DataFrameWriter;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;
import io.hops.util.Hops
import org.apache.spark.sql._
import org.apache.spark.sql.types._
import java.sql.Date
import java.sql.Timestamp
import org.apache.hadoop.fs.{FileSystem, Path}

object InspectHudiresults {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder.getOrCreate()
    val sc = spark.sparkContext

    (FileSystem.get(sc.hadoopConfiguration)
      .listStatus(new Path(s"hdfs:///Projects/${Hops.getProjectName}/Resources/hello_hudi_1"))
      .map(_.getPath).foreach(println)
      )

    HoodieDataSourceHelpers.latestCommit(FileSystem.get(sc.hadoopConfiguration),
      s"hdfs:///Projects/${Hops.getProjectName}/Resources/hello_hudi_1")

    HoodieDataSourceHelpers.allCompletedCommitsCompactions(FileSystem.get(sc.hadoopConfiguration),
      s"hdfs:///Projects/${Hops.getProjectName}/Resources/hello_hudi_1").toString


    val hello_hudi_df = (spark.read.format("org.apache.hudi")
      .load(s"hdfs:///Projects/${Hops.getProjectName}/Resources/hello_hudi_1/*/*"))

    hello_hudi_df.registerTempTable("hello_hudi_df")
    spark.sql("describe hello_hudi_df").show()

    // Inspect the updated commit timeline
    HoodieDataSourceHelpers.latestCommit(FileSystem.get(sc.hadoopConfiguration),
      s"hdfs:///Projects/${Hops.getProjectName}/Resources/hello_hudi_1")

    HoodieDataSourceHelpers.allCompletedCommitsCompactions(FileSystem.get(sc.hadoopConfiguration),
      s"hdfs:///Projects/${Hops.getProjectName}/Resources/hello_hudi_1").toString

    val timeline = HoodieDataSourceHelpers.allCompletedCommitsCompactions(FileSystem.get(sc.hadoopConfiguration),
      s"hdfs:///Projects/${Hops.getProjectName}/Resources/hello_hudi_1")
    val firstTimestamp = timeline.firstInstant.get.getTimestamp
    val secondTimestamp = timeline.nthInstant(1).get.getTimestamp

    // Time Travel
    spark.sql(s"select id, value, date, country from hello_hudi_1 where _hoodie_commit_time=$firstTimestamp").show()
    spark.sql(s"select id, value, date, country from hello_hudi_1 where _hoodie_commit_time=$secondTimestamp").show()


    // Pull changes that happened *after* the first commit
    val incrementalDf_a = spark.read.format("org.apache.hudi")
      .option("hoodie.datasource.view.type", "incremental")
      .option("hoodie.datasource.read.begin.instanttime", firstTimestamp)
      .load(s"hdfs:///Projects/${Hops.getProjectName}/Resources/hello_hudi_1")

    incrementalDf_a.registerTempTable("incremental_df_a")
    spark.sql("select id, value, date, country from incremental_df").show(20)

    // Pull changes that include both commits (from 2017):
    val incrementalDf_b = spark.read.format("org.apache.hudi")
      .option("hoodie.datasource.view.type", "incremental")
      .option("hoodie.datasource.read.begin.instanttime", "20170830115554")
      .load(s"hdfs:///Projects/${Hops.getProjectName}/Resources/hello_hudi_1")
    incrementalDf_b.registerTempTable("incremental_df_b")
    spark.sql("select id, value, date, country from incremental_df").show(20)

  }

}
