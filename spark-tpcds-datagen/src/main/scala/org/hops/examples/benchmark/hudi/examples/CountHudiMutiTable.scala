package org.hops.examples.benchmark.hudi.examples

import io.hops.util.Hops
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.StructType

object CountHudiMutiTable {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder.getOrCreate()
    val sc = spark.sparkContext
    import spark.implicits._

    var hudi_table_count = scala.collection.mutable.Map[String, Long]()

    val hudi_tables = FileSystem.get(sc.hadoopConfiguration)
      .listStatus(new Path(args(0)))

    for (table_dict <- hudi_tables){

      val table_path = table_dict.getPath

      println("*" * 80)
      println(table_path)
      println("*" * 80)

      var hudiSnapshotDF = spark.sqlContext.emptyDataFrame

      try {
        hudiSnapshotDF = spark.
          read.
          format("org.apache.hudi").
          load(table_path + "/*/*/")
      } catch {
        case e: org.apache.spark.sql.AnalysisException => hudiSnapshotDF = spark.
          read.
          format("org.apache.hudi").
          load(table_path + "/*")
      }

      hudi_table_count += ( table_path.toString -> hudiSnapshotDF.count())

    }

    val df = hudi_table_count.toSeq.toDF("name", "count")

    df
      .coalesce(1)
      .write.format("com.databricks.spark.csv")
      .option("header", "true")

      .save(args(1))

  }

}


//********************************************************************************
//hdfs://rpc.namenode.service.consul:8020/Projects/hudi_tpcds_benchmarks/hudi_tpcds_benchmarks_Training_Datasets/test/call_center
//********************************************************************************
//********************************************************************************
//hdfs://rpc.namenode.service.consul:8020/Projects/hudi_tpcds_benchmarks/hudi_tpcds_benchmarks_Training_Datasets/test/catalog_page
//********************************************************************************
