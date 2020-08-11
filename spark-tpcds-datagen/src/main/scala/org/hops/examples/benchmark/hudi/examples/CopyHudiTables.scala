package org.hops.examples.benchmark.hudi.examples

import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.{FloatType, IntegerType, LongType, StructField}
import org.apache.spark.sql.functions.col
object CopyHudiTables {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder.getOrCreate()
    val sc = spark.sparkContext
    import spark.implicits._

    val dataLocation = args(0)
    val outLocation = args(1)

    val tables = Seq("catalog_page", "catalog_returns", "customer", "customer_address",
      "customer_demographics", "date_dim", "household_demographics", "inventory", "item",
      "promotion", "store", "store_returns", "catalog_sales", "web_sales", "store_sales",
      "web_returns", "web_site", "reason", "call_center", "warehouse", "ship_mode", "income_band",
      "time_dim", "web_page")



    for (tableName <- tables){
      var hudiSnapshotDF = spark.sqlContext.emptyDataFrame

      try {
        hudiSnapshotDF = spark.
          read.
          format("org.apache.hudi").
          load(s"$dataLocation/$tableName" + "/*/*/")
      } catch {
        case e: org.apache.spark.sql.AnalysisException =>
          hudiSnapshotDF = spark.read.
          format("org.apache.hudi").
          load(s"$dataLocation/$tableName" + "/*")
      }



      val Array(training, test) = hudiSnapshotDF.randomSplit(Array(0.9, 0.2))

      var result = spark.sqlContext.emptyDataFrame
      result = test

      for (f <- result.schema.fields) {

        if(f.dataType.isInstanceOf[IntegerType]){

          result = result.withColumn(f.name, col(f.name) * 2)

        } else if (f.dataType.isInstanceOf[LongType]){

          result = result.withColumn(f.name, col(f.name) * 2)

        } else if (f.dataType.isInstanceOf[FloatType]){

          result = result.withColumn(f.name, col(f.name) * 2.0)

        }

      }

      result.repartition(1).write.mode("overwrite").parquet(s"$outLocation/$tableName")

    }
  }
}

// hdfs:///Projects/benchmark/benchmark_Training_Datasets/tpcds hdfs:///Projects/benchmark/benchmark_Training_Datasets/tmp_tpcds

//********************************************************************************
//hdfs://rpc.namenode.service.consul:8020/Projects/hudi_tpcds_benchmarks/hudi_tpcds_benchmarks_Training_Datasets/test/call_center
//********************************************************************************
//********************************************************************************
//hdfs://rpc.namenode.service.consul:8020/Projects/hudi_tpcds_benchmarks/hudi_tpcds_benchmarks_Training_Datasets/test/catalog_page
//********************************************************************************

