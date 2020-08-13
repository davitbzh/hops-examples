package org.hops.examples.benchmark.hudi

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.execution.benchmark.Tables

object TPCDSupsertLoop {

  def main(args: Array[String]): Unit = {

    //--times-upserts 10 --scale-factor 5 --num-partitions 5 --base-path hdfs:///Projects/benchmark/benchmark_Training_Datasets/tpcds

    val datagenArgs = new TPCDSHOODIEDatagenArguments(args)
    val spark = SparkSession.builder.getOrCreate()

    val tpcdsTables = new Tables(spark, datagenArgs.scaleFactor.toInt)

    1 until 10

    for (_ <- 1 until datagenArgs.timesUpserts.toInt){

      tpcdsTables.genHoodieData(
        datagenArgs.format,
        "COPY_ON_WRITE",
        "upsert",
        "append",
        "hiveserver.hive.service.consul",
        datagenArgs.filterOutNullPartitionValues,
        datagenArgs.tableFilter,
        datagenArgs.numPartitions.toInt,
        datagenArgs.basePath
      )

    }


  }
}
