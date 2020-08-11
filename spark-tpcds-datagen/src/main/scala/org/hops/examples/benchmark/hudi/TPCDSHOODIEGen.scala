package org.hops.examples.benchmark.hudi

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.execution.benchmark.{Tables}

object TPCDSHOODIEGen {

  def main(args: Array[String]): Unit = {
    //--scale-factor 5 --num-partitions 5 --hoodie-storage-type "COPY_ON_WRITE" --hoodie-operation "bulk_insert" --hoodie-save-mode "overwrite" --hive-ip-adderss "hiveserver.hive.service.consul" --base-path hdfs:///Projects/benchmark/benchmark_Training_Datasets/tpcds
    //--scale-factor 5 --num-partitions 5 --hoodie-storage-type COPY_ON_WRITE --hoodie-operation upsert --hoodie-save-mode append --hive-ip-adderss hiveserver.hive.service.consul --base-path hdfs:///Projects/benchmark/benchmark_Training_Datasets/tpcds

    val datagenArgs = new TPCDSHOODIEDatagenArguments(args)
    val spark = SparkSession.builder.getOrCreate()

    val tpcdsTables = new Tables(spark, datagenArgs.scaleFactor.toInt)

    tpcdsTables.genHoodieData(
      datagenArgs.format,
      datagenArgs.hoodieStorageType,
      datagenArgs.hoodieOperation,
      datagenArgs.hoodieSaveMode,
      datagenArgs.hiveIpadderss,
      datagenArgs.filterOutNullPartitionValues,
      datagenArgs.tableFilter,
      datagenArgs.numPartitions.toInt,
      datagenArgs.basePath
    )
  }
}
