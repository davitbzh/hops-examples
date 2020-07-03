package org.hops.examples.benchmark.hudi

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.execution.benchmark.{Tables}

object TPCDSHOODIEGen {

  def main(args: Array[String]): Unit = {
    //--scale-factor 20 --num-partitions 100 --hoodie-storage-type "COPY_ON_WRITE" --hoodie-operation "bulk_insert" --hoodie-save-mode "overwrite" --hive-ip-adderss "hiveserver.hive.service.consul"
    //--scale-factor 10 --num-partitions 50 --hoodie-storage-type COPY_ON_WRITE --hoodie-operation upsert --hoodie-save-mode append --hive-ip-adderss hiveserver.hive.service.consul

    val datagenArgs = new TPCDSHOODIEDatagenArguments(args)
    val spark = SparkSession.builder.getOrCreate()

    val tpcdsTables = new Tables(spark.sqlContext, datagenArgs.scaleFactor.toInt)

    tpcdsTables.genHoodieData(
      datagenArgs.format,
      datagenArgs.hoodieStorageType,
      datagenArgs.hoodieOperation,
      datagenArgs.hoodieSaveMode,
      datagenArgs.hiveIpadderss,
      datagenArgs.filterOutNullPartitionValues,
      datagenArgs.tableFilter,
      datagenArgs.numPartitions.toInt
    )
  }
}
