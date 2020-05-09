package org.hops.examples.benchmark.hudi

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.execution.benchmark.{Tables}

object TPCDSHOODIEGen {

  def main(args: Array[String]): Unit = {
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
