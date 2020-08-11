package org.apache.spark.sql.execution.benchmark

import org.apache.spark.sql.SparkSession

object TPCDSDataGen {

  def main(args: Array[String]): Unit = {
    val datagenArgs = new TPCDSDatagenArguments(args)
    val spark = SparkSession.builder.getOrCreate()
    val tpcdsTables = new Tables(spark, datagenArgs.scaleFactor.toInt)

    tpcdsTables.genData(
      datagenArgs.outputLocation,
      datagenArgs.format,
      datagenArgs.overwrite,
      datagenArgs.partitionTables,
      datagenArgs.useDoubleForDecimal,
      datagenArgs.clusterByPartitionColumns,
      datagenArgs.filterOutNullPartitionValues,
      datagenArgs.tableFilter,
      datagenArgs.numPartitions.toInt
    )
  }

}
