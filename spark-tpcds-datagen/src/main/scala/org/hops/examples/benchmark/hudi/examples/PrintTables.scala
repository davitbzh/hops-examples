package org.hops.examples.benchmark.hudi.examples

import org.apache.spark.sql.SparkSession

object PrintTables {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder.getOrCreate()

    val df = spark.read.parquet(args(0))

    df.select("Name","mode", "mlResult", "benchmarkId", "Runtime").show(1000, false)
    println("-----------------------------------")

  }

}
