package org.hops.examples.benchmark.hudi.examples

import org.apache.spark.sql.SparkSession

object PrintTables {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder.getOrCreate()

    val df = spark.read.parquet(args(0))

    df.select("Name", "Runtime")
      .coalesce(1)
      .write.format("com.databricks.spark.csv")
      .option("header", "true")
      .save(args(1) + ".csv")


    df.coalesce(1)
      .write
      .json(args(1) + ".json")
  }

}
