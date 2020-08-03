package org.hops.examples.benchmark.hudi.examples

import org.apache.spark.sql.SparkSession

object Count {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder.getOrCreate()

    val df = spark.
      read.
      format("org.apache.hudi").
      load(args(0) + "/*/*")


    println("*" * 80)
    println(df.count())
    println("*" * 80)

  }

}

//********************************************************************************
// scale factor 2
// 2880058
//********************************************************************************

//********************************************************************************
// scale factor 500
// 720015053
//********************************************************************************

