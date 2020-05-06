package org.hops.examples.benchmark.hudi

import io.hops.util.Hops
import org.apache.spark.sql.SparkSession

import org.apache.log4j.Logger
import org.apache.spark.sql.SparkSession
import io.hops.util.Hops
import scala.collection.JavaConversions._

object BechmarkTables2FeatureStore {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder.config(conf).getOrCreate()
    import spark.implicits._

    val name = args(0)
    val input = args(1)
    val pk = args(2)
    val df = spark.read.parquet(input)

    Hops.createFeaturegroup(name).setDataframe(df)
      .setDescription(s"Features from $name").setPrimaryKey(List(pk)).write

  }

}
