package org.hops.examples.benchmark.hudi

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.execution.benchmark.TPCDSQueryBenchmark.conf

object BulkInsert {

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder.config(conf).getOrCreate()

    val catalog_sales = spark.read.parquet("hdfs:///Projects/tf2/tf2_Training_Datasets/TPCDS/catalog_sales")
    val catalog_returns = spark.read.parquet("hdfs:///Projects/tf2/tf2_Training_Datasets/TPCDS/catalog_returns")
    val inventory = spark.read.parquet("hdfs:///Projects/tf2/tf2_Training_Datasets/TPCDS/inventory")
    val store_returns = spark.read.parquet("hdfs:///Projects/tf2/tf2_Training_Datasets/TPCDS/store_returns")
    val web_sales = spark.read.parquet("hdfs:///Projects/tf2/tf2_Training_Datasets/TPCDS/web_sales")
    val web_returns = spark.read.parquet("hdfs:///Projects/tf2/tf2_Training_Datasets/TPCDS/web_returns")
    val call_center = spark.read.parquet("hdfs:///Projects/tf2/tf2_Training_Datasets/TPCDS/call_center")
    val catalog_page = spark.read.parquet("hdfs:///Projects/tf2/tf2_Training_Datasets/TPCDS/catalog_page")
    val customer = spark.read.parquet("hdfs:///Projects/tf2/tf2_Training_Datasets/TPCDS/customer")
    val customer_address = spark.read.parquet("hdfs:///Projects/tf2/tf2_Training_Datasets/TPCDS/customer_address")
    val customer_demographics = spark.read.parquet("hdfs:///Projects/tf2/tf2_Training_Datasets/TPCDS/customer_demographics")
    val household_demographics = spark.read.parquet("hdfs:///Projects/tf2/tf2_Training_Datasets/TPCDS/household_demographics")
    val income_band = spark.read.parquet("hdfs:///Projects/tf2/tf2_Training_Datasets/TPCDS/income_band")
    val item = spark.read.parquet("hdfs:///Projects/tf2/tf2_Training_Datasets/TPCDS/item")
    val promotion = spark.read.parquet("hdfs:///Projects/tf2/tf2_Training_Datasets/TPCDS/promotion")
    val reason = spark.read.parquet("hdfs:///Projects/tf2/tf2_Training_Datasets/TPCDS/reason")
    val ship_mode = spark.read.parquet("hdfs:///Projects/tf2/tf2_Training_Datasets/TPCDS/ship_mode")
    val store = spark.read.parquet("hdfs:///Projects/tf2/tf2_Training_Datasets/TPCDS/store")
    val time_dim = spark.read.parquet("hdfs:///Projects/tf2/tf2_Training_Datasets/TPCDS/time_dim")
    val warehous = spark.read.parquet("hdfs:///Projects/tf2/tf2_Training_Datasets/TPCDS/warehouse")
    val web_page = spark.read.parquet("hdfs:///Projects/tf2/tf2_Training_Datasets/TPCDS/web_page")
    val web_site = spark.read.parquet("hdfs:///Projects/tf2/tf2_Training_Datasets/TPCDS/web_site")

  }

}
