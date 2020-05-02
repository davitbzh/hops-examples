package org.hops.examples.benchmark.hudi

import org.apache.spark.sql.execution.benchmark.TPCDSQueryBenchmark.conf

import org.apache.spark.sql.SparkSession;
import io.hops.util.Hops

object HoodieOp {

  def main(args: Array[String]): Unit = {

    val parquetPath = args(0)
    val hoodieTableName = args(1)
    val partitionField = args(2)
    val fssyncTable = args(3)
    val hoodieStorageType = args(4)
    val hoodieOperation = args(5)
    val ipadderss = args(6)
    val saveTablePath = args(7)
    val hoodieSaveMode = args(8)
    val primaryKey = args(8)

    val trustStore = Hops.getTrustStore
    val pw = Hops.getKeystorePwd
    val keyStore = Hops.getKeyStore
    val hiveDb = Hops.getProjectFeaturestore.read
    val jdbcUrl = (s"jdbc:hive2://$ipadderss:9085/$hiveDb;"
      + s"auth=noSasl;ssl=true;twoWay=true;sslTrustStore=$trustStore;"
      + s"trustStorePassword=$pw;sslKeyStore=$keyStore;keyStorePassword=$pw"
      )

    val spark = SparkSession.builder.config(conf).getOrCreate()
    import spark.implicits._

    val upsertDf = spark.read.parquet(parquetPath)

//    upsertDf.queryExecution.logical.collect()

    val writer = (upsertDf.write.format("org.apache.hudi")
      .option("hoodie.table.name", hoodieTableName)
      .option("hoodie.datasource.write.storage.type", hoodieStorageType) //"COPY_ON_WRITE"
      .option("hoodie.datasource.write.operation", hoodieOperation) //"upsert" or "bulk_insert"
      .option("hoodie.datasource.write.recordkey.field",primaryKey)
      .option("hoodie.datasource.write.partitionpath.field", partitionField) // date
      .option("hoodie.datasource.write.precombine.field", partitionField) // date
      .option("hoodie.datasource.hive_sync.enable", "true")
      .option("hoodie.datasource.hive_sync.table", fssyncTable)
      .option("hoodie.datasource.hive_sync.database", hiveDb)
      .option("hoodie.datasource.hive_sync.jdbcurl", jdbcUrl)
      .option("hoodie.datasource.hive_sync.partition_fields", partitionField) // date
      .option("hoodie.datasource.hive_sync.partition_extractor_class", "org.apache.hudi.hive.MultiPartKeysValueExtractor")
      .mode(hoodieSaveMode)) //"append"
    writer.save(s"hdfs:///Projects/${Hops.getProjectName}/Resources/$saveTablePath")

  }



}


//val call_center = spark.read.parquet("hdfs://10.128.0.3:8020/Projects/tf2/tf2_Training_Datasets/TPCDS/call_center")
//val catalog_page = spark.read.parquet("hdfs://10.128.0.3:8020/Projects/tf2/tf2_Training_Datasets/TPCDS/catalog_page")
//val catalog_returns = spark.read.parquet("hdfs://10.128.0.3:8020/Projects/tf2/tf2_Training_Datasets/TPCDS/catalog_returns")
//val catalog_sales = spark.read.parquet("hdfs://10.128.0.3:8020/Projects/tf2/tf2_Training_Datasets/TPCDS/catalog_sales")
//val customer = spark.read.parquet("hdfs://10.128.0.3:8020/Projects/tf2/tf2_Training_Datasets/TPCDS/customer")
//val customer_address = spark.read.parquet("hdfs://10.128.0.3:8020/Projects/tf2/tf2_Training_Datasets/TPCDS/customer_address")
//val customer_demographics = spark.read.parquet("hdfs://10.128.0.3:8020/Projects/tf2/tf2_Training_Datasets/TPCDS/customer_demographics")
//val date_dim = spark.read.parquet("hdfs://10.128.0.3:8020/Projects/tf2/tf2_Training_Datasets/TPCDS/date_dim")
//val household_demographics = spark.read.parquet("hdfs://10.128.0.3:8020/Projects/tf2/tf2_Training_Datasets/TPCDS/household_demographics")
//val income_band = spark.read.parquet("hdfs://10.128.0.3:8020/Projects/tf2/tf2_Training_Datasets/TPCDS/income_band")
//val inventory = spark.read.parquet("hdfs://10.128.0.3:8020/Projects/tf2/tf2_Training_Datasets/TPCDS/inventory")
//val item = spark.read.parquet("hdfs://10.128.0.3:8020/Projects/tf2/tf2_Training_Datasets/TPCDS/item")
//val promotion = spark.read.parquet("hdfs://10.128.0.3:8020/Projects/tf2/tf2_Training_Datasets/TPCDS/promotion")
//val reason = spark.read.parquet("hdfs://10.128.0.3:8020/Projects/tf2/tf2_Training_Datasets/TPCDS/reason")
//val ship_mode = spark.read.parquet("hdfs://10.128.0.3:8020/Projects/tf2/tf2_Training_Datasets/TPCDS/ship_mode")
//val store = spark.read.parquet("hdfs://10.128.0.3:8020/Projects/tf2/tf2_Training_Datasets/TPCDS/store")
//val store_returns = spark.read.parquet("hdfs://10.128.0.3:8020/Projects/tf2/tf2_Training_Datasets/TPCDS/store_returns")
//val store_sales = spark.read.parquet("hdfs://10.128.0.3:8020/Projects/tf2/tf2_Training_Datasets/TPCDS/store_sales")
//val time_dim = spark.read.parquet("hdfs://10.128.0.3:8020/Projects/tf2/tf2_Training_Datasets/TPCDS/time_dim")
//val warehouse = spark.read.parquet("hdfs://10.128.0.3:8020/Projects/tf2/tf2_Training_Datasets/TPCDS/warehouse")
//val web_page = spark.read.parquet("hdfs://10.128.0.3:8020/Projects/tf2/tf2_Training_Datasets/TPCDS/web_page")
//val web_returns = spark.read.parquet("hdfs://10.128.0.3:8020/Projects/tf2/tf2_Training_Datasets/TPCDS/web_returns")
//val web_sales = spark.read.parquet("hdfs://10.128.0.3:8020/Projects/tf2/tf2_Training_Datasets/TPCDS/web_sales")
//val web_site = spark.read.parquet("hdfs://10.128.0.3:8020/Projects/tf2/tf2_Training_Datasets/TPCDS/web_site")
