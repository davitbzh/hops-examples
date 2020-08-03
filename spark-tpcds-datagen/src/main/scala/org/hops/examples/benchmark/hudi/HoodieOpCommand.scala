package org.hops.examples.benchmark.hudi

import io.hops.util.Hops
import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.rogach.scallop.ScallopConf


object HoodieOpCommand {

  class Conf(arguments: Seq[String]) extends ScallopConf(arguments) {

    val inputTable = opt[String]("inputTable", required = true)
    val inputTableFormat = opt[String]("inputTableFormat", required = true ,
      descr = "Must be either avro or parquet", validate = x => validateTableFormat(x))
    val hoodieTableName = opt[String]("hoodieTableName", required = true)
    val partitionField = opt[String]("partitionField", required = true)
    val fssyncTable = opt[String]("fssyncTable", required = true)
    val hoodieStorageType  = opt[String]("", required = true)
    val hoodieOperation = opt[String]("", required = true)
    val ipadderss = opt[String]("", required = true)
    val saveTablePath = opt[String]("", required = true)
    val hoodieSaveMode = opt[String]("", required = true)
    val primaryKey = opt[String]("", required = true)

    def validateTableFormat (x: String): Boolean = x match {
      case "avro" => true
      case "parquet" => true
      case _ => false
    }

    verify()
  }


  def main(args: Array[String]): Unit = {

    val argconf = new Conf(args)

    val inputTable = argconf.inputTable()
    val inputTableFormat = argconf.inputTableFormat()
    val hoodieTableName = argconf.hoodieTableName()
    val partitionField = argconf.partitionField()
    val fssyncTable = argconf.fssyncTable()
    val hoodieStorageType = argconf.hoodieStorageType()
    val hoodieOperation = argconf.hoodieOperation()
    val ipadderss = argconf.ipadderss()
    val saveTablePath = argconf.saveTablePath()
    val hoodieSaveMode = argconf.hoodieSaveMode()
    val primaryKey = argconf.primaryKey()

    val trustStore = Hops.getTrustStore
    val pw = Hops.getKeystorePwd
    val keyStore = Hops.getKeyStore
    val hiveDb = Hops.getProjectFeaturestore.read
    val jdbcUrl = (s"jdbc:hive2://$ipadderss:9085/$hiveDb;"
      + s"auth=noSasl;ssl=true;twoWay=true;sslTrustStore=$trustStore;"
      + s"trustStorePassword=$pw;sslKeyStore=$keyStore;keyStorePassword=$pw"
      )

    // Setup Spark
    val sparkConf: SparkConf = new SparkConf()
    val spark = SparkSession.builder.config(sparkConf).getOrCreate()

    val df: DataFrame = spark.read.format(inputTableFormat).load(inputTable)

    val writer = (df.write.format("org.apache.hudi")
      .option("hoodie.table.name", hoodieTableName)
      .option("hoodie.datasource.write.storage.type", hoodieStorageType) //"COPY_ON_WRITE"
      .option("hoodie.datasource.write.operation", hoodieOperation) //"upsert" or "bulk_insert"
      .option("hoodie.datasource.write.recordkey.field",primaryKey)
      .option("hoodie.datasource.write.partitionpath.field", partitionField) // date
      .option("hoodie.datasource.write.precombine.field", partitionField) // date
      .option("hoodie.datasource.hive_sync.enable", "true")
      .option("hoodie.datasource.hive_sync.table", fssyncTable) // ??
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
