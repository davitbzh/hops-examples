package org.hops.examples.benchmark.hudi

import java.util.Calendar
import java.time.LocalDateTime

import org.apache.spark.sql.{DataFrame, DataFrameWriter, Row, SparkSession}
import io.hops.util.Hops
import org.apache.hudi.DataSourceWriteOptions
import org.apache.hudi.common.util.TypedProperties
import org.apache.hudi.hive.MultiPartKeysValueExtractor
import org.apache.hudi.keygen.ComplexKeyGenerator
import org.apache.spark.sql.functions.{col, lit}
import org.apache.spark.sql.types.{FloatType, IntegerType, LongType}

object HoodieOp {

//  // https://github.com/itcabb/apache-incubator-hudi/blob/078d4825d909b2c469398f31c97d2290687321a8/hudi-spark/src/test/scala/TestDataSourceDefaults.scala
//  private def getKeyConfig(recordKeyFieldName: String, partitionPathField: String, hiveStylePartitioning: String): TypedProperties = {
//
//    val props = new TypedProperties()
//    props.setProperty(DataSourceWriteOptions.RECORDKEY_FIELD_OPT_KEY, recordKeyFieldName)
//    props.setProperty(DataSourceWriteOptions.PARTITIONPATH_FIELD_OPT_KEY, partitionPathField)
//    props.setProperty(DataSourceWriteOptions.HIVE_STYLE_PARTITIONING_OPT_KEY, hiveStylePartitioning)
//    props
//  }
//
//  var baseRecord: GenericRecord = _
//
//  val hk1 = new ComplexKeyGenerator(getKeyConfig("field1,name", "field1,name", "false")).getKey(baseRecord)
//  assertEquals("field1:field1,name:name1", hk1.getRecordKey)
//  assertEquals("field1/name1", hk1.getPartitionPath)

  def huodieops(spark: SparkSession, ipadderss: String, df: DataFrame, hoodieTableName: String, hoodieStorageType: String,
                 hoodieOperation: String, primaryKeys: List[String], partitionField: String,
                 fssyncTable: String, hoodieSaveMode: String, base_path: String): Unit = {


//    import org.apache.hadoop.fs.{FileSystem, Path}
//    val filesystem = FileSystem.get(spark.sparkContext.hadoopConfiguration);
//    val output_stream = filesystem.listStatus(new Path(s"hdfs:///Projects/benchmark/Logs/"));

    var resultDF = spark.sqlContext.emptyDataFrame

    val h = LocalDateTime.now().getHour
    val d = LocalDateTime.now().getDayOfMonth
    val m = LocalDateTime.now().getMonth
    val y = LocalDateTime.now().getYear
    resultDF = df.withColumn("date_modified", lit(s"$h-$d-$m-$y"))

    val rnd = new scala.util.Random
    val randnum = 2 + rnd.nextInt(55 - 2)


    if (hoodieOperation.equals("upsert")){

      for (f <- resultDF.schema.fields) {

        if(!primaryKeys.contains(f.name)){
          if (f.dataType.isInstanceOf[IntegerType]) {

            resultDF = resultDF.withColumn(f.name, col(f.name) * randnum)

          } else if (f.dataType.isInstanceOf[LongType]) {

            resultDF = resultDF.withColumn(f.name, col(f.name) * randnum)

          } else if (f.dataType.isInstanceOf[FloatType]) {

            resultDF = resultDF.withColumn(f.name, col(f.name) * randnum.toFloat)
          }
        }
      }

    }

    val trustStore = Hops.getTrustStore
    val pw = Hops.getKeystorePwd
    val keyStore = Hops.getKeyStore
    val hiveDb = Hops.getProjectFeaturestore.read
    val jdbcUrl = (s"jdbc:hive2://$ipadderss:9085/$hiveDb;"
      + s"auth=noSasl;ssl=true;twoWay=true;sslTrustStore=$trustStore;"
      + s"trustStorePassword=$pw;sslKeyStore=$keyStore;keyStorePassword=$pw"
      )

    var primaryKey = new String

    if (primaryKeys.length > 1){
      primaryKey = primaryKeys.mkString(",")
    } else {
      primaryKey = primaryKeys(0)
    }
    // https://github.com/apache/hudi/issues/933
    if (primaryKeys.length > 1){
      resultDF.write.format("org.apache.hudi")
        .option("hoodie.table.name", hoodieTableName)
        .option("hoodie.datasource.write.storage.type", hoodieStorageType) //"COPY_ON_WRITE"
        .option("hoodie.datasource.write.operation", hoodieOperation)      //"upsert" or "bulk_insert"
        .option("hoodie.datasource.write.partitionpath.field", "date_modified") // date
        .option("hoodie.datasource.write.precombine.field", "date_modified")    // date
        .option("hoodie.datasource.hive_sync.enable", "true")
        .option("hoodie.datasource.hive_sync.table", fssyncTable) // ??
        .option("hoodie.datasource.hive_sync.database", hiveDb)
        .option("hoodie.datasource.hive_sync.jdbcurl", jdbcUrl)
        .option("hoodie.datasource.hive_sync.partition_fields", "date_modified") // date
        .option("hoodie.datasource.hive_sync.partition_extractor_class", "org.apache.hudi.hive.MultiPartKeysValueExtractor")
        .option("hoodie.parquet.max.file.size", String.valueOf(1024 * 1024 * 1024))
        .option("hoodie.parquet.small.file.limit", String.valueOf(32 * 1024 * 1024))
        .option("hoodie.parquet.compression.ratio", String.valueOf(0.5))
        .option("hoodie.datasource.write.recordkey.field",primaryKey)
        .option("hoodie.datasource.write.keygenerator.class","org.apache.hudi.ComplexKeyGenerator")
        .mode(hoodieSaveMode) //"append"
        .save(s"$base_path/$hoodieTableName")
    } else {
      resultDF.write.format("org.apache.hudi")
        .option("hoodie.table.name", hoodieTableName)
        .option("hoodie.datasource.write.storage.type", hoodieStorageType) //"COPY_ON_WRITE"
        .option("hoodie.datasource.write.operation", hoodieOperation)      //"upsert" or "bulk_insert"
        .option("hoodie.datasource.write.partitionpath.field", "date_modified") // date
        .option("hoodie.datasource.write.precombine.field", "date_modified")    // date
        .option("hoodie.datasource.hive_sync.enable", "true")
        .option("hoodie.datasource.hive_sync.table", fssyncTable) // ??
        .option("hoodie.datasource.hive_sync.database", hiveDb)
        .option("hoodie.datasource.hive_sync.jdbcurl", jdbcUrl)
        .option("hoodie.datasource.hive_sync.partition_fields", "date_modified") // date
        .option("hoodie.datasource.hive_sync.partition_extractor_class", "org.apache.hudi.hive.MultiPartKeysValueExtractor")
        .option("hoodie.parquet.max.file.size", String.valueOf(1024 * 1024 * 1024))
        .option("hoodie.parquet.small.file.limit", String.valueOf(32 * 1024 * 1024))
        .option("hoodie.parquet.compression.ratio", String.valueOf(0.5))
        .option("hoodie.datasource.write.recordkey.field",primaryKey)
        .option("hoodie.datasource.write.keygenerator.class","org.apache.hudi.SimpleKeyGenerator")
        .mode(hoodieSaveMode) //"append"
        .save(s"$base_path/$hoodieTableName")
    }

  }
}

/*
https://github.com/tensorflow/docs/blob/cf2a57e77485c371f04cc486d9d1e632ef552739/site/en/guide/distributed_training.ipynb
https://www.tensorflow.org/guide/function
https://www.logicalclocks.com/blog/mlops-with-a-feature-store
https://www.youtube.com/watch?v=ZnukSLKEw34
https://www.youtube.com/watch?v=e4_4D7uNvf8
https://www.youtube.com/watch?v=pXHAQIhhMhI
https://www.tensorflow.org/tutorials/customization/custom_training_walkthrough
https://www.tensorflow.org/guide/profiler
https://www.tensorflow.org/tensorboard/tensorboard_profiling_keras
https://www.youtube.com/watch?v=6ovfZW8pepo
https://www.tensorflow.org/guide/data_performance
https://www.tensorflow.org/api_docs/python/tf/nn/compute_average_loss
https://www.tensorflow.org/guide/data_performance
https://www.tensorflow.org/api_docs/python/tf/data/Dataset

 */

//    //https://hudi.apache.org/docs/configurations.html
//    //https://github.com/nmukerje/EMR-Hudi-Workshop/blob/225cc1d3fa41bd0abdbe337de67ac5dcf427b03c/notebooks/2_Consume_Streaming_Updates.ipynb
//    val hudiOptions = Map[String,String](
//      DataSourceWriteOptions.RECORDKEY_FIELD_OPT_KEY -> hudiTableRecordKey,
//      DataSourceWriteOptions.PARTITIONPATH_FIELD_OPT_KEY -> hudiTablePartitionKey,
//      DataSourceWriteOptions.PRECOMBINE_FIELD_OPT_KEY -> hudiTablePrecombineKey,
//      DataSourceWriteOptions.HIVE_SYNC_ENABLED_OPT_KEY -> "true",
//      DataSourceWriteOptions.HIVE_PARTITION_FIELDS_OPT_KEY -> hudiHiveTablePartitionKey,
//      DataSourceWriteOptions.HIVE_ASSUME_DATE_PARTITION_OPT_KEY -> "false",
//      DataSourceWriteOptions.HIVE_PARTITION_EXTRACTOR_CLASS_OPT_KEY -> classOf[MultiPartKeysValueExtractor].getName,
//      "hoodie.parquet.max.file.size" -> String.valueOf(1024 * 1024 * 1024),
//      "hoodie.parquet.small.file.limit" -> String.valueOf(64 * 1024 * 1024),
//      "hoodie.parquet.compression.ratio" -> String.valueOf(0.5),
//      "hoodie.insert.shuffle.parallelism" -> String.valueOf(2))


