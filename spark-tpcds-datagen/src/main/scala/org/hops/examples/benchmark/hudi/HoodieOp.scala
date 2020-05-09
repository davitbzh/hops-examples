package org.hops.examples.benchmark.hudi

import org.apache.spark.sql.DataFrame
import io.hops.util.Hops
import org.apache.hudi.DataSourceWriteOptions
import org.apache.hudi.common.util.TypedProperties
import org.apache.hudi.keygen.ComplexKeyGenerator

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

  def huodieops( ipadderss: String, df: DataFrame, hoodieTableName: String, hoodieStorageType: String,
                 hoodieOperation: String, primaryKey: String, partitionField: String,
                 fssyncTable: String, hoodieSaveMode: String): Unit = {


//    import org.apache.hadoop.fs.{FileSystem, Path}
//    val filesystem = FileSystem.get(spark.sparkContext.hadoopConfiguration);
//    val output_stream = filesystem.listStatus(new Path(s"hdfs:///Projects/benchmark/Logs/"));

    val trustStore = Hops.getTrustStore
    val pw = Hops.getKeystorePwd
    val keyStore = Hops.getKeyStore
    val hiveDb = Hops.getProjectFeaturestore.read
    val jdbcUrl = (s"jdbc:hive2://$ipadderss:9085/$hiveDb;"
      + s"auth=noSasl;ssl=true;twoWay=true;sslTrustStore=$trustStore;"
      + s"trustStorePassword=$pw;sslKeyStore=$keyStore;keyStorePassword=$pw"
      )

    if (partitionField != null){
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
        writer.save(s"hdfs:///Projects/${Hops.getProjectName}/${Hops.getProjectName}_Training_Datasets/hoodie/$hoodieTableName")

    } else {
      val writer = (df.write.format("org.apache.hudi")
        .option("hoodie.table.name", hoodieTableName)
        .option("hoodie.datasource.write.storage.type", hoodieStorageType) //"COPY_ON_WRITE"
        .option("hoodie.datasource.write.operation", hoodieOperation) //"upsert" or "bulk_insert"
        .option("hoodie.datasource.write.recordkey.field",primaryKey)
        .option("hoodie.datasource.write.precombine.field", primaryKey) // TODO: this is terrible just for testing
        .option("hoodie.datasource.write.keygenerator.class", "org.apache.hudi.NonpartitionedKeyGenerator")
        .option("hoodie.datasource.hive_sync.partition_extractor_class", "org.apache.hudi.hive.NonPartitionedExtractor")
        .option("hoodie.datasource.hive_sync.enable", "true")
        .option("hoodie.datasource.hive_sync.table", fssyncTable) // ??
        .option("hoodie.datasource.hive_sync.database", hiveDb)
        .option("hoodie.datasource.hive_sync.jdbcurl", jdbcUrl)
        .option("hoodie.datasource.hive_sync.partition_extractor_class", "org.apache.hudi.hive.MultiPartKeysValueExtractor")
        .mode(hoodieSaveMode)) //"append"
      writer.save(s"hdfs:///Projects/${Hops.getProjectName}/${Hops.getProjectName}_Training_Datasets/hoodie/$hoodieTableName")
    }


  }

}
