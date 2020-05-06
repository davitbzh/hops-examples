package org.hops.examples.benchmark.hudi

import org.apache.spark.sql.{DataFrame}
import io.hops.util.Hops


object HoodieOp {

  def huodieops( ipadderss: String, df: DataFrame, hoodieTableName: String, hoodieStorageType: String,
                 hoodieOperation: String, primaryKey: String, partitionField: String,
                 fssyncTable: String, hoodieSaveMode: String): Unit = {


    val trustStore = Hops.getTrustStore
    val pw = Hops.getKeystorePwd
    val keyStore = Hops.getKeyStore
    val hiveDb = Hops.getProjectFeaturestore.read
    val jdbcUrl = (s"jdbc:hive2://$ipadderss:9085/$hiveDb;"
      + s"auth=noSasl;ssl=true;twoWay=true;sslTrustStore=$trustStore;"
      + s"trustStorePassword=$pw;sslKeyStore=$keyStore;keyStorePassword=$pw"
      )

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
      writer.save(s"hdfs:///Projects/${Hops.getProjectName}/${Hops.getProjectName}_Training_Datasets/$hoodieTableName")

  }

}
