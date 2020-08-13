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

  def huodieops(spark: SparkSession, ipadderss: String, df: DataFrame, hoodieTableName: String, hoodieStorageType: String,
                 hoodieOperation: String, primaryKeys: List[String], partitionFields: Seq[String],
                 fssyncTable: String, hoodieSaveMode: String, base_path: String): Unit = {


    var resultDF = spark.sqlContext.emptyDataFrame

    val h = LocalDateTime.now().getHour
    val d = LocalDateTime.now().getDayOfMonth
    val m = LocalDateTime.now().getMonth
    val y = LocalDateTime.now().getYear
    resultDF = df.withColumn("date_modified", lit(s"$h-$d-$m-$y"))

    val rnd = new scala.util.Random

    var partitionField = s"$h-$d-$m-$y"
    if (partitionFields.nonEmpty) {
      partitionField = s"$h-$d-$m-$y" + "," + partitionFields.mkString(",")
    }


    if (hoodieOperation.equals("upsert")){

      for (f <- resultDF.schema.fields) {

        if(!primaryKeys.contains(f.name)){
          if (f.dataType.isInstanceOf[IntegerType]) {

            resultDF = resultDF.withColumn(f.name, col(f.name) * (2 + rnd.nextInt(10 - 2)))

          } else if (f.dataType.isInstanceOf[LongType]) {

            resultDF = resultDF.withColumn(f.name, col(f.name) * (2 + rnd.nextInt(100 - 2)))

          } else if (f.dataType.isInstanceOf[FloatType]) {

            resultDF = resultDF.withColumn(f.name, col(f.name) * (2 + rnd.nextInt(100 - 2)).toFloat)
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
      .option("hoodie.datasource.hive_sync.partition_fields", partitionField) // date
      .option("hoodie.datasource.hive_sync.partition_extractor_class", "org.apache.hudi.hive.MultiPartKeysValueExtractor")
      .option("hoodie.parquet.max.file.size", String.valueOf(1024 * 1024 * 1024))
      .option("hoodie.parquet.small.file.limit", String.valueOf(32 * 1024 * 1024))
      .option("hoodie.parquet.compression.ratio", String.valueOf(0.5))
      .option("hoodie.datasource.write.recordkey.field",primaryKey)
      .option("hoodie.datasource.write.keygenerator.class","org.apache.hudi.ComplexKeyGenerator")
      .mode(hoodieSaveMode) //"append"
      .save(s"$base_path/$hoodieTableName")

  }
}


