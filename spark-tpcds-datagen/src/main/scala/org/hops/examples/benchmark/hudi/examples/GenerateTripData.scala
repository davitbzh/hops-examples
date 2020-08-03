package org.hops.examples.benchmark.hudi.examples
import java.util
import java.util.List

import io.hops.util.Hops
import org.apache.hudi.QuickstartUtils._

import scala.collection.JavaConversions._
import org.apache.spark.sql.SaveMode._
import org.apache.hudi.DataSourceReadOptions._
import org.apache.hudi.DataSourceWriteOptions._
import org.apache.hudi.common.model.{HoodieRecord, HoodieRecordPayload}
import org.apache.hudi.config.HoodieWriteConfig._
import org.apache.spark.sql.SparkSession

object GenerateTripData {

  def main(args: Array[String]): Unit = {

    val ipadderss = args(0)
    val basePath = args(1)
    val tableName = args(2) //"hudi_trips_cow"
    val hudiOp =args(3)

    val trustStore = Hops.getTrustStore
    val pw = Hops.getKeystorePwd
    val keyStore = Hops.getKeyStore
    val hiveDb = Hops.getProjectFeaturestore.read
    val jdbcUrl = (s"jdbc:hive2://$ipadderss:9085/$hiveDb;"
      + s"auth=noSasl;ssl=true;twoWay=true;sslTrustStore=$trustStore;"
      + s"trustStorePassword=$pw;sslKeyStore=$keyStore;keyStorePassword=$pw"
      )

    val spark = SparkSession.builder.getOrCreate()
//    val sc = spark.sparkContext
//    import spark.implicits._

    val dataGen = new DataGenerator


    var df = spark.emptyDataFrame
    //Insert data
    if (hudiOp.equals("insert")) {
      val gen_data = convertToStringList(dataGen.generateInserts(10))
      df = spark.read.json(spark.sparkContext.parallelize( gen_data, 2))
    } else if (hudiOp.equals("update")){
      val gen_data =  convertToStringList(dataGen.generateUpdates(10))
      df = spark.read.json(spark.sparkContext.parallelize( gen_data, 2))
    }

    val result_df = df.
      write.format("org.apache.hudi").
      options(getQuickstartWriteConfigs).
      option(PRECOMBINE_FIELD_OPT_KEY, "ts").
      option(RECORDKEY_FIELD_OPT_KEY, "uuid").
      option(PARTITIONPATH_FIELD_OPT_KEY, "partitionpath").
      option(TABLE_NAME, tableName)

    if (hudiOp.equals("insert")) {
      result_df.mode(Overwrite).save(basePath)
    } else if (hudiOp.equals("update")){
      result_df.mode(Append).save(basePath)
    }

  }

}


