package org.apache.spark.sql.execution.benchmark

import java.io.File

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.util.resourceToString

object TestDir {

  def getListOfFiles(dir: String):List[File] = {
    val d = new File(dir)
    if (d.exists && d.isDirectory) {
      d.listFiles.filter(_.isFile).toList
    } else {
      List[File]()
    }
  }

  def main(args: Array[String]): Unit = {

    val conf =
      new SparkConf()
        //      .setMaster("local[1]")
        .setAppName("test-sql-context")
        .set("spark.sql.parquet.compression.codec", "snappy")
        .set("spark.sql.shuffle.partitions", "4")
        //      .set("spark.driver.memory", "3g")
        //      .set("spark.executor.memory", "3g")
        .set("spark.sql.autoBroadcastJoinThreshold", (20 * 1024 * 1024).toString)
        .set("spark.sql.crossJoin.enabled", "true")

    val spark = SparkSession.builder.getOrCreate()

    import scala.io.Source

//    val filename = "/q39a.sql"
//    for (line <- Source.fromFile(getClass.getClassLoader().getResource(filename).getPath).getLines) {
//      println(line)
//    }

    val queryString = resourceToString("q39a.sql",
      classLoader = Thread.currentThread().getContextClassLoader)

    println("AAAAAAAAAAAAAAAAAAAAAAAA")
    println(queryString)
    println("AAAAAAAAAAAAAAAAAAAAAAAA")

    val path = getClass.getResource("/").getPath()
    val folder = new File(path)
    if (folder.exists && folder.isDirectory)
      folder.listFiles
        .toList
        .foreach(file => println("---ZZZZZZ---\n" + file.getName + "\n---ZZZZZZ---"))

   }

  val path = getClass.getClassLoader().getResource("").getPath
  val folder = new File(path)
  if (folder.exists && folder.isDirectory)
    folder.listFiles
      .toList
      .foreach(file => println("---DDDDDD---\n" + file.getName + "\n---DDDDDDD---"))





}
