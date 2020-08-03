package org.hops.examples.benchmark.hudi

import java.util.Locale

import scala.util.Try


class TPCDSHOODIEDatagenArguments(val args: Array[String]) {

  var scaleFactor = "1"
  var format = "parquet"
  var hoodieStorageType = ""
  var hoodieOperation  = ""
  var hoodieSaveMode = ""
  var hiveIpadderss = ""
  var filterOutNullPartitionValues = false
  var tableFilter: Set[String] = Set.empty
  var numPartitions = "100"
  var basePath = ""


  parseArgs(args.toList)
  validateArguments()

  private def parseArgs(inputArgs: List[String]): Unit = {
    var args = inputArgs

    while(args.nonEmpty) {
      args match {

        case ("--scale-factor") :: value :: tail =>
          scaleFactor = value
          args = tail

        case ("--hoodie-storage-type") :: value :: tail =>
          hoodieStorageType = value
          args = tail

        case ("--hoodie-operation") :: value :: tail =>
          hoodieOperation = value
          args = tail

        case ("--hoodie-save-mode") :: value :: tail =>
          hoodieSaveMode = value
          args = tail

        case ("--hive-ip-adderss") :: value :: tail =>
          hiveIpadderss = value
          args = tail

        case ("--filter-out-null-partition-values") :: tail =>
          filterOutNullPartitionValues = true
          args = tail

        case ("--table-filter") :: value :: tail =>
          tableFilter = value.toLowerCase(Locale.ROOT).split(",").map(_.trim).toSet
          args = tail

        case ("--num-partitions") :: value :: tail =>
          numPartitions = value
          args = tail

        case ("--base-path") :: value :: tail =>
          basePath = value
          args = tail

        case ("--help") :: tail =>
          printUsageAndExit(0)

        case _ =>
          // scalastyle:off println
          System.err.println("Unknown/unsupported param " + args)
          // scalastyle:on println
          printUsageAndExit(1)
      }
    }
  }

  private def printUsageAndExit(exitCode: Int): Unit = {
    // scalastyle:off
    System.err.println("""
                         |Usage: spark-submit --class <this class> --conf key=value <spark tpcds datagen jar> [Options]
                         |Options:
                         |  --output-location [STR]                Path to an output location
                         |  --scale-factor [NUM]                   Scale factor (default: 1)
                         |  --format [STR]                         Output format (default: parquet)
                         |  --overwrite                            Whether it overwrites existing data (default: false)
                         |  --partition-tables                     Whether it partitions output data (default: false)
                         |  --use-double-for-decimal               Whether it prefers double types (default: false)
                         |  --cluster-by-partition-columns         Whether it cluster output data by partition columns (default: false)
                         |  --filter-out-null-partition-values     Whether it filters out NULL partitions (default: false)
                         |  --table-filter [STR]                   Queries to filter, e.g., catalog_sales,store_sales
                         |  --num-partitions [NUM]                 # of partitions (default: 100)
                         |
      """.stripMargin)
    // scalastyle:on
    System.exit(exitCode)
  }

  private def validateArguments(): Unit = {

    if (Try(scaleFactor.toInt).getOrElse(-1) <= 0) {
      // scalastyle:off println
      System.err.println("Scale factor must be a positive number")
      // scalastyle:on println
      printUsageAndExit(-1)
    }
    if (Try(numPartitions.toInt).getOrElse(-1) <= 0) {
      // scalastyle:off println
      System.err.println("Number of partitions must be a positive number")
      // scalastyle:on println
      printUsageAndExit(-1)
    }
  }
}

