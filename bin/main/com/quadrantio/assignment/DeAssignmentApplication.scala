package com.quadrantio.assignment

import com.quadrantio.assignment.assignment1.assignment1.{findDuplicateRecords, loadDataFromCsv, writeDataToParquet}
import com.quadrantio.assignment.assignment2.assignment2.{completenessPercentage, dailyAverageUser, distributionOfHorizontal, eventPerUserPerDay}
import org.apache.spark.sql.{DataFrame, SparkSession}

/**
 * The main entrypoint of the DeAssignment pipeline to building QuadrantIO Data.
 *
 * @author helfi.pangestu
 */
object DeAssignmentApplication {
  /**
   * Start the DeAssignmentApplication.
   *
   * @param args(0) source path
   * @param args(1) destination path
   *
   */
  def main(args: Array[String]): Unit = {
    val spark:SparkSession = SparkSession.builder()
      .master("local")
      .appName("quadrantio")
      .getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")
    import spark.implicits._

    // Assignment 1
    // 1. Load Data from path
    val sourcePath: String = args.toSeq.seq(0)
    val destinationPath: String = args.toSeq.seq(1)
    val df:DataFrame = loadDataFromCsv(spark=spark, sourcePath=sourcePath)

    // 2. Write to parquet files
    writeDataToParquet(dataFrame = df, destinationPath)

    // 3. Find duplicate records
    val duplicateRecord: DataFrame = findDuplicateRecords(dataFrame = df)
    duplicateRecord.show(truncate = false)

    // Assignment 2
    // 1. Daily Average Users
    val dau: DataFrame = dailyAverageUser(dataFrame = df)
    dau.show(truncate = false)

    // 2. Completeness Percentage of each of the attributes.
    val completeness: DataFrame = completenessPercentage(spark = spark, dataFrame = df)
    completeness.show(truncate = false)

    // 3. Events per User per Day.
    val evetUserPerDay: DataFrame = eventPerUserPerDay(spark = spark, dataFrame = df)
    evetUserPerDay.show(truncate = false)

    // 4. Distribution of Horizontal Accuracy.
    val distributionOfHorizontalAcc = distributionOfHorizontal(spark=spark,dataFrame = df)
    distributionOfHorizontalAcc.show(truncate = false)
  }
}
