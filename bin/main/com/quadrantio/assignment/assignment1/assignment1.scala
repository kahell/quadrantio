package com.quadrantio.assignment.assignment1

import com.quadrantio.assignment.transforms.Transformers.dataframeNewDfTransforms
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

object assignment1 {
  def loadDataFromCsv(spark: SparkSession, sourcePath: String): DataFrame = {
    val schema: StructType = new StructType()
      .add("device_id", StringType, nullable = false)
      .add("id_type", StringType, nullable = false)
      .add("latitude", DoubleType, nullable = false)
      .add("longitude", DoubleType, nullable = false)
      .add("horizontal_accuracy", DoubleType, nullable = false)
      .add("timestamp", LongType)
      .add("ip_address", StringType, nullable = false)
      .add("device_os", StringType, nullable = false)
      .add("os_version", StringType, nullable = false)
      .add("user_agent", StringType, nullable = false)
      .add("country", StringType, nullable = false)
      .add("source_id", StringType, nullable = false)
      .add("publisher_id", StringType, nullable = false)
      .add("app_id", StringType, nullable = false)
      .add("location_context", StringType, nullable = false)
      .add("geohash", StringType, nullable = false)

    val df = spark.read
      .format("csv")
      .option("header", "true")
      .schema(schema)
      .load(sourcePath)
    df
  }

  def writeDataToParquet(dataFrame: DataFrame, destinationPath: String): Unit = {
    import dataFrame.sparkSession.implicits._
    dataFrame
      .withColumn("date_partition", to_date(from_unixtime(ceil($"timestamp".divide(lit(1000.0))))))
      .select($"*")
      .write
      .mode("Overwrite")
      .partitionBy("date_partition")
      .parquet(destinationPath)
  }

  def findDuplicateRecords(dataFrame: DataFrame): DataFrame = {
    import dataFrame.sparkSession.implicits._
    val newDf = dataframeNewDfTransforms(dataFrame = dataFrame)

    val countDuplicate = newDf.groupBy(
      col("device_id"),
      col("round_latitude"),
      col("round_longitude"),
      col("datetime")
    )
      .count()
      .filter("count > 1")
      .agg(sum("count").as("total_duplicate_records"))
      .select(col("total_duplicate_records"))

    countDuplicate
  }

}
