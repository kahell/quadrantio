package com.quadrantio.assignment.assignment2

import com.quadrantio.assignment.transforms.Transformers.dataframeNewDfTransforms
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{DoubleType, LongType, StringType, StructField, StructType}

object assignment2 {
  def dailyAverageUser(dataFrame: DataFrame): DataFrame = {
    import dataFrame.sparkSession.implicits._
    dataFrame
      .withColumn("datetime", from_unixtime(ceil($"timestamp".divide(lit(1000.0)))))
      .groupBy(to_date(col("datetime")).as("day"))
      .agg(countDistinct(col("device_id"), col("latitude"), col("longitude"), col("datetime")).as("num_deviceid_perday"))
  }

  def completenessPercentage(spark: SparkSession, dataFrame: DataFrame): DataFrame = {
    val countFilledValue = dataFrame.dtypes.foldLeft(dataFrame) { (tempDf, listCol) =>
      tempDf.withColumn(listCol._1, when(col(listCol._1).isNull, 0).otherwise(1))
    }
    var selectQuery = Array[String]()

    dataFrame.dtypes.map(data => {
      selectQuery :+= "concat((sum(" + data._1 + ") / count('*') * 100), ' %')  as " + "percentage_" + data._1 + ""
    })

    countFilledValue.createOrReplaceTempView("completeness_table")
    val percentageFilledValues = spark.sql("SELECT " + selectQuery.mkString(",") + " FROM completeness_table")
    percentageFilledValues
  }

  def eventPerUserPerDay(spark: SparkSession, dataFrame: DataFrame): DataFrame = {
    import dataFrame.sparkSession.implicits._

    // transform
    val transformDf = dataframeNewDfTransforms(dataFrame = dataFrame)

    // drop duplicate
    val dropDupDf = transformDf.dropDuplicates("device_id","round_latitude","round_longitude","datetime")

    dropDupDf.createOrReplaceTempView("eventperuser_table")
    val query = {
      """
        | select
        | distinct device_id, count(*) as count_event,  date(from_unixtime(timestamp / 1000)) as day
        |      FROM eventperuser_table
        |      GROUP BY device_id, day
        |      ORDER BY day DESC
        |""".stripMargin
    }
    val adhoc = spark.sql(query)
    adhoc
  }

  def totalEventsByHA(spark: SparkSession, ha: Array[Int] = Array(), ha_range: String): DataFrame = {
    var where = ""
    if (ha.length == 1) {
      where = s"> ${ha.toSeq.seq(0)}"
    } else {
      where = s"between ${ha.toSeq.seq(0)} and ${ha.toSeq.seq(1)}"
    }

    val query =
      s"""
      SELECT "${ha_range}" as ha_range, horizontal_accuracy, CASE
                WHEN SUM(count_ha) is null THEN 0
                ELSE SUM(count_ha)
              END
              as total_events_ha
      FROM (
        select
        horizontal_accuracy, count(horizontal_accuracy) as count_ha
        FROM dummy_table
        WHERE horizontal_accuracy ${where}
        GROUP BY horizontal_accuracy
      )
      GROUP BY horizontal_accuracy
      """
    spark.sql(query)
  }

  def distributionOfHorizontal(spark: SparkSession, dataFrame: DataFrame): DataFrame = {
    val rdd = spark.sparkContext.parallelize(
      Seq(
        ("0 to 5"),
        ("6 to 10"),
        ("11 to 25"),
        ("26 to 50"),
        ("51 to 100"),
        ("101 to 500"),
        ("over 501"),
      )
    )
    val rowRDD: RDD[Row] = rdd.map(t => Row(t))
    val schema: StructType = new StructType()
      .add("ha_range", StringType, nullable = false)

    val distributeHa: DataFrame = spark.createDataFrame(rowRDD, schema)

    // Create Temp View tables
    dataFrame.createOrReplaceTempView("distribution_table")

    // Get totalEventsByHA
    var seqRow = Seq[Row]()

    distributeHa.collect().map(data => {
      val haRangeName: String = data.toString().replace("[", "").replace("]", "")
      var rangeHa: Array[Int] = Array()
      if (haRangeName contains "to") {
        val cleanHaRangeName = haRangeName.replaceAll(" ", "")
        rangeHa = cleanHaRangeName.split("to").map(data => data.toInt)
        val totalEventsDf = totalEventsByHA(spark = spark, rangeHa, haRangeName)
        val totalEventsSeq = totalEventsDf.select(col("*")).collect().toSeq
        val totalEventsSeqRow: Seq[Row] = totalEventsSeq.map(data => {
          Row(data(0), data(1), data(2))
        })

        seqRow = seqRow ++ totalEventsSeqRow
      } else {
        val cleanHaRangeName = haRangeName.replaceAll("over", "").replaceAll(" ", "")
        rangeHa = Array(cleanHaRangeName.toInt)
        val totalEventsDf = totalEventsByHA(spark = spark, rangeHa, haRangeName)
        val totalEventsSeq = totalEventsDf.select(col("*")).collect().toSeq
        val totalEventsSeqRow: Seq[Row] = totalEventsSeq.map(data => {
          Row(data(0), data(1), data(2))
        })

        seqRow = seqRow ++ totalEventsSeqRow
      }
    })

    val newSchema: List[StructField] = List(
      StructField("ha_range_name", StringType, false),
      StructField("horizontal_accuracy", DoubleType, false),
      StructField("total_events_ha", LongType, false)
    )

    val totalEventsDF: DataFrame = spark.createDataFrame(spark.sparkContext.parallelize(seqRow),StructType(newSchema))

    // get percentage
    val totalRow = dataFrame.count()
    val percentage = udf((total_events: Long) => {
      val percetage:Double = total_events * 100.0 / totalRow
      percetage
    })

    val percentageDf = totalEventsDF.withColumn("percentage", percentage(col("total_events_ha")))
    percentageDf.show(false)

    // get cumulative percentage
    import org.apache.spark.sql.expressions.Window
    val windowSpecAgg  = Window.partitionBy("ha_range_name").orderBy().rowsBetween(-Int.MaxValue,0)
    val cumulativeDf = percentageDf.withColumn("cumulative", sum(col("percentage")).over(windowSpecAgg))

    cumulativeDf
  }
}
