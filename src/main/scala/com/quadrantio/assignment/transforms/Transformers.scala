package com.quadrantio.assignment.transforms

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.{ceil, from_unixtime, lit, round}

object Transformers {
  def dataframeNewDfTransforms(dataFrame: DataFrame): DataFrame ={
    import dataFrame.sparkSession.implicits._
    dataFrame
      .withColumn("datetime", from_unixtime(ceil($"timestamp".divide(lit(1000.0)))))
      .withColumn("round_latitude", round($"latitude", 4))
      .withColumn("round_longitude", round($"longitude", 4))
      .select("*")
  }
}
