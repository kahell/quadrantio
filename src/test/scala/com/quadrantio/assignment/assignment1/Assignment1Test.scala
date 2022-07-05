package com.quadrantio.assignment.assignment1

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.col
import org.junit.runner.RunWith
import org.mockito.MockitoSugar
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.must.Matchers
import org.scalatest.matchers.should.Matchers.convertToAnyShouldWrapper
import org.scalatestplus.junit.JUnitRunner

@RunWith(classOf[JUnitRunner])
class Assignment1Test extends AnyFunSuite with Matchers with MockitoSugar {
  val spark: SparkSession = SparkSession.builder()
    .master("local[*]")
    .appName("test")
    .getOrCreate()
  spark.sparkContext.setLogLevel("ERROR")
  import spark.implicits._

  test("loadDataFromCsv should load csv file from source path") {
    val sourcePath = "/Applications/XAMPP/xamppfiles/htdocs/quadrantio/src/test/resources/sample_data.csv"
    val actual = assignment1.loadDataFromCsv(spark=spark, sourcePath=sourcePath)

    // test
    actual.count() shouldEqual 7
    actual.columns shouldEqual Array("device_id", "id_type", "latitude", "longitude", "horizontal_accuracy", "timestamp", "ip_address", "device_os", "os_version", "user_agent", "country", "source_id", "publisher_id", "app_id", "location_context", "geohash")
    val filteredActual = actual.filter(col("device_id") === "4FFC297D831389495402E589362B9E77DE1106C990032669E978A0489F188C4E").select(col("*"))
    filteredActual.count() shouldEqual 1
  }

  test("findDuplicateRecords should return total number duplicate records") {
    val sourcePath = "/Applications/XAMPP/xamppfiles/htdocs/quadrantio/src/test/resources/sample_data.csv"
    val sampleDf = assignment1.loadDataFromCsv(spark=spark, sourcePath=sourcePath)
    val actual = assignment1.findDuplicateRecords(dataFrame = sampleDf)

    // test
    actual.count() shouldEqual 1
  }

  test("findDuplicateRecords should return total number duplicate records") {
    val sourcePath = "/Applications/XAMPP/xamppfiles/htdocs/quadrantio/src/test/resources/sample_data.csv"
    val sampleDf = assignment1.loadDataFromCsv(spark=spark, sourcePath=sourcePath)
    val actual = assignment1.findDuplicateRecords(dataFrame = sampleDf)

    // test
    actual.count() shouldEqual 1
  }

}
