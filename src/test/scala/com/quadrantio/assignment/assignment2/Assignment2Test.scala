package com.quadrantio.assignment.assignment2

import com.quadrantio.assignment.assignment1.assignment1
import org.apache.spark.sql.SparkSession
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

  test("dailyAverageUser should calculate dau") {
    val sourcePath = "/Applications/XAMPP/xamppfiles/htdocs/quadrantio/src/test/resources/sample_data.csv"
    val loadDataDF = assignment1.loadDataFromCsv(spark = spark, sourcePath = sourcePath)
    val actual = assignment2.dailyAverageUser(dataFrame = loadDataDF)

    // test
    val actualSeq = actual.collect().toSeq.seq
    actualSeq(0).toString() shouldEqual "[2022-01-22,4]"
    actualSeq(1).toString() shouldEqual "[2022-01-21,2]"
  }

  test("eventPerUserPerDay should calculate Completeness Percentage of each of the attributes") {
    val sourcePath = "/Applications/XAMPP/xamppfiles/htdocs/quadrantio/src/test/resources/sample_data.csv"
    val loadDataDF = assignment1.loadDataFromCsv(spark = spark, sourcePath = sourcePath)
    val actual = assignment2.completenessPercentage(spark=spark, dataFrame = loadDataDF)

    // test
    val actual_head = actual.collect().head
    actual_head.toString() shouldEqual "[100.0 %,85.71428571428571 %,100.0 %,100.0 %,100.0 %,100.0 %,0.0 %,100.0 %,0.0 %,0.0 %,100.0 %,100.0 %,100.0 %,100.0 %,0.0 %,100.0 %]"
  }

  test("eventPerUserPerDay should calculates events per user per day") {
    val sourcePath = "/Applications/XAMPP/xamppfiles/htdocs/quadrantio/src/test/resources/sample_data.csv"
    val loadDataDF = assignment1.loadDataFromCsv(spark = spark, sourcePath = sourcePath)
    val actual = assignment2.eventPerUserPerDay(spark=spark, dataFrame = loadDataDF)

    // test
    actual.show(false)
    val actual_seq = actual.collect().toSeq.seq
    actual_seq(0).toString() shouldEqual "[ASU2CA796238261DA346A7391B70360F8B8552E0E52793FCCF267BE252470741,2,2022-01-22]"
    actual_seq(1).toString() shouldEqual "[6EA2CA796238261DA346A7391B70360F8B8552E0E52793FCCF267BE25247074E,1,2022-01-22]"
    actual_seq(2).toString() shouldEqual "[4FFC297D831389495402E589362B9E77DE1106C990032669E978A0489F188C4E,1,2022-01-22]"
    actual_seq(3).toString() shouldEqual "[ASU2CA796238261DA346A7391B70360F8B8552E0E52793FCCF267BE252470741,1,2022-01-21]"
    actual_seq(4).toString() shouldEqual "[B1AA7DEA6AED8ACCFCEA82CADB35DF5871C9147EB5CF81CDC839F62B1EF526A8,1,2022-01-21]"
  }
}