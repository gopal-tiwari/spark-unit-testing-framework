package com.connected.frameworks

import com.connected.frameworks.testing.SparkDatasetTester
import org.apache.spark.sql.SparkSession

object Main extends SparkDatasetTester {
  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder
      .appName("Parser").master("local[2]")
      .getOrCreate()
    import spark.implicits._

    val someDF = Seq(
      (8, "bat"),
      (64, "mouse"),
      (-27, "horse")
    ).toDF("number", "word")


    val someDF1 = Seq(
      (8, "bat"),
      (64, "mouse"),
      (-3, "horse")
    ).toDF("number", "word")


    assertDatasetEquality(someDF, someDF1, orderedComparision = false)
  }
}
