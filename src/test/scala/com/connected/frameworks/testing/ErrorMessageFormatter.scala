package com.connected.frameworks.testing

import org.apache.spark.sql.Dataset

object ErrorMessageFormatter {

  def schemaErrorMessageString[T](actualDs: Dataset[T], expectedDs: Dataset[T]): String = {

    s"""
       |
       |Actual Data Schema  :${actualDs.schema}
       |Expected Data Schema:${expectedDs.schema}
     """.stripMargin

  }

  def countErrorMessageString(actualCount: Long, expectedCount: Long): String = {
    s"""
       |Actual Record Count : $actualCount is not equal to Expected Record Count : $expectedCount
     """.stripMargin
  }


  def dataErrorMessageString[T](actualRecordsArray: Array[T], expectedRecordsArray: Array[T]): String = {

    "\n . \n" + actualRecordsArray
      .zip(expectedRecordsArray)
      .foreach {
        case (r1, r2) =>
          if (r1.equals(r2))
            println(s"\033[0;34m$r1 | $r2\033[0m")
          else
            println(s"\033[0;31m$r1 | $r2\033[0m")

      }
    "Actual data rows did not matched with Expected data rows"
  }
}
