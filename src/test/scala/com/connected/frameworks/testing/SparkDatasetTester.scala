package com.connected.frameworks.testing

import com.connected.frameworks.exceptions.{ContentMismatchException, SchemaMismatchException}
import org.apache.spark.sql.Dataset
import org.apache.spark.sql.functions.col
import com.connected.frameworks.testing.ErrorMessageFormatter._
import com.connected.frameworks.testing.DatasetSchemaComparator._

trait SparkDatasetTester {
  def assertDatasetEquality[T](actualDs: Dataset[T], expectedDs: Dataset[T], compareNulls: Boolean = false, compareColumnNames: Boolean = false, orderedComparision: Boolean = true): Unit = {


    val actualCount = actualDs.count()
    val expectedCount = expectedDs.count()
    if (actualCount != expectedCount) {
      throw SchemaMismatchException(
        countErrorMessageString(actualCount, expectedCount)
      )
    }

/*    if (!DatasetSchemaComparator.compareSchema(actualDs, expectedDs, compareNulls, compareColumnNames)) {
      throw SchemaMismatchException(
        schemaErrorMessageString(actualDs, expectedDs)
      )
    }*/

    if (!actualDs.isEqualSchema(expectedDs, compareNulls, compareColumnNames)) {
      throw SchemaMismatchException(
        schemaErrorMessageString(actualDs, expectedDs)
      )
    }

    if (orderedComparision) {
      val actualData = actualDs.collect()
      val expectedData = expectedDs.collect()
      if (!actualData.sameElements(expectedData)) {
        throw ContentMismatchException(
          dataErrorMessageString(actualData, expectedData)
        )
      }
    }
    else {
      val actualSorted = defaultDatasetSort(actualDs.toDF(actualDs.columns.mkString(",").replaceAll(" +", "").toLowerCase.split(","): _*)).collect
      val expectedSorted = defaultDatasetSort(expectedDs.toDF(expectedDs.columns.mkString(",").replaceAll(" +", "").toLowerCase.split(","): _*)).collect
      if (!actualSorted.sameElements(expectedSorted)) {
        throw ContentMismatchException(
          dataErrorMessageString(actualSorted, expectedSorted)
        )
      }
    }
  }

  def defaultDatasetSort[T](ds: Dataset[T]): Dataset[T] = {
    val colNames = ds.columns.sorted
    val cols = colNames.map(col)
    ds.sort(cols: _*)
  }
}
