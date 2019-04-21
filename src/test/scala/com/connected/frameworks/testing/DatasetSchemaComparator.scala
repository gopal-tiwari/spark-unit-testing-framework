package com.connected.frameworks.testing

import org.apache.spark.sql.Dataset

object DatasetSchemaComparator {

  implicit class CheckInt[T](val actualDs: Dataset[T]) extends AnyVal {

    def isEqualSchema(expectedDs: Dataset[T], compareNulls: Boolean = false, compareColumnNames: Boolean = false): Boolean = {
      if (actualDs.schema.length != expectedDs.schema.length)
        false
      else {
        val structFields = actualDs.schema.zip(expectedDs.schema)
        structFields.forall {
          schemaPair =>
            (schemaPair._1.dataType == schemaPair._2.dataType) &&
              ((schemaPair._1.name.toLowerCase == schemaPair._2.name.toLowerCase) || compareColumnNames) &&
              ((schemaPair._1.nullable == schemaPair._2.nullable) || compareNulls)
        }
      }
    }
  }
}
