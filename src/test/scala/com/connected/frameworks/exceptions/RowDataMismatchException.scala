package com.connected.frameworks.exceptions

/**
  * This class is used to throw custom message in case of row data mismatch.
  *
  * @param message [[String]] containing message to be shown with Exception.
  */
case class RowDataMismatchException(message: String) extends Exception(message)
