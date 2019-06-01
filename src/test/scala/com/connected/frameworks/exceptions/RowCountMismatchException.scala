package com.connected.frameworks.exceptions

/**
  * This class is used to throw custom message in case of no of records are not equal.
  * @param message [[String]] containing message to be shown with Exception.
  */
case class RowCountMismatchException(message: String) extends Exception(message)