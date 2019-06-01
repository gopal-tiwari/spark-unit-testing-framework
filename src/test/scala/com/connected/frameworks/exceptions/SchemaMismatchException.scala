package com.connected.frameworks.exceptions

/**
  * This class is used to throw custom message in case of table structure is not same.
  * @param message [[String]] containing message to be shown with Exception.
  */
case class SchemaMismatchException(message: String) extends Exception(message)