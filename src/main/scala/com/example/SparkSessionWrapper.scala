package com.example

import org.apache.spark.sql.SparkSession

trait SparkSessionWrapper extends Serializable {

  // Check how to make the create spark session
  lazy val spark: SparkSession = ???

}