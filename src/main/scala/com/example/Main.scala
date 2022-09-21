package com.example

import org.apache.spark.sql.{Dataset, Row, SaveMode}
import org.apache.spark.sql.functions.lit
import org.apache.spark.sql.cassandra._
import com.datastax.spark.connector._

import java.lang

object Main extends SparkSessionWrapper {

  def main(args: Array[String]): Unit = {
    println("APP Name : " + spark.sparkContext.appName)
    println("Deploy Mode : " + spark.sparkContext.deployMode)
    println("Master : " + spark.sparkContext.master)
    println("sparkUser : " + spark.sparkContext.sparkUser)

    val rangeDf: Dataset[lang.Long] = spark.range(1000 * 1000 * 1000)
    println("rangeDf.count() = " + rangeDf.count())

    val yoloDf: Dataset[Row] = spark.range(1000).withColumn("yolo_col", lit("you only live once")).repartition(3)
    yoloDf.show(10, truncate = false)


    val outputFormats = List("csv", "orc", "parquet")
    outputFormats foreach { f =>
      yoloDf.write.format(f)
        .option("spark.sql.orc.impl", "native")
        .option("header", "false")
        .option("delimiter", ";")
        .mode(SaveMode.Overwrite)
        .save(s"hdfs://quickstart.cloudera:8020/tmp/yoloData2/$f")
    }

  }
}
