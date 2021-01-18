package com.as.structuredstreaming.event

import java.sql.Timestamp

import org.apache.commons.lang3.StringUtils
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.streaming.Trigger

object EventDemo {

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().appName(this.getClass.getName).master("local[*]")
      .config("spark.sql.shuffle.partitions", 4)
      .getOrCreate()
    val sc = spark.sparkContext

    import spark.implicits._
    import org.apache.spark.sql.functions._

    sc.setLogLevel("WARN")

    val df = spark.readStream
      .format("socket")
      .option("host", "node1")
      .option("port", 9999)
      .load()
    val data = df.as[String].filter(StringUtils.isNotBlank(_)).map(value => {
      val arr = value.split(",")
      (Timestamp.valueOf(arr(0)), arr(1))
    }).toDF("timestamp", "word")

    val result = data.withWatermark("timestamp", "10 seconds")
      .groupBy(
      window('timestamp, "10 seconds", "5 seconds"),
      'word
    ).count()

    result.writeStream
      .format("console")
      .outputMode("update")
      .option("truncate", false)
      .trigger(Trigger.ProcessingTime("5 seconds"))
      .start()
      .awaitTermination()

    spark.stop()


  }

}
