package com.as.structurspedstreaming.kafka

import com.alibaba.fastjson.JSON
import com.as.structuredstreaming.kafka.DeviceData
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}

object IotStreaming {

  def main(args: Array[String]): Unit = {
    println(this.getClass.getName)
    val spark = SparkSession.builder().appName(this.getClass.getName).master("local[*]")
      .config("spark.sql.shuffle.partitions", 4)
      .getOrCreate()
    val sc = spark.sparkContext

    import spark.implicits._
    import org.apache.spark.sql.functions._

    sc.setLogLevel("WARN")

    val kafkaDF = spark.readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", "node1:9092,node2:9092,node3:9092")
      .option("subscribe", "iotTopic")
      .load()

    val data: Dataset[DeviceData] = kafkaDF.selectExpr("CAST(value AS STRING)").as[String]
      .map(value => {
        JSON.parseObject(value, classOf[DeviceData])
      })

    val device = data.where('signal > 30)

    val deviceType = device.groupBy('deviceType)
      .agg(
        count('device) as "num",
        avg('signal) as "avg_signal"
      ).orderBy('num.desc)


    device.writeStream
      .format("console")
      .outputMode("append")
      .start()

    deviceType.writeStream
      .format("console")
      .outputMode("complete")
      .start()
      .awaitTermination()

    spark.stop()


  }

}
