package com.as.structuredstreaming.kafka

import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}

object StationStreaming {

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().appName(this.getClass.getName).master("local[*]")
      .config("spark.sql.shuffle.partitions", "4")
      .getOrCreate()
    val sc = spark.sparkContext
    sc.setLogLevel("WARN")
    import spark.implicits._

    val kafkaDF: DataFrame = spark.readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", "node1:9092,node2:9092,node3:9092")
      .option("subscribe", "stations")
      .load()

    val kafkaDS: Dataset[String] = kafkaDF.selectExpr("CAST(value AS STRING)").as[String]

    val result: Dataset[String] = kafkaDS.filter(_.contains("success"))


    result.writeStream
      .format("kafka")
      .option("kafka.bootstrap.servers", "node1:9092,node2:9092,node3:9092")
      .option("topic", "etlTopic")
      .option("checkpointLocation", "./checkpoint")
      .start()
      .awaitTermination()

    spark.stop()
  }

}
