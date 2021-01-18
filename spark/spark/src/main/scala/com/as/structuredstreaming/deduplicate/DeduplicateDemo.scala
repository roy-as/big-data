package com.as.structuredstreaming.deduplicate

import com.alibaba.fastjson.JSON
import com.google.gson.Gson
import org.apache.spark.sql.{Dataset, SparkSession}

object DeduplicateDemo {

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().appName(this.getClass.getSimpleName).master("local[*]")
      .config("spark.sql.shuffle.partitions", 4)
      .getOrCreate()
    val sc = spark.sparkContext

    sc.setLogLevel("WARN")

    import spark.implicits._

    val df = spark.readStream
      .format("socket")
      .option("host", "node1")
      .option("port", 9999)
      .load()

    val ds: Dataset[Student] = df.as[String].map(value => {
      JSON.parseObject(value, classOf[Student])
    })

    val deduplicate = ds.dropDuplicates("name", "sex")

    val result = deduplicate
      .groupBy('sex)
      .count()

    deduplicate.writeStream
        .format("console")
        .outputMode("update")
        .start()

    result.writeStream
      .format("console")
      .outputMode("complete")
      .start()
      .awaitTermination()

    spark.stop()

  }

}

case class Student(name: String, age: Int, sex: String)
