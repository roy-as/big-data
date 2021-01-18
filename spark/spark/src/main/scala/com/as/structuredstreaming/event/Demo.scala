package com.as.structuredstreaming.event

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}

object Demo {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().appName(this.getClass.getName).master("local[*]").getOrCreate()
    val sc = spark.sparkContext
    sc.setLogLevel("WARN")

    import spark.implicits._
    import org.apache.spark.sql.functions._
    val rdd: RDD[Row] = sc.textFile("./file/score").map(value => {
      println(value)
      val arr = value.split(",")
      println(arr.toBuffer)
      Row(arr(0).toInt, arr(1), arr(2).toInt)
    })

    val fields: List[StructField] = List(
      StructField("sid", IntegerType, false),
      StructField("qid", StringType, false),
      StructField("score", IntegerType, false)
    )


    val df = spark.createDataFrame(rdd, StructType(fields))

    val result: DataFrame = df.groupBy('sid)
      .agg(
        min('score as "min_score"),
        first('qid)
      )

    val orderDf = df.orderBy('score)
    val result1 = orderDf.groupBy('sid)
      .agg(
        min('score as "min_score"),
        first('qid)
      )
    println("*************")
    result1.show()
    println("*************")
    result.show()
    spark.stop()
  }


}
