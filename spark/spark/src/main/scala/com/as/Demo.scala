package com.as

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}

object Demo {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().appName(this.getClass.getSimpleName).master("local[*]")
      .getOrCreate()
    val sc = spark.sparkContext
    sc.setLogLevel("WARN")


    val rdd: RDD[String] = sc.textFile("./file/score")

    val row = rdd.map(value => {
      val arr = value.split(",")
      Row(arr(0).toInt, arr(1), arr(2).toInt)
    })

    val df = spark.createDataFrame(row, StructType(List(
      StructField("sid", IntegerType, false),
      StructField("qid", StringType, false),
      StructField("score", IntegerType, false),
    )))

    import org.apache.spark.sql.functions._

    df.orderBy("score").groupBy("sid")
      .agg(
        min("score") as "min_score",
        first("qid")
      ).show()

    df.groupBy("sid").agg(
      min("score") as "score"
    ).join(df, Array("sid", "score")).show()

    spark.close()


  }
}
