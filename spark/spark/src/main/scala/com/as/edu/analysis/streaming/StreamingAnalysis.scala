package com.as.edu.analysis.streaming

import com.as.edu.bean.Answer
import com.google.gson.Gson
import org.apache.spark.sql.SparkSession

object StreamingAnalysis {

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().appName(this.getClass.getSimpleName).master("local[*]")
      .config("spark.sql.shuffle.partitions", 10)
      .getOrCreate()
    val sc = spark.sparkContext
    sc.setLogLevel("WARN")
    import org.apache.spark.sql.functions._
    import spark.implicits._

    val kafkaDF = spark.readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", "node1:9092,node2:9092,node3:9092")
      .option("subscribe", "edu3")
      //.option("startingOffsets", """{"education2":{"0":-1,"1":-1,"2":-1}}""")
      .load()


    val answerDS = kafkaDF.selectExpr(
      "CAST(value AS STRING)"
    ).as[String].map(value => {
      val gson = new Gson()
      gson.fromJson(value, classOf[Answer])
    })

    // 最活跃年届top10
    val result1 = answerDS.groupBy('grade_id)
      .count()
      .orderBy('count.desc)
      //.limit(10)

    // top10热点题
    val result2 = answerDS.groupBy('question_id)
      .count()
      .orderBy('count.desc)
      .limit(10)

    val result3 = answerDS.orderBy('score)
      .groupBy('student_id)
      .agg(
        first('question_id),
        min('score) as "min_score"
      )
      .orderBy('min_score.desc)
      .limit(10)

    // 每位学生最低分top10
    val result4 = answerDS.groupBy('student_id, 'question_id)
      .agg(
        min('score) as "min_score"
      ).orderBy('min_score)
      .limit(10)

    // 最热题及科目top10
    val result5 = answerDS.groupBy('question_id)
      .agg(
        count('question_id) as "count",
        first('subject_id)
      ).orderBy('count.desc)
      .limit(10)


    result1.writeStream
      .format("console")
      .outputMode("complete")
      .option("checkpointLocation", "./backup")
      .start()
      .awaitTermination()

//    result2.writeStream
//      .format("console")
//      .outputMode("complete")
//      .start()
//      .awaitTermination()
//
//    result3.writeStream
//      .format("console")
//      .outputMode("complete")
//      .start()
//
//    result4.writeStream
//      .format("console")
//      .outputMode("complete")
//      .start().awaitTermination()
//
//    result5.writeStream
//      .format("console")
//      .outputMode("complete")
//      .start()
//      .awaitTermination()

    spark.stop()
  }

}
