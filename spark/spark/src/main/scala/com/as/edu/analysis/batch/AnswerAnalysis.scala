package com.as.edu.analysis.batch

import java.util.Properties

import com.as.edu.bean.AnswerWithRecommendations
import org.apache.spark.sql.{DataFrame, SparkSession}

object AnswerAnalysis {

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().appName(this.getClass.getSimpleName).master("local[*]")
      .config("spark.sql.shuffle.partitions", 10)
      .getOrCreate()

    val sc = spark.sparkContext
    sc.setLogLevel("WARN")

    val prop = new Properties()

    prop.setProperty("user", "root")
    prop.setProperty("password", "idsbg123.")
    import spark.implicits._
    import org.apache.spark.sql.functions._

    val ds = spark.read
      .jdbc("jdbc:mysql://119.3.169.76:3306/edu", "t_answer", prop)
      .as[AnswerWithRecommendations]

    val top50 = ds.groupBy('question_id)
      .count()
      .orderBy('count.desc)
      .limit(50)

    //top50.show(false)

    top50.join(ds.dropDuplicates("question_id"), "question_id")
      .groupBy('subject_id)
      .agg(
        count('*) as "hot"
      ).orderBy('hot.desc)
    //.show()

    val top20 = ds.groupBy('question_id)
      .count()
      .orderBy('count.desc)
      .limit(20)

    // top20.show(false)


    top20.join(ds, "question_id").show()

    val result1 = top20.join(ds, "question_id")
      .select('subject_id, explode(split('recommendations, ",")) as "question_id")
      .dropDuplicates("question_id")
      .groupBy('subject_id)
      .agg(
        count('*) as "num"
      ).orderBy('num.desc)
    top20.join(ds, "question_id")
      .select(explode(split('recommendations, ",")) as "question_id")
      .dropDuplicates("question_id").coalesce(1).write.csv("./file/result1")
    //.show(false)

    println("test1", top20.join(ds, "question_id")
      .select('subject_id, explode(split('recommendations, ",")) as "question_id")
      .dropDuplicates("question_id").count())

    val questionId = top20.join(ds, "question_id")
      .select(
        explode(split('recommendations, ",")) as "question_id"
      ).dropDuplicates("question_id")

    println("test2", questionId.count())

    questionId.join(ds.dropDuplicates("question_id"), "question_id")
      .groupBy('subject_id)
      .agg(
        count('*) as "nums"
      ).orderBy('nums.desc)
      .show()


    spark.stop()


  }

}
