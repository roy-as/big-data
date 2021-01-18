package com.as.edu.analysis.streaming

import java.lang
import java.util.Properties

import com.alibaba.fastjson.JSON
import com.as.edu.bean.Answer
import com.as.edu.utils.RedisUtil
import com.google.gson.Gson
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.ml.recommendation.ALSModel
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}
import org.apache.spark.streaming.dstream.InputDStream
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.streaming.kafka010.{ConsumerStrategies, KafkaUtils, LocationStrategies}

object RecommendStreaming {

  def main(args: Array[String]): Unit = {

    val ssc = StreamingContext.getOrCreate("./recommend", getStreamingContext)
    //val ssc = getStreamingContext()
    ssc.start()
    ssc.awaitTermination()
    ssc.stop(true, true)

  }

  def getStreamingContext(): StreamingContext = {

    val spark = SparkSession.builder().appName(this.getClass.getSimpleName).master("local[*]")
      .config("spark.sql.shuffle.partitions", 4)
      .getOrCreate()
    val sc = spark.sparkContext

    sc.setLogLevel("WARN")

    import org.apache.spark.sql.functions._
    import spark.implicits._

    val ssc = new StreamingContext(sc, Seconds(5))

    val kafkaConfig = Map(
      "bootstrap.servers" -> "node1:9092", //kafka集群地址
      "key.deserializer" -> classOf[StringDeserializer], //key的反序列化规则
      "value.deserializer" -> classOf[StringDeserializer], //value的反序列化规则
      "group.id" -> "StreamingRecommend", //消费者组名称
      "auto.offset.reset" -> "latest",
      "auto.commit.interval.ms" -> "1000", //自动提交的时间间隔
      "enable.auto.commit" -> (true: lang.Boolean) //是否自动提交
    )

    val kafkaDstream: InputDStream[ConsumerRecord[String, String]] = KafkaUtils.createDirectStream[String, String](
      ssc,
      LocationStrategies.PreferConsistent,
      ConsumerStrategies.Subscribe[String, String](List("roy3"), kafkaConfig)
    )

    val valueStream = kafkaDstream.map(_.value())

    valueStream.foreachRDD(rdd => {
      if (!rdd.isEmpty()) {
        val string2Int = udf((str: String) => {
          str.split("_")(1).toInt
        })
        // 获取jedis
        val jedis = RedisUtil.pool.getResource

        val path = jedis.hget("als_model", "recommended_question_id")
        // 加载模型
        val model = ALSModel.load(path)

        val df = rdd.coalesce(1).map(str => {
          val gson = new Gson()
          gson.fromJson(str, classOf[Answer])
        }).toDF

        val ids: DataFrame = df.select(string2Int('student_id) as "student_id")

        val recommend = model.recommendForUserSubset(ids, 10)

        recommend.show(false)

        print(recommend.count())

        if (recommend.count() > 0) {
          val recommendDF = recommend.as[(String, Array[(Int, Float)])].map(t => {
            val sid = "学生ID_" + t._1
            val questions = t._2.map("题目ID_" + _._1).mkString(",")
            (sid, questions)
          }).toDF("student_id", "recommendations")

          val result = df.join(recommendDF, "student_id")

          if (result.count() > 0) {
            val prop = new Properties()
            prop.setProperty("user", "root")
            prop.setProperty("password", "idsbg123.")
            result.write
                .mode(saveMode = SaveMode.Append)
              .jdbc("jdbc:mysql://119.3.169.76:3306/edu", "t_answer", prop)
          }
        }
        jedis.close()

      }
    })

    ssc


  }

}
