package com.as.edu.bean

case class Rating(
                   student_id: Long, //学生id
                   question_id: Long, //题目id
                   rating: Float //推荐指数
                 ) extends Serializable

