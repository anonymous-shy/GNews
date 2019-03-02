package com.donews.streaming

import com.donews.utils.MysqlUtils

import scala.collection.immutable.HashMap


/**
  * Donews Company
  * Created by Liu dh 
  * 2017/10/20.15:13
  */
object Constant {
  val FIELD_TYPE_STR:String="str"
  val FIELD_TYPE_INT:String="int"
  val ES_INDEX_KEY:String = "index_of_es"
  val ES_TYPE_KEY:String = "type_of_es"
  val ES_ID_KEY:String = "record_id"
  val FIELD_ERROR_KEY:String = "error_message"
  final val LOCAL_TIME_ZONE=8
  final val LONGTEXT_LENGTH:Double = Math.pow(2,32)-1
  final val MEDIUMTEXT_LENGTH:Double = Math.pow(2,24)-1
  final val TEXT_LENGTH:Double = Math.pow(2,16)-1
  final val TAGS_KEY_ARR_NUM = 10000
  final val RDS_SIMI_TAGS_PREFIX:String = "GNEWS_SIMI_TAGS"

  val ARTICLE_GENRE = HashMap[String,Int]("article"->101,"gallery"->102,"video"->103)
}
