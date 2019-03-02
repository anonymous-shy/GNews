package com.donews.streaming

import com.donews.utils.MysqlUtils

import scala.collection.immutable.HashMap


/**
  * Donews Company
  * Created by Liu dh 
  * 2017/10/20.15:13
  */
object Constant {
  val ES_INDEX_KEY:String = "index_of_es"
  val ES_TYPE_KEY:String = "type_of_es"
  val ES_ID_KEY:String = "record_key"
  val FIELD_ERROR_KEY:String = "error_message"
  val ARTICLE_GENRE = HashMap[String,Int]("article"->101,"gallery"->102,"video"->103)
}
