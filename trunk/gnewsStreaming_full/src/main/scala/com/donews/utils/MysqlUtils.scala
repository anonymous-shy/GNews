package com.donews.utils

import java.sql.{Connection, DriverManager}
import java.text.SimpleDateFormat
import java.time.LocalDateTime
import java.util
import java.util.Properties

import com.donews.streaming.GnewsDataService
import com.mysql.jdbc.Driver

import scala.collection.mutable.ArrayBuffer
/**
  * Donews Company
  * Created by Liu dh 
  * 2017/9/18.17:29
  */
object MysqlUtils {
  private val driver = "com.mysql.jdbc.Driver"
  private val url = "jdbc:mysql://mysql-database/niuer_news"
  private val username = "kafka2es"
  private val password = "roxipCWK(0}C~a"
  private val encoding = "utf-8"

  private var properties:Properties = null
  Class.forName(driver)

  def getProperties():Properties = {
    if(null == properties){
      Class.forName(driver)
      properties = new Properties()
      var connection:Connection = null
      try {
        connection = DriverManager.getConnection(url, username, password)
        val statement = connection.createStatement()
        val resultSet = statement.executeQuery("select props_name, props_value from niuer_propertis")
        while ( resultSet.next() ) {
          val name = resultSet.getString("props_name")
          val value = resultSet.getString("props_value")
          properties.setProperty(name,value)
//          println("name, value = " + name + ", " + value)
        }
      } catch {
        case e => e.printStackTrace

      }
      connection.close()
    }

    properties
  }


  def getBlackList():Array[String]={
    val list = new ArrayBuffer[String]()
    var connection:Connection = null
    try {
      connection = DriverManager.getConnection(url, username, password)
      val statement = connection.createStatement()
      val resultSet = statement.executeQuery("select field_name, relation, keyword, field_type from gnews_blacklist")
      while ( resultSet.next() ) {
        val field_name = resultSet.getString("field_name")
        val relation = resultSet.getString("relation")
        val keyword = resultSet.getString("keyword")
        val field_type = resultSet.getString("field_type")
        //三个下划线做分割
        var black_info:String=s"${field_name}___${relation}___${keyword}___$field_type"
//        println(black_info)
        list.append(black_info)
      }
    } catch {
      case e => e.printStackTrace

    }
    connection.close()
    list.toArray[String]
  }

  def getSourceWhiteList():Array[String]={
    val list = new ArrayBuffer[String]()
    var connection:Connection = null
    try {
      connection = DriverManager.getConnection(url, username, password)
      val statement = connection.createStatement()
      val resultSet = statement.executeQuery("select white_db_table from gnews_whitelist")
      while ( resultSet.next() ) {
        val white_db_table = resultSet.getString("white_db_table")
//        println(white_db_table)
        list.append(white_db_table)
      }
    } catch {
      case e => e.printStackTrace

    }
    connection.close()
    list.toArray[String]
  }


  def getSensetiveWords():Array[String]={
    val list = new ArrayBuffer[String]()
    var connection:Connection = null
    try {
      connection = DriverManager.getConnection(url, username, password)
      val statement = connection.createStatement()
      val resultSet = statement.executeQuery("select words from cms_sensitive_words where status=1")
      while ( resultSet.next() ) {
        val words = resultSet.getString("words")
        //        println(white_db_table)
        list.append(words)
      }
    } catch {
      case e => e.printStackTrace

    }
    connection.close()
    list.toArray[String]
  }

  def getTopicESIndexes():Array[String]={
    val list = new ArrayBuffer[String]()
    var connection:Connection = null
    try {
      connection = DriverManager.getConnection(url, username, password)
      val statement = connection.createStatement()
      val resultSet = statement.executeQuery("select topic,es_index,es_error_index,redis_task_prefix from topic_index")
      while ( resultSet.next() ) {
        val topic = resultSet.getString("topic")
        val es_index = resultSet.getString("es_index")
        val es_error_index = resultSet.getString("es_error_index")
        val redis_task_prefix = resultSet.getString("redis_task_prefix")
        val topic_es_info = s"${topic}##${es_index}::${es_error_index}::$redis_task_prefix"
        list.append(topic_es_info)
      }
    } catch {
      case e => e.printStackTrace
    }
    connection.close()
    list.toArray[String]
  }

  def main(args: Array[String]): Unit = {


//    println(LocalDateTime.now().minusHours(9).getHour)
//    val day:String = "2018-04-20"
//    println(day.replaceAll("-",""))
    val sdf = new  SimpleDateFormat("yyyy-MM-dd HH:mm:ss")

    val str:String = "nihao::hello"
    val values = str.split("::")
    println(values)

//   val str = "北湖:0.4187,郴州市:0.3766,业主:0.3579,开发商:0.2552,法院:0.2383,商品房买卖:0.2206,案件:0.1839,湖南省--ca6ff1e39cc0f922a7c12c470b73a6a6"
//     val values_arr=str.split("--")
//    val old_tag = values_arr(0); val data_index = values_arr(1)
//    val similarScore = GNewsUtil.similarity(old_tag, "回宿舍:0.3502,流量不限量:0.321,燕燕:0.3057")
//    println(similarScore)
  }



}
