package com.donews.streaming

import java.security.MessageDigest
import java.time.{LocalDate, LocalDateTime}
import java.util
import java.util.Properties

import com.fasterxml.jackson.databind.{JsonNode, ObjectMapper}
import com.fasterxml.jackson.databind.node.ObjectNode
import org.apache.spark.{SparkConf, SparkContext}
import org.slf4j.LoggerFactory
import com.donews.utils.CmdArg
import org.apache.spark.sql.SQLContext
import org.elasticsearch.spark._
import org.elasticsearch.spark.rdd.EsSpark

import scala.collection.JavaConversions._
case class UrlInfo(url:String)
object GnewsValidate {

  val LOG = LoggerFactory.getLogger(GnewsValidate.getClass)


  def main(args: Array[String]): Unit = {
    val validate_info =  CmdArg.parse(args)
    val path = validate_info.path
    val day = validate_info.day
    val next_day = LocalDate.parse(day).plusDays(1).toString
    val hdfs_path ="hdfs://HdfsHA"+ path

    val conf = new SparkConf()
    conf.set("es.nodes", "spider_slave05,spider_slave06,spider_slave07")
    conf.set("es.port", "9200")
    conf.set("es.scroll.size", "1000")
    val sc = new SparkContext(conf)
    val sqlContext = new SQLContext(sc)
    import sqlContext.implicits._


    val hdfs_day_day = sc.textFile(hdfs_path).map { line => {
      GnewsDataService.forJson(line,day)
    }}.filter(_!=null).map{bean=>UrlInfo(bean.url)}.toDF().distinct()

    hdfs_day_day.registerTempTable("hdfs_data")

    val yearMonth = day.substring(0,7)
    val valid_index:String = s"gnews_raw_data_full_$yearMonth"
    val error_index = "gnews_error_raw_data_full"

   hdfs_day_day.show(20,false)

    val query =
      s"""
         |{
         |  "query":{
         |    "constant_score":{
         |      "filter":{
         |        "range":{
         |          "timestamp":{
         |            "gte":"${day}T00:00:00",
         |            "lt":"${next_day}T00:00:00"
         |          }
         |        }
         |      }
         |    }
         |  }
         |}
          """.stripMargin
     val valid_es_df =  sc.esRDD(valid_index,query).map{es_data=>
        val timestamp = es_data._2.get("timestamp").get.toString
        val url = es_data._2.get("shareurl").get.toString
         GnewsBean(url,timestamp)
      }.toDF()

    valid_es_df.show(50,false)
    val error_es_df =  sc.esRDD(error_index,query).map{es_data=>
      val timestamp = es_data._2.get("timestamp").get.toString
      val url = es_data._2.get("url").get.toString
      GnewsBean(url,timestamp)
    }.toDF()

    error_es_df.show(50,false)
    val es_df = valid_es_df.unionAll(error_es_df).selectExpr("url AS es_url").distinct()
    es_df.registerTempTable("es_data")
    val result_df = sqlContext.sql("select d.url from (select h.url,e.es_url from hdfs_data h left join es_data e on h.url=e.es_url) d where d.es_url is null")
    result_df.show(500,false)




//    val queryRdd = sc.esRDD(valid_index, query)





  }











}