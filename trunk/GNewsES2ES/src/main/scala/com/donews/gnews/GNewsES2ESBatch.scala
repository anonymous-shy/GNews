package com.donews.gnews

import java.time.{Instant, LocalDate, LocalDateTime}
import java.util.{HashMap => JavaHashMap, List => JavaList}

import com.donews.utils.{GNewsUtil, RedisClusterHelper}
import org.apache.commons.lang3.StringUtils
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import org.elasticsearch.common.xcontent.XContentFactory
import org.elasticsearch.spark._
import org.elasticsearch.spark.rdd.EsSpark
import org.slf4j.{Logger, LoggerFactory}

import scala.collection.mutable.{ArrayBuffer, ListBuffer, HashMap => MutableHashMap}


/**
  * Created by Shy on 2017/11/14
  */

object GNewsES2ESBatch {

  val LOG: Logger = LoggerFactory.getLogger(getClass)

  def main(args: Array[String]): Unit = {
    LOG.info(s">>>>> [${getClass.getSimpleName}]开始时间[${LocalDateTime.now}] <<<<<")
    val ts = System.currentTimeMillis
    val conf = new SparkConf().setAppName(getClass.getSimpleName)
    conf.set("es.nodes", "spider_slave05,spider_slave06,spider_slave07")
    conf.set("es.port", "9200")
    conf.set("es.scroll.size", "1000")
    val sc = new SparkContext(conf)
    val read_index = "gnews_raw_data"
    val write_index = "gnews_valid_data"
    val types = Array[String]("article", "gallery", "video", "ec", "novel", "cartoon", "stock")
    var gteTimeStamp: String = null
    var ltTimeStamp: String = null
    if (args.length == 2) {
      gteTimeStamp = args(0)
      ltTimeStamp = args(1)
    } else if (args.length == 1) {
      gteTimeStamp = args(0)
      ltTimeStamp = StringUtils.substringBeforeLast(LocalDateTime.now.toString, ":")
    }
    val yearMonth = StringUtils.substringBeforeLast(LocalDate.now.toString, "-")
    val query =
      s"""
         |{
         |  "query":{
         |    "constant_score":{
         |      "filter":{
         |        "range":{
         |          "store_time":{
         |            "gte":"$gteTimeStamp",
         |            "lt":"$ltTimeStamp"
         |          }
         |        }
         |      }
         |    }
         |  }
         |}
      """.stripMargin
    println(query)
    for (doc_type <- types) {
      val queryRdd = sc.esRDD(s"${read_index}_$yearMonth/$doc_type", query)
      val resRdd = checkGNewsValid(queryRdd)
      EsSpark.saveToEs(resRdd, s"${write_index}_$yearMonth/$doc_type", Map("es.mapping.id" -> "doc_id"))
    }
    LOG.info(s">>>>> 写入es完成时间[${LocalDateTime.now}],耗时[${System.currentTimeMillis - ts}]毫秒. <<<<<")
  }

  def checkGNewsValid(news: RDD[(String, scala.collection.Map[String, AnyRef])]) = {
    news mapPartitions (iter => {
      val connection = RedisClusterHelper.getConnection
      val esClient = GNewsUtil.getEsClient
      val listBuffer = new ListBuffer[Map[String, Any]]
      iter.foreach(newsTuple => {
        try {
          var filterBoolean = true
          var error_meg = ""
          val _id = newsTuple._1
          val news = newsTuple._2
          // 获取字段类型
          val genre = news.getOrElse("article_genre", null).toString
          val news_mode = GNewsUtil.transNewsMode(genre)
          val shareurl = news.getOrElse("url", "").toString
          val oID = news.getOrElse("id", "").toString
          val sourceurl = if (oID.length > 2000 && oID.contains("?")) {
            oID.split("\\?")(0)
          } else oID
          val datasourceid = news.getOrElse("data_source_id", null)
          val source = news.getOrElse("media", null)
          val title = news.getOrElse("title", null)
          val content = news.getOrElse("parsed_content", null)
          val contenttext = news.getOrElse("parsed_content_main_body", null)
          val small_img_location = news.getOrElse("small_img_location", null)
          val img_location = news.getOrElse("img_location", null)
          val video_location = news.getOrElse("video_location", null)
          val ktags = news.getOrElse("tags", null)
          val keywords = if (ktags != null) {
            ktags.toString.replace("Buffer(", "").replace(")", "")
          } else ""

          val comment_count = news.getOrElse("comment_count", null)
          val commentcount = if (comment_count != null) comment_count else 0
          val like_count = news.getOrElse("like_count", null)
          val likecount = if (like_count != null) like_count else 0
          // val parsed_content_char_count: Long = news.getOrElse("parsed_content_char_count", 0).asInstanceOf[Long]
          val cnt1 = news.getOrElse("parsed_content_char_count", null)
          val parsed_content_char_count = if (cnt1 != null) cnt1.toString.toInt else 0
          // val small_img_location_count: Long = news.getOrElse("small_img_location_count", 0).asInstanceOf[Long]
          val cnt2 = news.getOrElse("small_img_location_count", null)
          val small_img_location_count = if (cnt2 != null) cnt2.toString.toInt else 0
          // val img_location_count: Long = news.getOrElse("img_location_count", 0).asInstanceOf[Long]
          val cnt3 = news.getOrElse("img_location_count", null)
          val img_location_count = if (cnt3 != null) cnt3.toString.toInt else 0
          // val video_location_count: Long = news.getOrElse("video_location_count", 0).asInstanceOf[Long]
          val cnt4 = news.getOrElse("video_location_count", null)
          val video_location_count = if (cnt4 != null) cnt4.toString.toInt else 0
          var extractTags: String = null
          var datavalid: Int = -2
          var thumbnailimglists: JavaList[JavaHashMap[String, Any]] = null
          var coverthumbnailimglists: JavaList[JavaHashMap[String, Any]] = null
          var videoList: List[Map[String, Any]] = null
          var duration: Int = 0
          val ctime: Long = Instant.now.getEpochSecond
          val ctimestr: String = StringUtils.substringBefore(LocalDateTime.now.toString, ".")
          // 补齐publishtime && 过滤发布时间大于抓取时间数据
          val timestamp: String = GNewsUtil.en2cnTimestamp(news.getOrElse("timestamp", "").toString)
          val publishtime: String = if (news.getOrElse("publish_time", null) != null) {
            val pt: String = GNewsUtil.en2cnTimestamp(news.getOrElse("publish_time", "").toString)
            if (pt > timestamp) {
              filterBoolean = false
              error_meg = "publish_time>timestamp||"
            }
            pt
          } else StringUtils.substringBefore(LocalDateTime.now.toString, ".")
          // 处理视频 新闻与视频
          if (genre != "gallery" && video_location != null && video_location_count > 0) {
            videoList = GNewsUtil.transVideoList(video_location, video_location_count)._1
            duration = GNewsUtil.transVideoList(video_location, video_location_count)._2
            if (duration < 5) {
              filterBoolean = false
              error_meg = "视频时长小于5秒||"
            }
          }
          // 处理图集
          if (small_img_location != null && small_img_location_count > 0) {
            thumbnailimglists = GNewsUtil.transImgList(small_img_location, small_img_location_count)
          }
          if (genre != "video" && img_location != null && img_location_count > 0) {
            coverthumbnailimglists = GNewsUtil.transImgList(img_location, img_location_count)
          }
          if (title.toString.length > 100) {
            filterBoolean = false
            error_meg = "title长度大于100||"
          }
          if (shareurl.length > 330) {
            filterBoolean = false
            error_meg = "shareurl长度大于330||"
          }
          if ("article".equals(genre)) {
            // 若文章字数小于150
            if (parsed_content_char_count < 150) {
              filterBoolean = false
              error_meg += "文章字数小于150字||"
            }
            // 剔除乱码数据
            if (title != null && contenttext != null && parsed_content_char_count >= 150) {
              val dirtyCode = GNewsUtil.checkArticleCode(title.toString, contenttext.toString)
              if (!dirtyCode) {
                filterBoolean = false
                error_meg = "文章乱码||"
              }
            }
            // 标签提取
            if (title != null && contenttext != null) {
              extractTags = GNewsUtil.extractTags(title.toString, contenttext.toString, news_mode.toString)
              // tags发送Redis热词队列
              val jsonArr = extractTags.split(",")
              val tags = ArrayBuffer[String]()
              jsonArr.foreach(tag => tags += StringUtils.substringBefore(tag, ":"))
              connection.lpush("DMT_NEW_QUEUE:TAGS", tags.toArray: _*)
              //去重 对比 2days 数据
              /*tagsIterator foreach { tags =>
                val similarScore = GNewsUtil.similarity(tags, extractTags)
                if (similarScore >= 0.85) {
                  filterBoolean = false
                  LOG.error(s"_id: ${_id}, meg: 文章相似度 > 0.85")
                }
              }*/
            }
            datavalid =
              if (small_img_location != null && content != null && contenttext != null) 1
              else if (small_img_location != null && content != null && contenttext == null) 2
              else if (small_img_location != null && content == null && contenttext != null) 3
              else if (small_img_location == null && content != null && contenttext != null) 4
              else if (small_img_location == null && content == null && contenttext != null) 5
              else if (small_img_location == null && content != null && contenttext == null) 6
              else if (small_img_location != null && content == null && contenttext == null) 0
              else {
                // 过滤 datavalid = -1 数据
                filterBoolean = false
                error_meg += "datavalid = -1||"
                -1
              }
          } else if ("gallery".equals(genre)) {
            datavalid = if (shareurl == null || datasourceid == null || title == null ||
              source == null || news.getOrElse("publish_time", null) == null || small_img_location == null || img_location == null) 0
            else 1
          } else if ("video".equals(genre)) {
            datavalid = if (shareurl == null || datasourceid == null || title == null ||
              source == null || news.getOrElse("publish_time", null) == null) 0
            else 1
          } else if ("cartoon".equals(genre) || "novel".equals(genre) || "ec".equals(genre)) {
            datavalid = if (small_img_location != null) 1
            else if (small_img_location == null) 4
            else {
              filterBoolean = false
              error_meg += "datavalid = -1||"
              -1
            }
          } else if ("stock".equals(genre)) {
            datavalid = if (content != null && contenttext != null) 1
            else if (content != null && contenttext == null) 2
            else if (content == null && contenttext != null) 3
            else if (content == null && contenttext == null) 0
            else {
              // 过滤 datavalid = -1 数据
              filterBoolean = false
              error_meg += "datavalid = -1||"
              -1
            }
          } else {
          }
          if (filterBoolean) {
            val resultMap = new MutableHashMap[String, Any]
            resultMap += ("doc_id" -> _id)
            resultMap += ("article_genre" -> genre)
            resultMap += ("newsmode" -> news_mode)
            resultMap += ("datasourceid" -> datasourceid)
            if (genre == "novel")
              resultMap += ("title" -> news.getOrElse("info_source", null))
            else resultMap += ("title" -> title)
            resultMap += ("source" -> source)
            resultMap += ("sourceurl" -> sourceurl)
            resultMap += ("shareurl" -> shareurl)
            resultMap += ("author" -> news.getOrElse("info_source", null))
            resultMap += ("content" -> content)
            resultMap += ("contenttext" -> contenttext)
            resultMap += ("thumbnailimglists" -> thumbnailimglists)
            if (genre == "gallery") {
              resultMap += ("imglists" -> coverthumbnailimglists)
              resultMap += ("status" -> datavalid)
            }
            else {
              resultMap += ("coverthumbnailimglists" -> coverthumbnailimglists)
              resultMap += ("status" -> 1)
            }
            resultMap += ("imgcount" -> img_location_count)
            resultMap += ("commentcount" -> commentcount)
            resultMap += ("likecount" -> likecount)
            resultMap += ("tags" -> extractTags) // tags提取
            resultMap += ("keywords" -> keywords)
            resultMap += ("ctime" -> ctime)
            resultMap += ("ctimestr" -> ctimestr)
            resultMap += ("utime" -> ctime)
            resultMap += ("utimestr" -> ctimestr)
            resultMap += ("timestamp" -> timestamp)
            resultMap += ("publishtime" -> publishtime)
            resultMap += ("store_time" -> GNewsUtil.en2cnTimestamp(news.getOrElse("store_time", "").toString))
            resultMap += ("img_dispose" -> 1)
            resultMap += ("datavalid" -> datavalid)
            resultMap += ("datasourcesubid" -> news.getOrElse("data_source_sub_id", null))
            resultMap += ("sourcesubname" -> news.getOrElse("sub_channel", null))
            resultMap += ("videolists" -> videoList)
            resultMap += ("videotime" -> duration)
            resultMap += ("batch_id" -> news.getOrElse("batch_id", null))
            resultMap += ("saved_data_location" -> news.getOrElse("saved_data_location", null))
            listBuffer += resultMap.toMap
          } else {
            val errRes = esClient.prepareIndex("gnews_error_valid_data", genre, _id)
              .setSource(XContentFactory.jsonBuilder()
                .startObject()
                .field("doc_id", _id)
                .field("article_genre", genre)
                .field("parsed_content_char_count", parsed_content_char_count)
                .field("shareurl", shareurl)
                .field("timestamp", timestamp)
                .field("publishtime", publishtime)
                .field("store_time", GNewsUtil.en2cnTimestamp(news.getOrElse("store_time", "").toString))
                .field("batch_id", news.getOrElse("batch_id", null))
                .field("error_meg", error_meg)
                .endObject())
              .get
            LOG.error(s">>>>> Error Index: ${errRes.getResult},_id: ${_id},Error Message: $error_meg <<<<<")
          }
        } catch {
          case ex: Exception => LOG.error(s"${ex.printStackTrace()}")
        }
      })
      esClient.close()
      listBuffer.toIterator
    })
  }
}
