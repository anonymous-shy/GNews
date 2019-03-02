package com.donews.gnews

import java.time.{Instant, LocalDate, LocalDateTime}
import java.util.concurrent.TimeUnit
import java.util.{HashMap => JavaHashMap, List => JavaList}

import com.donews.utils.{GNewsUtil, RedisClusterHelper}
import org.apache.commons.lang3.StringUtils
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import org.elasticsearch.common.xcontent.XContentFactory
import org.elasticsearch.spark._
import org.slf4j.{Logger, LoggerFactory}

import scala.collection.mutable.{ArrayBuffer, ListBuffer, HashMap => MutableHashMap}
import scala.util.control.Breaks._

/**
  * Created by Shy on 2017/11/14
  */

object GNewsES2ES_T {

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
    val doc_type = "article"
    val yearMonth = StringUtils.substringBeforeLast(LocalDate.now.toString, "-")
    val query =
      s"""
         |{
         |  "query":{
         |    "constant_score":{
         |      "filter":{
         |        "range":{
         |          "store_time":{
         |            "gte":"2017-11-17T17:00",
         |            "lt":"2017-11-17T17:10"
         |          }
         |        }
         |      }
         |    }
         |  }
         |}
          """.stripMargin

    val query2days =
      s"""
         |{
         |  "query":{
         |    "constant_score":{
         |      "filter":{
         |        "range":{
         |          "store_time":{
         |            "gte":"2017-11-17T16:00",
         |            "lt":"2017-11-17T16:30"
         |          }
         |        }
         |      }
         |    }
         |  }
         |}
          """.stripMargin
    val queryRdd = sc.esRDD(s"${read_index}_$yearMonth/$doc_type", query)

    val tagsRdd: RDD[(String, String)] = sc.esRDD(s"${write_index}_$yearMonth/$doc_type", query2days) map { news =>
      val tags = news._2.getOrElse("tags", "").asInstanceOf[String]
      val doc_id = news._2.getOrElse("doc_id", "").asInstanceOf[String]
      (doc_id, tags)
    }
    val tagsIterator = tagsRdd.toLocalIterator.toArray
    val BTags: Broadcast[Array[(String, String)]] = queryRdd.sparkContext.broadcast(tagsIterator)
    val tags = BTags.value
    val resRdd = checkGNewsValid(queryRdd, tags)
    //      EsSpark.saveToEs(resRdd, s"${write_index}_$yearMonth/$doc_type", Map("es.mapping.id" -> "doc_id"))
    val rcnt = resRdd.count()
    LOG.info(s">>>>> tagsRdd.count[${tags.length}],resRdd.count[$rcnt]. <<<<<")
    LOG.info(s">>>>> 写入es完成时间[${LocalDateTime.now}],耗时[${System.currentTimeMillis - ts}]毫秒. <<<<<")
    BTags.destroy()
  }

  def checkGNewsValid(news: RDD[(String, scala.collection.Map[String, AnyRef])], oldTags: Array[(String, String)]) = {
    news mapPartitions (iter => {
      val connection = RedisClusterHelper.getConnection

      val listBuffer = new ListBuffer[Map[String, Any]]
      iter.foreach(newsTuple => {

        var filterBoolean = true
        var error_meg = ""
        val _id = newsTuple._1
        val news = newsTuple._2
        // 获取字段类型
        val genre = news.getOrElse("article_genre", null).toString
        val news_mode = GNewsUtil.transNewsMode(genre)
        val shareurl = news.getOrElse("url", null)
        val datasourceid = news.getOrElse("data_source_id", null)
        val source = news.getOrElse("media", null)
        val title = news.getOrElse("title", null)
        val content = news.getOrElse("parsed_content", null)
        val contenttext = news.getOrElse("parsed_content_main_body", null)
        val small_img_location = news.getOrElse("small_img_location", null)
        val img_location = news.getOrElse("img_location", null)
        val video_location = news.getOrElse("video_location", null)

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
        val ctimestr: String = StringUtils.substringBefore(Instant.now.plusMillis(TimeUnit.HOURS.toMillis(8)).toString, ".")
        // 补齐publishtime && 过滤发布时间大于抓取时间数据
        val timestamp: String = GNewsUtil.en2cnTimestamp(news.getOrElse("timestamp", "").toString)
        val publishtime: String = if (news.getOrElse("publish_time", null) != null) {
          val pt: String = GNewsUtil.en2cnTimestamp(news.getOrElse("publish_time", "").toString)
          if (pt > timestamp) {
            filterBoolean = false
            error_meg = "publish_time>timestamp||"
            break
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
        if (filterBoolean) {
          // 标签提取
          if (title != null && contenttext != null) {
            extractTags = GNewsUtil.extractTags(title.toString, contenttext.toString, news_mode.toString)
            LOG.error(s"提取标签内容: [$extractTags]")
            // tags发送Redis热词队列
            val jsonArr = extractTags.split(",")
            val tags = ArrayBuffer[String]()
            jsonArr.foreach(tag => tags += StringUtils.substringBefore(tag, ":"))
            connection.lpush("DMT_NEW_QUEUE:TAGS", tags.toArray: _*)
            // 去重 对比 2days 数据
            oldTags foreach  { tagsT =>
              val cmpTags = tagsT._2
              LOG.info(s"获取过去标签内容: [$cmpTags]")
              val similarScore = GNewsUtil.similarity(cmpTags, extractTags)
              LOG.info(s"计算标签得分: [$similarScore]")
              if (similarScore >= 0.85) {
                filterBoolean = false
                LOG.error(s"_id: ${_id}, meg: 文章相似度 > 0.85")
              }
            }
          }
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
          resultMap += ("sourceurl" -> news.getOrElse("id", null))
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
          resultMap += ("shareurl" -> shareurl)
          resultMap += ("imgcount" -> img_location_count)
          resultMap += ("commentcount" -> commentcount)
          resultMap += ("likecount" -> likecount)
          resultMap += ("tags" -> extractTags) // tags提取
          resultMap += ("keywords" -> news.getOrElse("tags", null))
          resultMap += ("ctime" -> ctime)
          resultMap += ("ctimestr" -> ctimestr)
          resultMap += ("utime" -> ctime)
          resultMap += ("utimestr" -> ctimestr)
          resultMap += ("timestamp" -> timestamp)
          resultMap += ("publishtime" -> publishtime)
          resultMap += ("img_dispose" -> 1)
          resultMap += ("datavalid" -> datavalid)
          resultMap += ("datasourcesubid" -> news.getOrElse("data_source_class_id", null))
          resultMap += ("sourcesubname" -> news.getOrElse("data_source_class", null))
          resultMap += ("subchannel" -> news.getOrElse("sub_channel", null))
          resultMap += ("videolists" -> videoList)
          resultMap += ("videotime" -> duration)
          resultMap += ("batch_id" -> news.getOrElse("batch_id", null))
          resultMap += ("saved_data_location" -> news.getOrElse("saved_data_location", null))
          listBuffer += resultMap.toMap
        } else {
          //          val errRes = esClient.prepareIndex("gnews_error_valid_data", genre, _id)
          //            .setSource(XContentFactory.jsonBuilder()
          //              .startObject()
          //              .field("doc_id", _id)
          //              .field("article_genre", genre)
          //              .field("parsed_content_char_count", parsed_content_char_count)
          //              .field("shareurl", shareurl)
          //              .field("timestamp", timestamp)
          //              .field("publishtime", publishtime)
          //              .field("batch_id", news.getOrElse("batch_id", null))
          //              .field("error_meg", error_meg)
          //              .endObject())
          //            .get
          LOG.error(s">>>>> _id: ${_id},Error Message: $error_meg <<<<<")
        }

      })
      listBuffer.toIterator
    })
  }
}
