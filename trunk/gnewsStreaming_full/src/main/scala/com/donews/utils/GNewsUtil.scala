package com.donews.utils

import java.net.InetAddress
import java.text.SimpleDateFormat
import java.time.LocalDateTime
import java.util.Locale

import com.alibaba.fastjson.JSON
import com.donews.streaming.GnewsDataService
import com.fasterxml.jackson.databind.node.ObjectNode
import org.apache.commons.lang3.StringUtils
import org.slf4j.LoggerFactory
import redis.clients.jedis.JedisCluster

import scala.collection.mutable
//import kafka.producer.{KeyedMessage, Producer, ProducerConfig}
import java.util.{ArrayList => JavaArrayList, HashMap => JavaHashMap, List => JavaList}

import org.apache.http.client.entity.UrlEncodedFormEntity
import org.apache.http.client.methods.{CloseableHttpResponse, HttpPost}
import org.apache.http.impl.client.HttpClients
import org.apache.http.message.BasicNameValuePair
import org.apache.http.util.EntityUtils

import scala.collection.mutable.{ListBuffer, Buffer => MutableBuffer, HashMap => MutableHashMap, LinkedHashMap => MLinkedHashMap}


/**
  * Created by Shy on 2017/10/23
  */

object GNewsUtil {

  val LOG = LoggerFactory.getLogger(getClass)
  // 合格率
  private val qualified_rate = 0.8
  // 抽取样本的比率
  private val sample_rate = 0.3

  private def getContentSample(content: String): String = {
    val content_len = content.length
    val sample_len = (content_len * sample_rate).toInt
    val head_len = sample_len / 3
    val mid_len = sample_len / 3
    val tail_len = sample_len - head_len - mid_len
    val head = content.substring(0, head_len)
    val mid_position = content_len / 2
    val mid = content.substring(mid_position, mid_position + mid_len)
    val tail = content.reverse.substring(0, tail_len)
    return head + mid + tail
  }

  //传入未被转码过的文章标题和文章内容
  def checkArticleCode(title: String, content: String): Boolean = {
    val reg = "[^\u4e00-\u9fa5|\\pP|a-z|A-Z|0-9]"
    val title_len = title.replaceAll(reg, "").length.toFloat
    if (title_len / title.length < qualified_rate) return false
    val sample_content = getContentSample(content)
    val sample_len = sample_content.replaceAll(reg, "").length.toFloat
    if (sample_len / sample_content.length < qualified_rate)
      return false
    true
  }

  /**
    * 标签提取
    *
    * @param title
    * @param contenttext
    * @param newsmode
    * @return 红米pro:0.7049,骁龙660:0.2773,全面屏:0.2495,再曝:0.2311,mah:0.2107,猛料:0.2062,99元:0.175,小米:0.132
    */
  def extractTags(title: String, contenttext: String, newsmode: String): String = {
    val httpClient = HttpClients.createDefault
    val inner_interface = "http://10.44.153.64/process/getResults"
    val post = new HttpPost(inner_interface)

    import java.util.{ArrayList => JavaArrayList}

    val nvps = new JavaArrayList[BasicNameValuePair]()
    nvps.add(new BasicNameValuePair("title", title))
    nvps.add(new BasicNameValuePair("contenttext", contenttext))
    nvps.add(new BasicNameValuePair("newsmode", newsmode))
    val data = new UrlEncodedFormEntity(nvps, "utf-8")
    post.setEntity(data)
    val response: CloseableHttpResponse = httpClient.execute(post)
    try {
      val entity = response.getEntity
      val result = EntityUtils.toString(entity, "utf-8")
      val jSONObject = JSON.parseObject(result).get("tags").asInstanceOf[String]
      jSONObject
    } finally {
      response.close()
      httpClient.close()
    }
  }

  /**
    * 将提取tags转为 Map[String, Float]("生鲜电商" -> 0.5786)
    * 生鲜电商:0.5786,易果生鲜:0.3921,冷链物流:0.2591,安鲜达:0.2156,冷链:0.1656,生鲜冷链:0.1644,生鲜:0.112,门槛:0.108
    *
    * @param vec
    * @return
    */
  def vec2Map(vec: String): Map[String, Float] = {
    val map = mutable.Map[String, Float]()
    if ("".equals(vec) || vec == null)
      map.toMap
    else {
      val vecArr = vec.split(",")
      vecArr foreach { word =>
        val ws = word.split(":")
        if(ws.length>=2)
            map += (ws(0) -> ws(1).toFloat)
      }
      map.toMap
    }
  }

  /**
    * 转化得分
    *
    * @param vecMap
    * @return
    */
  def mod(vecMap: Map[String, Float]): Double = {
    var score = 0.0
    vecMap.values foreach { s =>
      score += s * s
    }
    Math.sqrt(score)
  }

  /**
    * 计算相似度
    *
    * @param tag1
    * @param tag2
    * @return
    */
  def similarity(tag1: String, tag2: String): Double = {
    var similarScore = 0.0
    val m1 = vec2Map(tag1)
    val m2 = vec2Map(tag2)
    if (m1.isEmpty || m2.isEmpty) {
      similarScore
    } else {
      val interSet = m1.keySet & m2.keySet
      if (interSet.isEmpty)
        similarScore
      else {
        val unionSet = m1.keySet ++ m2.keySet
        var sum = 0.0
        unionSet foreach { key =>
          if (m1.contains(key) && m2.contains(key)) {
            sum += m1(key) * m2(key)
          }
        }
        val mod1 = mod(m1)
        val mod2 = mod(m2)
        similarScore = sum / (mod1 * mod2)
        similarScore
      }
    }
  }



  def en2cnTimestamp(en_timestamp: String): String = {
    val en_sdf = new SimpleDateFormat("EEE MMM dd HH:mm:ss z yyyy", Locale.ENGLISH)
    val cn_sdf = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss")
    cn_sdf.format(en_sdf.parse(en_timestamp))
  }

  // 视频时长转化 s
  def videoTime2Sec(duration: String): Int = {
    val ds = duration.split(":")
    var videoSec: Int = 0
    if (ds.size == 3)
      videoSec = (ds(0) * 3600 + ds(1) * 60 + ds(2) * 1).toInt
    else if (ds.size == 2)
      videoSec = (ds(0) * 60 + ds(1) * 1).toInt
    else if (ds.size == 1)
      videoSec = if (ds(0).contains(".")) ds(0).split("\\.")(0).toInt else ds(0).toInt
    videoSec
  }


  def main(args: Array[String]): Unit = {
    val tags1:String = "百度外卖:0.81,裁撤:0.2914,代理商:0.192,渠道城市经理:0.1871,部分城:0.1757,渠道:0.1326,经理:0.1185,更名:0.1084"
    val tags2:String = "百度外卖:0.7425,渠道城市经理:0.4572,裁撤:0.2594,饿了么:0.1708,猎云网:0.1501,代理商:0.129,渠道:0.1111,经理:0.0869"



    val value =  similarity(tags1,tags2)
//    val json_str4=""
//    val messageNode = JsonNodeUtils.getJsonNodeFromStringContent(json_str4).asInstanceOf[ObjectNode]
//    val contenttext_node = GnewsDataService.getFieldNode(messageNode,"parsed_content_main_body")
//    val title = messageNode.get("title").toString
//
//    val dirtyCode = !GNewsUtil.checkArticleCode(title.toString, contenttext_node.textValue())
    println(value)
  }

}
