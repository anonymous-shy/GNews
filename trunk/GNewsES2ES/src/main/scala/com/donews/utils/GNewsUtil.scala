package com.donews.utils

import java.net.InetAddress
import java.text.SimpleDateFormat
import java.time.LocalDateTime
import java.util.Locale

import com.alibaba.fastjson.JSON
import org.apache.commons.lang3.StringUtils
import org.elasticsearch.client.transport.TransportClient
import org.elasticsearch.common.settings.Settings
import org.elasticsearch.common.transport.InetSocketTransportAddress
import org.elasticsearch.transport.client.PreBuiltTransportClient
import org.slf4j.LoggerFactory
import redis.clients.jedis.JedisCluster
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
  private val qualified_rate = 0.5
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
    //        val reg = "[^\u4e00-\u9fa5|\\pP|a-z|A-Z|0-9|\\sS]"
    val reg =
      """[^\u4e00-\u9fa5|\pP|a-z|A-Z|0-9|\sS|+——！，。？：、’~@#￥%……&*（）《》【】“”「®·\.\!\/_,:$%^*()+\"\'-{;}\n@]"""
    //    val title_len = title.replaceAll(reg, "").length.toFloat
    //    if (title_len / title.length < qualified_rate) return false
    val transContent = if (content.contains("\uFEFF"))
      content.replace('\uFEFF', ' ')
    else content
    val sample_content = getContentSample(transContent)
    //    val sample_content = getContentSample(content)
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
    val map = collection.mutable.Map[String, Float]()
    if ("".equals(vec) || vec == null)
      map.toMap
    else {
      val vecArr = vec.split(",")
      vecArr foreach { word =>
        val ws = word.split(":")
        map += (ws(0) -> ws(1).toFloat)
      }
      map.toMap
    }
  }

  /**
    * 文章打分
    *
    * @param vecMap
    * @return
    */
  def mod(vecMap: Map[String, Float]): Double = {
    var score = 0.0
    vecMap.values foreach { s =>
      score += s * s
    }
    //返回x的平方根
    math.sqrt(score)
  }

  /**
    * 计算相似度
    * *余弦相似度*
    * （1）使用 TF-IDF 算法，找出两篇文章的关键词；
      （2）每篇文章各取出若干个关键词（比如 20 个），合并成一个集合，计算每篇文章对于这个集合中的词的词频（为了避免文章长度的差异，可以使用相对词频）；
      （3）生成两篇文章各自的词频向量；
      （4）计算两个向量的余弦相似度，值越大就表示越相似。
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

  /*def sendData2Kafka(topic: String, jsonData: String) {
    val props = new Properties()
    props.put("metadata.broker.list", "slave01:9092, slave02:9092, slave03:9092")
    props.put("serializer.class", "kafka.serializer.StringEncoder")
    props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer")
    props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer")
    //props.put("partitioner.class", "example.producer.SimplePartitioner")
    props.put("request.required.acks", "1")
    //创建Kafka ProducerConfig
    val config = new ProducerConfig(props)
    //create Producer instance
    val producer: Producer[String, String] = new Producer[String, String](config)
    //def this(topic: String, key: K, message: V) 主题, partition, message
    val message = new KeyedMessage[String, String](topic, jsonData)
    producer.send(message)
    producer.close()
  }*/

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

  // 判断Redis中TS
  def setLastTimeStamp(connection: JedisCluster, gnewsLastTimeStampKey: String, timestamp: String) {
    val lastTimeStamp: String = if (connection.get(gnewsLastTimeStampKey) != null)
      connection.get(gnewsLastTimeStampKey)
    else "2017-11-01T00:00:00"
    if (timestamp > lastTimeStamp) {
      connection.set(gnewsLastTimeStampKey, timestamp)
    }
  }

  // 获取 Redis 队列中上次更新时间戳
  def getLastTimeStamp(gnewsLastTimeStampKey: String): String = {
    val connection = RedisClusterHelper.getConnection
    val lastTimeStamp: String = if (connection.get(gnewsLastTimeStampKey) != null)
      connection.get(gnewsLastTimeStampKey)
    else StringUtils.substringBefore(LocalDateTime.now.minusMinutes(10).toString, ".")
    lastTimeStamp
  }

  def transNewsMode(genre: String): Int = genre match {
    case "article" => 1
    case "gallery" => 2
    case "video" => 3
    case "ec" => 6
    case "novel" => 7
    case "cartoon" => 8
    case "stock" => -1
    case _ => 0
  }

  def getEsClient: TransportClient = {
    val settings = Settings.builder
      .put("cluster.name", "tagtic-es-cluster")
      .put("transport.type", "netty3")
      .put("http.type", "netty3")
      .build
    val client = new PreBuiltTransportClient(settings)
      .addTransportAddress(new InetSocketTransportAddress(InetAddress.getByName("spider_slave05"), 9300))
      .addTransportAddress(new InetSocketTransportAddress(InetAddress.getByName("spider_slave06"), 9300))
      .addTransportAddress(new InetSocketTransportAddress(InetAddress.getByName("spider_slave07"), 9300))
    client
  }

  def transImgList(img_location: Any, img_location_count: Int): JavaList[JavaHashMap[String, Any]] = {
    // val img_list = img_location.asInstanceOf[MutableBuffer[MutableHashMap[String, Any]]]
    val img_list = img_location match {
      case x: MutableBuffer[MutableHashMap[String, Any]] => img_location.asInstanceOf[MutableBuffer[MutableHashMap[String, Any]]]
      case y: MutableBuffer[MLinkedHashMap[String, Any]] => img_location.asInstanceOf[MutableBuffer[MLinkedHashMap[String, Any]]]
    }
    val aList = new JavaArrayList[JavaHashMap[String, Any]]()
    img_list foreach { m =>
      val m1 = new JavaHashMap[String, Any]()
      m1.put("title", m.getOrElse("img_desc", ""))
      m1.put("filepath", m.getOrElse("img_path", ""))
      m1.put("width", m.getOrElse("img_width", 0))
      m1.put("height", m.getOrElse("img_height", 0))
      m1.put("shareurl", m.getOrElse("img_src", ""))
      aList.add(m1)
    }
    // 若图集数量大于100则取前100
    val picsList = if (aList.size() > 101) aList.subList(0, 101) else aList
    /*// 写入Redis Queue KEY = "img_task_processor"
      val resMap = new JavaHashMap[String, Any]()
      resMap.put("news_id", _id)
      resMap.put("news_mode", news_mode)
      resMap.put("small_img_count", small_img_location_count)
      resMap.put("img_location", coverthumbnailimglists)
      val jsonData = JSON.toJSONString(resMap, false)
      connection.lpush("img_task_processor", jsonData)*/
    picsList
  }

  def transVideoList(video_location: Any, video_location_count: Long): (List[Map[String, Any]], Int) = {
    var duration: Int = 0
    val videoList: ListBuffer[Map[String, Any]] = new ListBuffer[Map[String, Any]]
    val video_list = video_location.asInstanceOf[MutableBuffer[MutableHashMap[String, Any]]]
    for (i <- video_list.indices) {
      if (i == 0) {
        val video_duration = video_list(i).getOrElse("video_duration", "0").asInstanceOf[String]
        duration = GNewsUtil.videoTime2Sec(video_duration)
      }
      videoList += Map("content" -> video_list(i).getOrElse("video_desc", ""),
        "shareurl" -> video_list(i).getOrElse("video_src", ""),
        "filepath" -> video_list(i).getOrElse("video_path", ""),
        "duration" -> video_list(i).getOrElse("video_duration", ""),
        "videowidth" -> video_list(i).getOrElse("video_width", 0),
        "videoheight" -> video_list(i).getOrElse("video_height", 0))
    }
    (videoList.toList, duration)
  }

  def main(args: Array[String]): Unit = {
    val t1 = "人民币市场汇价（11月22日）"

    val c1 = "```````````````````````````````````````````````````````````````````````````````````````````````````````````````````````````````````````````````````````````````````````````````````````````````````````````````````````````````````````````````````````````````````````````````````````````"
    val c2 = "日志聚合是YARN提供的日志中央化管理功能，它能将运行完成的Container/任务日志上传到HDFS上，从而减轻NodeManager负载，且提供一个中央化存储和分析机制"
    val c3 = "England-born opener Matt Renshaw, 21, is left out in favour of uncapped 24-year-old Cameron Bancroft, while batsman Shaun Marsh, 34, earns a recall and is set to bat at number six."
    val c4 = "일전 도문시 수남촌의 빈곤호부축 사업이 잘 추진된다는 소문에 기자는 직접 찾아갔다.</p><p>수남촌은 도문시 석현진에 위치해있다. 이 마을의 빈곤상황에 대해 현지 간부들이 몇개 층면을 나누어 소개했다. 토지가 "
    val c5 = "原标题:拜金女同时和多男交往，直到最后才发现...\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF提醒：请在WIFI下观看，土豪随意！"
    val c6 = "原标题:随州一男子路上捡到旧轮胎，带回家一看震惊了！点击随州散咵关注我哟☀ 传递随州方言文化，尽在随州散咵。不定期发布随州新闻、阿光散咵、国内热点事件等精彩内容。商业推广、新闻爆料请加微信：sxw2345每次家里有旧的轮胎你是不是都着急扔掉呢如果家里有旧的轮胎可别扔了哦因为你会后悔的现在就跟小编来看旧轮胎如何大变身 沙发椅准备材料：旧轮胎，圆木板，麻绳，电钻，热胶枪，刷子螺丝钉、透明胶、固体胶、螺丝、剪刀等先用电钻在两块木板上各钻出四个孔然后用螺丝刀把螺丝钉入木板这样就可以将轮胎和木板固定住了然后把固体胶装入热胶枪融化在木板中间再将麻绳从木板中间一圈圈盘起盘的时候记得用热胶枪固定麻绳不要等盘好了再固定没有固定的话麻绳很容易散开一直这样盘下来底部可以不用盘最后用剪刀剪掉多余的麻绳再在表面刷上透明胶即可如果加上几个脚再涂上一些颜色就会像下面的椅子这样哦用轮胎做椅子涂上鲜艳的颜色放在小院满满的田园风格套上好看的布配上抱枕你看得出它的前身是轮胎吗放到阳台或者是休闲咖啡厅都很有味道简单涂上颜色加上坐垫、靠垫和抱枕相信你会一见钟情的要是有足够的轮胎弄上一套家具都没问题连体桌椅这个创意也是棒棒哒景区特色客栈这样弄也不错哦想和你喝一个下午茶听你述说故事清新简约风大概就是这样吧这样置物篮好想要啊做法可以像前面那样哦只不过这个上盖不用固定和加了一个里布狗狗的小窝现在才知道洗手台可以这样有趣几个轮胎堆起来妥妥的垃圾桶截掉一半就成了小孩的玩具弄上几个孔就成了雨伞架了还不用担心水会滴在房间里哦新年装饰挂在树上真的不要太美了挂这么多在墙上都成一个花篮展览区了轮胎杯子为园艺点zan融入到城市中现代与创意感特别浓装饰成一个水桶种花是永不枯竭的意思吗种种菜也是挺实用的垒起来的轮胎涂成五颜六色实在太养眼在轮胎上画画再种上自己喜欢的花爱好这东西什么都挡不住老司机们，赶紧拿旧轮胎装饰你们的家吧！\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF\uFEFF"
    val c7 = "新华社北京１１月２２日电　中国外汇交易中心１１月２２日受权公布人民币对美元、欧元、日元、港元、英镑、澳元、新西兰元、新加坡元、瑞士法郎、加元、林吉特、卢布、兰特、韩元、迪拉姆、里亚尔、福林、兹罗提、丹麦克朗、瑞典克朗、挪威克朗、里拉及比索的市场汇价。１１月２２日人民币汇率中间价如下：１００美元　　　　　６６２．９０人民币１００欧元　　　　　７７８．３６人民币１００日元　　　　　５．８９９３人民币１００港元　　　　　８４．８６８人民币１００英镑　　　　　８７８．０４人民币１００澳大利亚元　　５０２．６６人民币１００新西兰元　　　４５３．５６人民币１００新加坡元　　　４８９．６７人民币１００瑞士法郎　　　６６８．９９人民币１００加拿大元　　　５１８．７３人民币１００人民币　　　　６２．３４５马来西亚林吉特１００人民币　　　　８９１．８３俄罗斯卢布１００人民币　　　　２１０．８５南非兰特１００人民币　　　　１６４７２韩元１００人民币　　　　５５．４０４阿联酋迪拉姆１００人民币　　　　５６．５７３沙特里亚尔１００人民币　　　　４０２５．２２匈牙利福林１００人民币　　　　５４．２１３波兰兹罗提１００人民币　　　　９５．６２丹麦克朗１００人民币　　　　１２７．１６瑞典克朗１００人民币　　　　１２３．９２挪威克朗１００人民币　　　　５９．７５２土耳其里拉１００人民币　　　　２８３．８３墨西哥比索+1【纠错】  责任编辑： 刘阳"
    val c8 = "爱情机遇颇好，极易在街头偶遇昔日心仪的对象，单身者若能大胆与其攀谈，你们有所发展的机率很高哦;今天能尝到勤俭节约带来的甜头;前段时间在工作上的辛苦耕耘，今天总算看到了些希望。总运: ★★★★☆爱情: ★★★★★财运: ★★★☆☆工作: ★★★☆☆金牛座爱情需要两个人共同经营，夫妻在沟通时意见不合的机率高，需要你先做出让步，才不会发生剧烈的争执。喜爱购物的人今天是不错的好机会，许多幸运的事情正在向你招手。亮丽的打扮也能为好运加分哦!总运: ★★★☆☆爱情: ★★☆☆☆财运: ★★★★☆工作: ★★★☆☆双子座桃花运极强，今天有参加婚宴的朋友大可不必把自己当成配角，其实你也将是一段良缘的主角。财运良好，会有大赚一笔的机会，瞅准时机，果断出手。求职者不妨多出去走走，会有好机会。总运: ★★★★☆爱情: ★★★★★财运: ★★★★☆工作: ★★★★☆巨蟹座想法颇多，会有不安全感，对身边的人、事、物感觉敏锐，稍有风吹草动你都会有所察觉，工作也易受到影响。做好自己的事情就好，别想得太复杂。有什么心事、压力可以与父母长辈或另一半交流，会让你很快释然。总运: ★★☆☆☆爱情: ★★☆☆☆财运: ★★★☆☆工作: ★★☆☆☆狮子座财运较普通，必要时应积极争取自己的劳动所得。工作不是那么顺利，因得失而苦恼，与你同病相怜的患难之交，成为今天收获的意外财富。爱情运相对较好，内心郁闷者不妨找另一半谈谈，让内心的积雪彻底融化。总运: ★☆☆☆☆爱情: ★★☆☆☆财运: ★☆☆☆☆工作: ★☆☆☆☆处女座今天很积极地做事，为他人付出，可收获甚微，会让你想不通。付出不一定有回报，抛开得失心会让你释怀。内心苦闷不妨多与另一半交流，对方很乐意为你分担，会让你感到温暖。总运: ★★☆☆☆爱情: ★★★☆☆财运: ★★★☆☆工作: ★☆☆☆☆天秤座浪漫的咖啡馆是今天约会的幸运地，能够相互产生情愫，瞬间来电，爱恋立刻升温。财运不错，对时机的把握相当敏锐，所以赚取钱财较顺利。工作上今天非常适合开始新计划，扩展新业务，有很不错的机会等着你。总运: ★★★★★爱情: ★★★★☆财运: ★★★★☆工作: ★★★★★天蝎座与人群接触的机会多，有机会在众人面前即兴表演节目，展现才华。机会来时别错过，因此交到知己的机率也颇高。求职者在求职时遇到挫折别丧气，在招聘网有针对性地投几份简历，会有不错的回馈。总运: ★★★☆☆爱情: ★★☆☆☆财运: ★★★☆☆工作: ★★★★☆射手座爱情平淡而和睦，双方都能保持一种平静的心态相处，比较融洽。工作表现比较出色，敏锐的直觉让你轻易做出新的突破，得到上级的肯定与赞赏。晚上有机会与专业人士共同讨论生财之道。总运: ★★★★☆爱情: ★★★☆☆财运: ★★★☆☆工作: ★★★★☆摩羯座对着镜子说“茄子”，然后用这张脸去面对每一个人，自然气氛就亲和融洽了;恋爱上也不妨积极一点，即使对方一时没什么回应，但是保守却只会让幸福溜走;新目标的订立，将会让你在公司过得更充实。总运: ★★★☆☆爱情: ★★★☆☆财运: ★★★★☆工作: ★★★☆☆水瓶座爱情方面，很想突现自己强势的地位，易给对方带来压抑感，沟通会有阻碍;财产需要及时的打理，有计划的消费才能达到收支的平衡;工作时有机会偷懒，忙里偷闲能让你缓解压力，但不可过于松懈。总运: ★★★☆☆爱情: ★★☆☆☆财运: ★★★☆☆工作: ★★☆☆☆双鱼座与另一半有约会应按时赴约，没时间就别随便答应对方的要求，以免发生不愉快的争吵。有许多繁琐的工作需要尽快处理，应沉静下来认真对待，心情浮躁会让你频频出错。购物时多留意商标和生产日期，小心买到过期商品。总运: ★★☆☆☆爱情: ★★☆☆☆财运: ★★☆☆☆"
    val arr = Array(c1, c2, c3, c4, c5, c6, c7, c8)
    for (c <- arr) {
      println(checkArticleCode(t1, c))
    }
  }
}
