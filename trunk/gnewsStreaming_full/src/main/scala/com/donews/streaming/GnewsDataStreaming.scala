package com.donews.streaming

import java.time.{LocalDate, LocalDateTime}
import java.util
import java.util.{Properties, Timer, TimerTask}

import com.donews.utils._
import com.fasterxml.jackson.databind.{JsonNode, ObjectMapper}
import com.fasterxml.jackson.databind.node.{JsonNodeType, ObjectNode, TextNode}
import kafka.common.TopicAndPartition
import kafka.message.MessageAndMetadata
import kafka.serializer.StringDecoder
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.streaming.{Milliseconds, StreamingContext}
import org.apache.spark.streaming.kafka._
import org.elasticsearch.spark.rdd.EsSpark
import org.slf4j.LoggerFactory
import Constant._
import org.apache.spark.broadcast.Broadcast
import java.util.{LinkedHashMap => LKMap,ArrayList => AList}
import redis.clients.jedis.JedisCluster

import scala.collection.JavaConversions._

object GnewsDataStreaming {

  val LOG = LoggerFactory.getLogger(GnewsDataStreaming.getClass)
  val HOUR_MILLISECONDS:Long = 3600*1000L
  val FIVE_MILLISECONDS:Long = 300*1000L
  def main(args: Array[String]): Unit = {
    val properties = MysqlUtils.getProperties()
    val prefix = properties.getProperty("prefix")
    val topicIndexMap = buildTopicIndexMap()
    println(topicIndexMap)
    val topics: Set[String] = topicIndexMap.keySet().toSet
    LOG.info("topicsSet.toString()===>"+topics.toString())

    val max_rate = properties.getProperty("kafka2es_stream_max_rate")
    val duration_secs = properties.getProperty("kafka2es_strea_duration_secs").toInt
    val news_mode_map = getNewsModeMap(properties)
    LOG.info(s"######## duration_milli=$duration_secs ###########")
    val es_nodes = properties.getProperty("es_nodes")



    val conf = new SparkConf()
      .setAppName("Gnews")
      .set("spark.driver.port", "18080")
      .set("spark.streaming.kafka.maxRatePerPartition" ,s"$max_rate")
      //      .set("spark.streaming.backpressure.enabled","true")
      .set("spark.streaming.concurrentJobs" ,s"2")
      .set("spark.streaming.stopGracefullyOnShutdown","true")
      .set("es.mapping.date.rich","false")
      .set("es.index.read.missing.as.empty","true")

    val options = Map("es.index.auto.create" -> "true",
      "pushdown" -> "true",
      "es.nodes" -> es_nodes,
      "es.port" -> "9200",
      "es.http.timeout" -> "2m",
      "es.mapping.id" -> ES_ID_KEY)


    val ssc = new StreamingContext(conf, Milliseconds(duration_secs*1000))
    val BROKER_LIST = properties.getProperty("kafka_broker_list")

    var minuteBrdcst = ssc.sparkContext.broadcast[Int](GnewsDataService.getNowMinuteNo())
    //定期更新黑名单
    var blacklistBrdcst:Broadcast[Array[String]] =
      ssc.sparkContext.broadcast[Array[String]](MysqlUtils.getBlackList())

    var whitelistBrdcst:Broadcast[Array[String]] =
      ssc.sparkContext.broadcast[Array[String]](MysqlUtils.getSourceWhiteList())

    var senseWordsBrdcst:Broadcast[Array[String]] =
      ssc.sparkContext.broadcast[Array[String]](MysqlUtils.getSensetiveWords())

    var tags_redis_flag:Int = 0
    var tags_rds_flg_Broadcast:Broadcast[Int] = ssc.sparkContext.broadcast[Int](tags_redis_flag)
    //2018-04-02 去掉去重逻辑
    //2018-05-22 打开去重逻辑
    //2018-06-05 灵活控制去重逻辑
    val quchong = GnewsDataService.getQuchongValue()
    if(quchong.equals("1")){
      println("###########  执行去重 #############")
      GnewsDataService.nquerryOldTags(ssc.sparkContext,options,tags_rds_flg_Broadcast)
    }else {
      println("###########  不执行去重 #############")
    }

    val kafkaParam = Map[String, String](
      "metadata.broker.list" -> BROKER_LIST,
      "auto.offset.reset" -> "smallest",
      "fetch.message.max.bytes"->s"${15*1024*1024}"
    )
    //这个会将 kafka 的消息进行 transform，最终 kafka 的数据都会变成 (topic_name, message) 这样的 tuple
    val messageHandler = (mmd: MessageAndMetadata[String, String]) => (mmd.topic, mmd.message())
    val kafkaHelper = new KafkaClusterHelper(kafkaParam)
    val article_min_words = Integer.parseInt(properties.getProperty("article_min_words"))
    topics.foreach{topic=>
      val indexInfo = topicIndexMap.get(topic)
      val indexValues = indexInfo.split("::")
      val ES_INDEX = prefix+indexValues(0)
      val ES_ERROR_INDEX = prefix+indexValues(1)
      val redis_task_prefix = prefix+indexValues(2)
      println(s"topic=$topic   index=$ES_INDEX   error_index=$ES_ERROR_INDEX")

      val fromOffsets = ZookeeperHelper.loadOffsets(Set(topic), kafkaHelper.getFromOffsets(kafkaParam, Set(topic)),prefix)
      val kafkaStream =  KafkaUtils
        .createDirectStream[String, String, StringDecoder, StringDecoder, (String, String)](ssc, kafkaParam, fromOffsets, messageHandler)
      var offsetRanges = Array[OffsetRange]()

      kafkaStream.transform { rdd =>
        offsetRanges = rdd.asInstanceOf[HasOffsetRanges].offsetRanges //得到该rdd对应kafka的消息的 offset
        rdd
      }.map(msg => msg._2).foreachRDD { rdd =>

        val minute = GnewsDataService.getNowMinuteNo()
        val distance = Math.abs(minute-minuteBrdcst.value)
        val format_topic = GnewsDataService.getFormalTopic()
        if(topic.equals(format_topic) && distance>=10 && distance<=50){//至少间隔10分钟才更新
          println(s"######## last_minute=${minuteBrdcst.value} current_minute=$minute minute distance: $distance")
          minuteBrdcst.unpersist()
          minuteBrdcst = ssc.sparkContext.broadcast[Int](minute)

          println("####### update blacklist #######")
          blacklistBrdcst.unpersist()
          blacklistBrdcst =
            ssc.sparkContext.broadcast[Array[String]](MysqlUtils.getBlackList())

          println("####### update whitelist #######")
          whitelistBrdcst.unpersist()
          whitelistBrdcst
            = ssc.sparkContext.broadcast[Array[String]](MysqlUtils.getSourceWhiteList())

          println("####### update oldTags #######")
          val n_tags_rds_flg_value = (tags_rds_flg_Broadcast.value+1)%3
          tags_rds_flg_Broadcast = ssc.sparkContext.broadcast[Int](n_tags_rds_flg_value)
          //2018-04-02 去掉去重逻辑
          //2018-05-22加上去重逻辑

          val quchong = GnewsDataService.getQuchongValue()
          if(quchong.equals("1")){
            println("###########  执行去重 #############")
            GnewsDataService.nquerryOldTags(ssc.sparkContext,options,tags_rds_flg_Broadcast)
          }
          println("####### update sense words #######")
          senseWordsBrdcst =
            ssc.sparkContext.broadcast[Array[String]](MysqlUtils.getSensetiveWords())
        }
        val ret_rdd = rdd.mapPartitions(kvmessages=> {
          mapPartitionsFunc(redis_task_prefix,
            news_mode_map,
            blacklistBrdcst,
            whitelistBrdcst,
            tags_rds_flg_Broadcast,
            senseWordsBrdcst,
            ES_INDEX,
            ES_ERROR_INDEX,
            kvmessages,article_min_words,topic)
        })

        val es_rdd = ret_rdd.filter(!_.get("need2redis").asInstanceOf[Boolean])
        es_rdd.cache()
        EsSpark.saveToEs(es_rdd,s"{$ES_INDEX_KEY}/{$ES_TYPE_KEY}",options)

        //对TopicName进行offset的存储
        val nextOffsets=offsetRanges.map(x => (TopicAndPartition(x.topic, x.partition), x.untilOffset)).toMap
        ZookeeperHelper.storeOffsets(nextOffsets,prefix)

      }
    }
    ssc.start() // 真正启动程序
    ssc.awaitTermination() //阻塞等待
  }

  private def mapPartitionsFunc(redis_task_prefix: String, news_mode_map: util.HashMap[String, Int],
                                blacklistBrdcst: Broadcast[Array[String]], whitelistBrdcst: Broadcast[Array[String]],
                                tags_rds_flg_Broadcast: Broadcast[Int], senseWordsBrdcst: Broadcast[Array[String]],
                                ES_INDEX: String, ES_ERROR_INDEX: String,
                                kvmessages: Iterator[String],article_min_words:Int,topic:String):Iterator[util.HashMap[String,Object]] = {
    val redis = RedisClusterHelper.getConnection
    val messages = kvmessages.map(msg => {
      //将数据转换成jsonNode格式，便于数据验证
      val messageNode = datajson2JsonNode(msg)

      if (messageNode.hasNonNull("article_genre")) {
        addExtraInfo(messageNode, news_mode_map)
        common_validate(messageNode, ES_INDEX, ES_ERROR_INDEX, redis, tags_rds_flg_Broadcast,article_min_words,topic)
      } else {
        messageNode.put(ES_TYPE_KEY, "error_data_type")
        GnewsDataService.addErrorInfo(messageNode, ES_ERROR_INDEX, "the field article_genre is Null!")
      }

      val formal_topic = GnewsDataService.getFormalTopic()
      //验证黑名单白名单逻辑
      if(topic.equals(formal_topic))//只有正式的topic才验证
         validateBWlist(ES_ERROR_INDEX, blacklistBrdcst, whitelistBrdcst, messageNode)
      //为数据生成es的id
      messageNode.put(Constant.ES_ID_KEY, produceRecordId(messageNode, ES_ERROR_INDEX))
      //替换旧字段
      GnewsDataService.renameFields(messageNode)

      //因为有替换字段的过程，最后要验证字段长度，应该在替换字段后进行
      //验证一些字段的长度限制url,tags,title,author
      GnewsDataService.validate_fields_length(messageNode, ES_ERROR_INDEX)
      if (!messageNode.has(Constant.FIELD_ERROR_KEY)) {
        //发送图片处理任务到Redis  //20180103 暂不发送图片任务
        GnewsDataService.addSensetiveInfo(messageNode, senseWordsBrdcst.value)
        GnewsDataService.addExtraInfo(messageNode)
        GnewsDataService.sendImgRedisTask(messageNode, redis, redis_task_prefix)
      }
      jsonNode2Map(messageNode) //最终将数据转换成map形式，便于发送至es
    })
    messages
  }

  //mysql中查询topic对应对的es_index信息 key形式：topic   value形式：<es_index>::<es_error_index>
  private def buildTopicIndexMap():util.HashMap[String,String]={
    val topicindexes:Array[String] = MysqlUtils.getTopicESIndexes()
    val map = new util.HashMap[String,String]()
    topicindexes.foreach{topic_index=>
      val values = topic_index.split("##")
      map.put(values(0),values(1))
    }
    map
  }

  private def validateBWlist(ES_ERROR_INDEX: String, blacklistBrdcst: Broadcast[Array[String]], whitelistBrdcst: Broadcast[Array[String]], messageNode: ObjectNode) = {
    if (!messageNode.has(Constant.FIELD_ERROR_KEY)) {
      //加上验证黑名单逻辑
      GnewsDataService.validateBlacklist(messageNode, blacklistBrdcst, ES_ERROR_INDEX)
      //加上验证白名单逻辑
      GnewsDataService.validateWhitelist(messageNode, whitelistBrdcst, ES_ERROR_INDEX)
    }
  }

  /**
    * 添加store_time和news_mode字段
    *
    * @param messageNode
    */
  def addExtraInfo(messageNode: ObjectNode,news_mode_map:util.HashMap[String,Int]): Unit ={
    val article_genre = messageNode.get("article_genre").textValue()
    //解决莫名其妙索引19越界异常问题
    messageNode.put("store_time",GnewsDataService.getNowTime())
    messageNode.put("news_mode",news_mode_map.get(article_genre))
  }



  def getNewsModeMap(properties: Properties):util.HashMap[String,Int]={
    val map = new util.HashMap[String,Int]()
    val newsmode_str = properties.getProperty("news_mode")
    newsmode_str.split(",").foreach(type_mode=>{
      val data = type_mode.split(":")
      map.put(data(0),data(1).toInt)
    })
    map
  }

  private def datajson2JsonNode(msg: String) = {
    val messageNode = JsonNodeUtils.getJsonNodeFromStringContent(msg).asInstanceOf[ObjectNode]
    messageNode
  }

  protected def jsonNode2Map(messageNode: ObjectNode): util.HashMap[String, Object] = {
    val objectMapper = new ObjectMapper
    objectMapper.readValue(messageNode.toString, classOf[util.HashMap[String,Object]])
  }

  def produceRecordId(messageNode:ObjectNode,error_index:String):String={
    val url = messageNode.get("url").textValue()
    var record_id:String = ""
    if(url==null || url.trim.length==0){//url会出现为空的情况
      GnewsDataService.addErrorInfo(messageNode,error_index,"url为空！")
      record_id = MD5Utils.md5(LocalDateTime.now().toString)
    }else{
       record_id = MD5Utils.md5(url)
      if(messageNode.has(FIELD_ERROR_KEY)){
        record_id = s"${LocalDate.now()}_$record_id"
      }
    }
    //加密后的字符串

    record_id
  }



  def common_validate(messageNode:ObjectNode,index:String,index_error:String,jedis:JedisCluster,
                      tags_rds_flg_Broadcast: Broadcast[Int],article_min_words:Int,topic:String): Unit ={
    messageNode.remove("body")
    //将tags字段由数组类型转变为字符串类型
    GnewsDataService.processTags(messageNode)
    val content_node = GnewsDataService.getFieldNode(messageNode,"parsed_content")
    val contenttext_node = GnewsDataService.getFieldNode(messageNode,"parsed_content_main_body")
    val s_img_lc_node = GnewsDataService.getFieldNode(messageNode,"small_img_location")
    val img_lc_node = GnewsDataService.getFieldNode(messageNode,"img_location")
    var publish_time_node = GnewsDataService.getFieldNode(messageNode,"publish_time")
    val timestamp_node =  GnewsDataService.getFieldNode(messageNode,"timestamp")
    val article_genre =  messageNode.get("article_genre").textValue()

    if(timestamp_node == null){
      val now_time = GnewsDataService.getNowTime()
      messageNode.put("timestamp",now_time)
      GnewsDataService.addErrorInfo(messageNode,index_error,"缺少timestamp字段！")
    }

    if(publish_time_node == null){
      messageNode.set("publish_time",messageNode.get("timestamp"))
    }

    messageNode.put(ES_TYPE_KEY,article_genre)

    //获取datavalid值
    val datavalid = GnewsDataService.getDataValid(messageNode,index_error,article_genre,
      s_img_lc_node,content_node,contenttext_node,publish_time_node,img_lc_node)
    messageNode.put("datavalid",datavalid)
    GnewsDataService.setStatus(messageNode,datavalid,article_genre)//设置status值
    //设置小说标题
    GnewsDataService.setNovelTitle(messageNode,article_genre)
    //视频图集的正文字段都设置为空
    //    GnewsDataService.setGalleryVideoContenttext(messageNode,article_genre)



    val timestamp = messageNode.get("timestamp").textValue()
    //按照当前月生成动态索引
    GnewsDataService.buildEsIndexOnMonth(messageNode,index,timestamp)

    val publish_time = messageNode.get("publish_time").textValue()
    if(!GnewsDataService.chargeTimeField(messageNode,timestamp,publish_time,index_error))return
    //验证必须含有的字段
    val error_msg = JsonNodeUtils.validateData(messageNode,Schemas.base_schema)
    if(error_msg.trim.length > 0){
      GnewsDataService.addErrorInfo(messageNode,index_error,error_msg)
    }
    //验证复合类型字段
    Schemas.based_arraytype_fields.foreach(field=>{
      if(!messageNode.has(field)){
        GnewsDataService.addErrorInfo(messageNode,index_error,"field $field is needed ")
      }
    })

    GnewsDataService.imgCommonTrans(messageNode,s_img_lc_node,"small_img_location",article_genre)
    GnewsDataService.imgCommonTrans(messageNode,img_lc_node,"img_location",article_genre)
    GnewsDataService.videoTrans(messageNode,"video_location",article_genre,index_error)
    if("article".equals(article_genre)) {
      //验证文章
      GnewsDataService.validate_article(messageNode, index_error, jedis, content_node, contenttext_node,
        s_img_lc_node,tags_rds_flg_Broadcast,article_min_words,topic)
    }



  }

}