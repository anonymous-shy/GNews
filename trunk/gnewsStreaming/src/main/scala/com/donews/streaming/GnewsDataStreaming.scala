package com.donews.streaming

import java.security.MessageDigest
import java.time.{LocalDate, LocalDateTime}
import java.util
import java.util.Properties

import com.donews.utils._
import com.fasterxml.jackson.databind.{JsonNode, ObjectMapper}
import com.fasterxml.jackson.databind.node.ObjectNode
import kafka.common.TopicAndPartition
import kafka.message.MessageAndMetadata
import kafka.serializer.StringDecoder
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.streaming.{Milliseconds, StreamingContext}
import org.apache.spark.streaming.dstream.InputDStream
import org.apache.spark.streaming.kafka._
import org.elasticsearch.spark.rdd.EsSpark
import org.slf4j.LoggerFactory

import Constant._
import org.apache.spark.rdd.RDD

import scala.collection.JavaConversions._
case class ProcessLog(var topic:String,
                      var day_str:String,
                      var process_time:String,
                      var total_data_num:Int,
                      var valid_data_num:Int,
                      var error_data_num:Int)
object GnewsDataStreaming {

  val LOG = LoggerFactory.getLogger(GnewsDataStreaming.getClass)

  def main(args: Array[String]): Unit = {
    val properties = MysqlUtils.getProperties()
    val topic_s = properties.getProperty("niuer_kafka_topics")
    val topics: Set[String] = topic_s.split(",").toSet
    LOG.info("topicsSet.toString()===>"+topics.toString())

    val max_rate = properties.getProperty("kafka2es_stream_max_rate")
    val duration_secs = properties.getProperty("kafka2es_strea_duration_secs").toInt
    LOG.info(s"######## duration_milli=$duration_secs ###########")

    val ES_INDEX = properties.getProperty("dmt_index")
    val ES_ERROR_INDEX = properties.getProperty("dmt_error_index")


    val conf = new SparkConf()
      .setAppName("Gnews")
      .set("spark.driver.port", "18080")
      .set("spark.streaming.kafka.maxRatePerPartition" ,s"$max_rate")
      .set("spark.streaming.backpressure.enabled","true")
      .set("spark.streaming.concurrentJobs" ,s"2")
      .set("spark.streaming.stopGracefullyOnShutdown","true")

    val options = Map("es.index.auto.create" -> "true",
      "pushdown" -> "true",
      "es.nodes" -> "10.27.80.3,10.27.72.7,10.27.246.45",
      "es.port" -> "9200",
      "es.mapping.id" -> ES_ID_KEY)


    val ssc = new StreamingContext(conf, Milliseconds(duration_secs*1000))
    val BROKER_LIST = properties.getProperty("kafka_broker_list")

    val kafkaParam = Map[String, String](
      "metadata.broker.list" -> BROKER_LIST,
      "auto.offset.reset" -> "smallest",
      "fetch.message.max.bytes"->s"${20*1024*1024}"
    )

    val messageHandler = (mmd: MessageAndMetadata[String, String]) => (mmd.topic, mmd.message()) //这个会将 kafka 的消息进行 transform，最终 kafka 的数据都会变成 (topic_name, message) 这样的 tuple
    val kafkaHelper = new KafkaClusterHelper(kafkaParam)

    topics.foreach{topic=>
      val fromOffsets = ZookeeperHelper.loadOffsets(Set(topic), kafkaHelper.getFromOffsets(kafkaParam, Set(topic)))

      val kafkaStream: InputDStream[(String, String)] =  KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder, (String, String)](ssc, kafkaParam, fromOffsets, messageHandler)
      var offsetRanges = Array[OffsetRange]()

      kafkaStream.transform { rdd =>
        offsetRanges = rdd.asInstanceOf[HasOffsetRanges].offsetRanges //得到该rdd对应kafka的消息的 offset
        rdd
      }.map(msg => msg._2).foreachRDD { rdd =>

        val ret_rdd = rdd.mapPartitions(kvmessages=> {
          val messages = kvmessages.map(msg=>{
            //将数据转换成jsonNode格式，便于数据验证
            val messageNode = datajson2JsonNode(msg)


            GnewsDataService.processSavedLocation(messageNode)
//            messageNode.put(ES_INDEX_KEY,ES_INDEX)//默认数据的index都是正确的index
            addExtraInfo(messageNode)
            if(messageNode.hasNonNull("article_genre")){
              //对初步正确的数据做进一步的各种验证
              val article_genre = messageNode.findValue("article_genre").textValue()
              messageNode.put(ES_TYPE_KEY,article_genre)
              validate_json(messageNode,ES_INDEX,ES_ERROR_INDEX)
//              addExtraInfo(messageNode,news_mode_map)
            }else{
              messageNode.put(ES_INDEX_KEY,ES_ERROR_INDEX)
              messageNode.put(ES_TYPE_KEY,"error_data_type")
              messageNode.put(FIELD_ERROR_KEY,"the field article_genre is Null! line 98")
            }
            //为数据生成es的id
            messageNode.put(Constant.ES_ID_KEY,produceRecordId(messageNode))
            messageNode.remove("_id")
            jsonNode2Map(messageNode)//最终将数据转换成map形式，便于发送至es
          })
          messages
        })
//
//       printData(properties, ret_rdd,topic)
//        val keyRdd = ret_rdd.map{map=>
//          val timestamp = map.get("timestamp").toString
//          var result:String = timestamp
//          map.keys.foreach(result+=" "+_)
//          result
//        }
//
//        keyRdd.take(500).foreach(println(_))

        EsSpark.saveToEs(ret_rdd,s"{$ES_INDEX_KEY}/{$ES_TYPE_KEY}",options)

        //对TopicName进行offset的存储
        val nextOffsets=offsetRanges.map(x => (TopicAndPartition(x.topic, x.partition), x.untilOffset)).toMap
        ZookeeperHelper.storeOffsets(nextOffsets)

      }
    }
    ssc.start() // 真正启动程序
    ssc.awaitTermination() //阻塞等待
  }
//给数据一个存入es的时间
  private def addExtraInfo(messageNode:ObjectNode): Unit ={
    messageNode.put("store_time",LocalDateTime.now().toString.substring(0,19))
  }
  private def printData(properties: Properties, ret_rdd: RDD[util.HashMap[String, Object]],topic:String) = {
    if (properties.getProperty("kafka2es_need_print_data").equals("yes")) {
      ret_rdd.cache()
      val error_data = ret_rdd.filter(_.containsKey(FIELD_ERROR_KEY)).collect()
      val valid_data = ret_rdd.filter(!_.containsKey(FIELD_ERROR_KEY)).collect()
//      println(s"有效数据: ${valid_data.length}    错误数据: ${error_data.length}")
//      if (valid_data.length > 4) {
//        for (i <- 0 to 3) {
//          println("timestamp=> " + valid_data(i).getOrElse("timestamp", ""))
//        }
//      }

    var timestamp = ""
      var day_str =""
    if(valid_data.length>0){
      timestamp = valid_data(0).getOrElse("timestamp", "").toString
      day_str=timestamp.substring(0,10)
    }else if(error_data.length>0){
      timestamp = valid_data(0).getOrElse("timestamp", "").toString
      if(timestamp.trim.length>10)day_str=timestamp.substring(0,10)
    }

      val process_time =  LocalDateTime.now().toString.substring(0,19).replace("T"," ")
      val log = ProcessLog(topic,day_str,process_time,
        valid_data.length+error_data.length,
        valid_data.length,error_data.length)

      if(log.total_data_num>0){
        MysqlUtils.writeLog2Mysql(log)
      }
    }
  }



  private def datajson2JsonNode(msg: String) = {
    val messageNode = JsonNodeUtils.getJsonNodeFromStringContent(msg).asInstanceOf[ObjectNode]
    messageNode
  }

  protected def jsonNode2Map(messageNode: ObjectNode): util.HashMap[String, Object] = {
    val objectMapper = new ObjectMapper
    objectMapper.readValue(messageNode.toString, classOf[util.HashMap[String,Object]])
  }

 def produceRecordId(messageNode:ObjectNode):String={
   val response_url = messageNode.findValue("url").textValue()
   //加密后的字符串
   val correct_data_id = MD5Utils.md5(response_url)
   if(messageNode.has(FIELD_ERROR_KEY)){
     val rid = s"${LocalDate.now()}_$correct_data_id"
     return rid
   }
   correct_data_id
 }

  def chargeTimeField(messageNode:ObjectNode,time_field_name:String,index_error:String): Boolean ={
    var is_valid:Boolean = true
    if(messageNode.hasNonNull(time_field_name)){
      val regex = "[1-2]\\d{3}-(0[1-9]|1[0-2])-(0[1-9]|[1-2][0-9]|3[0-1]) (20|21|22|23|[0-1]\\d):[0-5]\\d:[0-5]\\d"
       val time_field = messageNode.findValue(time_field_name).textValue()
      if(!time_field.matches(regex)){
        messageNode.put(ES_INDEX_KEY,index_error)
        messageNode.put(FIELD_ERROR_KEY,s"the field $time_field_name is Invalid! " +
          s"$time_field_name:${messageNode.findValue(time_field_name).textValue()} line 166")

        is_valid = false
      }else{
        messageNode.put(time_field_name,time_field.trim.replace(" ","T"))
      }
    }else {
      if(time_field_name.equals("timestamp")){
          messageNode.put(ES_INDEX_KEY,index_error)
          messageNode.put(FIELD_ERROR_KEY,"timestamp field is Null! line 173")
        is_valid = false
      }
    }
    is_valid

  }

  def common_validate(messageNode:ObjectNode,index:String,index_error:String): Unit ={
    messageNode.remove("body")
    chargeTimeField(messageNode,"publish_time",index_error)
    val is_timestamp_valid = chargeTimeField(messageNode,"timestamp",index_error)

    val error_msg = JsonNodeUtils.validateData(messageNode,Schemas.base_schema)
    if(error_msg.trim.length > 0){
      messageNode.put(ES_INDEX_KEY,index_error)
      messageNode.put(FIELD_ERROR_KEY,s"${error_msg} line 188")
      return
    }

    //按月建立索引
    if(is_timestamp_valid){
      val timestamp = messageNode.get("timestamp").textValue()
      val y_month = timestamp.substring(0,7)
      val es_index = s"${index}_$y_month"
      messageNode.put(ES_INDEX_KEY,es_index)
    }
    //验证复合类型字段
    Schemas.based_arraytype_fields.foreach(field=>{
      if(!messageNode.has(field)){
        messageNode.put(ES_INDEX_KEY,index_error)
        messageNode.put(FIELD_ERROR_KEY,s"field $field is needed line 197")
        return
      }
    })

  }


  def validate_json(messageNode:ObjectNode,index:String,index_error:String): Unit ={
      common_validate(messageNode,index,index_error)

  }



}