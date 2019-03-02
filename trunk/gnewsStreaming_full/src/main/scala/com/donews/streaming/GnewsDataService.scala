package com.donews.streaming;


import java.time.{Instant, LocalDateTime, ZoneOffset}
import java.util

import com.donews.utils.{GNewsUtil, JsonNodeUtils, MysqlUtils, RedisClusterHelper}
import com.fasterxml.jackson.databind.JsonNode
import com.fasterxml.jackson.databind.node._
import org.apache.commons.lang3.StringUtils
import org.apache.spark.SparkContext
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.elasticsearch.spark.rdd.EsSpark
import redis.clients.jedis.JedisCluster
import java.util.{ArrayList => AList, LinkedHashMap => LKMap}

import scala.collection.JavaConverters._
import Constant._

import scala.collection.mutable.ArrayBuffer


/**
  * Donews Company
  * Created by Liu dh 
  * 2017/10/13.18:17
  */
object GnewsDataService {
    val replaceOldFields = new java.util.ArrayList[String](util.Arrays.asList("data_source_id", "parsed_content",
      "parsed_content_main_body", "media", "id", "info_source",
      "publish_time",  "small_img_location","small_img_location_count","news_mode",
      "img_location","img_location_count","video_location", "video_location_count", "url", "article_genre", "comment_count",
      "click_count", "like_count", "data_source_sub_id", "sub_channel","_id"))



  /**
    *  替换字段
    * @param messageNode
    */
   def renameFields( messageNode:ObjectNode){
     //添加新字段
       messageNode.set("datasourceid",messageNode.get("data_source_id"))
       messageNode.set("content",messageNode.get("parsed_content"))
       messageNode.set("contenttext",messageNode.get("parsed_content_main_body"))
       messageNode.set("source",messageNode.get("media"))
       messageNode.set("sourceurl",messageNode.get("id"))
       messageNode.set("author",messageNode.get("info_source"))
       messageNode.set("publishtime",messageNode.get("publish_time"))
       messageNode.set("thumbnailimglists",messageNode.get("small_img_location"))
       messageNode.set("coverthumbnailimglists",messageNode.get("img_location"))
       messageNode.set("imglists",messageNode.get("img_location"))
       messageNode.set("videolists",messageNode.get("video_location"))

     //替换换url的工作应适配于所有数据，包括正确，错误,已经单独提出去了
       messageNode.set("shareurl",messageNode.get("url"))
       messageNode.set("newsmode",messageNode.get("news_mode"))
       messageNode.set("commentcount",messageNode.get("comment_count"))
//       messageNode.set("videotime",messageNode.get("video_duration"));//在处理视频列表的时候处理过了
       if(!messageNode.has("imgcount")){
         messageNode.set("imgcount",messageNode.get("small_img_location_count"))
       }
//       messageNode.set("imgcount",messageNode.get("small_img_location_count"));
       messageNode.set("likecount",messageNode.get("click_count"))

       produceKeywords(messageNode)//产生keyword字段
       messageNode.set("datasourcesubid",messageNode.get("data_source_sub_id"))
       messageNode.set("sourcesubname",messageNode.get("sub_channel"))
       val videotime_node = getFieldNode(messageNode,"videotime")
       if(videotime_node==null){
         messageNode.put("videotime",0) //videotime
       }
      //删掉老字段
       messageNode.remove(replaceOldFields)

   }

   def produceKeywords(messageNode:ObjectNode): Unit ={
     if(!messageNode.has("keywords")){
       val tags_nd = getFieldNode(messageNode,"tags")
       if(tags_nd!=null && !tags_nd.textValue().trim.equals("")){
         val news_tags = tags_nd.textValue().trim.replaceAll(":0\\.\\d+| +","")
         messageNode.put("keywords",news_tags)
       }else{
         messageNode.put("keywords","")
       }
     }

   }

  /**
    * 确保有publish_time字段,如果没有，就用timestamp补足
    * @param messageNode
    */
  def supplyPublishTime(messageNode:ObjectNode,publish_time_node:JsonNode): Unit ={
    if(null == publish_time_node){
       messageNode.set("publish_time",messageNode.get("timestamp"))
    }
  }

  /**
    * 确保有publish_time字段
    * @param messageNode
    */
  def validateTimeStamp(messageNode:ObjectNode,index_error:String): Boolean ={
    var is_valid = true
    if(!messageNode.hasNonNull("timestamp")){
      addErrorInfo(messageNode,index_error,"timestamp mustn't be null")
      is_valid=false
    }
    is_valid
  }


  /**
    * 验证timestamp,publish_time两个字段,以及比较他们的值
    * @param messageNode
    * @param timestamp
    * @param publish_time
    * @param index_error
    * @return
    */
  def chargeTimeField(messageNode:ObjectNode,timestamp:String,publish_time:String,index_error:String): Boolean ={
    var is_valid:Boolean = true
    val regex = "[1-2]\\d{3}-(0[1-9]|1[0-2])-(0[1-9]|[1-2][0-9]|3[0-1]) (20|21|22|23|[0-1]\\d):[0-5]\\d:[0-5]\\d"
    val time_fields =  Array[String](timestamp,publish_time)
    for(i<- 0.until(time_fields.length)){
      var time_field_name="timestamp"
      if(i==1){
         time_field_name="publish_time"
      }
      val time_field =  time_fields(i)
      if(!time_field.matches(regex)){
        addErrorInfo(messageNode,index_error,s"the field $time_field_name is Invalid! " +
          s"$time_field_name:$time_field line 166")
        is_valid = false
      }else{
        messageNode.put(time_field_name,time_field.trim.replace(" ","T"))
      }
    }

    if(publish_time > timestamp){//publish_time不能大于timestamp
      addErrorInfo(messageNode,index_error,"Error_Data: publish_time is Later than timstamp!!!")
      is_valid=false
    }
    is_valid
  }

  def getNowTime():String={
    var now_time = LocalDateTime.now().toString
    if(now_time.length>=20){
      now_time=now_time.substring(0,19)
    }
    now_time
  }

  def getNowMinuteNo():Int={
    getNowTime().substring(14,16).toInt
  }

  def chinaTimeToZeroTime(time:LocalDateTime):Instant={
    time.toInstant(ZoneOffset.ofHours(Constant.LOCAL_TIME_ZONE))
  }
  def chinaTime2EpochSecond(timestamp:String):Long={
    val localdateTime = LocalDateTime.parse(timestamp)
    val instant = chinaTimeToZeroTime(localdateTime)
    instant.getEpochSecond
  }
  /**
    * 生成需要的额外字段 store_time,ctime,utime,uid, niuerid, // 在里面一起处理status
    * @param messageNode
    */
  def addExtraInfo(messageNode:ObjectNode): Unit ={
    val timestamp = messageNode.get("timestamp").textValue()
    val ctime: Long = chinaTime2EpochSecond(timestamp)
    val ctimestr:String  = timestamp

    val utime: Long = Instant.now.getEpochSecond
    val utimestr:String  = getNowTime()
    messageNode.put("ctime",ctime)
    messageNode.put("ctimestr",ctimestr)
    messageNode.put("utime",utime)
    messageNode.put("utimestr",utimestr)

    messageNode.put("store_time",getNowTime())
    messageNode.set("uid",messageNode.get(Constant.ES_ID_KEY))
    messageNode.put("niuerid",0)


  }


  def buildEsIndexOnMonth(messageNode:ObjectNode,index:String,timestamp:String): Unit ={
   //正确数据的索引
      val y_month = timestamp.substring(0,7)
      val es_index = s"${index}_$y_month"
      messageNode.put(ES_INDEX_KEY,es_index)

  }

  def getFormalTopic():String={
    val formal_topic = MysqlUtils.getProperties().getProperty("formal_topic")
    formal_topic
  }

  def getQuchongValue():String={
    val quchong = MysqlUtils.getProperties().getProperty("quchong")
    quchong
  }
  def validate_article(messageNode:ObjectNode,error_index:String,jedis:JedisCluster,
                       content_node:JsonNode,contenttext_node:JsonNode,
                       small_img_location:JsonNode,
                       tags_rds_flg_Broadcast: Broadcast[Int],
                       article_min_words:Int,
                       topic:String): Unit ={
    val ctnt_count_node = getFieldNode(messageNode,"parsed_content_char_count")
    var ct_count:Int = 0
    if(null != ctnt_count_node){
      ct_count =  ctnt_count_node.toString.toInt
    }
    //add by liudinghua 2018-03-02 季家震提的需求
//    if(ct_count<article_min_words){
//      addErrorInfo(messageNode,error_index,s"文章字数少于 $article_min_words 字！")
//
//    }
    val title = messageNode.get("title").toString
    // 剔除乱码数据
    if (title != null && contenttext_node != null && ct_count >= 150) {
      //乱码校验
      val dirtyCode = !GNewsUtil.checkArticleCode(title.toString, contenttext_node.textValue())
      if (dirtyCode) {
        addErrorInfo(messageNode,error_index,"文章乱码了，不可用！")
      }
    }
    val news_mode = messageNode.get("news_mode").toString

    var extractTags:String = null
    // 标签提取
    if (title != null && contenttext_node != null) {
      extractTags = GNewsUtil.extractTags(title.toString, contenttext_node.textValue(), news_mode)
      // tags发送Redis热词队列
      val jsonArr = extractTags.split(",")
      val tags = ArrayBuffer[String]()
      jsonArr.foreach(tag => tags += StringUtils.substringBefore(tag, ":"))

      jedis.lpush("DMT_NEW_QUEUE:TAGS", tags.toArray: _*)
      messageNode.put("tags",extractTags)
      //验证相似性 add by liudinghua at 2018-01-08
      //2018-04-02 去掉去重逻辑
      //2018-05-22 加上去重逻辑
      val formal_topic = getFormalTopic()
      val quchong = getQuchongValue() //增加去重控制逻辑
      if(topic.equals(formal_topic) && quchong.equals("1"))
            nvalidate_similary(messageNode,extractTags,error_index,jedis,tags_rds_flg_Broadcast)//2018-02-06注掉，太耗时！
    }

  }


  //tags值的样例 "tags": "养猪业:0.4648,生猪:0.3422,我国:0.3179,猪场:0.2138,动物福利:0.2114"
  def getTagsIndex(tags:String):Int={
    val first_tag = tags.split(":")(0)
    Math.abs(first_tag.hashCode)%TAGS_KEY_ARR_NUM
  }
  def nvalidate_similary(messageNode:ObjectNode,extractTags:String,error_index:String,jedis:JedisCluster,tags_rds_flg_Broadcast: Broadcast[Int]): Unit ={
    val tags_rds_flag = tags_rds_flg_Broadcast.value
    val tags_index = getTagsIndex(extractTags)
    val redis_key = s"$RDS_SIMI_TAGS_PREFIX:$tags_index:$tags_rds_flag"
    if(jedis.exists(redis_key)){
     val tags_list =  jedis.lrange(redis_key,0,-1)
      val tags_iter = tags_list.iterator()
      var need_iter:Boolean = true
      while (need_iter && tags_iter.hasNext){
        val old_tag_and_index_str = tags_iter.next()
        val values_arr = old_tag_and_index_str.split("--")
        val old_tag = values_arr(0); val data_index = values_arr(1)
        val similarScore = GNewsUtil.similarity(old_tag, extractTags)
        val score_threshold = getScoreThreshold()
        if (similarScore >= score_threshold) {
          addErrorInfo(messageNode,error_index,s"文章与 _id=${data_index} 的数据相似度> $score_threshold")
          //找到过去一条相似的即可
          need_iter=false
        }
      }
    }



  }
  private def getScoreThreshold():Float={
    val score_str = MysqlUtils.getProperties().getProperty("similar_score")
    score_str.toFloat
  }
  def getChannel(messageNode:ObjectNode):String={
    var channel = getStrField(messageNode,"channel")
    if(channel.trim.length<=0)channel="kong"
    channel
  }
  def getStrField(messageNode:ObjectNode,fieldName:String):String={
    val field_node = getFieldNode(messageNode,fieldName)
    var field_value:String=""
    if(field_node != null){
      field_value=field_node.textValue()
    }
    field_value
  }
  //将tags字段由数组类型转变为字符串类型
  def processTags(messageNode:ObjectNode): Unit ={
    var tags_str:String = ""
    val tagNode = getFieldNode(messageNode,"tags")
    if(tagNode!=null){
      tags_str = tagNode.toString.replaceAll("\"|\\[|\\]","")
      //将原始的非空tags字符串作为keywords
      //最后验证完的数据还没有keywords字段，需要用算法算出的tags，去掉分值，作为keywords值
      if(tags_str.trim.size>0)
        messageNode.put("keywords",tags_str)
    }
    messageNode.put("tags",tags_str)
  }
  def getDataValid(messageNode:ObjectNode,
                   error_index:String,
                   genre:String,
                   small_img_location:JsonNode,
                   content_node:JsonNode,
                   contenttext_node:JsonNode,
                   publish_time_node:JsonNode,
                   img_location:JsonNode
                  ):Int={
    val url_node = GnewsDataService.getFieldNode(messageNode,"url")
    val datasourceid_node = GnewsDataService.getFieldNode(messageNode,"data_source_id")
    val title_node = GnewsDataService.getFieldNode(messageNode,"title")
    val source_node = GnewsDataService.getFieldNode(messageNode,"media")
    //验证文章的datavalid
    var datavalid:Int = -2
    var error_msg=""
    if(genre.equals("article")){//文章类型的datavalid
      datavalid = if (small_img_location != null && content_node != null && contenttext_node != null) 1
      else if (small_img_location != null && content_node != null && contenttext_node == null) 2
      else if (small_img_location != null && content_node == null && contenttext_node != null) 3
      else if (small_img_location == null && content_node != null && contenttext_node != null) 4
      else if (small_img_location == null && content_node == null && contenttext_node != null) 5
      else if (small_img_location == null && content_node != null && contenttext_node == null) 6
      else if (small_img_location != null && content_node == null && contenttext_node == null) 0
      else {        // 过滤 datavalid = -1 数据
        error_msg+=" || article datavalid = -1"
        -1
      }
    }else if ("gallery".equals(genre)){
      if (url_node == null || datasourceid_node == null || title_node == null || source_node == null ||
        publish_time_node == null || small_img_location == null || img_location == null){
        datavalid = 0
      } else{
        datavalid = 1
      }

    } else if ("gallery".equals(genre)) {
      datavalid = if (url_node == null || datasourceid_node == null || title_node == null ||
        source_node == null || publish_time_node == null || small_img_location == null || img_location == null) 0
      else 1
    } else if ("video".equals(genre)) {
      datavalid = if (url_node == null || datasourceid_node == null || title_node == null ||
        source_node == null || publish_time_node == null) 0
      else 1
    } else if("cartoon".equals(genre) || "novel".equals(genre) || "ec".equals(genre)){
      if (small_img_location != null) {
        datavalid=1
      } else if (small_img_location == null) {
        datavalid = 4
      } else {
        error_msg+= " || cartoon novel ec datavalid = -1"
        datavalid = -1
      }
    } else if ("stock".equals(genre)) {
      if (content_node != null && contenttext_node != null) datavalid = 1
      else if (content_node != null && contenttext_node == null) datavalid = 2
      else if (content_node == null && contenttext_node != null) datavalid = 3
      else if (content_node == null && contenttext_node == null) datavalid = 0
      else {        // 过滤 datavalid = -1 数据
        error_msg += " || stock datavalid = -1"
        datavalid = -1
      }
    }
    if(datavalid == -1)
      addErrorInfo(messageNode,error_index,error_msg)
    datavalid
  }

  def setStatus(messageNode:ObjectNode,datavalid:Int,article_genre:String): Unit ={
    messageNode.put("status",1)
    if(article_genre.equals("gallery")){
      messageNode.put("status",datavalid)
    }
  }
  def setNovelTitle(messageNode:ObjectNode,article_genre:String): Unit ={
    if("novel".equals(article_genre)){
      val title_node = GnewsDataService.getFieldNode(messageNode,"title")
      val info_source_node = GnewsDataService.getFieldNode(messageNode,"info_source")
      if(null != title_node && null != info_source_node){
        messageNode.put("title",info_source_node.textValue()+" "+title_node.textValue())
      }
    }

  }

  def setGalleryVideoContenttext(messageNode:ObjectNode,article_genre:String): Unit ={
    if("gallery".equals(article_genre)||"video".equals(article_genre)){
      messageNode.put("parsed_content","")
      messageNode.put("parsed_content_main_body","")
    }

  }


  //验证一些字段的长度限制
  def validate_fields_length(messageNode:ObjectNode,error_index:String): Unit ={
    validate_field_len(messageNode,"shareurl",error_index,330)
    processTitle(messageNode)//去除title两端的空格
    validate_field_len(messageNode,"title",error_index,100)
    validate_field_len(messageNode,"keywords",error_index,500)
    validate_field_len(messageNode,"author",error_index,100)
    validate_field_len(messageNode,"contenttext",error_index,Constant.LONGTEXT_LENGTH)
    validate_field_len(messageNode,"content",error_index,Constant.MEDIUMTEXT_LENGTH)
  }
  // {"enrionment": "formal", "collection": "bdfyb", "database": "dmt_jh_data"}
//  def processSavedLocation(messageNode:ObjectNode): Unit ={
//    val save_location_nd = getFieldNode(messageNode,"saved_data_location")
//    if(null != save_location_nd){
//      if(save_location_nd.getNodeType.equals(JsonNodeType.OBJECT)){
//        val enrionment = if(save_location_nd.has("enrionment"))save_location_nd.get("enrionment").textValue() else ""
//        val db = if(save_location_nd.has("database"))save_location_nd.get("database").textValue() else ""
//        val table =if(save_location_nd.has("collection"))save_location_nd.get("collection").textValue() else ""
//        messageNode.put("saved_data_location",s"${enrionment}#${db}#$table")
//      }
//    }else messageNode.put("saved_data_location","")
//  }
  def processTitle(messageNode:ObjectNode): Unit ={
    val title_node = getFieldNode(messageNode,"title")
    if(title_node!=null){
      val title = title_node.textValue().trim
      messageNode.put("title",title)
    }
  }
  def validate_field_len(messageNode:ObjectNode,field_name:String,error_index:String,max_len:Double): Unit ={
    val field_length = getFieldLength(messageNode,field_name)
    if(field_length > max_len){
      addErrorInfo(messageNode,error_index,s"$field_name 字段长度超出 $max_len 字节最大限制！")
    }
  }

  def getFieldLength(messageNode:ObjectNode,field_name:String):Int={
    val node = getFieldNode(messageNode,field_name)
    var length:Int = 0
    if(null != node){
      val field_value = node.textValue()
      //按照字数个数算长度
      length = field_value.length
    }
    length
  }

  def validate_video(messageNode:ObjectNode,error_index:String): Unit ={
    val vd_count_node = messageNode.get("video_location_count")
    var vd_count=0
    if(!vd_count_node.getNodeType.equals(JsonNodeType.NULL)){
      vd_count =  vd_count_node.toString.toInt
    }

  }

  def getCountTypeField(messageNode:ObjectNode,count_field_name:String):Int={
    val count_node = messageNode.get(count_field_name)
    var count=0
    if(null!=count_node && !count_node.getNodeType.equals(JsonNodeType.NULL)){
      count =  count_node.toString.toInt
    }
    count
  }


  /**
    * @param messageNode
    * @param img_location
    * @param img_field
    */
  def imgCommonTrans(messageNode:ObjectNode,img_location:JsonNode,img_field:String,genre:String): Unit ={
    if(img_location != null){
      val new_list_nodes = new ArrayNode(JsonNodeFactory.instance)
      val img_list_nodes = img_location.asInstanceOf[ArrayNode].elements()
      val img_task_processor_img_list_nodes = new ArrayNode(JsonNodeFactory.instance)

      var index:Int=0 //只截取100张
      while (img_list_nodes.hasNext && index<100){
        val img_node = img_list_nodes.next().asInstanceOf[ObjectNode]
        processImgNode(img_node)
        if(0==index){
          val filepath = getFieldNode(img_node,"filepath")
          if(filepath!=null && filepath.textValue().toLowerCase.endsWith("webp")){
            //图片后缀是webp将data value设置成7
            messageNode.put("datavalid",7)
          }
        }

        new_list_nodes.add(img_node)
        index=index+1
      }
      //处理imgcount字段
      if(genre.equals("gallery") && img_field.equals("img_location")){
        messageNode.put("imgcount",new_list_nodes.size())
      }else if(!genre.equals("video") && !genre.equals("gallery") && img_field.equals("small_img_location")){//video类型数据不做处理
        messageNode.put("imgcount",new_list_nodes.size())
      }
      messageNode.set(img_field,new_list_nodes)
    }
  }

  def addErrorInfo(messageNode:ObjectNode,error_index:String,error_msg:String): Unit ={
    var error_message:String = error_msg
    messageNode.put(Constant.ES_INDEX_KEY,error_index)
    if(messageNode.hasNonNull(Constant.FIELD_ERROR_KEY)){
      val last_error_msg = messageNode.get(Constant.FIELD_ERROR_KEY).textValue()
      error_message+=" || "+last_error_msg
    }
    messageNode.put(Constant.FIELD_ERROR_KEY,error_message)
  }
  def videoTrans(messageNode:ObjectNode,video_field:String,genre:String,error_index:String): Unit ={
    val video_location = GnewsDataService.getFieldNode(messageNode,"video_location")
    if(video_location==null){
      if("video".equals(genre)){
        addErrorInfo(messageNode,error_index,"video类型的数据，视频列表为空")
      }
    }else{
      val vd_list_nodes = video_location.asInstanceOf[ArrayNode].elements()

      var index=0;
      while (vd_list_nodes.hasNext ){
        val vd_node = vd_list_nodes.next().asInstanceOf[ObjectNode]
        if(0==index && !genre.equals("gallery")){//gallery类型数据不做处理
        //如果有多个视频，把第一个视频的属性设置为此数据的视频属性
        val duration_str = vd_node.findValue("video_duration").textValue()
          val duration = GNewsUtil.videoTime2Sec(duration_str)
          messageNode.put("videotime",duration)
          if(duration<5){
            addErrorInfo(messageNode,error_index,"视频长度小于5秒！")
          }
          //          messageNode.put("videotime",)
        }
        processVideoNode(vd_node)
        index=index+1
      }
    }
  }

  /**
    *
    * @param img_node
    */
  def processImgNode(img_node:ObjectNode): Unit ={
    img_node.remove("img_index")
    processImgVideoSubNode(img_node,"img_path","filepath",Constant.FIELD_TYPE_STR)
    processImgVideoSubNode(img_node,"img_src","shareurl",Constant.FIELD_TYPE_STR)
    processImgVideoSubNode(img_node,"img_desc","title",Constant.FIELD_TYPE_STR)
    processImgVideoSubNode(img_node,"img_width","width",Constant.FIELD_TYPE_INT)
    processImgVideoSubNode(img_node,"img_height","height",Constant.FIELD_TYPE_INT)
  }

  /**
    *
    * @param video_node
    */
  def processVideoNode(video_node:ObjectNode): Unit ={
    video_node.remove("video_index")
    processImgVideoSubNode(video_node,"video_desc","content",Constant.FIELD_TYPE_STR)
    processImgVideoSubNode(video_node,"video_src","shareurl",Constant.FIELD_TYPE_STR)
    processImgVideoSubNode(video_node,"video_path","filepath",Constant.FIELD_TYPE_STR)
    processImgVideoSubNode(video_node,"video_duration","duration",Constant.FIELD_TYPE_STR)
    processImgVideoSubNode(video_node,"video_width","videowidth",Constant.FIELD_TYPE_STR)
    processImgVideoSubNode(video_node,"video_height","videoheight",Constant.FIELD_TYPE_STR)
  }

  def processImgVideoSubNode(img_video_node:ObjectNode,old_field:String,new_field:String,field_value_type:String): Unit ={
    val sub_field_node = ensureExistSubNode(img_video_node,old_field)
    if(sub_field_node==null){
      if(field_value_type.equals(Constant.FIELD_TYPE_STR))
        img_video_node.put(new_field,"")
      else img_video_node.put(new_field,0)
    }else
      img_video_node.set(new_field,sub_field_node)
    img_video_node.remove(old_field)//删除原有的老的字字段



  }

  private def getArrayNode(messageNode:ObjectNode,fieldName:String):ArrayNode={
    val field_node = messageNode.get(fieldName)
    if(null==field_node || field_node.getNodeType.equals(JsonNodeType.NULL)){
      return null
    }else field_node.asInstanceOf[ArrayNode]
  }
  def sendImgRedisTask(messageNode:ObjectNode,redis:JedisCluster,redis_task_prefix:String): Unit ={
    val small_img_count = getCountTypeField(messageNode,"imgcount")
    val img_location_node = getArrayNode(messageNode,"imglists")

    val thumbnailimglists = getArrayNode(messageNode,"thumbnailimglists")
    val news_mode = messageNode.get("newsmode").asInt()
    val data_source_key_node = getFieldNode(messageNode,"data_source_key")
    var data_source_key:String = ""
    messageNode.put("need2redis",false) //默认不需要发送redis
    if(data_source_key_node != null){
      data_source_key = data_source_key_node.textValue()
    }
    if(news_mode==3 || (img_location_node != null && img_location_node.size() > 0)){
      if(news_mode==3 &&
        (data_source_key.equals("CZZ-MP") || data_source_key.startsWith("YLZX-MP"))
        && null != thumbnailimglists
        && thumbnailimglists.size()>0){//包含缩略图的秒拍直接到es
        messageNode.put("img_status",104)
        messageNode.set("img_time",messageNode.get("store_time"))
      }else sendImgNode2Redis(messageNode, redis, img_location_node,news_mode,redis_task_prefix)

    }else {
      messageNode.put("img_status",101)
      messageNode.set("img_time",messageNode.get("store_time"))

    }


  }

  private def sendImgNode2Redis(messageNode: ObjectNode, redis: JedisCluster,
                                img_location_node: JsonNode,news_mode:Int,prefix:String) = {
    //发送redis条件
    messageNode.put("need2redis",true)
    messageNode.put("img_status", 0)
    messageNode.put("img_time", "1970-01-01T00:00:00")
    messageNode.put("redis_time",getNowTime())
    if(news_mode==3){
      redis.lpush(s"${prefix}_video_task_processor", messageNode.toString)
    }else redis.lpush(s"${prefix}_img_task_processor", messageNode.toString)



  }
  //不是线上运行的，仅用于本地测试
  def sendImgtask2Redis(messageNode:ObjectNode): Unit ={
    val small_img_count = getCountTypeField(messageNode,"small_img_location_count")
    val img_location_node = getFieldNode(messageNode,"img_location")
    if(small_img_count>0 && img_location_node != null){
      val img_task_node = new ObjectNode(JsonNodeFactory.instance)
      img_task_node.set("news_id",messageNode.get(Constant.ES_ID_KEY))
      img_task_node.set("news_mode",messageNode.get("news_mode"))
      img_task_node.put("small_img_count",small_img_count)
      img_task_node.set("img_location",img_location_node)
      println("要打印了啊")
      println(img_task_node.toString)
    }

  }
  def ensureExistSubNode(node:ObjectNode,sub_field:String): JsonNode ={
    val sub_node = node.get(sub_field)
    if(null==sub_node || JsonNodeType.NULL.equals(sub_node.getNodeType)){
      return null
    }else sub_node
  }
  def getFieldNode(messageNode:ObjectNode,obj_type_field:String): JsonNode ={
    val field_node = messageNode.get(obj_type_field)
    if(null==field_node || field_node.getNodeType.equals(JsonNodeType.NULL)){
      return null
    }else field_node
  }

  def validateBlacklist(messageNode:ObjectNode,
                        blacklistBrdcst:Broadcast[Array[String]]
                        ,index_error:String): Unit ={
    val blacklist:Array[String] = blacklistBrdcst.value
    for(black_info<-blacklist){
      val infos = black_info.split("___")
      val field_name = infos(0)
      val relation = infos(1)
      val keyword = infos(2)
      val field_type = infos(3)
      if(messageNode.hasNonNull(field_name)){
        if("contains".equals(relation) && "str".equals(field_type)){//包含内容
          if(messageNode.get(field_name).textValue().contains(keyword)){
            addErrorInfo(messageNode,index_error,s"$field_name 字段包含 $keyword 内容")
          }
        }
      }
    }
  }

  def validateWhitelist(messageNode:ObjectNode,
                        whitelistBrdcst:Broadcast[Array[String]]
                        ,index_error:String): Unit ={
    val whitelist:Array[String] = whitelistBrdcst.value
    if(messageNode.hasNonNull("saved_data_location")){
      val saved_data_location = messageNode.get("saved_data_location").textValue()
      if(!whitelist.contains(saved_data_location)){
        addErrorInfo(messageNode,index_error,"save_data_location 值不在白名单内")
      }
    }else{
      addErrorInfo(messageNode,index_error,"缺乏 saved_data_location 字段或者其值为空")
    }

  }

  def common_validate(messageNode:ObjectNode,index:String,index_error:String): Unit ={
    messageNode.remove("body")
    //将tags字段由数组类型转变为字符串类型
    GnewsDataService.processTags(messageNode)
    val content_node = GnewsDataService.getFieldNode(messageNode,"parsed_content")
    val contenttext_node = GnewsDataService.getFieldNode(messageNode,"parsed_content_main_body")
    val s_img_lc_node = GnewsDataService.getFieldNode(messageNode,"small_img_location")
    val img_lc_node = GnewsDataService.getFieldNode(messageNode,"img_location")
    val publish_time_node = GnewsDataService.getFieldNode(messageNode,"publish_time")
    val article_genre =  messageNode.get("article_genre").textValue()

    messageNode.put(ES_TYPE_KEY,article_genre)

    //获取datavalid值
    val datavalid = GnewsDataService.getDataValid(messageNode,index_error,article_genre,
      s_img_lc_node,content_node,contenttext_node,publish_time_node,img_lc_node)
    messageNode.put("datavalid",datavalid)
    GnewsDataService.setStatus(messageNode,datavalid,article_genre)//设置status值
    //设置小说标题
    GnewsDataService.setNovelTitle(messageNode,article_genre)

    //补足publish_time字段
    GnewsDataService.supplyPublishTime(messageNode,publish_time_node)
    if(!GnewsDataService.validateTimeStamp(messageNode,index_error))return //必须要有timestamp字段

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


    //    val tn = messageNode.get("title")
    //    val tn1 = messageNode.get("title")
    //验证一些字段的长度限制url,tags,title,author
    GnewsDataService.validate_fields_length(messageNode,index_error)
  }


  def querryOldTags(sc:SparkContext,options:Map[String,String],tags_rds_flg_Broadcast:Broadcast[Int]): Broadcast[Map[String,AList[String]]] ={
    val nowDateTime:LocalDateTime = LocalDateTime.now
    //获取两天内 article tags
    val now = nowDateTime.toString.substring(0, 19)
    val twoDaysAgo = nowDateTime.minusDays(2).toString.substring(0, 19)
    val yearMonth = nowDateTime.toString.substring(0,7)
    val read_index:String="gnews_raw_data_full"

    val query2days =
      s"""
         |{
         | "query":{
         |    "constant_score":{
         |      "filter":{
         |        "range":{
         |          "timestamp":{
         |            "gte":"$twoDaysAgo",
         |            "lt":"$now"
         |          }
         |        }
         |      }
         |    }
         |  }
         |}
        """.stripMargin


    val tagsRdd = EsSpark.esRDD(sc,s"${read_index}/article", query2days,options).repartition(8).map{news=>
      var tags = news._2.get("tags") match {
        case Some(v) => if (v == None) null else v.asInstanceOf[String]
        case None => null
      }
      var channel = news._2.get("channel") match {
        case Some(v) => if (v == None) "kong" else v.asInstanceOf[String]
        case None => "kong"
      }


      val data_index = news._1
      if(null!=tags){
        tags = s"${tags}--$data_index"
      }
      (channel, tags)
    }.filter(_._2!=null)


    val tags_map = tagsRdd.aggregateByKey(new AList[String]())(seqOp = (tagsList,tagsStr) => {

      tagsList.add(tagsStr)
      tagsList
    },
      combOp = (tagsList1,tagsList2) => {
        tagsList1.addAll(tagsList2)
        tagsList1
      }).collect().toMap


    val keys_iter = tags_map.keySet.iterator
    val tags_map_broadCast = sc.broadcast[Map[String,AList[String]]](tags_map)
    tags_map_broadCast
  }

  private def get_similar_data_days():Int={
    val days_str = MysqlUtils.getProperties().getProperty("similar_data_days")
    days_str.toInt
  }
  def nquerryOldTags(sc:SparkContext,options:Map[String,String],tags_rds_flg_Broadcast:Broadcast[Int]): Unit ={
    val nowDateTime:LocalDateTime = LocalDateTime.now
    val days = get_similar_data_days()
    //获取两天内 article tags
    val now = nowDateTime.toString.substring(0, 19)
    val twoDaysAgo = nowDateTime.minusDays(days).toString.substring(0, 19)
    val yearMonth = nowDateTime.toString.substring(0,7)
    val read_index:String="gnews_raw_data_full"

    val query2days =
      s"""
         |{
         | "query":{
         |    "constant_score":{
         |      "filter":{
         |        "range":{
         |          "timestamp":{
         |            "gte":"$twoDaysAgo",
         |            "lt":"$now"
         |          }
         |        }
         |      }
         |    }
         |  }
         |}
        """.stripMargin


    val tagsRdd = EsSpark.esRDD(sc,s"${read_index}/article", query2days,options).repartition(8).map{news=>
      var tags = news._2.get("tags") match {
        case Some(v) => if (v == None) null else v.asInstanceOf[String]
        case None => null
      }

      var tags_index:Int = TAGS_KEY_ARR_NUM
      val data_index = news._1
      if(null!=tags && tags.trim.length>1){
        tags_index = getTagsIndex(tags)
        tags = s"${tags}--$data_index"
      }
      (tags_index, tags)
    }.filter(_._2!=null)

    val redis_flag = tags_rds_flg_Broadcast.value
   tagsRdd.aggregateByKey(new AList[String]())(seqOp = (tagsList,tagsStr) => {
      tagsList.add(tagsStr)
      tagsList
    },
      combOp = (tagsList1,tagsList2) => {
        tagsList1.addAll(tagsList2)
        tagsList1
      }).foreachPartition{tags_datas =>
      val jedis = RedisClusterHelper.getConnection
      tags_datas.foreach{data=>
        val tags_index = data._1
        val redis_key = s"$RDS_SIMI_TAGS_PREFIX:$tags_index:$redis_flag"
        jedis.lpush(redis_key,data._2.asScala:_*)
        jedis.expire(redis_key,15*60)//key的有效时间15分钟
      }
    }

  }


  def addSensetiveInfo(messageNode:ObjectNode,senseWords:Array[String]): Unit ={
    val iter = senseWords.iterator
    var sense_word_get:Boolean = false
    val title = messageNode.get("title").asText("")
    val content = messageNode.get("content").asText("")
    val sense_node = new ObjectNode(JsonNodeFactory.instance)

    while (iter.hasNext && !sense_word_get){
      val word = iter.next()
      val title_sense = title.contains(word)
      var content_sense:Boolean = false //默认内容不包含敏感词，并且按需查询，提升效率
      if(title_sense){
        sense_node.put("title",word)
        messageNode.put("datavalid",8)
      }else {
        content_sense = content.contains(word)
        if(content_sense){
          sense_node.put("body",word)
          messageNode.put("datavalid",9)
        }
      }
      sense_word_get = title_sense || content_sense
      }

     messageNode.put("contenttext",sense_node.toString)
    }



}
