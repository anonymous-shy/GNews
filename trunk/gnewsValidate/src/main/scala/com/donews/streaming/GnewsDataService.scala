package com.donews.streaming

import java.time.LocalDate
import java.util

import com.fasterxml.jackson.databind.{JsonNode, ObjectMapper}
import com.fasterxml.jackson.databind.node.{ArrayNode, JsonNodeType, ObjectNode}
import com.fasterxml.jackson.module.scala.DefaultScalaModule


/**
  * Donews Company
  * Created by Liu dh 
  * 2017/10/13.18:17
  */
object GnewsDataService {

  val mapper = new ObjectMapper()
  mapper.registerModule(DefaultScalaModule)
  private def datajson2JsonNode(msg: String):ObjectNode = {
    val messageNode = JsonNodeUtils.getJsonNodeFromStringContent(msg).asInstanceOf[ObjectNode]
    messageNode
  }

  protected def jsonNode2Map(messageNode: ObjectNode): util.HashMap[String, Object] = {
    val objectMapper = new ObjectMapper
    objectMapper.readValue(messageNode.toString, classOf[util.HashMap[String,Object]])
  }
  def forJson(json:String,day:String):GnewsBean={
   val bean =  mapper.readValue(json,classOf[GnewsBean])
   if(null == bean || !bean.timestamp.startsWith(day))return null
   else bean
  }

  def getFieldNode(messageNode:ObjectNode,obj_type_field:String): JsonNode ={
    val field_node = messageNode.findValue(obj_type_field)
    if(null==field_node || field_node.getNodeType.equals(JsonNodeType.NULL)){
      return null
    }else field_node
  }



}
