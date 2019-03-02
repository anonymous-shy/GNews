package com.donews.streaming

import com.donews.streaming.GnewsDataService.getFieldNode
import com.fasterxml.jackson.databind.JsonNode
import com.fasterxml.jackson.databind.node.{ArrayNode, JsonNodeType, ObjectNode}


/**
  * Donews Company
  * Created by Liu dh 
  * 2017/10/13.18:17
  */
object GnewsDataService {



  // {"enrionment": "formal", "collection": "bdfyb", "database": "dmt_jh_data"}
  def processSavedLocation(messageNode:ObjectNode): Unit ={
    val save_location_nd = getFieldNode(messageNode,"saved_data_location")
    if(null != save_location_nd){
      if(save_location_nd.getNodeType.equals(JsonNodeType.OBJECT)){
        val enrionment = if(save_location_nd.has("enrionment"))save_location_nd.get("enrionment").textValue() else ""
        val db = if(save_location_nd.has("database"))save_location_nd.get("database").textValue() else ""
        val table =if(save_location_nd.has("collection"))save_location_nd.get("collection").textValue() else ""
        messageNode.put("saved_data_location",s"${enrionment}#${db}#$table")
      }
    }else messageNode.put("saved_data_location","")
  }

  def getFieldNode(messageNode:ObjectNode,obj_type_field:String): JsonNode ={
    val field_node = messageNode.get(obj_type_field)
    if(null==field_node || field_node.getNodeType.equals(JsonNodeType.NULL)){
      return null
    }else field_node
  }



}
