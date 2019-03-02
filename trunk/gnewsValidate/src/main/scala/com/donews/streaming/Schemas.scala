package com.donews.streaming

import com.fasterxml.jackson.annotation.JsonIgnoreProperties

/**
  * Donews Company
  * Created by Liu dh 
  * 2017/10/18.16:50
  */

/**
  * clid
channelid
userid
channelsubid
page
pagesize
replycommentid
replyuserid
reasonid
logintype
commentid
platform
ztid
  */
@JsonIgnoreProperties(ignoreUnknown = true)
case class GnewsBean(var url :String,
                    var timestamp :String = "")


