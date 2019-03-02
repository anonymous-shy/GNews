package com.donews.utils

import java.io.InputStreamReader
import java.security.{MessageDigest, NoSuchAlgorithmException}
import java.time.Instant
import java.util.concurrent.TimeUnit

import com.donews.streaming.{GnewsDataService, JsonNodeUtils}
import com.fasterxml.jackson.databind.node._
import com.google.common.io.CharStreams
import org.apache.commons.lang3.StringUtils
import org.apache.http.NameValuePair
import org.apache.http.client.entity.UrlEncodedFormEntity
import org.apache.http.client.methods.{HttpGet, HttpPost}
import org.apache.http.impl.client.HttpClients
import org.apache.http.message.BasicNameValuePair

/**
  * Created by lihui on 2016/8/2.
  */
object Https {
  private val httpClient = HttpClients.createDefault()

  def get(url: String): String = {
    val _get = new HttpGet(url)
    val resp = httpClient.execute(_get)
    try {
      if (resp.getStatusLine.getStatusCode != 200) {
        throw new RuntimeException("error: " + resp.getStatusLine)
      }
      CharStreams.toString(new InputStreamReader(resp.getEntity.getContent))
    } finally {
      resp.close()
    }
  }


//  def post(url: String, content: String): String = {
//    val _post = new HttpPost(url)
//    _post.setHeader("Content-Type", "application/json")
//    _post.setEntity(new StringEntity(content,"utf-8"))
//    val resp = httpClient.execute(_post)
//    try {
//      CharStreams.toString(new InputStreamReader(resp.getEntity.getContent))
//    } finally {
//      resp.close()
//    }
//
//  }

  def post(url: String, content: String): String = {
    val _post = new HttpPost(url)
    val pair1 = new BasicNameValuePair("data", content)
                 //将准备好的键值对对象放置在一个List当中
    val pairs = new java.util.ArrayList[NameValuePair]()
    pairs.add(pair1)
    //创建代表请求体的对象（注意，是请求体）
    val requestEntity = new UrlEncodedFormEntity(pairs)
                   //将请求体放置在请求对象当中
    _post.setEntity(requestEntity)
    val resp = httpClient.execute(_post)
    try {
      CharStreams.toString(new InputStreamReader(resp.getEntity.getContent))
    } finally {
      resp.close()
    }

  }

  /**
    *
    * @param plainText
    *            明文
    * @return 32位密文
    */
  def encryption(plainText:String):String = {
    var md5_str = ""
    try {
      val md = MessageDigest.getInstance("MD5")
      md.update(plainText.getBytes())
      val b = md.digest()
      var i = -1
      val buf = new StringBuffer("");
      for (offset <- 0 until(b.length)) {
        i = b(offset);
        if (i < 0)
          i += 256;
        if (i < 16)
          buf.append("0")
        buf.append(Integer.toHexString(i))
      }
      md5_str = buf.toString();

    } catch  {
      case e:NoSuchAlgorithmException => e.printStackTrace()
    }
    md5_str
  }


  def main(args: Array[String]): Unit = {

//
    val response = get("http://spider_slave05:9200/gnews_raw_data_full_2017-11/_mappings")



  }

}
