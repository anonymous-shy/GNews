package com.donews.utils

import java.io.InputStreamReader
import java.security.{MessageDigest, NoSuchAlgorithmException}

import java.util

import com.google.common.io.CharStreams
import org.apache.commons.lang3.StringUtils
import org.apache.http.{Consts, NameValuePair}
import org.apache.http.client.entity.UrlEncodedFormEntity
import org.apache.http.client.methods.{HttpGet, HttpPost}
import org.apache.http.entity.StringEntity
import org.apache.http.impl.client.HttpClients
import org.apache.http.message.BasicNameValuePair
import org.apache.http.util.EntityUtils
import org.apache.spark.{SparkConf, SparkContext}
import scala.collection.JavaConversions._

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

  def get(url: String,paramStr:String): String = {
    //封装请求参数
    val params = new util.ArrayList[NameValuePair]();
    params.add(new BasicNameValuePair("d", paramStr));
    val str = EntityUtils.toString(new UrlEncodedFormEntity(params, Consts.UTF_8));
    val _get = new HttpGet(url+"?"+str)
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

  def post(url: String, content: String): String = {
    val _post = new HttpPost(url)
    _post.setHeader("Content-Type", "application/json")
    _post.setEntity(new StringEntity(content,"utf-8"))
    val resp = httpClient.execute(_post)
    try {
      CharStreams.toString(new InputStreamReader(resp.getEntity.getContent))
    } finally {
      resp.close()
    }

  }

//  def post(url: String, content: String): String = {
//    val _post = new HttpPost(url)
//    val pair1 = new BasicNameValuePair("data", content)
//                 //将准备好的键值对对象放置在一个List当中
//    val pairs = new java.util.ArrayList[NameValuePair]()
//    pairs.add(pair1)
//    //创建代表请求体的对象（注意，是请求体）
//    val requestEntity = new UrlEncodedFormEntity(pairs)
//                   //将请求体放置在请求对象当中
//    _post.setEntity(requestEntity)
//    val resp = httpClient.execute(_post)
//    try {
//      CharStreams.toString(new InputStreamReader(resp.getEntity.getContent))
//    } finally {
//      resp.close()
//    }
//
//  }

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

  import java.util.{LinkedHashMap => LKMap,ArrayList => AList}

  def transTitle(title:String):String={
    if(title.contains("国家")){
      return "GJ"
    }else if(title.contains("")){

    }
    ""
  }
  def main(args: Array[String]): Unit = {

//    val tags_map = new LKMap[String,AList[String]]()
//
//    val tuples = Array[(String,String)](("kong","用户"),("yonghu","标签"),("网易","wangY"),("网易","WY"),(null,"woshinull"))
    val conf = new SparkConf()
    conf.setAppName("rdd")
      .setMaster("local[2]")
    val sc = new SparkContext(conf)

   val rdd1 =  sc.textFile("D:\\interview\\data02.txt").map{line=>
      val values = line.split(",\t")
      val record_id = values(0).split(":")(1).replaceAll("\"|\\{|\\}","")
      val title = values(1).split(":")(1).replaceAll("\"|\\{|\\}","")
      val tags = values(2).split(":")(1).replaceAll("\"|\\{|\\}","")
      (record_id,title,tags)
    }

   println(rdd1.take(5).toBuffer)

    val prefix = "USER:"
  val rdd2 =  rdd1.map{data=>{
     val record_id = data._1
     val tags = data._3
     val hashKey = record_id.substring(0,2)
     val key = prefix+hashKey
     (key,tags)
   }}.aggregateByKey(new util.ArrayList[String]())(seqOp=(userList,tags)=>{
     userList.add(tags)
     userList
   },
     combOp = (userList1,userList2)=>{
       userList1.addAll(userList2)
       userList1
     })


    println("##################")
   println(rdd2.take(5).toBuffer)


//
//    val title:String = "\\u7ef1\\u3220\\u51f9\\u7039\\uff45\\u7af7CEO\\u9a9e\\u5145\\u7c33\\u6d93\\ufffd\\u6fb6\\ue0a2\\u76a2\\u9357\\u9550\\u6362 CFO\\u93ba\\u30e4\\u6362"
//    val context:String = "\\u7ef1\\u3220\\u51f9\\u93c2\\u9881\\u6362CEO\\u935a\\u590c\\u656f\\u934b\\u30e4\\u7af4\\u95ae\\ufffd\\u9351\\u3085\\u56a2\\u7f03\\u6220\\ue756\\u93b6\\ufffd\\u7481\\ufffd \\u93b9\\ue1bf\\u77fe\\u95ab\\u5fd5\\u305e\\u9356\\u693e\\u542b\\u93c3\\u5815\\u68ff2\\u93c8\\ufffd2\\u93c3\\u30e6\\u59e4\\u95ac\\u64c4\\u7d1d\\u7ef1\\u3220\\u51f9\\u934f\\ue100\\u5f83\\u935b\\u3124\\u7c32\\u7039\\uff45\\u7af7\\u951b\\u5c7d\\u53d5\\u9359\\u7aa9FO\\u935a\\u590c\\u656f\\u934b\\u30e4\\u7af4\\u95ae\\ufffd(Kenichiro Yoshida)\\u704f\\u55d8\\u5e34\\u93c7\\u57ae\\u94a9\\u6d5c\\u66da\\u7af4\\u6fb6\\ufffd(Kazuo Hirai)\\u93b7\\u546c\\u6362\\u934f\\ue100\\u5f83\\u93c2\\u9881\\u6362CEO\\u951b\\ufffd2018\\u9a9e\\ufffd4\\u93c8\\ufffd1\\u93c3\\u30e7\\u6553\\u93c1\\u581b\\ufffd\\ufffd\\u9a9e\\u5145\\u7c33\\u6d93\\ufffd\\u6fb6\\ue0a2\\u6e6a2012\\u9a9e\\u5b58\\u579a\\u6d93\\u8679\\u50a8\\u704f\\u7cc3EO\\u951b\\u5c7c\\u7cac\\u704f\\u55d8\\u5ab4\\u6d60\\u8364\\u50a8\\u704f\\u8270\\u61c0\\u6d5c\\u5b2e\\u66b1\\u9286\\u509a\\u6e6a\\u9a9e\\u5145\\u7c33\\u6d93\\ufffd\\u6fb6\\ue0a3\\u5ab4\\u6d60\\u8364\\u50a8\\u704f\\u7cc3EO\\u9428\\ufffd6\\u9a9e\\u64ae\\u68ff\\u951b\\u5c80\\u50a8\\u704f\\u5978\\ufffd\\ufffd\\u9351\\u70d8\\u57a8\\u9353\\u5a42\\u567a\\u6d5c\\u54d6C\\u9286\\u4f7a\\u6578\\u7459\\u55d9\\u74d1\\u6d93\\u6c2c\\u59df\\u951b\\u5c7d\\u82df\\u93b6\\u64b2\\u7d87\\u6d5c\\u55d8\\u6ae4\\u9473\\u82a5\\u6e80\\u934f\\u78cb\\u6363\\u93b7\\u590a\\u59e9\\u9365\\u60e7\\u511a\\u6d7c\\u72b3\\u5285\\u9363\\u3129\\u6e36\\u59f9\\u509c\\u6b91\\u93c8\\u6d2a\\u4ea3\\u9286\\ufffd\\u935a\\u590c\\u656f\\u934b\\u30e4\\u7af4\\u95ae\\u5ea4\\ue766\\u6fb6\\u682b\\u666b\\u93c5\\ue1c0\\u4eb6\\u7459\\u55d5\\u8d1f\\u7ef1\\u3220\\u51f9\\u7481\\u7a3f\\ue63f\\u9359\\u6a40\\u6f7b\\u9428\\u52eb\\u7bb7\\u935a\\u5ea2\\u5e39\\u93b5\\u5b28\\ufffd\\u509d\\u7e56\\u6d5c\\u6d98\\u5f49\\u95c8\\u2544\\u803d\\u5bf0\\u693e\\u7c21\\u93b6\\u66e1\\u796b\\u9470\\u546f\\u6b91\\u74a7\\u70b6\\u797b\\u9286\\ufffd\\u7ef1\\u3220\\u51f9\\u935a\\u5c7e\\u6902\\u7039\\uff45\\u7af7\\u951b\\u5c7c\\u6362\\u935b\\u85c9\\u5d04\\u93c3\\u60f0\\ue5da\\u93cd\\ufffd(Hiroki Totoki)\\u6d93\\u54c4\\u53d5\\u9359\\u544a\\u67ca\\u6d60\\u7c46FO\\u9286\\ufffd\\u9366\\u3128\\u7e56\\u6d93\\ufffd\\u6d5c\\u8f70\\u7c28\\u6d60\\u8bf2\\u61e1\\u7039\\uff45\\u7af7\\u935a\\u5eaf\\u7d1d\\u7ef1\\u3220\\u51f9\\u9472\\u2032\\u73af\\u7ed4\\u5b2a\\u5d46\\u6d93\\u5a43\\u5b9a2%\\u9286\\u509c\\u50a8\\u704f\\u5978\\ue569\\u7481\\u2605\\u7d1d\\u93c8\\ue103\\u50a8\\u9a9e\\u78cb\\u60c0\\u6d93\\u6c2c\\u57c4\\u5a11\\ufe40\\u76a2\\u9352\\u6d97\\u7b05\\u7efe\\ue044\\u7d8d\\u9286\\ufffd(\\u7f02\\u682c\\u7627/\\u7ee0\\ue0a6\\u6d26)\\u93c2\\u4f34\\u77de\\u93c8\\u590b\\u67a1\\u9428\\u52ea\\u9a87\\u6d93\\u6c2d\\u67ca\\u95c2\\u6c47\\ufffd\\u4f79\\u7e41\\u934f\\u30e6\\u796c\\u9351\\u8679\\u6b91\\u6d7c\\u4f77\\u7b1f\\u752f\\u509a\\u6e80\\u9352\\u55d8\\u703d\\u951b\\u5c83\\u4ea4\\u93c9\\u70ac\\u6e41\\u74d2\\uff47\\u6b91\\u7ec9\\u621e\\u59a7\\u6d5c\\u8679\\u58bf\\u935a\\u612d\\u042b\\u9286\\u509a\\u5696\\u9351\\u626e\\u7d89\\u7ec9\\u621e\\u59a7\\u951b\\u571bD: ifeng_tech\\u951b\\u591b\\u7d1d\\u7481\\u2543\\ue756\\u93b6\\ufffd\\u93c7\\u5b58\\ufffd\\u0444\\u5285\\u9286\\ufffd"
//    val do_str = new String(title.getBytes,"utf-8")
//    val code = GNewsUtil.checkArticleCode(title,context)
//    println(code)
//    val rdd = sc.parallelize(tuples)
//    val map = rdd.map{case (k,v)=>
//      var nkey = k
//      if(null==k){
//        nkey="kong"
//      }
//      (nkey,v)
//    }.aggregateByKey(new AList[String]())(seqOp = (tagsList,tagsStr) => {
//      tagsList.add(tagsStr)
//      tagsList
//    },
//      combOp = (tagsList1,tagsList2) => {
//        tagsList1.addAll(tagsList2)
//        tagsList1
//      }).collect().toMap
//
//     val list = new util.ArrayList[(String,String)]
//     list.add(("ni","ni01"))
//     list.add(("ni","ni02"))
//     list.add(("wo","wo01"))
//     list.add(("wo","wo02"))
//     list.add(("wo","wo03"))
//    val dataRdd = sc.parallelize[(String,String)](list)
//    dataRdd.foreach(println(_))
//
////      .groupByKey().map{case (key,iter)=>
////       val list = new AList[String]()
////       iter.foreach(list.add(_))
////      (key,list)
////    }.collect().toMap
//    println(map)

//    rdd.foreach{chnl_tags=>
//      var key=""
//      if(chnl_tags._1!=null){
//        key=chnl_tags._1
//      }else{
//        key="kong"
//      }
//      val tags = tags_map.getOrDefault(key,new AList[String]())
//      tags.add(chnl_tags._2)
//      tags_map.put(key,tags)
//
//    }

//    println(tags_map)
//  val map = Map[String,AnyRef]("ni"->None,"da"->"yd")
//   val ni_wrap = map.get("ni")
//    val ni = ni_wrap match {
//      case Some(v) => v
//      case None => null
//    }
//    println(ni)
//
//    val da_wrap = map.get("da")
//    val da = da_wrap match {
//      case Some(v) => v.asInstanceOf[String]
//      case None => null
//    }
//    println(da)

//    val jsonStr:String = "{\"img_location\": [{\"img_desc\": \"\\u54c8\\u55bd\\uff0c\\u5404\\u4f4d\\u85cf\\u53cb\\u4eec\\u5927\\u5bb6\\u597d\\uff0c\\u53c8\\u5230\\u4e00\\u5929\\u7684\\u6b23\\u8d4f\\u65f6\\u95f4\\u5566\\uff0c\\u611f\\u8c22\\u5927\\u5bb6\\u7684\\u652f\\u6301\\u548c\\u65f6\\u95f4~\", \"img_path\": \"/data/shareimg_oss/big_media_img/2017/12/27/626a96dcbbba9b3c9cb80006ab69a699.JPEG\", \"img_width\": 460, \"img_height\": 516, \"img_src\": \"http://p1.pstatp.com/origin/50a600035cc2153768b1\", \"img_index\": 1}, {\"img_desc\": \"\\u4eca\\u5929\\u6b23\\u8d4f\\u7684\\u662f\\u4e00\\u4ef6\\u63cf\\u91d1\\u7c89\\u5f69\\u82b1\\u5349\\u76d8\\uff0c\\u6700\\u8fd1\\u54b1\\u4eec\\u4e00\\u76f4\\u90fd\\u6b23\\u8d4f\\u662f\\u7684\\u76d8\\u5b50\\uff0c\\u660e\\u5929\\u4f1a\\u4e0a\\u4f20\\u4e00\\u4ef6\\u7acb\\u4ef6\\u513f\\u7684\\u54e6\\u3002\", \"img_path\": \"/data/shareimg_oss/big_media_img/2017/12/27/c14f3a8cd02e97d50232d1e60b2b5fde.JPEG\", \"img_width\": 459, \"img_height\": 609, \"img_src\": \"http://p3.pstatp.com/origin/50ab0000fdb39b4f7977\", \"img_index\": 2}, {\"img_desc\": \"\\u6e05\\u65b0\\u6de1\\u96c5\\u4e2d\\u7684\\u82b3\\u9999\\uff0c\\u6805\\u680f\\u5916\\u4e0e\\u6805\\u680f\\u5185\\u7684\\u82b1\\u5349\\u4e89\\u76f8\\u6597\\u8273\\uff0c\\u4ead\\u4ead\\u7389\\u7acb\\uff0c\\u4e0d\\u8513\\u4e0d\\u679d\\u3002\", \"img_path\": \"/data/shareimg_oss/big_media_img/2017/12/27/0dddaa8d38eabe4a1ddce6dc1d45a6b9.JPEG\", \"img_width\": 462, \"img_height\": 604, \"img_src\": \"http://p3.pstatp.com/origin/50a80003367e109fb1a8\", \"img_index\": 3}, {\"img_desc\": \"\\u8fd9\\u4e5f\\u662f\\u4e00\\u4ef6\\u5916\\u9500\\u74f7\\uff0c\\u4e0d\\u5f97\\u4e0d\\u8bf4\\uff0c\\u5916\\u56fd\\u4eba\\u8fd8\\u662f\\u628a\\u54b1\\u4eec\\u4e2d\\u56fd\\u7684\\u74f7\\u5668\\u4fdd\\u62a4\\u5f97\\u633a\\u597d\\u7684\\uff0c\\u8fd9\\u8fd8\\u662f\\u4e2a\\u5168\\u54c1\\uff0c\\u4f30\\u8ba1\\u5728\\u56fd\\u5185\\uff0c\\u547d\\u8fd0\\u5c31\\u5f88\\u60b2\\u50ac\\u4e86\\u3002\", \"img_path\": \"/data/shareimg_oss/big_media_img/2017/12/27/4c89fb69bec941ca82c598dd0a589040.JPEG\", \"img_width\": 451, \"img_height\": 608, \"img_src\": \"http://p3.pstatp.com/origin/50aa0002d99900d39c04\", \"img_index\": 4}, {\"img_desc\": \"\\u8fd9\\u4e2a\\u8fde\\u91d1\\u90fd\\u6ca1\\u600e\\u4e48\\u6389\\uff0c\\u5f69\\u4e5f\\u4e00\\u6837\\uff0c\\u662f\\u4e0d\\u662f\\u5f88\\u517b\\u773c\\u5462\\uff1f\", \"img_path\": \"/data/shareimg_oss/big_media_img/2017/12/27/a38ffe14ed45a6558385cdfa23323b99.JPEG\", \"img_width\": 458, \"img_height\": 600, \"img_src\": \"http://p9.pstatp.com/origin/50a90002e3ff411541c5\", \"img_index\": 5}, {\"img_desc\": \"\\u8fb9\\u4e0a\\u4e5f\\u7ed8\\u6709\\u82b1\\u679d\\u7f20\\u7ed5\\uff0c\\u9753\\u4e3d\\u8131\\u4fd7\\u3002\", \"img_path\": \"/data/shareimg_oss/big_media_img/2017/12/27/a288d90e3acbd4087ad99c1ec3ed9eaa.JPEG\", \"img_width\": 458, \"img_height\": 599, \"img_src\": \"http://p3.pstatp.com/origin/50a600035cc93e7afa9e\", \"img_index\": 6}, {\"img_desc\": \"\\u6bcf\\u665a\\u516b\\u70b9\\uff0c\\u53bf\\u4ee4\\u90fd\\u4f1a\\u5728\\u718a\\u732b\\u7ed9\\u5927\\u5bb6\\u8fd1\\u8ddd\\u79bb\\u7684\\u5c55\\u793a\\u8001\\u7269\\u4ef6\\u7684\\u54e6\\u3002\", \"img_path\": \"/data/shareimg_oss/big_media_img/2017/12/27/097b268e232cf09d3942bad11edb3873.JPEG\", \"img_width\": 449, \"img_height\": 550, \"img_src\": \"http://p1.pstatp.com/origin/50a600035ccb61c5d6b0\", \"img_index\": 7}, {\"img_desc\": \"\\u8859\\u95e8\\u7684\\u5b97\\u65e8\\uff1a\\u6587\\u73a9\\u5fc5\\u771f\\uff01\\u53e4\\u73a9\\u5fc5\\u8001\\uff01\\uff01\\uff01\", \"img_path\": \"/data/shareimg_oss/big_media_img/2017/12/27/be303075ae1864efa608549ddd442511.JPEG\", \"img_width\": 459, \"img_height\": 598, \"img_src\": \"http://p1.pstatp.com/origin/50a700034b40febc6416\", \"img_index\": 8}, {\"img_desc\": \"\\u73b0\\u5728\\u8bf4\\u8bf4\\u5e74\\u4ee3\\u54c8 \\uff0c\\u8fd9\\u662f\\u4e00\\u4ef6\\u4e7e\\u9686\\u7684\\u7c89\\u5f69\\u76d8\\u54e6\\uff0c\\u8fd9\\u662f\\u9274\\u5b9a\\u7ed3\\u679c\\uff0c\\u563f\\u563f\\u3002\", \"img_path\": \"/data/shareimg_oss/big_media_img/2017/12/27/5b7f3d2d62ce9432589c5f2a36bc9443.JPEG\", \"img_width\": 429, \"img_height\": 565, \"img_src\": \"http://p3.pstatp.com/origin/50a80003368bfaf40dbd\", \"img_index\": 9}, {\"img_desc\": \"\\u6bcf\\u665a\\u516b\\u70b9\\uff0c\\u718a\\u732b\\uff08649640\\uff09\\uff0c\\u53bf\\u4ee4\\u4e0e\\u5927\\u5bb6\\u4e0d\\u89c1\\u4e0d\\u6563\\uff0c\\u4e00\\u8d77\\u4ea4\\u6d41\\u8001\\u7269\\u4ef6\\u80cc\\u540e\\u7684\\u5386\\u53f2\\u6587\\u5316\\uff0c\\u987a\\u4fbf\\u89e3\\u8bf4\\u80cc\\u540e\\u7684\\u9274\\u5b9a\\u70b9\\u54e6\\u3002\", \"img_path\": \"/data/shareimg_oss/big_media_img/2017/12/27/1253cd57fa8a20211e9eea68b01407d0.JPEG\", \"img_width\": 444, \"img_height\": 562, \"img_src\": \"http://p1.pstatp.com/origin/50ad00008e61297a6616\", \"img_index\": 10}, {\"img_desc\": \"\\u6bcf\\u665a\\u516b\\u70b9\\uff0c\\u718a\\u732b\\uff08649640\\uff09\\uff0c\\u60f3\\u8981\\u770b\\u66f4\\u591a\\uff0c\\u70b9\\u70b9\\u5173\\u6ce8\\u54e6\\uff0c\\u6bcf\\u5929\\u54b1\\u4eec\\u7684\\u4e1c\\u897f\\u90fd\\u4e0d\\u4e00\\u6837\\u54e6~~\\u4e0d\\u91cd\\u590d~~~\", \"img_path\": \"/data/shareimg_oss/big_media_img/2017/12/27/7f9f671ebc5833771f9110790ecb88a1.JPEG\", \"img_width\": 455, \"img_height\": 566, \"img_src\": \"http://p1.pstatp.com/origin/50ad00008e64d08b58b0\", \"img_index\": 11}], \"data_source_sub_id\": 154, \"crawlid\": \"27\", \"data_source_unique_id\": \"CZZ-JH-001318#154\", \"status_code\": 200, \"data_source_key\": \"CZZ-JH-B\", \"video_location_count\": null, \"saved_data_location\": \"formal#crawled_TTH_web_page#crawl_toutiao_gallery\", \"toutiao_refer_url\": null, \"parsed_content_main_body\": \"\\u54c8\\u55bd\\uff0c\\u5404\\u4f4d\\u85cf\\u53cb\\u4eec\\u5927\\u5bb6\\u597d\\uff0c\\u53c8\\u5230\\u4e00\\u5929\\u7684\\u6b23\\u8d4f\\u65f6\\u95f4\\u5566\\uff0c\\u611f\\u8c22\\u5927\\u5bb6\\u7684\\u652f\\u6301\\u548c\\u65f6\\u95f4~\\u4eca\\u5929\\u6b23\\u8d4f\\u7684\\u662f\\u4e00\\u4ef6\\u63cf\\u91d1\\u7c89\\u5f69\\u82b1\\u5349\\u76d8\\uff0c\\u6700\\u8fd1\\u54b1\\u4eec\\u4e00\\u76f4\\u90fd\\u6b23\\u8d4f\\u662f\\u7684\\u76d8\\u5b50\\uff0c\\u660e\\u5929\\u4f1a\\u4e0a\\u4f20\\u4e00\\u4ef6\\u7acb\\u4ef6\\u513f\\u7684\\u54e6\\u3002\\u6e05\\u65b0\\u6de1\\u96c5\\u4e2d\\u7684\\u82b3\\u9999\\uff0c\\u6805\\u680f\\u5916\\u4e0e\\u6805\\u680f\\u5185\\u7684\\u82b1\\u5349\\u4e89\\u76f8\\u6597\\u8273\\uff0c\\u4ead\\u4ead\\u7389\\u7acb\\uff0c\\u4e0d\\u8513\\u4e0d\\u679d\\u3002\\u8fd9\\u4e5f\\u662f\\u4e00\\u4ef6\\u5916\\u9500\\u74f7\\uff0c\\u4e0d\\u5f97\\u4e0d\\u8bf4\\uff0c\\u5916\\u56fd\\u4eba\\u8fd8\\u662f\\u628a\\u54b1\\u4eec\\u4e2d\\u56fd\\u7684\\u74f7\\u5668\\u4fdd\\u62a4\\u5f97\\u633a\\u597d\\u7684\\uff0c\\u8fd9\\u8fd8\\u662f\\u4e2a\\u5168\\u54c1\\uff0c\\u4f30\\u8ba1\\u5728\\u56fd\\u5185\\uff0c\\u547d\\u8fd0\\u5c31\\u5f88\\u60b2\\u50ac\\u4e86\\u3002\\u8fd9\\u4e2a\\u8fde\\u91d1\\u90fd\\u6ca1\\u600e\\u4e48\\u6389\\uff0c\\u5f69\\u4e5f\\u4e00\\u6837\\uff0c\\u662f\\u4e0d\\u662f\\u5f88\\u517b\\u773c\\u5462\\uff1f\\u8fb9\\u4e0a\\u4e5f\\u7ed8\\u6709\\u82b1\\u679d\\u7f20\\u7ed5\\uff0c\\u9753\\u4e3d\\u8131\\u4fd7\\u3002\\u6bcf\\u665a\\u516b\\u70b9\\uff0c\\u53bf\\u4ee4\\u90fd\\u4f1a\\u5728\\u718a\\u732b\\u7ed9\\u5927\\u5bb6\\u8fd1\\u8ddd\\u79bb\\u7684\\u5c55\\u793a\\u8001\\u7269\\u4ef6\\u7684\\u54e6\\u3002\\u8859\\u95e8\\u7684\\u5b97\\u65e8\\uff1a\\u6587\\u73a9\\u5fc5\\u771f\\uff01\\u53e4\\u73a9\\u5fc5\\u8001\\uff01\\uff01\\uff01\\u73b0\\u5728\\u8bf4\\u8bf4\\u5e74\\u4ee3\\u54c8 \\uff0c\\u8fd9\\u662f\\u4e00\\u4ef6\\u4e7e\\u9686\\u7684\\u7c89\\u5f69\\u76d8\\u54e6\\uff0c\\u8fd9\\u662f\\u9274\\u5b9a\\u7ed3\\u679c\\uff0c\\u563f\\u563f\\u3002\\u6bcf\\u665a\\u516b\\u70b9\\uff0c\\u718a\\u732b\\uff08649640\\uff09\\uff0c\\u53bf\\u4ee4\\u4e0e\\u5927\\u5bb6\\u4e0d\\u89c1\\u4e0d\\u6563\\uff0c\\u4e00\\u8d77\\u4ea4\\u6d41\\u8001\\u7269\\u4ef6\\u80cc\\u540e\\u7684\\u5386\\u53f2\\u6587\\u5316\\uff0c\\u987a\\u4fbf\\u89e3\\u8bf4\\u80cc\\u540e\\u7684\\u9274\\u5b9a\\u70b9\\u54e6\\u3002\\u6bcf\\u665a\\u516b\\u70b9\\uff0c\\u718a\\u732b\\uff08649640\\uff09\\uff0c\\u60f3\\u8981\\u770b\\u66f4\\u591a\\uff0c\\u70b9\\u70b9\\u5173\\u6ce8\\u54e6\\uff0c\\u6bcf\\u5929\\u54b1\\u4eec\\u7684\\u4e1c\\u897f\\u90fd\\u4e0d\\u4e00\\u6837\\u54e6~~\\u4e0d\\u91cd\\u590d~~~\", \"like_count\": null, \"toutiao_category_class_id\": null, \"video_location\": null, \"id\": \"http://is.snssdk.com/api/news/feed/v66/?concern_id=6213176655427406337&refer=1&count=20&last_refresh_sub_entrance_interval=&loc_mode=6&tt_from=enter_auto&cp=&plugin_enable=3&strict=1&iid=16290866656&device_id=37520174148&ac=wifi&channel=xiaomi&aid=13&app_name=news_article&version_code=639&version_name=6.3.9&device_platform=android&ab_version=188750%2C157646%2C193202%2C189866%2C190089%2C172662%2C171194%2C191911%2C191179%2C193196%2C192621%2C170350%2C180929%2C189827%2C186798%2C159169%2C192450%2C174397%2C192449%2C191531%2C177166%2C192824%2C184887%2C188584%2C186947%2C191536%2C31642%2C181966%2C190164%2C189241%2C187278%2C190772%2C186626%2C192844%2C170713%2C192478%2C176739%2C156262%2C188938%2C179385%2C174430%2C177258%2C192733%2C190770%2C192889%2C188856%2C192995%2C176600%2C176608%2C187095%2C188587%2C169176%2C188597%2C176617%2C170988%2C180928%2C188592%2C180383%2C176597%2C176652%2C188599%2C176614%2C183001%2C193275&ab_client=a1%2Cc4%2Ce1%2Cf2%2Cg2%2Cf7&ab_feature=102749%2C94563&abflag=3&uuid=866145031257682&openudid=1ee337ed3bffb003&_rticket=0\", \"title\": \"\\u767e\\u5e74\\u524d\\u7684\\u7eaf\\u91d1\\u4e0a\\u8272\\uff0c\\u81f3\\u4eca\\u672a\\u8131\\u843d\\uff0c\\u5982\\u6b64\\u9738\\u9053\\uff01\", \"sub_channel\": \"\\u4e01\\u4fca\\u6656\", \"small_img_location\": [{\"img_desc\": null, \"img_path\": \"/data/shareimg_oss/big_media_img/2017/12/25/371408ee3198e568f850f220398ffa27.JPEG\", \"img_width\": 172, \"img_height\": 120, \"img_src\": \"http://p9.pstatp.com/list/50a600035cc2153768b1\", \"img_index\": 1}, {\"img_desc\": null, \"img_path\": \"/data/shareimg_oss/big_media_img/2017/12/25/3ba8f96d215b38f9deb28574fd1ca004.JPEG\", \"img_width\": 172, \"img_height\": 120, \"img_src\": \"http://p3.pstatp.com/list/50ab0000fdb39b4f7977\", \"img_index\": 2}, {\"img_desc\": null, \"img_path\": \"/data/shareimg_oss/big_media_img/2017/12/25/df32d6d55d3a3cbe9c6c9790132cd293.JPEG\", \"img_width\": 172, \"img_height\": 120, \"img_src\": \"http://p3.pstatp.com/list/50ad00008e61297a6616\", \"img_index\": 3}], \"small_img_location_count\": 3, \"comment_count\": 1, \"authorized\": null, \"batch_id\": \"2017-12-27-08-50-01-j200007_200007\", \"info_source_url\": \"http://www.toutiao.com/c/user/6239885315/#mid=6239284618\", \"channel\": \"\\u65b0\\u95fb\\u805a\\u5408\", \"publish_time\": \"2017-12-25 22:29:22\", \"response_url\": \"https://www.toutiao.com/a6503491714571829773/?iid=16290866656&app=news_article\", \"timestamp\": \"2017-12-27 09:09:35\", \"appid\": \"crawl_big_media\", \"discovery_time\": \"2017-12-27 08:51:06\", \"parsed_content\": \"<p><img src=\\\"/data/shareimg_oss/big_media_img/2017/12/27/626a96dcbbba9b3c9cb80006ab69a699.JPEG\\\"/></p><p>\\u54c8\\u55bd\\uff0c\\u5404\\u4f4d\\u85cf\\u53cb\\u4eec\\u5927\\u5bb6\\u597d\\uff0c\\u53c8\\u5230\\u4e00\\u5929\\u7684\\u6b23\\u8d4f\\u65f6\\u95f4\\u5566\\uff0c\\u611f\\u8c22\\u5927\\u5bb6\\u7684\\u652f\\u6301\\u548c\\u65f6\\u95f4~</p><p><img src=\\\"/data/shareimg_oss/big_media_img/2017/12/27/c14f3a8cd02e97d50232d1e60b2b5fde.JPEG\\\"/></p><p>\\u4eca\\u5929\\u6b23\\u8d4f\\u7684\\u662f\\u4e00\\u4ef6\\u63cf\\u91d1\\u7c89\\u5f69\\u82b1\\u5349\\u76d8\\uff0c\\u6700\\u8fd1\\u54b1\\u4eec\\u4e00\\u76f4\\u90fd\\u6b23\\u8d4f\\u662f\\u7684\\u76d8\\u5b50\\uff0c\\u660e\\u5929\\u4f1a\\u4e0a\\u4f20\\u4e00\\u4ef6\\u7acb\\u4ef6\\u513f\\u7684\\u54e6\\u3002</p><p><img src=\\\"/data/shareimg_oss/big_media_img/2017/12/27/0dddaa8d38eabe4a1ddce6dc1d45a6b9.JPEG\\\"/></p><p>\\u6e05\\u65b0\\u6de1\\u96c5\\u4e2d\\u7684\\u82b3\\u9999\\uff0c\\u6805\\u680f\\u5916\\u4e0e\\u6805\\u680f\\u5185\\u7684\\u82b1\\u5349\\u4e89\\u76f8\\u6597\\u8273\\uff0c\\u4ead\\u4ead\\u7389\\u7acb\\uff0c\\u4e0d\\u8513\\u4e0d\\u679d\\u3002</p><p><img src=\\\"/data/shareimg_oss/big_media_img/2017/12/27/4c89fb69bec941ca82c598dd0a589040.JPEG\\\"/></p><p>\\u8fd9\\u4e5f\\u662f\\u4e00\\u4ef6\\u5916\\u9500\\u74f7\\uff0c\\u4e0d\\u5f97\\u4e0d\\u8bf4\\uff0c\\u5916\\u56fd\\u4eba\\u8fd8\\u662f\\u628a\\u54b1\\u4eec\\u4e2d\\u56fd\\u7684\\u74f7\\u5668\\u4fdd\\u62a4\\u5f97\\u633a\\u597d\\u7684\\uff0c\\u8fd9\\u8fd8\\u662f\\u4e2a\\u5168\\u54c1\\uff0c\\u4f30\\u8ba1\\u5728\\u56fd\\u5185\\uff0c\\u547d\\u8fd0\\u5c31\\u5f88\\u60b2\\u50ac\\u4e86\\u3002</p><p><img src=\\\"/data/shareimg_oss/big_media_img/2017/12/27/a38ffe14ed45a6558385cdfa23323b99.JPEG\\\"/></p><p>\\u8fd9\\u4e2a\\u8fde\\u91d1\\u90fd\\u6ca1\\u600e\\u4e48\\u6389\\uff0c\\u5f69\\u4e5f\\u4e00\\u6837\\uff0c\\u662f\\u4e0d\\u662f\\u5f88\\u517b\\u773c\\u5462\\uff1f</p><p><img src=\\\"/data/shareimg_oss/big_media_img/2017/12/27/a288d90e3acbd4087ad99c1ec3ed9eaa.JPEG\\\"/></p><p>\\u8fb9\\u4e0a\\u4e5f\\u7ed8\\u6709\\u82b1\\u679d\\u7f20\\u7ed5\\uff0c\\u9753\\u4e3d\\u8131\\u4fd7\\u3002</p><p><img src=\\\"/data/shareimg_oss/big_media_img/2017/12/27/097b268e232cf09d3942bad11edb3873.JPEG\\\"/></p><p>\\u6bcf\\u665a\\u516b\\u70b9\\uff0c\\u53bf\\u4ee4\\u90fd\\u4f1a\\u5728\\u718a\\u732b\\u7ed9\\u5927\\u5bb6\\u8fd1\\u8ddd\\u79bb\\u7684\\u5c55\\u793a\\u8001\\u7269\\u4ef6\\u7684\\u54e6\\u3002</p><p><img src=\\\"/data/shareimg_oss/big_media_img/2017/12/27/be303075ae1864efa608549ddd442511.JPEG\\\"/></p><p>\\u8859\\u95e8\\u7684\\u5b97\\u65e8\\uff1a\\u6587\\u73a9\\u5fc5\\u771f\\uff01\\u53e4\\u73a9\\u5fc5\\u8001\\uff01\\uff01\\uff01</p><p><img src=\\\"/data/shareimg_oss/big_media_img/2017/12/27/5b7f3d2d62ce9432589c5f2a36bc9443.JPEG\\\"/></p><p>\\u73b0\\u5728\\u8bf4\\u8bf4\\u5e74\\u4ee3\\u54c8 \\uff0c\\u8fd9\\u662f\\u4e00\\u4ef6\\u4e7e\\u9686\\u7684\\u7c89\\u5f69\\u76d8\\u54e6\\uff0c\\u8fd9\\u662f\\u9274\\u5b9a\\u7ed3\\u679c\\uff0c\\u563f\\u563f\\u3002</p><p><img src=\\\"/data/shareimg_oss/big_media_img/2017/12/27/1253cd57fa8a20211e9eea68b01407d0.JPEG\\\"/></p><p>\\u6bcf\\u665a\\u516b\\u70b9\\uff0c\\u718a\\u732b\\uff08649640\\uff09\\uff0c\\u53bf\\u4ee4\\u4e0e\\u5927\\u5bb6\\u4e0d\\u89c1\\u4e0d\\u6563\\uff0c\\u4e00\\u8d77\\u4ea4\\u6d41\\u8001\\u7269\\u4ef6\\u80cc\\u540e\\u7684\\u5386\\u53f2\\u6587\\u5316\\uff0c\\u987a\\u4fbf\\u89e3\\u8bf4\\u80cc\\u540e\\u7684\\u9274\\u5b9a\\u70b9\\u54e6\\u3002</p><p><img src=\\\"/data/shareimg_oss/big_media_img/2017/12/27/7f9f671ebc5833771f9110790ecb88a1.JPEG\\\"/></p><p>\\u6bcf\\u665a\\u516b\\u70b9\\uff0c\\u718a\\u732b\\uff08649640\\uff09\\uff0c\\u60f3\\u8981\\u770b\\u66f4\\u591a\\uff0c\\u70b9\\u70b9\\u5173\\u6ce8\\u54e6\\uff0c\\u6bcf\\u5929\\u54b1\\u4eec\\u7684\\u4e1c\\u897f\\u90fd\\u4e0d\\u4e00\\u6837\\u54e6~~\\u4e0d\\u91cd\\u590d~~~</p>\", \"toutiao_out_url\": null, \"parsed_content_char_count\": 2191, \"tags\": [\"\\u82b1\", \"\\u5927\\u718a\\u732b\", \"\\u74f7\\u5668\", \"\\u4e7e\\u9686\", \"\\u4e4c\\u8d3c\"], \"desc\": null, \"url_domain\": \"www.toutiao.com\", \"article_genre\": \"gallery\", \"toutiao_category_class\": \"news\", \"click_count\": 130, \"url\": \"http://m.toutiao.com/a6503491714571829773/?iid=16290866656&app=news_article\", \"author\": null, \"repost_count\": null, \"data_source_type\": \"\\u5782\\u76f4\\u7ad9\", \"data_source_id\": \"CZZ-JH-001318\", \"media\": \"\\u4eca\\u65e5\\u5934\\u6761\\u624b\\u673a-\\u4f53\\u80b2\", \"info_source\": \"\\u51ac\\u85cf\\u6625\\u79cb\", \"img_location_count\": 11}"
//    val messageNode = JsonNodeUtils.getJsonNodeFromStringContent(jsonStr).asInstanceOf[ObjectNode]
//   GnewsDataService.common_validate(messageNode,"es_index","index_error")
//    if(!messageNode.has(Constant.FIELD_ERROR_KEY))
//          GnewsDataService.sendImgtask2Redis(messageNode)
//    GnewsDataService.common_validate(messageNode,"es_index","error_index")
//    val tags_node = messageNode.get("tags").textValue()
//    println(tags_str)

  }

}
