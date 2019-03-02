package com.donews.utils

import kafka.common.TopicAndPartition
import org.apache.curator.framework.CuratorFrameworkFactory
import org.apache.curator.retry.ExponentialBackoffRetry
import org.slf4j.LoggerFactory

import scala.collection.JavaConversions._

/**
  * Created by yuhui on 16-6-8.
  */
object ZookeeperHelper {
  val LOG = LoggerFactory.getLogger(ZookeeperHelper.getClass)
  val client = {
    val client = CuratorFrameworkFactory
      .builder
      .connectString("slave03:2181,slave01:2181,slave02:2181")
      .retryPolicy(new ExponentialBackoffRetry(1000, 3))
      .namespace("niuer")
      .build()
    client.start()
    client
  }

  //zookeeper创建路径
  def ensurePathExists(path: String): Unit = {
    if (client.checkExists().forPath(path) == null) {
      client.create().creatingParentsIfNeeded().forPath(path)
    }
  }

  //zookeeper加载offset的方法
  def loadOffsets(topicSet: Set[String], defaultOffset: Map[TopicAndPartition, Long]): Map[TopicAndPartition, Long] = {
    val kafkaOffsetPath = s"/streaming/kafka_offsets"
    ensurePathExists(kafkaOffsetPath)
    val offsets = for {
    //t就是路径webstatistic/kafkaOffsets下面的子目录遍历
      t <- client.getChildren.forPath(kafkaOffsetPath)
      if topicSet.contains(t)
    //p就是新路径   /webstatistic/kafkaOffsets/donews_website
      p <- client.getChildren.forPath(s"$kafkaOffsetPath/$t")
    } yield {
      //遍历路径下面的partition中的offset
      val data = client.getData.forPath(s"$kafkaOffsetPath/$t/$p")
      //将data变成Long类型
      val offset = java.lang.Long.valueOf(new String(data)).toLong
      (TopicAndPartition(t, Integer.parseInt(p)), offset)
    }
    defaultOffset ++ offsets.toMap
  }

  //zookeeper存储offset的方法
  def storeOffsets(offsets: Map[TopicAndPartition, Long]): Unit = {
    val kafkaOffsetPath = s"/streaming/kafka_offsets"
    if (client.checkExists().forPath(kafkaOffsetPath) == null) {
      client.create().creatingParentsIfNeeded().forPath(kafkaOffsetPath)
    }
    for ((tp, offset) <- offsets) {
      val data = String.valueOf(offset).getBytes
      val path = s"$kafkaOffsetPath/${tp.topic}/${tp.partition}"
      ensurePathExists(path)
      client.setData().forPath(path, data)
    }
  }
}
