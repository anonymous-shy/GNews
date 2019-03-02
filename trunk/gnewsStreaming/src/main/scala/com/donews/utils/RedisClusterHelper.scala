package com.donews.utils

import java.util

import redis.clients.jedis._

/**
  * Created by HLS on 2016/11/2.
  */
object RedisClusterHelper {

  private var cluster: JedisCluster = null

  def getConnection: JedisCluster = {
    if(this.cluster == null){
      val config: JedisPoolConfig = initConnConfig
      val nodes: util.LinkedHashSet[HostAndPort] = initNodes
      this.cluster = new JedisCluster(nodes, config)
    }
    this.cluster
  }

  private def initNodes = {
    val nodes: util.LinkedHashSet[HostAndPort] = new util.LinkedHashSet[HostAndPort]()
    val properties = MysqlUtils.getProperties()
    val redis_cluster_connect: String = properties.getProperty("redis_cluster_connect")
    val host_port_arr: Array[String] = redis_cluster_connect.split(",")
    host_port_arr.foreach(it => {
      val host_port = it.split(":")
      val host = host_port(0)
      val port = host_port(1).toInt
      nodes.add(new HostAndPort(host, port))
    })
    nodes
  }

  private def initConnConfig = {
    val config = new JedisPoolConfig()
    //最大连接数
    config.setMaxTotal(10000)
    //最大空闲连接数
    config.setMaxIdle(100)
    //当调用borrow Object方法时，是否进行有效性检查
    config.setTestOnBorrow(true)
    //设置超时时间
    config.setMaxWaitMillis(3000)
    config
  }

  def main(args: Array[String]): Unit = {
    val jedisCluster = getConnection
    jedisCluster.set("liu","value20170620")
    val value = jedisCluster.get("liu")
    println(value)
  }
}
