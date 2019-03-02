package com.donews.utils

import java.util

import redis.clients.jedis._

/**
  * Created by HLS on 2016/11/2.
  */
object RedisClusterHelper {
  private val config = new JedisPoolConfig()
  //最大连接数
  config.setMaxTotal(10000)
  //最大空闲连接数
  config.setMaxIdle(100)
  //当调用borrow Object方法时，是否进行有效性检查
  config.setTestOnBorrow(true)
  //设置超时时间
  config.setMaxWaitMillis(3000)

  private val nodes: util.LinkedHashSet[HostAndPort] = new util.LinkedHashSet[HostAndPort]()

  private val redis_cluster_connect: String = "10.51.119.39:7000,10.44.136.182:7000,10.44.159.186:7000"
  private val host_port_arr: Array[String] = redis_cluster_connect.split(",")

  host_port_arr.foreach(it => {
    val host_port = it.split(":")
    val host = host_port(0)
    val port = host_port(1).toInt
    nodes.add(new HostAndPort(host, port))
  })

  private var cluster: JedisCluster = new JedisCluster(nodes, config)

  def getConnection: JedisCluster = {
    if(this.cluster == null)
      this.cluster = new JedisCluster(nodes,config)
    this.cluster
  }
}
