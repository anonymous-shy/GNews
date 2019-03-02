package com.donews.utils

import java.sql.{Connection, DriverManager}
import java.util.Properties

import com.donews.streaming.ProcessLog
import com.mysql.jdbc.Driver
/**
  * Donews Company
  * Created by Liu dh 
  * 2017/9/18.17:29
  */
object MysqlUtils {
  val driver = "com.mysql.jdbc.Driver"
  val url = "jdbc:mysql://mysql-database/niuer_news"
  val username = "kafka2es"
  val password = "roxipCWK(0}C~a"
  val encoding = "utf-8"
  Class.forName(driver)
  var properties:Properties = null

  def getProperties():Properties = {
    if(null == properties){
      properties = new Properties()
      var connection:Connection = null
      try {
        connection = DriverManager.getConnection(url, username, password)
        val statement = connection.createStatement()
        val resultSet = statement.executeQuery("select props_name, props_value from niuer_propertis")
        while ( resultSet.next() ) {
          val name = resultSet.getString("props_name")
          val value = resultSet.getString("props_value")
          properties.setProperty(name,value)
          println("name, value = " + name + ", " + value)
        }
      } catch {
        case e => e.printStackTrace

      }
      connection.close()
    }

    properties
  }

  def writeLog2Mysql(log:ProcessLog): Unit ={
    var connection:Connection = null
    var index=0
    try {
      connection = DriverManager.getConnection(url, username, password)
      val sql = "insert into niuer_news.niuer_data_info(topic,day_str,process_time,total_data_num," +
        "valid_data_num,error_data_num) values (?,?,?,?,?,?)"
      val ps = connection.prepareStatement(sql)

      ps.setString(1, log.topic)
      ps.setString(2, log.day_str)
      ps.setString(3, log.process_time)
      ps.setInt(4, log.total_data_num)
      ps.setInt(5, log.valid_data_num)
      ps.setInt(6, log.error_data_num)

      ps.execute()

    } catch {
      case e => e.printStackTrace

    }
    connection.close()
  }

  def main(args: Array[String]): Unit = {
    getProperties()


  }

}
