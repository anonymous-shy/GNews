package com.donews.utils

import org.apache.commons.cli.{GnuParser, HelpFormatter, Options}
case class ValidateInfo(path:String,day:String)
object CmdArg {
  def parse(args: Array[String]):ValidateInfo = {
    //Option[T]是容器
    val options = new Options()
      .addOption("p", "path", true, "hdfs数据的路径")
      .addOption("d", "day", true, "验证数据的日期")
      .addOption("h", "help", false, "打印帮助信息")

    val parser = new GnuParser()
    val cmdLine = parser.parse(options, args)

    if (cmdLine.hasOption("h")) {
      val formatter = new HelpFormatter()
      formatter.printHelp("streamingMain", options)
      System.exit(0)
    }

    val hdfs_path = cmdLine.getOptionValue("p")
    val day = cmdLine.getOptionValue("d")
    ValidateInfo(hdfs_path,day)
  }

  def main(args: Array[String]): Unit = {
    parse(args)
  }
}
