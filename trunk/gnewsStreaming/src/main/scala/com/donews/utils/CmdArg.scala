package com.donews.utils

import org.apache.commons.cli.{GnuParser, HelpFormatter, Options}

object CmdArg {
  def parse(args: Array[String]): (String) = {
    //Option[T]是容器
    val options = new Options()
      .addOption("t", "topic", true, "从kafka消费的topic名称")
      .addOption("h", "help", false, "打印帮助信息")

    val parser = new GnuParser()
    val cmdLine = parser.parse(options, args)

    if (cmdLine.hasOption("h")) {
      val formatter = new HelpFormatter()
      formatter.printHelp("streamingMain", options)
      System.exit(0)
    }

    val topic = cmdLine.getOptionValue("t")

    topic
  }

  def main(args: Array[String]): Unit = {
    parse(args)
  }
}
