package com.rackspace.feeds.ballista

import com.rackspace.feeds.ballista.config.CommandOptionsParser
import org.slf4j.LoggerFactory

class AppMain {

  val logger = LoggerFactory.getLogger(getClass)

  def doProcess(args: Array[String]): Unit = {

    CommandOptionsParser.getCommandOptions(args) match {
      case Some(commandOptions) =>
        new CommandProcessor().doProcess(commandOptions)
      case None =>
        println("Invalid command options. For help try with option --help")
    }
  }


}

object AppMain {

  def main(args: Array[String]) {
    new AppMain().doProcess(args)
  }
  
}