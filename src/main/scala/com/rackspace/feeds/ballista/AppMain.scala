package com.rackspace.feeds.ballista

import com.rackspace.feeds.ballista.config.{CommandOptionsParser, CommandOptions, AppConfig}
import com.rackspace.feeds.ballista.service.DefaultExportSvc
import org.slf4j.LoggerFactory

class AppMain {

  val logger = LoggerFactory.getLogger(getClass)

  def doProcess(args: Array[String]): Unit = {

    CommandOptionsParser.getCommandOptions(args) match {
      case Some(commandOptions) =>
        doProcess(commandOptions)
      case None =>
        println("Invalid command options. For help try with option --help")
    }
  }
  
  def doProcess(commandOptions: CommandOptions): Unit = {
    logger.info(s"Process is being run with these options $commandOptions")

    val queryParams: Map[String, Any] = Map("runDate" -> commandOptions.runDate)
    
    commandOptions.dbNames.foreach(
      new DefaultExportSvc(_).export(queryParams, commandOptions.overwrite)
    )
  }

}

object AppMain {

  def main(args: Array[String]) {
    new AppMain().doProcess(args)
  }
  
}