package com.rackspace.feeds.ballista

import com.rackspace.feeds.ballista.config.CommandOptionsParser
import org.slf4j.LoggerFactory

class AppMain {

  val logger = LoggerFactory.getLogger(getClass)

  /**
   * This method processes the given command and returns a corresponding exit
   * code.
   *  
   * @param args
   * @return
   */
  def doProcess(args: Array[String]): Int = {

    val exitCode = CommandOptionsParser.getCommandOptions(args) match {
      case Some(commandOptions) =>
        new CommandProcessor().doProcess(commandOptions)

      case None =>
        println("Invalid command options. For help try with option --help")
        CommandProcessor.EXIT_CODE_INVALD_ARGUMENTS
    }
    
    logger.info(s"Process completed with exit code $exitCode")
    exitCode
  }


}

object AppMain {

  def main(args: Array[String]) {
    val exitCode = new AppMain().doProcess(args)
    System.exit(exitCode)
  }
  
}