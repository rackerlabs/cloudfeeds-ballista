package com.rackspace.feeds.ballista.config

import org.joda.time.DateTime
import org.joda.time.format.DateTimeFormat
import org.scalatest.FunSuite

class CommandOptionsParserTest extends FunSuite {

  test ("verify default options are being sent when no command line options are given") {
    val args = Array[String]()

    CommandOptionsParser.getCommandOptions(args) match {
      case Some(commandOptions) =>
        assert(commandOptions.runDate === DateTime.now.minusDays(1).withTimeAtStartOfDay(), "runDate option has the wrong default value")
        assert(commandOptions.dbNames.size === 0, "dbNames option has the wrong default value")
        assert(commandOptions.overwrite === false, "overwrite option has the wrong default value")
      case None => fail("Unable to parse command options")
    }
  }

  test ("parsing validation should fail for invalid command options") {
    val args = Array[String]("-x", "invalid_command_option")

    CommandOptionsParser.getCommandOptions(args) match {
      case Some(commandOptions) =>
        fail("Parsing should not be successful with these command options")
      case None =>
    }
  }
  
  test ("parsing validation should fail for invalid rundate") {
    val args = Array[String]("-d", DateTimeFormat.forPattern("yyyy-MM-dd").print(DateTime.now))

    CommandOptionsParser.getCommandOptions(args) match {
      case Some(commandOptions) =>
        fail("Parsing should not be successful with these command options")
      case None => 
    }
  }
  
  Array[Array[String]](
    Array[String]("-n", ""),
    Array[String]("-n", "invalid_db_name"),
    Array[String]("-n", AppConfig.export.from.dbs.dbConfigMap.keySet.head + ",invalid_db_name")
  ).foreach { args =>

    test (s"parsing validation should fail for ${args(1)} specified") {
      CommandOptionsParser.getCommandOptions(args) match {
        case Some(commandOptions) =>
          fail("Parsing should not be successful with these command options")
        case None =>
      }
    }
    
  }

  test ("parsing validation should fail for invalid overwrite flag") {
    val args = Array[String]("-o", "yeah")

    CommandOptionsParser.getCommandOptions(args) match {
      case Some(commandOptions) =>
        fail("Parsing should not be successful with these command options")
      case None =>
    }
  }

  test ("verify options sent from command line are being set correctly") {
    val runDate: DateTime = DateTime.now.minusDays(10).withTimeAtStartOfDay()
    val dbNames: Set[String] = AppConfig.export.from.dbs.dbConfigMap.keySet
    val overwrite: Boolean = true
    
    val runDateStr: String = DateTimeFormat.forPattern("yyyy-MM-dd").print(runDate)
    val dbNamesStr: String = dbNames.mkString(",")
    val overwriteStr: String = overwrite.toString
    
    val args = Array[String]("--runDate", runDateStr, "--dbNames", dbNamesStr, "--overwrite", overwriteStr)

    CommandOptionsParser.getCommandOptions(args) match {
      case Some(commandOptions) =>
        assert(commandOptions.runDate === runDate, "runDate option is not set to the correct value")
        assert(commandOptions.dbNames === dbNames, "dbNames option is not set to the correct value")
        assert(commandOptions.overwrite === overwrite, "overwrite option is not set to the correct value")
      case None => fail("Unable to parse command options")
    }
  }
}
