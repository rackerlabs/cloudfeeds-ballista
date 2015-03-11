package com.rackspace.feeds.ballista.config

import org.joda.time.DateTime
import org.joda.time.format.{DateTimeFormatter, DateTimeFormat}
import org.junit.runner.RunWith
import org.scalatest.FunSuite
import org.scalatest.junit.JUnitRunner

@RunWith(classOf[JUnitRunner])
class CommandOptionsParserTest extends FunSuite {

  val dateTimePattern: DateTimeFormatter = DateTimeFormat.forPattern("yyyy-MM-dd")
  
  test ("verify default options are being sent when no command line options are given") {
    val args = Array[String]()

    CommandOptionsParser.getCommandOptions(args) match {
      case Some(commandOptions) =>
        assert(commandOptions.runDate === DateTime.now.minusDays(1).withTimeAtStartOfDay(), "runDate option has the wrong default value")
        assert(commandOptions.dbNames.size === AppConfig.export.from.dbs.dbConfigMap.keySet.size, "dbNames option has the wrong default value")
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

  val referenceDate = DateTime.now
  List(DateTime.now,
       DateTime.now.plusDays(1),
       DateTime.now.minusDays(AppConfig.export.daysDataAvailable + 1)
  ).foreach {dateTime =>
    test (s"rundate should be invalid when rundate is $dateTime on $referenceDate") {

      CommandOptionsParser.isValidRunDate(dateTime, referenceDate) match {
        case true =>
          fail(s"$dateTime as runDate should be invalid on $referenceDate")
        case false =>
      }
    }
  }
  
  (1 to AppConfig.export.daysDataAvailable).foreach{numberOfDays =>
    test (s"rundate should be valid when rundate is $numberOfDays days back from $referenceDate") {

      val runDate = referenceDate.minusDays(numberOfDays)
      CommandOptionsParser.isValidRunDate(runDate, referenceDate) match {
        case true =>
        case false =>
          fail(s"$runDate as runDate should be valid on $referenceDate")
      }
    }
  }
  
  test ("parsing validation should fail for invalid rundate") {
    val args = Array[String]("-d", dateTimePattern.print(DateTime.now))

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
    val runDate: DateTime = DateTime.now.minusDays(2).withTimeAtStartOfDay()
    val dbNames: Set[String] = AppConfig.export.from.dbs.dbConfigMap.keySet

    val runDateStr: String = dateTimePattern.print(runDate)
    val dbNamesStr: String = dbNames.mkString(",")

    val args = Array[String]("--runDate", runDateStr, "--dbNames", dbNamesStr, "--dryrun")

    CommandOptionsParser.getCommandOptions(args) match {
      case Some(commandOptions) =>
        assert(commandOptions.runDate === runDate, "runDate option is not set to the correct value")
        assert(commandOptions.dbNames === dbNames, "dbNames option is not set to the correct value")
        assert(commandOptions.dryrun === true, "dryrun option is not set to the correct value")
      case None => fail("Unable to parse command options")
    }
  }

}
