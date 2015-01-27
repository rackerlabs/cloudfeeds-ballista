package com.rackspace.feeds.ballista.config

import org.joda.time.DateTime
import org.joda.time.format.DateTimeFormat
import scopt.Read


object CommandOptionsParser {

  implicit val jodaTimeRead: Read[DateTime] = scopt.Read.reads {
    DateTimeFormat.forPattern("yyyy-MM-dd").parseDateTime
  }
  
  implicit val setOfStringsRead: Read[Set[String]] = scopt.Read.reads {
    (s: String) => s.split(",").map(_.trim).toSet
  }
  
  private val dbNamesSet = AppConfig.export.from.dbs.dbConfigMap.keySet
  
  val parser = new scopt.OptionParser[CommandOptions]("com.rackspace.feeds.ballista.AppMain") {
    head("Ballista")
    opt[DateTime]('d', "runDate") action { (dateTime, callback) =>
        callback.copy(runDate = dateTime) 
      } validate { dateTime =>
        if (dateTime.isBefore(DateTime.now.withTimeAtStartOfDay)) 
          success 
        else 
          failure(s"Invalid runDate[$dateTime] specified. runDate should be less than today")
      } text {
        """
          |runDate is a date in the format yyyy-MM-dd.
          |Data belonging to this date will be exported
        """.stripMargin.replaceAll("\n", " ")
      }
    opt[Set[String]]('n', "dbNames") action { (x, c) =>
        c.copy(dbNames = x) 
      } validate { dbs =>
        if (dbs.size > 0 && dbs.subsetOf(dbNamesSet)) 
          success 
        else 
          failure(s"Invalid dbNames[${dbs.toArray.mkString(",")}] specified. Available dbNames: ${dbNamesSet.mkString(",")}")
      } text {
        s"""
          |dbNames is comma separated list of database names to be exported
          |Available dbNames for this configuration: ${dbNamesSet.mkString(",")}
        """.stripMargin.replaceAll("\n", " ")
      }
    opt[Boolean]('o', "overwrite") action { (x, c) =>
        c.copy(overwrite = x) 
      } text {
        """
          |overwrite is a true/false flag.
          |Set this to true to overwrite the output file if already present
        """.stripMargin.replaceAll("\n", " ")
      } 
    help("help") text {
      """
        |Use this option to get detailed usage information of this utility.
      """.stripMargin.replaceAll("\n", " ")
    }
  }


  def getCommandOptions(args: Array[String]): Option[CommandOptions] = {
    CommandOptionsParser.parser.parse(args, CommandOptions())
  }

}
