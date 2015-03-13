package com.rackspace.feeds.ballista

import java.io.{PrintWriter, Writer}

import com.rackspace.feeds.ballista.config.AppConfig.export.from.dbs._
import com.rackspace.feeds.ballista.config.AppConfig.export.to.hdfs.scp._
import com.rackspace.feeds.ballista.config.CommandOptions
import com.rackspace.feeds.ballista.constants.DBProps
import com.rackspace.feeds.ballista.service.{DryRunProcessor, DefaultExportSvc, LocalFSClient}
import com.rackspace.feeds.ballista.util.{SCPSessionInfo, SCPUtil}
import org.apache.commons.io.IOUtils
import org.joda.time.DateTime
import org.joda.time.format.DateTimeFormat
import org.slf4j.LoggerFactory

import scala.collection.mutable
import scala.collection.mutable.{HashMap, MultiMap, Set}
import scala.util.{Failure, Success, Try}

object CommandProcessor {
  val EXIT_CODE_SUCCESS: Int = 0
  val EXIT_CODE_INVALD_ARGUMENTS: Int = 1
  val EXIT_CODE_FAILURE: Int = 2
}

/**
 * This class is not thread safe
 */
class CommandProcessor {

  val logger = LoggerFactory.getLogger(getClass)
  val fsClient = new LocalFSClient

  val scpUtil = new SCPUtil
  val sessionInfo = new SCPSessionInfo(user, password, host, port, privateKeyFilePath, privateKeyPassPhrase)
  
  val SUCCESS_FILE_NAME: String = "_SUCCESS"

  //creates a map of outputLocation and Set[dbNames] storing data in that output location
  val outputLocationMap = new mutable.HashMap[String, mutable.Set[String]] with mutable.MultiMap[String, String]
  
  def doProcess(commandOptions: CommandOptions): Int = {
    logger.info(s"Process is being run with these options $commandOptions")
    if (commandOptions.runDate.isAfter(DateTime.now.minusDays(1).withTimeAtStartOfDay())) {
      logger.info(s"!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!")
      logger.info(s"!!!!! WARNING: Ballista is being run for today's date. Exporting partial data. !!!!!!")
      logger.info(s"!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!")
    }
    
    
    commandOptions.dryrun match {
      case true => new DryRunProcessor().dryrun()
      case false => {
        val resultMap = commandOptions.dbNames
          .map( dbName => export(createQueryParams(commandOptions), dbName))
          .filter(_.isSuccess)
          .map(_.get)
          .toMap

        if (commandOptions.dbNames.size == resultMap.keySet.size) {
          createSuccessFile(resultMap, commandOptions.runDate)
          CommandProcessor.EXIT_CODE_SUCCESS
        } else {
          val dbNamesMissingResults = commandOptions.dbNames.filterNot(resultMap.keySet).mkString(",")
          logger.error(s"!!!!!! Results missing for some dbNames[$dbNamesMissingResults] !!!!!")
          CommandProcessor.EXIT_CODE_FAILURE
        }
      }
    }

  }

  def createQueryParams(commandOptions: CommandOptions): Map[String, DateTime] = {
    Map("runDate" -> commandOptions.runDate)
  }

  def export(queryParams: Map[String, Any], dbName: String): Try[(String, Long)] = {

    val result = Try(dbName -> getExportSvc(dbName).export(queryParams))
    
    result match {
      case Failure(ex) => logger.error(s"Exception exporting data from database [$dbName]", ex)
      case Success(_) =>
        val outputFileLocation = dbConfigMap(dbName)(DBProps.outputFileLocation).replaceFirst("/$", "")
        outputLocationMap.addBinding(outputFileLocation, dbName)
    }

    result
  }

  protected def getExportSvc(dbName: String): DefaultExportSvc = {
    new DefaultExportSvc(dbName)
  }

  /**
   * This method creates a _SUCCESS file for each unique output file location. If multiple databases
   * have the same output file location, the _SUCCESS file will indicate the success of each of these
   * databases and will have information of each of them.
   * 
   * Ths _SUCCESS file will contain data in the below format.
   * 
   * dbName1=<number of records exported> 
   * dbName2=<number of records exported>
   * 
   * @param resultMap contains dbName -> <number of records exported> mapping
   * @param runDate
   */
  def createSuccessFile(resultMap: Map[String, Long],
                        runDate: DateTime): Unit = {
    
    val runDateStr = DateTimeFormat.forPattern("yyyy-MM-dd").print(runDate)
    
    outputLocationMap.foreach {
      case (outputFileLocation, dbNameSet) => {
        logger.info(s"Writing success file in $outputFileLocation for databases[${dbNameSet.mkString(",")}]")
        
        val localFilePath: String = java.io.File.createTempFile("temp", "_success").getAbsolutePath
        val writer: Writer = getWriter(localFilePath)
        
        try {
          
          dbNameSet.foreach(dbName => {
            val numberOfRecordsWritten = resultMap.getOrElse(dbName, Long.MinValue)
            writer.write(s"$dbName=$numberOfRecordsWritten\n")
          })

        } finally {
          IOUtils.closeQuietly(writer)
        }
        logger.info(s"Completed writing success file $localFilePath")

        scpUtil.scp(sessionInfo, localFilePath, SUCCESS_FILE_NAME, outputFileLocation, runDateStr)

      }
    }
  }

  def getWriter(filePath: String) = {
    new PrintWriter(fsClient.getOutputStream(filePath))
  }

}
