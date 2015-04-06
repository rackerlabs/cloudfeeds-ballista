package com.rackspace.feeds.ballista

import java.io.{PrintWriter, Writer}

import com.rackspace.feeds.ballista.config.AppConfig.export.from.dbs._
import com.rackspace.feeds.ballista.config.AppConfig.export.to.hdfs.scp._
import com.rackspace.feeds.ballista.config.{AppConfig, CommandOptions}
import com.rackspace.feeds.ballista.constants.DBProps
import com.rackspace.feeds.ballista.service.{DryRunProcessor, DefaultExportSvc, LocalFSClient}
import com.rackspace.feeds.ballista.util.{SCPSessionInfo, SCPUtil}
import org.apache.commons.io.IOUtils
import org.apache.commons.lang3.StringUtils
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

        //create remote directories for each unique remote output locations across database names being exported
        val failureCount = commandOptions.dbNames
          .map(dbName => dbConfigMap(dbName)(DBProps.outputFileLocation))
          .toSet
          .map((outputFileLocation: String) => scpEmptyDirectory(commandOptions.runDate, outputFileLocation))
          .count(_.isFailure)

        if (failureCount > 0)
          return getExitCode(failureCount)
        
        if (commandOptions.scponly) {

          val scpFailureCount = commandOptions.dbNames
            .map(dbName => scpAndDeleteFile(dbName, commandOptions.runDate))
            .count(_.isFailure)

          getExitCode(scpFailureCount)

        } else {
          
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

  }

  def scpEmptyDirectory(runDate: DateTime, outputFileLocation: String): Try[Unit] = {

    val remoteDir: String = getRunDateString(runDate)
    val result = Try(scpUtil.scpEmptyDirectory(sessionInfo, outputFileLocation, remoteDir))

    result match {
      case Failure(ex) => logger.error(s"Exception creating empty directory[$remoteDir] at remote output location [$outputFileLocation]", ex)
      case Success(_) => logger.info(s"Successfully created empty directory[$remoteDir] at remote output location [$outputFileLocation]")
    }
    
    result
  }

  def getExitCode(failureCount: Int): Int = {
    if (failureCount == 0) {
      CommandProcessor.EXIT_CODE_SUCCESS
    } else {
      CommandProcessor.EXIT_CODE_FAILURE
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
        val outputFileLocation = dbConfigMap(dbName)(DBProps.outputFileLocation)
        outputLocationMap.addBinding(outputFileLocation, dbName)
    }

    result
  }

  protected def getExportSvc(dbName: String): DefaultExportSvc = {
    new DefaultExportSvc(dbName)
  }
  
  private def scpAndDeleteFile(dbName: String, runDate: DateTime) = {
    val result = Try(getExportSvc(dbName).scpAndDeleteTemporaryFile(runDate))

    result match {
      case Failure(ex) => logger.error(s"Exception scp'ing data of database[$dbName] for rundate[${getRunDateString(runDate)}]", ex)
      case Success(_) => logger.info(s"scp successful for database[$dbName] for rundate[${getRunDateString(runDate)}]")
    }
    
    result
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

        //remote directory is already created during export process
        scpUtil.scpFile(sessionInfo, localFilePath, s"$outputFileLocation/${getRunDateString(runDate)}", SUCCESS_FILE_NAME)

      }
    }
  }

  def getRunDateString(runDate: DateTime): String = {
    DateTimeFormat.forPattern("yyyy-MM-dd").print(runDate)
  }

  def getWriter(filePath: String) = {
    new PrintWriter(fsClient.getOutputStream(filePath))
  }

}
