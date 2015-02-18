package com.rackspace.feeds.ballista.service

import java.io.File

import com.rackspace.feeds.ballista.config.AppConfig
import com.rackspace.feeds.ballista.config.AppConfig.export.from.dbs.dbConfigMap
import com.rackspace.feeds.ballista.config.AppConfig.export.to.hdfs.scp._
import com.rackspace.feeds.ballista.constants.DBProps
import com.rackspace.feeds.ballista.queries.DBQuery
import com.rackspace.feeds.ballista.util.{SCPUtil, SCPSessionInfo, DataSourceRepository}
import org.joda.time.DateTime
import org.joda.time.format.DateTimeFormat
import org.slf4j.LoggerFactory
import scala.reflect.runtime.universe._

class DefaultExportSvc(dbName: String) extends ExportSvc {

  val DATE_FORMAT: String = "yyyy-MM-dd"
  val logger = LoggerFactory.getLogger(getClass)
  
  override val dataExport = new PGDataExport
  override val fsClient = new GZFSClient
  lazy val dataSource = DataSourceRepository.getDataSource(dbName)

  val scpUtil = new SCPUtil
  private val sessionInfo = new SCPSessionInfo(user, password, host, port, privateKeyFilePath, privateKeyPassPhrase)
  
  def export(queryParams: Map[String, Any]): Long = {

    val runDate: DateTime = queryParams("runDate").asInstanceOf[DateTime]
    val remoteOutputFilePath = getRemoteOutputFilePath(runDate)
    val query = getQuery(queryParams)
    logger.debug(s"query: $query")

    val tempOutputFilePath = getTempOutputFilePath(remoteOutputFilePath, AppConfig.export.tempOutputDir)
    
    logger.info(s"Exporting data from db:[$dbName] to temporary file:[$tempOutputFilePath]")
    
    val totalRecords = super.export(dataSource, query, tempOutputFilePath)
    
    logger.info(s"Exported [$totalRecords] records from db:[$dbName] to temporary file:[$tempOutputFilePath]")

    
    scpFile(tempOutputFilePath, remoteOutputFilePath, DateTimeFormat.forPattern(DATE_FORMAT).print(runDate))
    
    totalRecords
  }

  def scpFile(tempOutputFilePath: String, remoteOutputFilePath: String, subDir: String) {
    val remoteOutputFileLocation = remoteOutputFilePath.substring(0, remoteOutputFilePath.lastIndexOf("/"))
    val remoteOutputFileName = remoteOutputFilePath.substring(remoteOutputFilePath.lastIndexOf("/") + 1)
    
    logger.info(s"SCP start: local file [$tempOutputFilePath] to remote file [$remoteOutputFileLocation/$subDir]")
    scpUtil.scp(sessionInfo, tempOutputFilePath, remoteOutputFileName, remoteOutputFileLocation, subDir)
    logger.info(s"SCP completed successfully: [$tempOutputFilePath] to remote file [$remoteOutputFileLocation/$subDir]")
  }

  private def getQuery(queryParams: Map[String, Any]) = {
    val dbQuery = getInstance(dbConfigMap(dbName)(DBProps.queryClass))
    val datacenter = AppConfig.export.datacenter
  
    dbQuery.fetch(queryParams("runDate").asInstanceOf[DateTime], datacenter, dataSource, AppConfig.export.maxRowLimit)
  }

  /**
   * This method reflectively creates instance of the class name configured in 
   * queryClass parameter
   * * 
   * @param clsName
   * @return instance of DBQuery
   */
  private def getInstance(clsName: String): DBQuery = {
    val mirror = runtimeMirror(getClass.getClassLoader)
    val cls = mirror.classSymbol(Class.forName(clsName))

    val classMirror = mirror.reflectClass(cls)
    val constructor = cls.typeSignature.member(nme.CONSTRUCTOR).asMethod

    
    classMirror.reflectConstructor(constructor)().asInstanceOf[DBQuery]
  }
  
  /**
   * Generates the complete file path along with file name to be written
   * to HDFS. $outputFileLocation/${fileNamePrefix}_$dateTimeStr.txt
   *
   * @return
   */
  private def getRemoteOutputFilePath(dateTime: DateTime) = {

    val outputFileLocation = dbConfigMap(dbName)(DBProps.outputFileLocation).replaceFirst("/$", "")
    val fileNamePrefix = dbConfigMap(dbName)(DBProps.fileNamePrefix)
    val dateTimeStr = DateTimeFormat.forPattern(DATE_FORMAT).print(dateTime)

    s"$outputFileLocation/${fileNamePrefix}_${dbName}_$dateTimeStr.gz".toLowerCase
  }

  /**
   * Fetches the file name from the given $remoteOutputFilePath and returns a file path
   * for the temporary gzip file to be created in in $tempDir location
   *
   * @param remoteOutputFilePath
   * @param tempDir
   * @return
   */
  private def getTempOutputFilePath(remoteOutputFilePath: String, tempDir: String): String = {
    
    //Get the file name and change the file extension to .gz
    val tempFileName = remoteOutputFilePath.substring(remoteOutputFilePath.lastIndexOf("/") + 1)

    val tempOutputFilePath = if (tempDir.length > 0)
      s"$tempDir/$tempFileName"
    else
      tempFileName

    //create any intermediate directories in the path
    if (tempOutputFilePath.lastIndexOf("/") > 0)
      new File(tempOutputFilePath.substring(0, tempOutputFilePath.lastIndexOf("/"))).mkdirs()
    
    tempOutputFilePath
  }
}
