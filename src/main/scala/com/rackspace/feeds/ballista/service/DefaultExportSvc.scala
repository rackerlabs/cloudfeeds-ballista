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

  val isOutputFileDateDriven = dbConfigMap(dbName)(DBProps.isOutputFileDateDriven).toBoolean
  
  val scpUtil = new SCPUtil
  private val sessionInfo = new SCPSessionInfo(user, password, host, port, privateKeyFilePath, privateKeyPassPhrase)
  
  def export(queryParams: Map[String, Any]): Long = {

    val runDate: DateTime = queryParams("runDate").asInstanceOf[DateTime]
    val query = getQuery(queryParams)
    logger.debug(s"query: $query")

    val tempOutputFilePath = getTempOutputFilePath(runDate, AppConfig.export.tempOutputDir)

    logger.info(s"Exporting data from db:[$dbName] to temporary file:[$tempOutputFilePath]")

    val totalRecords = super.export(dataSource, query, tempOutputFilePath)

    logger.info(s"Exported [$totalRecords] records from db:[$dbName] to temporary file:[$tempOutputFilePath]")

    scpAndDeleteTempFile(tempOutputFilePath, runDate)

    totalRecords
  }

  /**
   * SCP the local file $tempOutputFilePath to <configured remote output file location>/$runDate
   * Once the scp is successful, delete the file
   *  
   * @param tempOutputFilePath
   * @param runDate
   */
  private def scpAndDeleteTempFile(tempOutputFilePath: String, runDate: DateTime) {
    val remoteOutputFileName = getRemoteFileName(runDate)
    val remoteOutputFileLocation = dbConfigMap(dbName)(DBProps.outputFileLocation)
    val subDir = if (isOutputFileDateDriven) DateTimeFormat.forPattern(DATE_FORMAT).print(runDate) else ""

    scpUtil.scp(sessionInfo, tempOutputFilePath, remoteOutputFileName, remoteOutputFileLocation, subDir)

    new File(tempOutputFilePath).delete()
    logger.info(s"Deleted $tempOutputFilePath file")

  }

  def getQuery(queryParams: Map[String, Any]) = {
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
   * Generates the file name specific to the dbName, current date etc
   *
   * @param runDate
   * @param tempDir
   * @return the file name specific to the dbName, current date
   */
  private def getTempOutputFilePath(runDate: DateTime, tempDir: String): String = {

    val tempFileName: String = getRemoteFileName(runDate)

    val tempOutputFilePath = if (tempDir.length > 0)
      s"$tempDir/$tempFileName"
    else
      tempFileName

    //create any intermediate directories in the path
    if (tempOutputFilePath.lastIndexOf("/") > 0)
      new File(tempOutputFilePath.substring(0, tempOutputFilePath.lastIndexOf("/"))).mkdirs()
    
    tempOutputFilePath
    
  }

  private def getRemoteFileName(runDate: DateTime): String = {
    val fileNamePrefix = dbConfigMap(dbName)(DBProps.fileNamePrefix)
    val runDateStr = DateTimeFormat.forPattern(DATE_FORMAT).print(runDate)

    if (isOutputFileDateDriven)
      s"${fileNamePrefix}_${dbName}_$runDateStr.gz"
    else
      s"${fileNamePrefix}_${dbName}.gz"
  }
}
