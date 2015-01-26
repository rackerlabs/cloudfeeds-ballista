package com.rackspace.feeds.ballista.service

import com.rackspace.feeds.ballista.config.AppConfig.export.from.dbs.dbConfigMap
import com.rackspace.feeds.ballista.constants.DBProps
import com.rackspace.feeds.ballista.util.DataSourceRepository
import org.joda.time.DateTime
import org.joda.time.format.DateTimeFormat
import org.slf4j.LoggerFactory


object DefaultExportSvc extends ExportSvc {

  val DATE_FORMAT: String = "yyyy-MM-dd"
  val logger = LoggerFactory.getLogger(getClass)
  
  override val dataExport = new PGDataExport
  override val fsClient = new HDFSClient
  
  def export(dbName: String) = {

    val dataSource = DataSourceRepository(dbName)
    val query = dbConfigMap(dbName)(DBProps.copyQuery)
    val outputFilePath = getOutputFilePath(dbName, DateTime.now)
    
    logger.info(s"Exporting data from db:[$dbName] to HDFS file:[$outputFilePath]")
    
    val totalRecords = super.export(dataSource, query, outputFilePath, overwriteFile = true)
    
    logger.info(s"Exported [$totalRecords] records from db:[$dbName] to HDFS file:[$outputFilePath]")
  }

  /**
   * Generates the complete file path along with file name to be written
   * to HDFS. $outputFileLocation/${fileNamePrefix}_$dateTimeStr.txt
   *
   * @param dbName
   * @return
   */
  private def getOutputFilePath(dbName: String, dateTime: DateTime) = {

    val outputFileLocation = dbConfigMap(dbName)(DBProps.outputFileLocation).replaceFirst("/$", "")
    val fileNamePrefix = dbConfigMap(dbName)(DBProps.fileNamePrefix)
    val dateTimeStr = DateTimeFormat.forPattern(DATE_FORMAT).print(dateTime)

    s"$outputFileLocation/${fileNamePrefix}_${dbName}_$dateTimeStr.txt".toLowerCase
  }
  
}
