package com.rackspace.feeds.ballista.service

import com.rackspace.feeds.ballista.config.AppConfig.export.from.dbs.dbConfigMap
import com.rackspace.feeds.ballista.constants.DBProps
import com.rackspace.feeds.ballista.util.DataSourceRepository
import org.joda.time.DateTime
import org.joda.time.format.DateTimeFormat
import org.slf4j.LoggerFactory


class DefaultExportSvc(dbName: String) extends ExportSvc {

  val DATE_FORMAT: String = "yyyy-MM-dd"
  val logger = LoggerFactory.getLogger(getClass)
  
  override val dataExport = new PGDataExport
  override val fsClient = new HDFSClient
  lazy val dataSource = DataSourceRepository.getDataSource(dbName)
  
  def export() = {

    val query = dbConfigMap(dbName)(DBProps.query)
    val outputFilePath = getOutputFilePath(DateTime.now)
    
    logger.info(s"Exporting data from db:[$dbName] to HDFS file:[$outputFilePath]")
    
    val totalRecords = super.export(dataSource, query, outputFilePath, overwriteFile = true)
    
    logger.info(s"Exported [$totalRecords] records from db:[$dbName] to HDFS file:[$outputFilePath]")
  }

  /**
   * Generates the complete file path along with file name to be written
   * to HDFS. $outputFileLocation/${fileNamePrefix}_$dateTimeStr.txt
   *
   * @return
   */
  private def getOutputFilePath(dateTime: DateTime) = {

    val outputFileLocation = dbConfigMap(dbName)(DBProps.outputFileLocation).replaceFirst("/$", "")
    val fileNamePrefix = dbConfigMap(dbName)(DBProps.fileNamePrefix)
    val dateTimeStr = DateTimeFormat.forPattern(DATE_FORMAT).print(dateTime)

    s"$outputFileLocation/${fileNamePrefix}_${dbName}_$dateTimeStr.txt".toLowerCase
  }
  
}
