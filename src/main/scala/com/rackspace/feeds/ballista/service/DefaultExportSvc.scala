package com.rackspace.feeds.ballista.service

import com.rackspace.feeds.ballista.config.AppConfig
import com.rackspace.feeds.ballista.config.AppConfig.export.from.dbs.dbConfigMap
import com.rackspace.feeds.ballista.constants.DBProps
import com.rackspace.feeds.ballista.queries.DBQuery
import com.rackspace.feeds.ballista.util.DataSourceRepository
import org.joda.time.DateTime
import org.joda.time.format.DateTimeFormat
import org.slf4j.LoggerFactory
import scala.reflect.runtime.universe._

class DefaultExportSvc(dbName: String) extends ExportSvc {

  val DATE_FORMAT: String = "yyyy-MM-dd"
  val logger = LoggerFactory.getLogger(getClass)
  
  override val dataExport = new PGDataExport
  override val fsClient = new HDFSClient
  lazy val dataSource = DataSourceRepository.getDataSource(dbName)
  
  def export(queryParams: Map[String, Any], overwriteFile: Boolean): Long = {

    val outputFilePath = getOutputFilePath(queryParams("runDate").asInstanceOf[DateTime])
    val query = getQuery(queryParams)
    logger.debug(s"query: $query")
    
    logger.info(s"Exporting data from db:[$dbName] to HDFS file:[$outputFilePath]")
    
    val totalRecords = super.export(dataSource, query, outputFilePath, overwriteFile)
    
    logger.info(s"Exported [$totalRecords] records from db:[$dbName] to HDFS file:[$outputFilePath]")
    
    totalRecords
  }

  
  private def getQuery(queryParams: Map[String, Any]) = {
    val dbQuery = getInstance(dbConfigMap(dbName)(DBProps.queryClass))
    val datacenter = AppConfig.export.datacenter
  
    dbQuery.fetch(queryParams("runDate").asInstanceOf[DateTime], datacenter, dataSource)
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
  private def getOutputFilePath(dateTime: DateTime) = {

    val outputFileLocation = dbConfigMap(dbName)(DBProps.outputFileLocation).replaceFirst("/$", "")
    val fileNamePrefix = dbConfigMap(dbName)(DBProps.fileNamePrefix)
    val dateTimeStr = DateTimeFormat.forPattern(DATE_FORMAT).print(dateTime)

    s"$outputFileLocation/$dateTimeStr/${fileNamePrefix}_${dbName}_$dateTimeStr.txt".toLowerCase
  }
  
}
