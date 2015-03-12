package com.rackspace.feeds.ballista.service

import java.sql.Connection

import com.jcraft.jsch.Session
import com.rackspace.feeds.ballista.CommandProcessor
import com.rackspace.feeds.ballista.config.AppConfig
import com.rackspace.feeds.ballista.config.AppConfig.export.to.hdfs.scp._
import com.rackspace.feeds.ballista.util.{DataSourceRepository, DataSourceUtil, SCPSessionInfo}
import org.apache.commons.dbutils.DbUtils
import org.slf4j.LoggerFactory

import scala.util.{Failure, Try}

/**
 * 
 * This class processes the dryrun command option. Verifies connections to 
 * all the databases and the scp server.
 *  
 */
class DryRunProcessor {

  val logger = LoggerFactory.getLogger(getClass)

  /**
   * Verifies connections to all databases configured and the connection to the 
   * scp server 
   * 
   * @return 0 or 2 indicating success or failure
   */
  def dryrun(): Int = {

    val dbErrorConfigMap = getDBErrorConfig

    val scpVerifyResult: Try[Unit] = verifySCPInfo()
    
    if (dbErrorConfigMap.size > 0 || scpVerifyResult.isFailure) {
      logger.error("!!!!!!  Dryrun failed     !!!!!!!!!")
      CommandProcessor.EXIT_CODE_FAILURE
    } else {
      logger.info("!!!!!!  Dryrun successful !!!!!!!!!")
      CommandProcessor.EXIT_CODE_SUCCESS
    }

  }

  def getDBErrorConfig: Map[String, Try[Unit]] = {
    val dbErrorConfigMap = AppConfig.export.from.dbs.dbConfigMap.keySet
      .map(dbName => verifyDBConfig(dbName))
      .filter(_._2.isFailure)
      .toMap

    dbErrorConfigMap.foreach {
      case (dbName, Failure(ex)) => logger.error(s"Db[$dbName] connection not successful. Failed with [${ex.getMessage}]")
      case _ =>
    }

    dbErrorConfigMap
  }

  private def verifyDBConfig(dbName: String): (String, Try[Unit]) = {

    dbName -> Try ({
      
      var connection: Connection = null
      try {

        val dataSource = DataSourceRepository.getDataSource(dbName)
        connection = DataSourceUtil.getConnection(dataSource)
        connection.prepareStatement("SELECT 1").executeQuery()
        
        logger.info(s"Db[$dbName] connection successful")
        
      } finally {
        DbUtils.closeQuietly(connection)
      }
      
    })
  }
  
  
  def verifySCPInfo(): Try[Unit] = {

    Try ({
      
      try {
        
        val sessionInfo = new SCPSessionInfo(user, password, host, port, privateKeyFilePath, privateKeyPassPhrase)
        val session: Session = sessionInfo.createSession()
        session.connect()
      } catch {
        
        case ex: Exception => {
          logger.error(s"Connection to host[$host] with user[$user] over port[$port] not successful")
          throw ex
        }
      }
    })
  }
  
}
