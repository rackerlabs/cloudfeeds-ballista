package com.rackspace.feeds.ballista.util

import java.sql.Connection
import javax.sql.DataSource

import org.postgresql.PGConnection
import org.slf4j.LoggerFactory

object DataSourceUtil {

  val logger = LoggerFactory.getLogger(getClass)
  
  def getConnection(dataSource: DataSource): Connection = {
    dataSource.getConnection
  }

  /**
   * Get underlying PG connection from DBCP
   * *
   * @param dataSource
   * @return pgConnection
   */
  def getPGConnection(dataSource: DataSource): PGConnection = {

    getConnection(dataSource).isWrapperFor(classOf[PGConnection]) match {
      case true =>  dataSource.getConnection.unwrap(classOf[PGConnection])
      case _=> throw new IllegalArgumentException("Underlying connection provided by dataSource is not PGConnection")
    }
  }
  
}
