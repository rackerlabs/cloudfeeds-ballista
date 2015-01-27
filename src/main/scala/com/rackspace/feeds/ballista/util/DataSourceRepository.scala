package com.rackspace.feeds.ballista.util

import javax.sql.DataSource
import com.rackspace.feeds.ballista.config.AppConfig
import com.rackspace.feeds.ballista.constants.DBProps._
import com.rackspace.feeds.ballista.constants.DBProps.propertyName
import org.apache.commons.dbcp2.BasicDataSource
import org.slf4j.LoggerFactory

class DataSourceRepository private() {

  val logger = LoggerFactory.getLogger(getClass)
  
  private def getDataSource(dbProps: Map[propertyName, String]) : DataSource = {
  
    logger.debug("Creating datasource for " + dbProps(dbName))
    
    val prefsDBdataSource: BasicDataSource = new BasicDataSource

    prefsDBdataSource.setDriverClassName(dbProps(driverClass))
    prefsDBdataSource.setUrl(dbProps(jdbcUrl))
    prefsDBdataSource.setUsername(dbProps(user))
    prefsDBdataSource.setPassword(dbProps(password))
    
    prefsDBdataSource.setAccessToUnderlyingConnectionAllowed(true)

    prefsDBdataSource.asInstanceOf[DataSource]
  }
  
}

object DataSourceRepository {
  
  val logger = LoggerFactory.getLogger(getClass)
  
  //generate all data sources
  private val dataSourceMap: Map[String, DataSource] = {
    AppConfig.export.from.dbs.dbConfigMap map {
      case(key, value) => key -> new DataSourceRepository().getDataSource(value)
    }
  }
 
  def getDataSource(dbName: String) = {
    dataSourceMap(dbName)
  }

}
