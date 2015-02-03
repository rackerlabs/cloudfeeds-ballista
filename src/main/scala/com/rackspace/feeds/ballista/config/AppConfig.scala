package com.rackspace.feeds.ballista.config

import com.rackspace.feeds.ballista.constants.DBProps._
import com.typesafe.config.ConfigFactory
import org.slf4j.LoggerFactory

import scala.collection.JavaConverters._

object AppConfig {

  val logger = LoggerFactory.getLogger(getClass)

  //load from classpath
  val classpathConfig = ConfigFactory.load("cloudfeeds-ballista.conf")
  /**
   * If external file is passed, load config from external file, if not load from the
   * file in classpath
   */
  val config = if (Option(System.getProperty("config.file")).nonEmpty) {

    logger.info("Loading config from external path:" + System.getProperty("config.file"))
    ConfigFactory.load()
      .withFallback(classpathConfig)

  } else {
    logger.info("Loading config from classpath")
    classpathConfig
  }

  object export {

    val datacenter = config.getString("appConfig.datacenter")
    val daysDataAvailable = config.getInt("appConfig.daysDataAvailable")

    object from {
      object dbs {

        val dbsConfigObject = config.getObject("appConfig.export.from.dbs")
        val dbsConfig = config.getConfig("appConfig.export.from.dbs")

        /**
         * Map[dbName, Map[dbProp, propValue]]
         */
        val dbConfigMap = dbsConfigObject.keySet().asScala.map(db => {
          db -> Map(dbName              -> db,
                    driverClass         -> dbsConfig.getString(s"$db.driverClass"),
                    jdbcUrl             -> dbsConfig.getString(s"$db.jdbcUrl"),
                    user                -> dbsConfig.getString(s"$db.user"),
                    password            -> dbsConfig.getString(s"$db.password"),
                    outputFileLocation  -> dbsConfig.getString(s"$db.outputFileLocation"),
                    fileNamePrefix      -> dbsConfig.getString(s"$db.fileNamePrefix"),
                    queryClass          -> dbsConfig.getString(s"$db.queryClass")
          )
        }).toMap
        
      }
    }

    object to {
      object hdfs {

        private val exportToHDFS = config.getConfig("appConfig.export.to.hdfs")

        val coreSitePath = exportToHDFS.getString("coreSitePath")
        val hdfsSitePath = exportToHDFS.getString("hdfsSitePath")

      }
    }

  }

  object log {
    private val log = config.getConfig("appConfig.log")

    val configFile = log.getString("configFile")
  }
}
