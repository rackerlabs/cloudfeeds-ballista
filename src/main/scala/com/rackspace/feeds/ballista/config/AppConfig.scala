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

  } else {
    logger.info("Loading config from classpath")
    classpathConfig
  }

  object export {

    val datacenter = config.getString("appConfig.datacenter")
    val daysDataAvailable = config.getInt("appConfig.daysDataAvailable")
    val tempOutputDir = config.getString("appConfig.tempOutputDir").replaceFirst("/$", "")
    val maxRowLimit = config.getString("appConfig.maxRowLimit")
    
    object from {
      object dbs {

        val dbsConfigObject = config.getObject("appConfig.export.from.dbs")
        val dbsConfig = config.getConfig("appConfig.export.from.dbs")

        /**
         * Map[dbName, Map[dbProp, propValue]]
         */
        val dbConfigMap = dbsConfigObject.keySet().asScala.map(db => {
          db -> Map(dbName                  -> db,
                    driverClass             -> dbsConfig.getString(s"$db.driverClass"),
                    jdbcUrl                 -> dbsConfig.getString(s"$db.jdbcUrl"),
                    user                    -> dbsConfig.getString(s"$db.user"),
                    password                -> dbsConfig.getString(s"$db.password"),
                    outputFileLocation      -> dbsConfig.getString(s"$db.outputFileLocation").replaceFirst("/$", ""),
                    fileNamePrefix          -> dbsConfig.getString(s"$db.fileNamePrefix"),
                    queryClass              -> dbsConfig.getString(s"$db.queryClass"),
                    isOutputFileDateDriven  -> dbsConfig.getString(s"$db.isOutputFileDateDriven")
          )
        }).toMap
        
      }
    }

    object to {
      object hdfs {

        private val exportToHDFS = config.getConfig("appConfig.export.to.hdfs")

        object scp {
          private val exportToHDFSScp = exportToHDFS.getConfig("scp")

          val user = exportToHDFSScp.getString("user")
          val host = exportToHDFSScp.getString("host")
          val port = exportToHDFSScp.getInt("port")
          val password = exportToHDFSScp.getString("password")
          val privateKeyFilePath = exportToHDFSScp.getString("privateKeyFilePath")
          val privateKeyPassPhrase = exportToHDFSScp.getString("privateKeyPassPhrase")
        }
      }
    }

  }

  object log {
    private val log = config.getConfig("appConfig.log")

    val configFile = log.getString("configFile")
  }
}
