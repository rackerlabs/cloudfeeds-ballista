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

  def validate() = {
    val dbConfigList = config.getConfigList("appConfig.export.from.dbs").asScala

    val listOfDBNames = dbConfigList.map(_.getString("dbName")).toList
    val duplicateDBNames = listOfDBNames.diff(listOfDBNames.distinct)

    if (duplicateDBNames.size > 0) {
      throw new RuntimeException("Invalid db's configuration. Duplicate db names found: " + duplicateDBNames.mkString(", "))
    }

  }

  object export {
    object from {
      object dbs {

        private val dbConfigList = config.getConfigList("appConfig.export.from.dbs").asScala

        val dbConfigMap = dbConfigList.map(dbConfig =>
          dbConfig.getString("dbName") -> Map(
            dbName -> dbConfig.getString("dbName"),
            driverClass -> dbConfig.getString("driverClass"),
            jdbcUrl -> dbConfig.getString("jdbcUrl"),
            user -> dbConfig.getString("user"),
            password -> dbConfig.getString("password")
          )).toMap

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
