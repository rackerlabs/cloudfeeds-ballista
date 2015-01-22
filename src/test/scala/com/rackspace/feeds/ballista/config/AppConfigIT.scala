package com.rackspace.feeds.ballista.config

import org.scalatest.FunSuite
import org.slf4j.LoggerFactory


class AppConfigIT extends FunSuite {

  val logger = LoggerFactory.getLogger(getClass)
  
  test ("Verify existence of property") {
    assert(AppConfig.log.configFile.length >= 0, "Property value for appConfig.log.configFile does not exist")
    assert(AppConfig.log.configFile == "logback-test.xml", "Property value for appConfig.log.configFile is not retreived from test conf file")
  }

  test("Should throw RuntimeException because of invalid configuration(duplicate dbName)") {
    
    try {
      AppConfig.validate()
      
      fail() //This will trigger failure
    } catch {
      case e: Exception => logger.info(e.getMessage)
    }
  }
  
}

