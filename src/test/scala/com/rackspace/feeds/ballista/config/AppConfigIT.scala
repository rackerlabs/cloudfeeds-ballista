package com.rackspace.feeds.ballista.config

import org.junit.runner.RunWith
import org.scalatest.FunSuite
import org.scalatest.junit.JUnitRunner
import org.slf4j.LoggerFactory

@RunWith(classOf[JUnitRunner])
class AppConfigIT extends FunSuite {

  val logger = LoggerFactory.getLogger(getClass)
  
  test ("Verify existence of property") {
    assert(AppConfig.log.configFile.length >= 0, "Property value for appConfig.log.configFile does not exist")
    assert(AppConfig.log.configFile == "logback.xml", "Property value for appConfig.log.configFile is not retreived from test conf file")
  }

}

