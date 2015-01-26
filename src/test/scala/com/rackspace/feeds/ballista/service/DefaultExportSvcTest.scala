package com.rackspace.feeds.ballista.service

import org.joda.time.format.DateTimeFormat
import org.scalatest.{FunSuite, PrivateMethodTester}

class DefaultExportSvcTest extends FunSuite with PrivateMethodTester {

  
  test("constructed file path should be in format $outputFileLocation/${fileNamePrefix}_$dateTimeStr.txt") {

    val testDateStr = "2014-01-21"
    val testDate = DateTimeFormat.forPattern(DefaultExportSvc.DATE_FORMAT).parseDateTime(testDateStr);
    
    val getOutputFilePath = PrivateMethod[String]('getOutputFilePath)
    val outputFilePath = DefaultExportSvc invokePrivate getOutputFilePath("newrelic", testDate)
    
    println(outputFilePath)
    assert(outputFilePath == "/etl/entries/ord_newrelic_" + testDateStr + ".txt", "output file path not accurate")
    
  }
  
  
  
}
