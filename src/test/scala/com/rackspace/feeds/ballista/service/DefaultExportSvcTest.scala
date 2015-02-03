package com.rackspace.feeds.ballista.service

import java.io.OutputStream
import javax.sql.DataSource

import org.joda.time.format.DateTimeFormat
import org.mockito.Matchers._
import org.mockito.Mockito._
import org.scalatest.mock.MockitoSugar
import org.scalatest.{FunSuite, PrivateMethodTester}

class DefaultExportSvcTest extends FunSuite with PrivateMethodTester with MockitoSugar {

  
  test("constructed file path should be in format $outputFileLocation/${fileNamePrefix}_$dateTimeStr.txt") {

    val defaultExportSvc = new DefaultExportSvc("newrelic")
    val testDateStr = "2014-01-21"
    val testDate = DateTimeFormat.forPattern(defaultExportSvc.DATE_FORMAT).parseDateTime(testDateStr);
    
    val getOutputFilePath = PrivateMethod[String]('getOutputFilePath)
    val outputFilePath = defaultExportSvc invokePrivate getOutputFilePath(testDate)
    
    println(outputFilePath)
    assert(outputFilePath == "/etl/entries/ord_newrelic_" + testDateStr + ".txt", "output file path not accurate")
    
  }
  
  test("verify export method of DataExport is being called") {
    val mockDataExport = mock[PGDataExport]
    val mockFSClient = mock[HDFSClient]
  
    val defaultExportSvc = new DefaultExportSvc("newrelic") {
      override val dataExport = mockDataExport
      override val fsClient = mockFSClient
      override lazy val dataSource = mock[DataSource]
    }

    when(mockFSClient.getOutputStream(anyString(), anyBoolean())).thenReturn(mock[OutputStream])
    
    defaultExportSvc.export(Map.empty, false)

    verify(mockDataExport).export(any[DataSource], anyString(), any[OutputStream])
  }
  
}
