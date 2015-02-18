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
    val mockFSClient = mock[GZFSClient]
  
    val defaultExportSvc = new DefaultExportSvc("newrelic") {
      override val dataExport = mockDataExport
      override val fsClient = mockFSClient
      override lazy val dataSource = mock[DataSource]
    }

    when(mockFSClient.getOutputStream(anyString())).thenReturn(mock[OutputStream])
    
    defaultExportSvc.export(Map.empty)

    verify(mockDataExport).export(any[DataSource], anyString(), any[OutputStream])
  }
  
  test("verify temp output file path based on $remoteFilePath and $tempDir locations") {
    val defaultExportSvc = new DefaultExportSvc("newrelic")
    
    val getTempOutputFilePath = PrivateMethod[String]('getTempOutputFilePath)

    val tempOutputFilePath = defaultExportSvc invokePrivate getTempOutputFilePath("/etl/entries/ord_newrelic1_2015-02-13.gz", "/tmp")
    assert(tempOutputFilePath == "/tmp/ord_newrelic1_2015-02-13.gz", "Temp output file path incorrect")

    val tempOutputFilePath1 = defaultExportSvc invokePrivate getTempOutputFilePath("/etl/entries/ord_newrelic1_2015-02-13.gz", "")
    assert(tempOutputFilePath1 == "ord_newrelic1_2015-02-13.gz", "Temp output file path incorrect")

  }
}
