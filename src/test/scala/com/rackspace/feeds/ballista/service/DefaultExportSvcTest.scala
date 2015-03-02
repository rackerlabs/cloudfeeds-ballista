package com.rackspace.feeds.ballista.service

import java.io.OutputStream
import javax.sql.DataSource

import com.rackspace.feeds.ballista.util.SCPUtil
import org.joda.time.DateTime
import org.joda.time.format.DateTimeFormat
import org.junit.runner.RunWith
import org.mockito.Matchers._
import org.mockito.Mockito._
import org.scalatest.junit.JUnitRunner
import org.scalatest.mock.MockitoSugar
import org.scalatest.{FunSuite, PrivateMethodTester}

@RunWith(classOf[JUnitRunner])
class DefaultExportSvcTest extends FunSuite with PrivateMethodTester with MockitoSugar {

  
  test("remote output file name should be ${fileNamePrefix}_${dbName}_$runDateStr.gz when isOutputFileDateDriven is set to true in conf file") {

    val defaultExportSvc = new DefaultExportSvc("newrelic")
    val runDate: DateTime = DateTime.now.minusDays(1).withTimeAtStartOfDay()
    val runDateStr = DateTimeFormat.forPattern("yyyy-MM-dd").print(runDate)

    val getRemoteFileName = PrivateMethod[String]('getRemoteFileName)
    val remoteFileName = defaultExportSvc invokePrivate getRemoteFileName(runDate)
    
    assert(remoteFileName == "ord_newrelic_" + runDateStr + ".gz", "output file path not accurate")
    
  }

  test("remote output file name should be ${fileNamePrefix}_${dbName}_$runDateStr.gz when isOutputFileDateDriven is set to false in conf file") {

    val defaultExportSvc = new DefaultExportSvc("prefs")
    val getRemoteFileName = PrivateMethod[String]('getRemoteFileName)
    val remoteFileName = defaultExportSvc invokePrivate getRemoteFileName(DateTime.now.minusDays(1).withTimeAtStartOfDay())

    assert(remoteFileName == "ord_prefs.gz", "output file path not accurate")

  }
  
  test("verify export method of DataExport is being called") {
    val mockDataExport = mock[PGDataExport]
    val mockFSClient = mock[GZFSClient]

    val queryParams: Map[String, DateTime] = Map("runDate" -> DateTime.now())

    val defaultExportSvc = new DefaultExportSvc("newrelic") {
      override val dataExport = mockDataExport
      override val fsClient = mockFSClient
      override val scpUtil = mock[SCPUtil]
      override lazy val dataSource = mock[DataSource]
      override def getQuery(queryParams: Map[String, Any]) = "select * from entries"
    }
    
    when(mockFSClient.getOutputStream(anyString())).thenReturn(mock[OutputStream])

    defaultExportSvc.export(queryParams)

    verify(mockDataExport).export(any[DataSource], anyString(), any[OutputStream])
  }
  
  test("verify temp output file path based on $remoteFilePath and $tempDir locations") {
    val defaultExportSvc = new DefaultExportSvc("newrelic")
    
    val getTempOutputFilePath = PrivateMethod[String]('getTempOutputFilePath)

    val runDate: DateTime = DateTime.now.minusDays(1).withTimeAtStartOfDay()
    val runDateStr = DateTimeFormat.forPattern("yyyy-MM-dd").print(runDate)
    
    val tempOutputFilePath = defaultExportSvc invokePrivate getTempOutputFilePath(runDate, "/tmp")
    assert(tempOutputFilePath == "/tmp/ord_newrelic_" + runDateStr + ".gz", "Temp output file path incorrect")

    val tempOutputFilePath1 = defaultExportSvc invokePrivate getTempOutputFilePath(runDate, "")
    assert(tempOutputFilePath1 == "ord_newrelic_" + runDateStr + ".gz", "Temp output file path incorrect")

  }
}
