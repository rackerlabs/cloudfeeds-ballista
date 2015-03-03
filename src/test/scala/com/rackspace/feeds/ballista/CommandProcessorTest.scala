package com.rackspace.feeds.ballista

import java.io.{ByteArrayOutputStream, FileOutputStream}

import com.rackspace.feeds.ballista.config.CommandOptions
import com.rackspace.feeds.ballista.service.{DefaultExportSvc, FSClient, LocalFSClient}
import com.rackspace.feeds.ballista.util.SCPUtil
import org.joda.time.DateTime
import org.joda.time.format.{DateTimeFormatter, DateTimeFormat}
import org.junit.runner.RunWith
import org.mockito.Matchers._
import org.mockito.Mockito
import org.mockito.Mockito._
import org.scalatest.FunSuite
import org.scalatest.junit.JUnitRunner
import org.scalatest.mock.MockitoSugar

import scala.collection.mutable
import scala.util.Try

class CommandProcessorTestable(afsClient: LocalFSClient,
                                ascpUtil: SCPUtil,
                                testOutputLocationMap: mutable.HashMap[String, mutable.Set[String]] with mutable.MultiMap[String, String]) extends CommandProcessor {
  override val scpUtil = ascpUtil
  override val outputLocationMap = testOutputLocationMap
  override val fsClient = afsClient
}

@RunWith(classOf[JUnitRunner])
class CommandProcessorTest extends FunSuite with MockitoSugar {

  test("one _SUCCESS file should be created for each unique output location with desired content") {

    val testOutputLocationMap = new mutable.HashMap[String, mutable.Set[String]] with mutable.MultiMap[String, String]
    testOutputLocationMap.addBinding("outLoc1", "prefs")
    testOutputLocationMap.addBinding("outLoc1", "newrelic")
    testOutputLocationMap.addBinding("outLoc2", "lbaas")

    val afsClient = mock[LocalFSClient]
    val commandProcessor = new CommandProcessorTestable(afsClient, mock[SCPUtil], testOutputLocationMap)

    val successFileContent = new ByteArrayOutputStream()
    val resultMap = Map("prefs" -> 5.toLong, "newrelic" -> 4.toLong, "lbaas" -> 8.toLong)
    when(afsClient.getOutputStream(anyString())).thenReturn(successFileContent)
    commandProcessor.createSuccessFile(resultMap, DateTime.now())
    
    verify(afsClient, times(2)).getOutputStream(anyString())

    val successFileMap = successFileContent.toString.split("\n").map(x => {
      val keyVal = x.split("=")
      keyVal(0) -> keyVal(1).toLong
    }).toMap

    assert(resultMap.get("prefs") == successFileMap.get("prefs"), "number of records exported, do not match with the number written to success file for prefs database")
    assert(resultMap.get("newrelic") == successFileMap.get("newrelic"), "number of records exported, do not match with the number written to success file for newrelic database")
    assert(resultMap.get("lbaas") == successFileMap.get("lbaas"), "number of records exported, do not match with the number written to success file for lbaas database")
  }
  
  test("failure during processing of one database should not halt processing of other databases") {
    
    val testOutputLocationMap = new mutable.HashMap[String, mutable.Set[String]] with mutable.MultiMap[String, String]
    val afsClient = mock[LocalFSClient]
    val commandProcessor = spy(new CommandProcessorTestable(afsClient, mock[SCPUtil], testOutputLocationMap))

    val commandOptions: CommandOptions = CommandOptions(DateTime.now, Set("prefs", "newrelic", "lbaas"))

    val mockExportSvc = mock[DefaultExportSvc]
    val queryParams: Map[String, Any] = Map("runDate" -> "2015-01-01")
    doReturn(queryParams).when(commandProcessor).createQueryParams(commandOptions)

    doReturn(Try("prefs" -> 5)).when(commandProcessor).export(queryParams, "prefs")
    doReturn(Try(throw new RuntimeException("I feel like it"))).when(commandProcessor).export(queryParams, "newrelic")
    doReturn(Try("lbaas" -> 8)).when(commandProcessor).export(queryParams, "lbaas")

    val returnCode = commandProcessor.doProcess(commandOptions)

    assert(returnCode != 0, "Failure is not getting propagated with return code")
    verify(commandProcessor, never()).createSuccessFile(any(), any())
  }

  test("Should return zero as return code when no failures happen during processing") {

    val testOutputLocationMap = new mutable.HashMap[String, mutable.Set[String]] with mutable.MultiMap[String, String]
    val afsClient = mock[LocalFSClient]
    val commandProcessor = spy(new CommandProcessorTestable(afsClient, mock[SCPUtil], testOutputLocationMap))

    val commandOptions: CommandOptions = CommandOptions(DateTime.now, Set("prefs", "newrelic", "lbaas"))

    val mockExportSvc = mock[DefaultExportSvc]
    val queryParams: Map[String, Any] = Map("runDate" -> "2015-01-01")
    doReturn(queryParams).when(commandProcessor).createQueryParams(commandOptions)

    doReturn(Try("prefs" -> 5)).when(commandProcessor).export(queryParams, "prefs")
    doReturn(Try("newrelic" -> 4)).when(commandProcessor).export(queryParams, "newrelic")
    doReturn(Try("lbaas" -> 8)).when(commandProcessor).export(queryParams, "lbaas")

    val returnCode = commandProcessor.doProcess(commandOptions)

    assert(returnCode == 0, "Should return a success return code")
    verify(commandProcessor).createSuccessFile(any(), any())
  }
}
