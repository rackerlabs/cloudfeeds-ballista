package com.rackspace.feeds.ballista

import java.io.{ByteArrayOutputStream, FileOutputStream}

import com.rackspace.feeds.ballista.config.CommandOptions
import com.rackspace.feeds.ballista.service.{DefaultExportSvc, FSClient, LocalFSClient}
import com.rackspace.feeds.ballista.util.{SCPSessionInfo, SCPUtil}
import org.joda.time.DateTime
import org.joda.time.format.{DateTimeFormatter, DateTimeFormat}
import org.junit.runner.RunWith
import org.mockito.Matchers._
import org.mockito.{ArgumentCaptor, Mockito}
import org.mockito.Mockito._
import org.scalatest.FunSuite
import org.scalatest.junit.JUnitRunner
import org.scalatest.mock.MockitoSugar

import scala.collection.mutable
import scala.util.Try

class CommandProcessorTestable(afsClient: LocalFSClient,
                                ascpUtil: SCPUtil,
                                testOutputLocationMap: mutable.HashMap[String, mutable.Set[(String, Boolean)]] with mutable.MultiMap[String, (String, Boolean)]) extends CommandProcessor {
  override val scpUtil = ascpUtil
  override val outputLocationMap = testOutputLocationMap
  override val fsClient = afsClient
}

@RunWith(classOf[JUnitRunner])
class CommandProcessorTest extends FunSuite with MockitoSugar {

  test("one _SUCCESS file should be created for each unique output location and isOutputFileDateDriven flag with desired content") {

    val testOutputLocationMap = new mutable.HashMap[String, mutable.Set[(String, Boolean)]] with mutable.MultiMap[String, (String, Boolean)]
    testOutputLocationMap.addBinding("outLoc1", ("dbaas", true))
    testOutputLocationMap.addBinding("outLoc1", ("newrelic", true))
    testOutputLocationMap.addBinding("outLoc2", ("lbaas", true))
    testOutputLocationMap.addBinding("outLoc2", ("prefs", false))

    val afsClient = mock[LocalFSClient]
    val mockSCPUtil: SCPUtil = mock[SCPUtil]
    val commandProcessor = new CommandProcessorTestable(afsClient, mockSCPUtil, testOutputLocationMap)

    val successFileContent = new ByteArrayOutputStream()
    val resultMap = Map("dbaas" -> 5.toLong, "newrelic" -> 4.toLong, "lbaas" -> 8.toLong, "prefs" -> 10.toLong)
    when(afsClient.getOutputStream(anyString())).thenReturn(successFileContent)
    commandProcessor.createSuccessFile(resultMap, DateTime.now())
    
    verify(afsClient, times(3)).getOutputStream(anyString())

    val argumentCapture = ArgumentCaptor.forClass(classOf[String])
    verify(mockSCPUtil, times(3)).scpFile(any[SCPSessionInfo](), anyString(), argumentCapture.capture(), anyString())

    val runDateString = commandProcessor.getRunDateString(DateTime.now())
    assert(argumentCapture.getAllValues.contains(s"outLoc1/$runDateString"), "Success file location for outLoc1(isOutputFileDateDriven=true) not present")
    assert(argumentCapture.getAllValues.contains(s"outLoc2/$runDateString"), "Success file location for outLoc2(isOutputFileDateDriven=true) not present")
    assert(argumentCapture.getAllValues.contains(s"outLoc2/${CommandProcessor.NON_DATA_FILE_PREFIX}$runDateString"), "Success file location for outLoc2(isOutputFileDateDriven=false) not present")
    
    val successFileMap = successFileContent.toString.split("\n").map(x => {
      val keyVal = x.split("=")
      keyVal(0) -> keyVal(1).toLong
    }).toMap

    assert(resultMap.get("dbaas") == successFileMap.get("dbaas"), "number of records exported, do not match with the number written to success file for dbaas database")
    assert(resultMap.get("newrelic") == successFileMap.get("newrelic"), "number of records exported, do not match with the number written to success file for newrelic database")
    assert(resultMap.get("lbaas") == successFileMap.get("lbaas"), "number of records exported, do not match with the number written to success file for lbaas database")
    assert(resultMap.get("prefs") == successFileMap.get("prefs"), "number of records exported, do not match with the number written to success file for prefs database")
  }
  
  test("failure during processing of one database should not halt processing of other databases") {
    
    val testOutputLocationMap = new mutable.HashMap[String, mutable.Set[(String, Boolean)]] with mutable.MultiMap[String, (String, Boolean)]
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

    val testOutputLocationMap = new mutable.HashMap[String, mutable.Set[(String, Boolean)]] with mutable.MultiMap[String, (String, Boolean)]
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
