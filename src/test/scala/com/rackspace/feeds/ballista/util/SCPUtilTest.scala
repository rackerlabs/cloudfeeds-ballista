package com.rackspace.feeds.ballista.util

import java.io.{ByteArrayInputStream, StringReader}
import java.nio.file.{Paths, Files}

import com.rackspace.feeds.ballista.config.AppConfig.export.to.hdfs.scp._
import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner
import org.scalatest.{FunSuite, PrivateMethodTester}


@RunWith(classOf[JUnitRunner])
class SCPUtilTest  extends FunSuite with PrivateMethodTester {
  
  test("should throw exception for an error message from scp") {
    val errorMessageStream = new ByteArrayInputStream("\u0002Test scp error message'\n".toCharArray.map(_.toByte))
    val scp:SCPUtil = new SCPUtil

    try {
      val isValidTransfer = PrivateMethod[Boolean]('isValidTransfer)
      val isValidTransferResult = scp invokePrivate isValidTransfer(errorMessageStream)

      fail("isValidTransfer did not throw exception")

    } catch {
      case e: RuntimeException => println(e)
    }
  }

  test ("scp'ing file") {
    
    try {

      SCPServer.configureAndStartServer()
      
      val sessionInfo = new SCPSessionInfo(user, password, host, port, privateKeyFilePath, privateKeyPassPhrase)
      val util: SCPUtil = new SCPUtil()

      val localFilePath: String = java.io.File.createTempFile("temp", ".gz").getAbsolutePath

      val remoteDir: String = "test-remote-dir"
      val remoteFileLocation: String = "/tmp"
      val remoteFileName: String = "test-db-data.gz"
      
      util.scpEmptyDirectory(sessionInfo, remoteFileLocation, remoteDir)
      util.scpFile(sessionInfo, localFilePath, s"$remoteFileLocation/$remoteDir", remoteFileName)

      val remoteFilePath: String = s"$remoteFileLocation/$remoteDir/$remoteFileName"
      assert(Files.exists(Paths.get(remoteFilePath)), s"Remote file[$remoteFilePath] after scp is not available.")
    
    } finally {
      SCPServer.stopServer()
    }
  }
}
