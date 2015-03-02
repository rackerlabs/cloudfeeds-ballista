package com.rackspace.feeds.ballista.util

import java.io.{ByteArrayInputStream, StringReader}

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
}
