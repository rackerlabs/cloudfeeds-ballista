package com.rackspace.feeds.ballista

import com.rackspace.feeds.ballista.config.CommandOptions
import com.rackspace.feeds.ballista.service.DryRunProcessor
import com.rackspace.feeds.ballista.util.SCPServer
import org.joda.time.DateTime
import org.junit.runner.RunWith
import org.scalatest.FunSuite
import org.scalatest.junit.JUnitRunner


@RunWith(classOf[JUnitRunner])
class DryRunProcessorTest extends FunSuite {

  
  test("verify that the invalid db configs gets returned") {

    assert(new DryRunProcessor().getDBErrorConfig.size == 3, "Wrong count of invalid db configs")
  }

  test("verify that SCP server connection is not successful") {

    assert(new DryRunProcessor().verifySCPInfo().isFailure, "Connection to scp server should not be successful")
  }

  test("verify that SCP server connection is successful") {
    try {

      SCPServer.configureAndStartServer()
      
      val scpVerifyResult = new DryRunProcessor().verifySCPInfo()
      assert(scpVerifyResult.isSuccess, "Connection to scp server should be successful")

    } finally {
      SCPServer.stopServer()
    }
  }
  
  test("verify dryrun command returns failure exit code") {
    val commandOptions: CommandOptions = CommandOptions(DateTime.now, Set("prefs", "newrelic", "lbaas", "testdb"), dryrun = true)    

    assert(new DryRunProcessor().dryrun() != 0, "dyrun should return non-zero exit code with invalid test config")
  }
}
