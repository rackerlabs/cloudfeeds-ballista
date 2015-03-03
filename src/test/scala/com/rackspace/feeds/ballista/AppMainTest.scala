package com.rackspace.feeds.ballista

import java.net.URLClassLoader

import org.scalatest.FunSuite

import scala.sys.process._

class AppMainTest extends FunSuite {
  
  test("should return exit code of 1 for invalid arguments") {
    
    val classpath = Thread.currentThread().getContextClassLoader.asInstanceOf[URLClassLoader].getURLs.mkString(":")
    val javaExe = System.getProperty("java.home") + "/bin/java"
    
    val exitCode = s"$javaExe -classpath $classpath com.rackspace.feeds.ballista.AppMain -x".!
    assert(exitCode == 1, "Incorrect exit code for running command with invalid arguments")
  }

  test("should return exit code of 2 for errors during processing") {

    val classpath = Thread.currentThread().getContextClassLoader.asInstanceOf[URLClassLoader].getURLs.mkString(":")
    val javaExe = System.getProperty("java.home") + "/bin/java"

    val exitCode = s"$javaExe -classpath $classpath com.rackspace.feeds.ballista.AppMain".!
    
    //the test conf file is not set up with real server configurations. So running the command would lead to exceptions
    assert(exitCode == 2, "Incorrect exit code for exceptions during processing")
  }
}
